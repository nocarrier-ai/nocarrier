package doctrine

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

const (
	HookAttacked        = "attacked"
	HookMoved           = "moved"
	DoctrineUpdatedType = "DoctrineUpdated"
	doctrineWordLimit   = 400
)

var (
	AllHooks = []string{HookAttacked, HookMoved}
)

var (
	ErrInvalid = errors.New("invalid doctrine")
)

var validAvatarID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

type DoctrineData struct {
	Orders []string          `json:"orders"`
	Hooks  map[string]string `json:"hooks"`
}

type UpdateDoctrine struct {
	AvatarID string `json:"avatar_id"`
	DoctrineData
}

type DoctrineUpdated struct {
	DoctrineData

	AvatarID string `json:"avatar_id"`
	Revision int64  `json:"revision"`
	Tick     int64  `json:"tick"`
	Hash     string `json:"hash"`
	Type     string `json:"type"`
}

type Handler struct {
	js jetstream.JetStream
}

func NewHandler(js jetstream.JetStream) *Handler {
	return &Handler{
		js: js,
	}
}

func (d DoctrineData) hash() string {
	h := sha256.New()
	writeLen := func(n int) {
		h.Write(binary.BigEndian.AppendUint64(nil, uint64(n)))
	}
	writeString := func(s string) {
		s = strings.TrimSpace(s)
		writeLen(len(s))
		h.Write([]byte(s))
	}

	writeLen(len(d.Orders))
	for _, order := range d.Orders {
		writeString(order)
	}
	writeLen(len(d.Hooks))

	for _, hook := range slices.Sorted(maps.Keys(d.Hooks)) {
		writeString(hook)
		writeString(d.Hooks[hook])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (h *Handler) Update(ctx context.Context, cmd UpdateDoctrine) (DoctrineUpdated, error) {
	if err := cmd.validate(); err != nil {
		return DoctrineUpdated{}, err
	}
	subject := streams.DoctrineSubject(cmd.AvatarID)

	var last DoctrineUpdated
	seq, err := streams.Last(ctx, h.js, streams.StreamEvents, subject, &last)
	if err != nil {
		return DoctrineUpdated{}, fmt.Errorf("read doctrine: %w", err)
	}
	hash := cmd.hash()
	if seq != 0 && last.Hash == hash {
		return last, nil
	}

	tick, err := clock.CurrentTick(ctx, h.js)
	if err != nil {
		return DoctrineUpdated{}, fmt.Errorf("read tick: %w", err)
	}

	ev := DoctrineUpdated{
		Type:         DoctrineUpdatedType,
		AvatarID:     cmd.AvatarID,
		Revision:     last.Revision + 1,
		Tick:         tick,
		Hash:         hash,
		DoctrineData: cmd.DoctrineData,
	}
	if _, err := streams.Append(ctx, h.js, subject, ev, seq); err != nil {
		return DoctrineUpdated{}, fmt.Errorf("append doctrine: %w", err)
	}
	return ev, nil
}

func (c UpdateDoctrine) validate() error {
	if c.AvatarID == "" {
		return fmt.Errorf("%w: missing avatar ID", ErrInvalid)
	}
	if !validAvatarID.MatchString(c.AvatarID) {
		return fmt.Errorf("%w: avatar ID %q may only contain letters, digits, '-' and '_'", ErrInvalid, c.AvatarID)
	}
	if len(c.Orders) == 0 {
		return fmt.Errorf("%w: no standing orders in doctrine", ErrInvalid)
	}

	words := 0
	for i, order := range c.Orders {
		n := len(strings.Fields(order))
		if n == 0 {
			return fmt.Errorf("%w: standing order %d is blank", ErrInvalid, i+1)
		}
		words += n
	}
	for _, hook := range slices.Sorted(maps.Keys(c.Hooks)) {
		if !slices.Contains(AllHooks, hook) {
			return fmt.Errorf("%w: unknown hook %q", ErrInvalid, hook)
		}
		n := len(strings.Fields(c.Hooks[hook]))
		if n == 0 {
			return fmt.Errorf("%w: hook %q is blank", ErrInvalid, hook)
		}
		words += n
	}
	if words > doctrineWordLimit {
		return fmt.Errorf("%w: %d words, limit is %d", ErrInvalid, words, doctrineWordLimit)
	}
	return nil
}
