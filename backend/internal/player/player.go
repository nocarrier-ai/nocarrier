package player

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

const (
	PlayerCreatedType = "PlayerCreated"

	displayNameLimit = 24
)

var (
	ErrInvalid           = errors.New("invalid player creation")
	ErrAlreadyExists     = errors.New("player already exists")
	ErrAlreadyRegistered = errors.New("oauth id already registered")

	validDialInCode = regexp.MustCompile(`^\d{4}-\d{4}-\d{4}$`)
)

type OAuthID struct {
	Provider string `json:"provider"`
	UID      string `json:"uid"`
}

func (id OAuthID) Key() string { return "oauth." + id.Provider + "." + id.UID }

type CreatePlayer struct {
	PlayerID    string  `json:"player_id"`
	OAuthID     OAuthID `json:"oauth_id"`
	DisplayName string  `json:"display_name"`
	DialInCode  string  `json:"dial_in_code"`
}

type PlayerCreated struct {
	PlayerID    string  `json:"player_id"`
	OAuthID     OAuthID `json:"oauth_id"`
	DisplayName string  `json:"display_name"`
	DialInCode  string  `json:"dial_in_code"`
	Tick        int64   `json:"tick"`
	Type        string  `json:"type"`
}

type OAuthIndex interface {
	PlayerIDByOAuthID(ctx context.Context, id OAuthID) (string, bool, error)
}

type Handler struct {
	js    jetstream.JetStream
	index OAuthIndex
}

func NewHandler(js jetstream.JetStream, index OAuthIndex) *Handler {
	return &Handler{js: js, index: index}
}

func (h *Handler) Create(ctx context.Context, cmd CreatePlayer) (PlayerCreated, error) {
	cmd.DisplayName = strings.TrimSpace(cmd.DisplayName)
	if err := cmd.validate(); err != nil {
		return PlayerCreated{}, err
	}
	if _, found, err := h.index.PlayerIDByOAuthID(ctx, cmd.OAuthID); err != nil {
		return PlayerCreated{}, fmt.Errorf("look up %s: %w", cmd.OAuthID.Key(), err)
	} else if found {
		return PlayerCreated{}, ErrAlreadyRegistered
	}
	tick, err := clock.CurrentTick(ctx, h.js)
	if err != nil {
		return PlayerCreated{}, fmt.Errorf("read tick: %w", err)
	}
	tick = max(tick, 0)

	ev := PlayerCreated{
		PlayerID:    cmd.PlayerID,
		OAuthID:     cmd.OAuthID,
		DisplayName: cmd.DisplayName,
		DialInCode:  cmd.DialInCode,
		Tick:        tick,
		Type:        PlayerCreatedType,
	}
	_, err = streams.Append(ctx, h.js, streams.PlayerSubject(cmd.PlayerID), ev, 0,
		jetstream.WithMsgID(PlayerCreatedType+"@"+cmd.OAuthID.Key()))
	switch {
	case errors.Is(err, streams.ErrConflict):
		return PlayerCreated{}, ErrAlreadyExists
	case errors.Is(err, streams.ErrDuplicate):
		return PlayerCreated{}, ErrAlreadyRegistered
	case err != nil:
		return PlayerCreated{}, fmt.Errorf("append player: %w", err)
	}
	return ev, nil
}

func (c CreatePlayer) validate() error {
	if c.PlayerID == "" {
		return fmt.Errorf("%w: missing player ID", ErrInvalid)
	}
	if !streams.ValidID(c.PlayerID) {
		return fmt.Errorf("%w: player ID %q may only contain letters, digits, '-' and '_'", ErrInvalid, c.PlayerID)
	}
	if !streams.ValidID(c.OAuthID.Provider) || !streams.ValidID(c.OAuthID.UID) {
		return fmt.Errorf("%w: oauth ID needs a provider and uid of letters, digits, '-' and '_'", ErrInvalid)
	}
	if err := streams.CheckName("display name", c.DisplayName, displayNameLimit); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if !validDialInCode.MatchString(c.DialInCode) {
		return fmt.Errorf("%w: dial-in code must be three groups of four digits", ErrInvalid)
	}
	return nil
}
