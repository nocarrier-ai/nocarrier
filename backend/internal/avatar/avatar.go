// Package avatar is the fleet admiral aggregate: one subject per avatar on
// EVENTS, owning identity and lifecycle. The flagship's mutable state (hull,
// fuel, credits, position) is not here; it changes when a sector resolves a
// tick, so those facts live in TickResolved on sector.<sector_id>.
package avatar

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

const (
	AdmiralCommissionedType = "AdmiralCommissioned"

	// nameLimit bounds both the admiral and flagship names. They are
	// player-supplied display text rendered into fixed-width telnet frames.
	nameLimit = 48
)

// StartingSector is where every new flagship appears, chosen by the handler and
// never accepted from the command. Sector ids are numeric, following TradeWars
// 2002. Sector 0 serves as the single shared spawn until the universe has a
// real map to pick from.
const StartingSector = "0"

var (
	ErrInvalid             = errors.New("invalid admiral commission")
	ErrAlreadyCommissioned = errors.New("admiral already commissioned")
)

// CommissionAdmiral asks for a new fleet admiral and flagship. It carries
// only what the player chooses; the spawn comes from the handler.
type CommissionAdmiral struct {
	AvatarID string `json:"avatar_id"`
	Name     string `json:"name"`
	ShipName string `json:"ship_name"`
}

// AdmiralCommissioned is the first event on avatar.<avatar_id> and the only one
// appended at an empty subject: who the admiral is, where the flagship starts,
// and when. Nothing about the flagship's condition or cargo belongs here, both
// because that changes through sector resolution and because there are no
// economy rules yet to be right about.
type AdmiralCommissioned struct {
	AvatarID   string `json:"avatar_id"`
	Name       string `json:"name"`
	ShipName   string `json:"ship_name"`
	HomeSector string `json:"home_sector"`
	Tick       int64  `json:"tick"`
	Type       string `json:"type"`
}

type Handler struct {
	js jetstream.JetStream
}

func NewHandler(js jetstream.JetStream) *Handler {
	return &Handler{
		js: js,
	}
}

// Commission appends AdmiralCommissioned at the avatar's empty subject. The
// guard is the uniqueness check: expecting last sequence 0 admits exactly one
// commission per avatar id, so a second attempt (or a concurrent duplicate
// submit) is rejected rather than creating a second admiral. There is no read
// first; the append decides.
func (h *Handler) Commission(ctx context.Context, cmd CommissionAdmiral) (AdmiralCommissioned, error) {
	cmd.Name = strings.TrimSpace(cmd.Name)
	cmd.ShipName = strings.TrimSpace(cmd.ShipName)
	if err := cmd.validate(); err != nil {
		return AdmiralCommissioned{}, err
	}
	tick, err := clock.CurrentTick(ctx, h.js)
	if err != nil {
		return AdmiralCommissioned{}, fmt.Errorf("read tick: %w", err)
	}
	// CurrentTick reports -1 between UniverseCreated and the first
	// TickAdvanced. Stamp tick 0 instead: this tick is what activates the
	// home sector, and a negative one would make the dispatcher replay
	// every tick from 0 on the admiral's first dispatch.
	tick = max(tick, 0)

	ev := AdmiralCommissioned{
		AvatarID:   cmd.AvatarID,
		Name:       cmd.Name,
		ShipName:   cmd.ShipName,
		HomeSector: StartingSector,
		Tick:       tick,
		Type:       AdmiralCommissionedType,
	}
	if _, err := streams.Append(ctx, h.js, streams.AvatarSubject(cmd.AvatarID), ev, 0); err != nil {
		if errors.Is(err, streams.ErrConflict) {
			return AdmiralCommissioned{}, ErrAlreadyCommissioned
		}
		return AdmiralCommissioned{}, fmt.Errorf("append avatar: %w", err)
	}
	return ev, nil
}

func (c CommissionAdmiral) validate() error {
	if c.AvatarID == "" {
		return fmt.Errorf("%w: missing avatar ID", ErrInvalid)
	}
	if !streams.ValidID(c.AvatarID) {
		return fmt.Errorf("%w: avatar ID %q may only contain letters, digits, '-' and '_'", ErrInvalid, c.AvatarID)
	}
	if err := checkName("admiral name", c.Name); err != nil {
		return err
	}
	return checkName("flagship name", c.ShipName)
}

// checkName bounds player-supplied display text. Control characters are
// rejected rather than stripped: they would break the telnet frames and ANSI
// sequences every other player sees this name through.
func checkName(what, name string) error {
	if name == "" {
		return fmt.Errorf("%w: missing %s", ErrInvalid, what)
	}
	if n := utf8.RuneCountInString(name); n > nameLimit {
		return fmt.Errorf("%w: %s is %d characters, limit is %d", ErrInvalid, what, n, nameLimit)
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: %s contains a control character", ErrInvalid, what)
		}
	}
	return nil
}
