// Package port is the trading post aggregate: one subject per port on EVENTS,
// port.<sector_id>, owning the port's books. Stance, capacity and regen are
// facts in the universe map; what the port owns is what is available per
// commodity. Every trade is one event here carrying the books after it, so
// the last event on the subject is the port's state.
//
// Trading and regeneration are separate commands. A trade is a ship's request
// and arrives at any time. Regeneration toward capacity happens when the port
// resolves a tick, from the dispatcher's fan-out; that command is not here
// yet.
package port

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

const TradeCompletedType = "TradeCompleted"

var (
	ErrInvalid      = errors.New("invalid trade")
	ErrNoPort       = errors.New("no port in sector")
	ErrInsufficient = errors.New("port cannot fill the trade")
)

// Available is a port's books: units on hand for a commodity the port sells,
// demand remaining for one it buys. Indexed by universe.Commodity.
type Available [len(universe.Commodities)]int

// Trade asks a port to trade one commodity with a ship. The direction is the
// port's stance in the map: a port that sells the commodity sells it to the
// ship, one that buys it buys from the ship. Commodities are numbered as in
// the map: 0 Fuel Ore, 1 Organics, 2 Equipment.
type Trade struct {
	SectorID  string             `json:"sector_id"`
	ShipID    string             `json:"ship_id"`
	Commodity universe.Commodity `json:"commodity"`
	Units     int                `json:"units"`
}

// TradeCompleted is appended on port.<sector_id> for every trade: what was
// traded, and the port's books after it.
type TradeCompleted struct {
	Type      string             `json:"type"`
	SectorID  string             `json:"sector_id"`
	ShipID    string             `json:"ship_id"`
	Commodity universe.Commodity `json:"commodity"`
	Units     int                `json:"units"`
	Available Available          `json:"available"`
	Tick      int64              `json:"tick"`
}

type Handler struct {
	js jetstream.JetStream
	u  *universe.Universe
}

func NewHandler(js jetstream.JetStream, u *universe.Universe) *Handler {
	return &Handler{js: js, u: u}
}

// Trade validates the command against the port's books and appends
// TradeCompleted. The guard is the sequence the books were read at, so two
// trades racing for the same stock cannot both succeed: the loser gets
// streams.ErrConflict and resubmits against the new books.
func (h *Handler) Trade(ctx context.Context, cmd Trade) (TradeCompleted, error) {
	if err := cmd.validate(); err != nil {
		return TradeCompleted{}, err
	}
	sector, _ := strconv.Atoi(cmd.SectorID)
	p, ok := h.u.PortInSector(sector)
	if !ok {
		return TradeCompleted{}, fmt.Errorf("%w: sector %s", ErrNoPort, cmd.SectorID)
	}
	subject := streams.PortSubject(cmd.SectorID)
	available, seq, err := h.books(ctx, subject, p)
	if err != nil {
		return TradeCompleted{}, fmt.Errorf("read port: %w", err)
	}
	if available[cmd.Commodity] < cmd.Units {
		return TradeCompleted{}, fmt.Errorf("%w: the port in sector %s has %d %s available, trade wants %d",
			ErrInsufficient, cmd.SectorID, available[cmd.Commodity], cmd.Commodity, cmd.Units)
	}
	// TODO(ship): validate the ship's side here, once the ship aggregate
	// exists. When the port buys cmd.Commodity the ship is selling from its
	// cargo hold, so read the hold from the ship-status read model (folded
	// from ship.<ship_id>) and reject with ErrInsufficientCargo when it holds
	// fewer units of cmd.Commodity than cmd.Units. Rejected before the tick is
	// read, like the port-side check, so nothing is appended.
	tick, err := clock.CurrentTick(ctx, h.js)
	if err != nil {
		return TradeCompleted{}, fmt.Errorf("read tick: %w", err)
	}

	available[cmd.Commodity] -= cmd.Units
	ev := TradeCompleted{
		Type:      TradeCompletedType,
		SectorID:  cmd.SectorID,
		ShipID:    cmd.ShipID,
		Commodity: cmd.Commodity,
		Units:     cmd.Units,
		Available: available,
		Tick:      max(tick, 0),
	}
	if _, err := streams.Append(ctx, h.js, subject, ev, seq); err != nil {
		return TradeCompleted{}, fmt.Errorf("append trade: %w", err)
	}
	return ev, nil
}

// books reads the port's state: the books the last event on its subject left,
// or every commodity at capacity when nothing has happened yet.
func (h *Handler) books(ctx context.Context, subject string, p universe.Port) (Available, uint64, error) {
	var last struct {
		Available Available `json:"available"`
	}
	seq, err := streams.Last(ctx, h.js, streams.StreamEvents, subject, &last)
	if err != nil {
		return Available{}, 0, err
	}
	if seq == 0 {
		return AtRest(p), 0, nil
	}
	return last.Available, seq, nil
}

// AtRest is a port's books before any trade: every commodity at capacity.
func AtRest(p universe.Port) Available {
	var a Available
	for c, g := range p.Goods {
		a[c] = g.Capacity
	}
	return a
}

func (c Trade) validate() error {
	if c.SectorID == "" {
		return fmt.Errorf("%w: missing sector ID", ErrInvalid)
	}
	// The subject token must be the canonical spelling of the number, or one
	// port could have two subjects.
	if n, err := strconv.Atoi(c.SectorID); err != nil || n < 1 || strconv.Itoa(n) != c.SectorID {
		return fmt.Errorf("%w: sector ID %q is not a sector number", ErrInvalid, c.SectorID)
	}
	if c.ShipID == "" {
		return fmt.Errorf("%w: missing ship ID", ErrInvalid)
	}
	if !streams.ValidID(c.ShipID) {
		return fmt.Errorf("%w: ship ID %q may only contain letters, digits, '-' and '_'", ErrInvalid, c.ShipID)
	}
	if int(c.Commodity) >= len(universe.Commodities) {
		return fmt.Errorf("%w: unknown commodity %d", ErrInvalid, c.Commodity)
	}
	if c.Units <= 0 {
		return fmt.Errorf("%w: units must be positive, got %d", ErrInvalid, c.Units)
	}
	return nil
}
