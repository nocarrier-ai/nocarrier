// Package port is the trading post aggregate: one subject per port on EVENTS,
// port.<sector_id>. The big bang creates each port with CreatePort, and
// PortCreated (the port's terms per commodity, with everything available)
// is the first event on the subject. Every event after it carries the port's
// state thereafter, so the last event on the subject is the port's current state.
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

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

const (
	PortCreatedType    = "PortCreated"
	TradeCompletedType = "TradeCompleted"
)

var (
	ErrInvalid       = errors.New("invalid port command")
	ErrAlreadyExists = errors.New("port already exists")
	ErrNoPort        = errors.New("no port in sector")
	ErrInsufficient  = errors.New("port cannot fill the trade")
)

// Available is a port's books: units on hand for a commodity the port sells,
// demand remaining for one it buys. Indexed by universe.Commodity.
type Available [len(universe.Commodities)]int

// State is the port, carried on every event: its terms per commodity and what
// is available.
type State struct {
	Commodities universe.Terms `json:"commodities"`
	Available   Available      `json:"available"`
}

// CreatePort brings a port into being on the given terms.
type CreatePort struct {
	SectorID    string         `json:"sector_id"`
	Commodities universe.Terms `json:"commodities"`
}

// PortCreated is the first event on port.<sector_id>: the terms, with every
// commodity at capacity.
type PortCreated struct {
	Type     string `json:"type"`
	SectorID string `json:"sector_id"`
	State
	Tick int64 `json:"tick"`
}

// Trade asks a port to trade one commodity with a ship. The direction is the
// port's stance: a port that sells the commodity sells it to the ship, one
// that buys it buys from the ship. Commodities are numbered as in the map:
// 0 Fuel Ore, 1 Organics, 2 Equipment.
type Trade struct {
	SectorID  string             `json:"sector_id"`
	ShipID    string             `json:"ship_id"`
	Commodity universe.Commodity `json:"commodity"`
	Units     int                `json:"units"`
}

// TradeCompleted is appended for every trade: what was traded, and the port
// after it.
type TradeCompleted struct {
	Type      string             `json:"type"`
	SectorID  string             `json:"sector_id"`
	ShipID    string             `json:"ship_id"`
	Commodity universe.Commodity `json:"commodity"`
	Units     int                `json:"units"`
	State
	Tick int64 `json:"tick"`
}

type Handler struct {
	js jetstream.JetStream
	u  *universe.Universe
}

func NewHandler(js jetstream.JetStream, u *universe.Universe) *Handler {
	return &Handler{js: js, u: u}
}

// Create appends PortCreated at the port's empty subject. Expecting last
// sequence 0 admits exactly one port per sector. The big bang creates ports
// before the universe clock exists, so a port created then is at tick 0.
func (h *Handler) Create(ctx context.Context, cmd CreatePort) (PortCreated, error) {
	if err := cmd.validate(h.u); err != nil {
		return PortCreated{}, err
	}
	tick, err := clock.CurrentTick(ctx, h.js)
	if err != nil && !errors.Is(err, clock.ErrNoClock) {
		return PortCreated{}, fmt.Errorf("read tick: %w", err)
	}

	ev := PortCreated{
		Type:     PortCreatedType,
		SectorID: cmd.SectorID,
		State:    State{Commodities: cmd.Commodities, Available: atRest(cmd.Commodities)},
		Tick:     max(tick, 0),
	}
	if _, err := streams.Append(ctx, h.js, streams.PortSubject(cmd.SectorID), ev, 0); err != nil {
		if errors.Is(err, streams.ErrConflict) {
			return PortCreated{}, ErrAlreadyExists
		}
		return PortCreated{}, fmt.Errorf("append port: %w", err)
	}
	return ev, nil
}

// Trade validates the command against the port's state and appends
// TradeCompleted. The guard is the sequence the state was read at, so two
// trades racing for the same stock cannot both succeed: the loser gets
// streams.ErrConflict and resubmits against the new state.
func (h *Handler) Trade(ctx context.Context, cmd Trade) (TradeCompleted, error) {
	if err := cmd.validate(); err != nil {
		return TradeCompleted{}, err
	}
	subject := streams.PortSubject(cmd.SectorID)
	var st State
	seq, err := streams.Last(ctx, h.js, streams.StreamEvents, subject, &st)
	if err != nil {
		return TradeCompleted{}, fmt.Errorf("read port: %w", err)
	}
	if seq == 0 {
		return TradeCompleted{}, fmt.Errorf("%w: sector %s", ErrNoPort, cmd.SectorID)
	}
	if st.Available[cmd.Commodity] < cmd.Units {
		return TradeCompleted{}, fmt.Errorf("%w: the port in sector %s has %d %s available, trade wants %d",
			ErrInsufficient, cmd.SectorID, st.Available[cmd.Commodity], cmd.Commodity, cmd.Units)
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

	st.Available[cmd.Commodity] -= cmd.Units
	ev := TradeCompleted{
		Type:      TradeCompletedType,
		SectorID:  cmd.SectorID,
		ShipID:    cmd.ShipID,
		Commodity: cmd.Commodity,
		Units:     cmd.Units,
		State:     st,
		Tick:      max(tick, 0),
	}
	if _, err := streams.Append(ctx, h.js, subject, ev, seq); err != nil {
		return TradeCompleted{}, fmt.Errorf("append trade: %w", err)
	}
	return ev, nil
}

// atRest is a port's books at creation: every commodity at capacity.
func atRest(terms universe.Terms) Available {
	var a Available
	for c, g := range terms {
		a[c] = g.Capacity
	}
	return a
}

func (c CreatePort) validate(u *universe.Universe) error {
	n, ok := streams.Number(c.SectorID)
	if !ok {
		return fmt.Errorf("%w: sector ID %q is not a sector number", ErrInvalid, c.SectorID)
	}
	if _, ok := u.Sector(n); !ok {
		return fmt.Errorf("%w: sector %s does not exist", ErrInvalid, c.SectorID)
	}
	for i, g := range c.Commodities {
		if g.Capacity <= 0 || g.Regen <= 0 {
			return fmt.Errorf("%w: %s has capacity %d regen %d", ErrInvalid, universe.Commodity(i), g.Capacity, g.Regen)
		}
	}
	return nil
}

func (c Trade) validate() error {
	if _, ok := streams.Number(c.SectorID); !ok {
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
