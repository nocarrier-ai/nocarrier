// Package planet is the planet aggregate: one subject per planet on EVENTS,
// planet.<planet_id>. The big bang creates each planet with CreatePlanet, and
// PlanetCreated (where it is, its class, and the colonists it starts with)
// is the first event on the subject. Colonies, production, and other future
// planetside activity will likely go through here.
package planet

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

const PlanetCreatedType = "PlanetCreated"

var (
	ErrInvalid       = errors.New("invalid planet command")
	ErrAlreadyExists = errors.New("planet already exists")
)

// CreatePlanet brings a planet into being. Planet IDs are numbers; the big
// bang assigns them in roster order, so Terra is 1.
type CreatePlanet struct {
	PlanetID  string               `json:"planet_id"`
	SectorID  string               `json:"sector_id"`
	Class     universe.PlanetClass `json:"class"`
	Colonists int                  `json:"colonists"`
}

// PlanetCreated is the first event on planet.<planet_id>.
type PlanetCreated struct {
	Type      string               `json:"type"`
	PlanetID  string               `json:"planet_id"`
	SectorID  string               `json:"sector_id"`
	Class     universe.PlanetClass `json:"class"`
	Colonists int                  `json:"colonists"`
	Tick      int64                `json:"tick"`
}

type Handler struct {
	js jetstream.JetStream
	u  *universe.Universe
}

func NewHandler(js jetstream.JetStream, u *universe.Universe) *Handler {
	return &Handler{js: js, u: u}
}

// Create appends PlanetCreated at the planet's empty subject. Expecting last
// sequence 0 admits exactly one planet per id. The big bang creates planets
// before the universe clock exists, so a planet created then is at tick 0.
func (h *Handler) Create(ctx context.Context, cmd CreatePlanet) (PlanetCreated, error) {
	if err := cmd.validate(h.u); err != nil {
		return PlanetCreated{}, err
	}
	tick, err := clock.CurrentTick(ctx, h.js)
	if err != nil && !errors.Is(err, clock.ErrNoClock) {
		return PlanetCreated{}, fmt.Errorf("read tick: %w", err)
	}

	ev := PlanetCreated{
		Type:      PlanetCreatedType,
		PlanetID:  cmd.PlanetID,
		SectorID:  cmd.SectorID,
		Class:     cmd.Class,
		Colonists: cmd.Colonists,
		Tick:      max(tick, 0),
	}
	if _, err := streams.Append(ctx, h.js, streams.PlanetSubject(cmd.PlanetID), ev, 0); err != nil {
		if errors.Is(err, streams.ErrConflict) {
			return PlanetCreated{}, ErrAlreadyExists
		}
		return PlanetCreated{}, fmt.Errorf("append planet: %w", err)
	}
	return ev, nil
}

func (c CreatePlanet) validate(u *universe.Universe) error {
	if _, ok := streams.Number(c.PlanetID); !ok {
		return fmt.Errorf("%w: planet ID %q is not a planet number", ErrInvalid, c.PlanetID)
	}
	n, ok := streams.Number(c.SectorID)
	if !ok {
		return fmt.Errorf("%w: sector ID %q is not a sector number", ErrInvalid, c.SectorID)
	}
	if _, ok := u.Sector(n); !ok {
		return fmt.Errorf("%w: sector %s does not exist", ErrInvalid, c.SectorID)
	}
	if int(c.Class) >= len(universe.PlanetClasses) {
		return fmt.Errorf("%w: unknown class %d", ErrInvalid, c.Class)
	}
	if c.Colonists < 0 {
		return fmt.Errorf("%w: %d colonists", ErrInvalid, c.Colonists)
	}
	return nil
}
