package bigbang

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/planet"
	"github.com/nocarrier-ai/nocarrier/internal/port"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

type Options struct {
	Sectors    int
	TickPeriod time.Duration
}

type Reporter interface {
	BigBangStarting(sectors int)
	BigBangLost()
	BigBangResuming()
}

type Result struct {
	Map     *universe.Universe
	Created clock.UniverseCreated
	Rolled  bool
	Ports   int
	Planets int
}

// Ensure brings the universe into being, or finds the one that already
// exists. The big bang is three steps, each done exactly once: store the map,
// create every port and planet, append UniverseCreated.
//
// The map goes first. Its KV create is the election — concurrent instances all
// generate a candidate, exactly one lands, and the losers read the winner's
// rather than their own. Ports and planets come next, from the roster rolled
// beside the map, each as the first event on its own subject. UniverseCreated
// goes last, carrying the seed of a map that is already durable, so there is
// never a moment where the clock says a universe exists but its geography does
// not. Until it lands, any instance that finds the map completes the rest.
func Ensure(ctx context.Context, js jetstream.JetStream, opts Options, rep Reporter) (Result, error) {
	store, err := universe.NewStore(ctx, js)
	if err != nil {
		return Result{}, err
	}

	var (
		bb     *universe.GenerationResult // the roster, when this instance rolled the map
		rolled bool
	)
	stored, err := store.Load(ctx)
	switch {
	case err == nil:
	case errors.Is(err, universe.ErrNoMap):
		rep.BigBangStarting(opts.Sectors)
		if bb, err = universe.Generate(time.Now().UnixNano(), opts.Sectors); err != nil {
			return Result{}, fmt.Errorf("generate universe: %w", err)
		}
		switch cerr := store.Create(ctx, bb.Map); {
		case cerr == nil:
			rolled = true
			stored = bb.Map
		case errors.Is(cerr, universe.ErrMapExists):
			rep.BigBangLost()
			bb = nil
			if stored, err = store.Load(ctx); err != nil {
				return Result{}, fmt.Errorf("read the winning universe map: %w", err)
			}
		default:
			return Result{}, cerr
		}
	default:
		return Result{}, err
	}

	created, found, err := clock.ReadUniverseCreated(ctx, js)
	if err != nil {
		return Result{}, err
	}
	if !found {
		if created, err = complete(ctx, js, opts.TickPeriod, rep, stored, bb); err != nil {
			return Result{}, err
		}
	}
	// The clock and the map must agree about which universe this is. A
	// mismatch means one of them was wiped independently, and starting anyway
	// would move every ship.
	if created.Seed != stored.Seed {
		return Result{}, fmt.Errorf("clock says universe seed %d but the stored map is seed %d",
			created.Seed, stored.Seed)
	}
	ports, err := streams.SubjectCount(ctx, js, streams.PortEvents)
	if err != nil {
		return Result{}, err
	}
	planets, err := streams.SubjectCount(ctx, js, streams.PlanetEvents)
	if err != nil {
		return Result{}, err
	}
	return Result{Map: stored, Created: created, Rolled: rolled, Ports: ports, Planets: planets}, nil
}

// complete creates every port and planet in the roster, then appends
// UniverseCreated. Every step is idempotent — a port or planet that exists is
// skipped, and the clock append expects an empty subject — so an instance that
// crashed part way, or several arriving together, converge on one universe.
// An instance without the roster rolls it again from the stored seed.
func complete(ctx context.Context, js jetstream.JetStream, tickPeriod time.Duration, rep Reporter, stored *universe.Universe, bb *universe.GenerationResult) (clock.UniverseCreated, error) {
	if bb == nil {
		rep.BigBangResuming()
		var err error
		if bb, err = universe.Regenerate(stored); err != nil {
			return clock.UniverseCreated{}, fmt.Errorf("regenerate universe: %w", err)
		}
	}
	ports := port.NewHandler(js, stored)
	for _, p := range bb.Ports {
		_, err := ports.Create(ctx, port.CreatePort{SectorID: strconv.Itoa(p.Sector), Commodities: p.Commodities})
		if err != nil && !errors.Is(err, port.ErrAlreadyExists) {
			return clock.UniverseCreated{}, fmt.Errorf("create port in sector %d: %w", p.Sector, err)
		}
	}
	planets := planet.NewHandler(js, stored)
	for i, p := range bb.Planets {
		_, err := planets.Create(ctx, planet.CreatePlanet{
			PlanetID:  strconv.Itoa(i + 1),
			SectorID:  strconv.Itoa(p.Sector),
			Class:     p.Class,
			Colonists: p.InitialColonists,
		})
		if err != nil && !errors.Is(err, planet.ErrAlreadyExists) {
			return clock.UniverseCreated{}, fmt.Errorf("create planet %d: %w", i+1, err)
		}
	}
	return clock.CreateUniverse(ctx, js, tickPeriod, stored.Seed, len(stored.Sectors))
}
