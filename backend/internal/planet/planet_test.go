package planet

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

func testUniverse() *universe.Universe {
	return &universe.Universe{Sectors: []universe.Sector{{ID: 1, Core: true}, {ID: 2}, {ID: 3}}}
}

func create(planetID, sectorID string, class universe.PlanetClass, colonists int) CreatePlanet {
	return CreatePlanet{PlanetID: planetID, SectorID: sectorID, Class: class, Colonists: colonists}
}

func planetEvents(t *testing.T, js jetstream.JetStream) map[string]uint64 {
	t.Helper()
	return natstest.SubjectCounts(t, js, streams.StreamEvents, streams.PlanetEvents)
}

func TestValidateAccepts(t *testing.T) {
	for name, c := range map[string]CreatePlanet{
		"terra":       create("1", "1", universe.ClassM, 1_000_000),
		"empty world": create("7", "3", universe.ClassU, 0),
	} {
		t.Run(name, func(t *testing.T) {
			if err := c.validate(testUniverse()); err != nil {
				t.Fatalf("validate: %v", err)
			}
		})
	}
}

func TestValidateRejects(t *testing.T) {
	for name, c := range map[string]CreatePlanet{
		"missing planet ID":  create("", "1", universe.ClassM, 0),
		"planet zero":        create("0", "1", universe.ClassM, 0),
		"non-numeric planet": create("terra", "1", universe.ClassM, 0),
		"zero-padded planet": create("01", "1", universe.ClassM, 0),
		"missing sector ID":  create("1", "", universe.ClassM, 0),
		"unknown sector":     create("1", "99", universe.ClassM, 0),
		"unknown class":      create("1", "1", universe.PlanetClass(len(universe.PlanetClasses)), 0),
		"negative colonists": create("1", "1", universe.ClassM, -1),
	} {
		t.Run(name, func(t *testing.T) {
			if err := c.validate(testUniverse()); !errors.Is(err, ErrInvalid) {
				t.Fatalf("validate err = %v, want ErrInvalid", err)
			}
		})
	}
}

// The big bang creates planets before the clock exists: tick 0.
func TestCreateBeforeTheClockIsTickZero(t *testing.T) {
	js := natstest.Start(t)
	ev, err := NewHandler(js, testUniverse()).Create(natstest.Context(t), create("1", "1", universe.ClassM, 1_000_000))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	want := PlanetCreated{Type: PlanetCreatedType, PlanetID: "1", SectorID: "1", Class: universe.ClassM, Colonists: 1_000_000}
	if ev != want {
		t.Errorf("event = %+v, want %+v", ev, want)
	}
	var stored PlanetCreated
	if !natstest.Last(t, js, streams.StreamEvents, streams.PlanetSubject("1"), &stored) {
		t.Fatal("nothing appended")
	}
	if stored != want {
		t.Errorf("stored = %+v, want %+v", stored, want)
	}
}

func TestCreateStampsTheCurrentTick(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	if _, err := streams.Append(ctx, js, streams.SubjectClock, clock.UniverseCreated{Type: "UniverseCreated"}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := streams.Append(ctx, js, streams.SubjectClock, clock.TickAdvanced{Type: "TickAdvanced", Tick: 0}, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := streams.Append(ctx, js, streams.SubjectClock, clock.TickAdvanced{Type: "TickAdvanced", Tick: 1}, 2); err != nil {
		t.Fatal(err)
	}
	ev, err := NewHandler(js, testUniverse()).Create(ctx, create("2", "3", universe.ClassK, 0))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if ev.Tick != 1 {
		t.Errorf("tick = %d, want 1", ev.Tick)
	}
}

func TestCreateIsOncePerPlanet(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	h := NewHandler(js, testUniverse())
	if _, err := h.Create(ctx, create("1", "1", universe.ClassM, 1_000_000)); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := h.Create(ctx, create("1", "2", universe.ClassH, 0)); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("second err = %v, want ErrAlreadyExists", err)
	}
	if n := planetEvents(t, js)["planet.1"]; n != 1 {
		t.Errorf("planet.1 has %d events, want 1", n)
	}
	var stored PlanetCreated
	natstest.Last(t, js, streams.StreamEvents, streams.PlanetSubject("1"), &stored)
	if stored.SectorID != "1" || stored.Class != universe.ClassM {
		t.Errorf("stored = %+v; the rejected create overwrote the original", stored)
	}
}

func TestCreateKeepsPlanetsSeparate(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	h := NewHandler(js, testUniverse())
	if _, err := h.Create(ctx, create("1", "1", universe.ClassM, 1_000_000)); err != nil {
		t.Fatalf("1: %v", err)
	}
	if _, err := h.Create(ctx, create("2", "1", universe.ClassO, 0)); err != nil {
		t.Fatalf("2, sharing the sector: %v", err)
	}
	counts := planetEvents(t, js)
	if counts["planet.1"] != 1 || counts["planet.2"] != 1 {
		t.Errorf("subject counts = %v", counts)
	}
}

func TestCreateRejectsInvalidWithoutAppending(t *testing.T) {
	js := natstest.Start(t)
	_, err := NewHandler(js, testUniverse()).Create(natstest.Context(t), create("1", "99", universe.ClassM, 0))
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if got := planetEvents(t, js); len(got) != 0 {
		t.Errorf("appended %v", got)
	}
}
