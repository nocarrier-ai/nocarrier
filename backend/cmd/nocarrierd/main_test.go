package main

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/planet"
	"github.com/nocarrier-ai/nocarrier/internal/port"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

func testConfig(period time.Duration) config {
	return config{replicas: 1, tickPeriod: period, sectors: 64}
}

func clockEvents(t *testing.T, js jetstream.JetStream) uint64 {
	t.Helper()
	return natstest.SubjectCounts(t, js, streams.StreamClock, streams.SubjectClock)[streams.SubjectClock]
}

func TestEnsureUniverseCreatesOnce(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)

	first, _, err := ensureUniverse(ctx, js, testConfig(time.Minute), newReporter(io.Discard, false))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.Type != "UniverseCreated" || first.TickPeriod != time.Minute {
		t.Errorf("created %+v", first)
	}

	second, _, err := ensureUniverse(ctx, js, testConfig(2*time.Minute), newReporter(io.Discard, false))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second != first {
		t.Errorf("second call returned %+v, want the original %+v", second, first)
	}
	if n := clockEvents(t, js); n != 1 {
		t.Errorf("clock events %d, want 1", n)
	}
}

func TestEnsureUniverseConcurrentInstancesAgree(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)

	const instances = 8
	got := make([]clock.UniverseCreated, instances)
	errs := make([]error, instances)
	var wg sync.WaitGroup
	for i := range instances {
		wg.Go(func() {
			got[i], _, errs[i] = ensureUniverse(ctx, js, testConfig(time.Minute), newReporter(io.Discard, false))
		})
	}
	wg.Wait()

	for i := range instances {
		if errs[i] != nil {
			t.Fatalf("instance %d: %v", i, errs[i])
		}
		if got[i] != got[0] {
			t.Errorf("instance %d saw %+v, instance 0 saw %+v", i, got[i], got[0])
		}
	}
	if n := clockEvents(t, js); n != 1 {
		t.Errorf("clock events %d, want 1", n)
	}
}

func TestEnsureUniverseRejectsForeignFirstEvent(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	if _, err := streams.Append(ctx, js, streams.SubjectClock, clock.TickAdvanced{Type: "TickAdvanced"}, 0); err != nil {
		t.Fatalf("seed clock: %v", err)
	}

	if _, _, err := ensureUniverse(ctx, js, testConfig(time.Minute), newReporter(io.Discard, false)); err == nil {
		t.Fatal("accepted a clock whose first event is not UniverseCreated")
	}
}

// The big bang creates every port and planet in the roster as the first event
// on its own subject, before UniverseCreated.
func TestEnsureUniverseCreatesPortsAndPlanets(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)

	_, u, err := ensureUniverse(ctx, js, testConfig(time.Minute), newReporter(io.Discard, false))
	if err != nil {
		t.Fatal(err)
	}
	bb, err := universe.Regenerate(u)
	if err != nil {
		t.Fatal(err)
	}
	ports := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.PortEvents)
	planets := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.PlanetEvents)
	if len(ports) != len(bb.Ports) || len(planets) != len(bb.Planets) {
		t.Errorf("%d ports and %d planets created, roster has %d and %d", len(ports), len(planets), len(bb.Ports), len(bb.Planets))
	}

	var spawnPort port.PortCreated
	if !natstest.Last(t, js, streams.StreamEvents, streams.PortSubject("1"), &spawnPort) {
		t.Fatal("the spawn has no port")
	}
	if spawnPort.Type != port.PortCreatedType || spawnPort.Commodities != bb.Ports[0].Commodities || spawnPort.Tick != 0 {
		t.Errorf("spawn port = %+v", spawnPort)
	}
	for c, g := range spawnPort.Commodities {
		if spawnPort.Available[c] != g.Capacity {
			t.Errorf("%s available %d at creation, want capacity %d", universe.Commodity(c), spawnPort.Available[c], g.Capacity)
		}
	}

	var terra planet.PlanetCreated
	if !natstest.Last(t, js, streams.StreamEvents, streams.PlanetSubject("1"), &terra) {
		t.Fatal("planet 1 does not exist")
	}
	if terra.SectorID != "1" || terra.Class != universe.ClassM || terra.Colonists != bb.Planets[0].InitialColonists || terra.Tick != 0 {
		t.Errorf("terra = %+v", terra)
	}
}

// A map with no UniverseCreated is a big bang that did not finish. The next
// instance rolls the roster again from the stored seed and completes it.
func TestEnsureUniverseCompletesAnInterruptedBigBang(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	if err := streams.Ensure(ctx, js, 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	store, err := universe.NewStore(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	bb, err := universe.Generate(3, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, bb.Map); err != nil {
		t.Fatal(err)
	}
	// Part of the roster landed before the crash.
	if _, err := port.NewHandler(js, bb.Map).Create(ctx, port.CreatePort{SectorID: "1", Commodities: bb.Ports[0].Commodities}); err != nil {
		t.Fatal(err)
	}

	created, u, err := ensureUniverse(ctx, js, testConfig(time.Minute), newReporter(io.Discard, false))
	if err != nil {
		t.Fatal(err)
	}
	if created.Seed != 3 || !u.Equal(bb.Map) {
		t.Errorf("completed a different universe: seed %d", created.Seed)
	}
	ports := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.PortEvents)
	planets := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.PlanetEvents)
	if len(ports) != len(bb.Ports) || len(planets) != len(bb.Planets) {
		t.Errorf("%d ports and %d planets, roster has %d and %d", len(ports), len(planets), len(bb.Ports), len(bb.Planets))
	}
	if ports["port.1"] != 1 {
		t.Errorf("port.1 has %d events; the port that already existed was recreated", ports["port.1"])
	}
	if n := clockEvents(t, js); n != 1 {
		t.Errorf("clock events %d, want 1", n)
	}
}
