package bigbang

import (
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

type quiet struct{}

func (quiet) BigBangStarting(int) {}
func (quiet) BigBangLost()        {}
func (quiet) BigBangResuming()    {}

func testOptions(period time.Duration) Options {
	return Options{Replicas: 1, TickPeriod: period, Sectors: 64}
}

func clockEvents(t *testing.T, js jetstream.JetStream) uint64 {
	t.Helper()
	return natstest.SubjectCounts(t, js, streams.StreamClock, streams.SubjectClock)[streams.SubjectClock]
}

func TestEnsureCreatesOnce(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)

	first, err := Ensure(ctx, js, testOptions(time.Minute), quiet{})
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.Created.Type != "UniverseCreated" || first.Created.TickPeriod != time.Minute || !first.Rolled {
		t.Errorf("first %+v", first)
	}

	second, err := Ensure(ctx, js, testOptions(2*time.Minute), quiet{})
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second.Created != first.Created || !second.Map.Equal(first.Map) || second.Rolled {
		t.Errorf("second call returned %+v, want the original %+v", second, first)
	}
	if n := clockEvents(t, js); n != 1 {
		t.Errorf("clock events %d, want 1", n)
	}
}

func TestEnsureConcurrentInstancesAgree(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)

	const instances = 8
	got := make([]Result, instances)
	errs := make([]error, instances)
	var wg sync.WaitGroup
	for i := range instances {
		wg.Go(func() {
			got[i], errs[i] = Ensure(ctx, js, testOptions(time.Minute), quiet{})
		})
	}
	wg.Wait()

	rolled := 0
	for i := range instances {
		if errs[i] != nil {
			t.Fatalf("instance %d: %v", i, errs[i])
		}
		if got[i].Created != got[0].Created || !got[i].Map.Equal(got[0].Map) {
			t.Errorf("instance %d saw %+v, instance 0 saw %+v", i, got[i].Created, got[0].Created)
		}
		if got[i].Rolled {
			rolled++
		}
	}
	if rolled != 1 {
		t.Errorf("%d instances rolled the map, want 1", rolled)
	}
	if n := clockEvents(t, js); n != 1 {
		t.Errorf("clock events %d, want 1", n)
	}
}

func TestEnsureRejectsForeignFirstEvent(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	if _, err := streams.Append(ctx, js, streams.SubjectClock, clock.TickAdvanced{Type: "TickAdvanced"}, 0); err != nil {
		t.Fatalf("seed clock: %v", err)
	}

	if _, err := Ensure(ctx, js, testOptions(time.Minute), quiet{}); err == nil {
		t.Fatal("accepted a clock whose first event is not UniverseCreated")
	}
}

// The big bang creates every port and planet in the roster as the first event
// on its own subject, before UniverseCreated.
func TestEnsureCreatesPortsAndPlanets(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)

	bang, err := Ensure(ctx, js, testOptions(time.Minute), quiet{})
	if err != nil {
		t.Fatal(err)
	}
	bb, err := universe.Regenerate(bang.Map)
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
func TestEnsureCompletesAnInterruptedBigBang(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
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

	bang, err := Ensure(ctx, js, testOptions(time.Minute), quiet{})
	if err != nil {
		t.Fatal(err)
	}
	if bang.Created.Seed != 3 || !bang.Map.Equal(bb.Map) || bang.Rolled {
		t.Errorf("completed a different universe: %+v", bang)
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
