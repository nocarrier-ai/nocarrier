package main

import (
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
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

	first, _, err := ensureUniverse(ctx, js, testConfig(time.Minute), natstest.Logger())
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.Type != "UniverseCreated" || first.TickPeriod != time.Minute {
		t.Errorf("created %+v", first)
	}

	second, _, err := ensureUniverse(ctx, js, testConfig(2*time.Minute), natstest.Logger())
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
			got[i], _, errs[i] = ensureUniverse(ctx, js, testConfig(time.Minute), natstest.Logger())
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

	if _, _, err := ensureUniverse(ctx, js, testConfig(time.Minute), natstest.Logger()); err == nil {
		t.Fatal("accepted a clock whose first event is not UniverseCreated")
	}
}
