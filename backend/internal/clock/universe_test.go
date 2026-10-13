package clock

import (
	"testing"

	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

func TestCreateUniverseOnce(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)

	if _, found, err := ReadUniverseCreated(ctx, js); err != nil || found {
		t.Fatalf("before the big bang: found %v, err %v", found, err)
	}
	first, err := CreateUniverse(ctx, js, MinTickPeriod, 7)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.Type != "UniverseCreated" || first.TickPeriod != MinTickPeriod || first.Seed != 7 {
		t.Errorf("created %+v", first)
	}

	second, err := CreateUniverse(ctx, js, 2*MinTickPeriod, 8)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second != first {
		t.Errorf("second create returned %+v, want the original %+v", second, first)
	}
	read, found, err := ReadUniverseCreated(ctx, js)
	if err != nil || !found || read != first {
		t.Errorf("read %+v found %v err %v, want %+v", read, found, err, first)
	}
	if n := natstest.SubjectCounts(t, js, streams.StreamClock, streams.SubjectClock)[streams.SubjectClock]; n != 1 {
		t.Errorf("clock events %d, want 1", n)
	}
}

func TestUniverseCreatedMustBeTheFirstClockEvent(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	if _, err := streams.Append(ctx, js, streams.SubjectClock, TickAdvanced{Type: "TickAdvanced"}, 0); err != nil {
		t.Fatalf("seed clock: %v", err)
	}

	if _, _, err := ReadUniverseCreated(ctx, js); err == nil {
		t.Error("read accepted a clock whose first event is not UniverseCreated")
	}
	if _, err := CreateUniverse(ctx, js, MinTickPeriod, 1); err == nil {
		t.Error("create accepted a clock whose first event is not UniverseCreated")
	}
}
