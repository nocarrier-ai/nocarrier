package projector

import (
	"testing"
	"time"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

func TestUniverseClockProjection(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	proj, err := NewUniverseClock(ctx, js)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	natstest.Run(t, NewLoop(js, natstest.Logger(), proj))

	if _, err := clock.CreateUniverse(ctx, js, time.Minute, 7, 20); err != nil {
		t.Fatalf("universe: %v", err)
	}
	want := ClockEntry{Seed: 7, Sectors: 20, Tick: 0, TickPeriodMS: 60000}
	if got := waitForClock(t, proj, func(e ClockEntry) bool { return e.Seed == 7 }); got != want {
		t.Errorf("after UniverseCreated %+v, want %+v", got, want)
	}

	if _, err := streams.Append(ctx, js, streams.SubjectClock, clock.TickAdvanced{Type: clock.TickAdvancedType, Tick: 3}, 1); err != nil {
		t.Fatalf("tick: %v", err)
	}
	want.Tick = 3
	if got := waitForClock(t, proj, func(e ClockEntry) bool { return e.Tick == 3 }); got != want {
		t.Errorf("after TickAdvanced %+v, want %+v", got, want)
	}
}

func waitForClock(t *testing.T, proj *UniverseClock, ready func(ClockEntry) bool) ClockEntry {
	t.Helper()
	ctx := natstest.Context(t)
	var got ClockEntry
	natstest.Eventually(t, 5*time.Second, func() bool {
		e, ok, err := proj.Clock(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		got = e
		return ok && ready(e)
	})
	return got
}

func TestUniverseCountsProjection(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	proj, err := NewUniverseCounts(ctx, js)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	natstest.Run(t, NewLoop(js, natstest.Logger(), proj))

	natstest.Publish(t, js, streams.PortSubject("1"), map[string]any{"type": "PortCreated"})
	natstest.Publish(t, js, streams.PortSubject("2"), map[string]any{"type": "PortCreated"})
	natstest.Publish(t, js, streams.PortSubject("1"), map[string]any{"type": "TradeCompleted"})
	natstest.Publish(t, js, streams.PlanetSubject("1"), map[string]any{"type": "PlanetCreated"})
	natstest.Publish(t, js, streams.PlayerSubject("p1"), map[string]any{"type": "PlayerCreated"})
	natstest.Publish(t, js, streams.AvatarSubject("a1"), map[string]any{"type": "AdmiralCommissioned"})

	want := CountsEntry{Ports: 2, Planets: 1, Players: 1}
	var got CountsEntry
	natstest.Eventually(t, 5*time.Second, func() bool {
		e, ok, err := proj.Counts(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		got = e
		return ok && e == want
	})
	if got != want {
		t.Errorf("counts %+v, want %+v", got, want)
	}
}
