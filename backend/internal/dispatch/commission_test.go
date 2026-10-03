package dispatch_test

import (
	"context"
	"maps"
	"testing"
	"time"

	"encoding/json"

	"github.com/nocarrier-ai/nocarrier/internal/avatar"
	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/decide"
	"github.com/nocarrier-ai/nocarrier/internal/dispatch"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/projector"
	"github.com/nocarrier-ai/nocarrier/internal/sector"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

// Commissioning an admiral is what starts the game for a player. This runs the
// real path end to end — command handler, both projections, dispatcher — and
// asserts the one thing that matters: the next tick dispatches work for the new
// flagship. Everything else in this slice is scaffolding for exactly this.
func TestCommissioningStartsTheGame(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)

	u := clock.UniverseCreated{Type: "UniverseCreated", TickPeriod: clock.MinTickPeriod}
	if _, err := streams.Append(ctx, js, streams.SubjectClock, u, 0); err != nil {
		t.Fatalf("universe: %v", err)
	}
	ev := clock.TickAdvanced{Type: "TickAdvanced", Tick: 41, WinnerInstanceID: "test"}
	if _, err := streams.Append(ctx, js, streams.SubjectClock, ev, 1); err != nil {
		t.Fatalf("advance: %v", err)
	}

	active, err := projector.NewActiveSectors(ctx, js, natstest.Logger())
	if err != nil {
		t.Fatalf("active sectors: %v", err)
	}
	due, err := projector.NewDueAvatars(ctx, js)
	if err != nil {
		t.Fatalf("due avatars: %v", err)
	}
	natstest.Run(t, projector.NewLoop(js, natstest.Logger(), active))
	natstest.Run(t, projector.NewLoop(js, natstest.Logger(), due))

	// Nothing to dispatch before any admiral exists.
	d := dispatch.New(js, active, due, natstest.Logger())
	if err := d.Dispatch(ctx, 41); err != nil {
		t.Fatalf("dispatch before commission: %v", err)
	}
	if got := natstest.SubjectCounts(t, js, streams.StreamExecute, "execute.>"); len(got) != 0 {
		t.Fatalf("executes dispatched into an empty universe: %v", got)
	}
	if got := natstest.SubjectCounts(t, js, streams.StreamDecide, "decide.>"); len(got) != 0 {
		t.Fatalf("decides dispatched into an empty universe: %v", got)
	}

	commissioned, err := avatar.NewHandler(js).Commission(ctx, avatar.CommissionAdmiral{
		AvatarID: "a1", Name: "Akbar", ShipName: "USS Cheesewheel",
	})
	if err != nil {
		t.Fatalf("commission: %v", err)
	}
	if commissioned.Tick != 41 {
		t.Fatalf("commissioned at tick %d, want 41", commissioned.Tick)
	}

	// Both projections have to see it before the next tick dispatches.
	natstest.Eventually(t, 5*time.Second, func() bool {
		sectors, err := active.ActiveSectors(ctx)
		if err != nil {
			t.Fatalf("active: %v", err)
		}
		avatars, err := due.DueAvatars(ctx, 41)
		if err != nil {
			t.Fatalf("due: %v", err)
		}
		// The sector is anchored one tick behind the commissioning tick,
		// because that is the last tick it actually resolved: nothing has
		// ever resolved on a brand new sector.
		return maps.Equal(sectors, map[string]int64{avatar.StartingSector: 40}) &&
			len(avatars) == 1 && avatars[0] == "a1"
	})

	if err := d.Dispatch(ctx, 41); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	// Exactly one execute for the home sector: the commissioning tick itself.
	// Anchoring to the tick is what keeps this from being a 41-deep catch-up
	// burst from the big bang.
	execSubject := streams.ExecuteSubject(avatar.StartingSector)
	wantExec := map[string]uint64{execSubject: 1}
	if got := natstest.SubjectCounts(t, js, streams.StreamExecute, "execute.>"); !maps.Equal(got, wantExec) {
		t.Errorf("execute counts %v, want %v", got, wantExec)
	}
	var exec sector.ExecuteTick
	want := sector.ExecuteTick{SectorID: avatar.StartingSector, Tick: 41}
	if !natstest.Last(t, js, streams.StreamExecute, execSubject, &exec) || exec != want {
		t.Errorf("last %s = %+v, want %+v", execSubject, exec, want)
	}

	// And one decide, so the admiral gets a first plan.
	wantDecide := map[string]uint64{"decide.a1": 1}
	if got := natstest.SubjectCounts(t, js, streams.StreamDecide, "decide.>"); !maps.Equal(got, wantDecide) {
		t.Errorf("decide counts %v, want %v", got, wantDecide)
	}
	var dec decide.DecideNow
	if !natstest.Last(t, js, streams.StreamDecide, "decide.a1", &dec) || dec != (decide.DecideNow{AvatarID: "a1", Tick: 41}) {
		t.Errorf("last decide.a1 = %+v", dec)
	}

	// The execute pool has to actually accept that command. A sector anchored
	// at the commissioning tick instead of the tick before it would leave the
	// handler naking forever, waiting for a predecessor that never resolved.
	natstest.Run(t, sector.NewPool(js, natstest.Logger(), 1, stubResolver{}))
	var resolved sector.TickResolved
	natstest.Eventually(t, 5*time.Second, func() bool {
		return natstest.Last(t, js, streams.StreamEvents, streams.SectorSubject(avatar.StartingSector), &resolved)
	})
	if resolved.Tick != 41 || !resolved.Pending {
		t.Errorf("resolved = %+v, want tick 41 still pending", resolved)
	}

	// Which advances the sector, so the next tick dispatches tick 42 and the
	// sector keeps resolving forward.
	natstest.Eventually(t, 5*time.Second, func() bool {
		sectors, err := active.ActiveSectors(ctx)
		if err != nil {
			t.Fatalf("active: %v", err)
		}
		return maps.Equal(sectors, map[string]int64{avatar.StartingSector: 41})
	})
	if err := d.Dispatch(ctx, 42); err != nil {
		t.Fatalf("dispatch 42: %v", err)
	}
	natstest.Eventually(t, 5*time.Second, func() bool {
		natstest.Last(t, js, streams.StreamEvents, streams.SectorSubject(avatar.StartingSector), &resolved)
		return resolved.Tick == 42
	})

	// Still no plan, so tick 42 asked for another decision.
	wantDecide = map[string]uint64{"decide.a1": 2}
	if got := natstest.SubjectCounts(t, js, streams.StreamDecide, "decide.>"); !maps.Equal(got, wantDecide) {
		t.Errorf("decide counts %v, want %v", got, wantDecide)
	}

	// Once a plan lands the admiral is no longer due, but the sector still is:
	// the flagship keeps executing its plan with no further decisions.
	natstest.Publish(t, js, streams.PlanSubject("a1"), decide.PlanRevised{
		Type: decide.PlanRevisedType, AvatarID: "a1", Tick: 42,
	})
	natstest.Eventually(t, 5*time.Second, func() bool {
		avatars, err := due.DueAvatars(ctx, 42)
		if err != nil {
			t.Fatalf("due: %v", err)
		}
		return len(avatars) == 0
	})
	// Due again at 43, one tick later: that is the decision cadence. The sector
	// resolved 43 either way, which is the point of the design — plans keep
	// executing whether or not a decision happens.
	if err := d.Dispatch(ctx, 43); err != nil {
		t.Fatalf("dispatch 43: %v", err)
	}
	wantDecide = map[string]uint64{"decide.a1": 3}
	if got := natstest.SubjectCounts(t, js, streams.StreamDecide, "decide.>"); !maps.Equal(got, wantDecide) {
		t.Errorf("decide counts %v at the next cadence tick, want %v", got, wantDecide)
	}
	natstest.Eventually(t, 5*time.Second, func() bool {
		natstest.Last(t, js, streams.StreamEvents, streams.SectorSubject(avatar.StartingSector), &resolved)
		return resolved.Tick == 43
	})
}

// stubResolver stands in for the real game rules: it only has to produce a
// TickResolved so the single-writer path runs.
type stubResolver struct{}

func (stubResolver) Resolve(_ context.Context, sectorID string, tick int64) (json.RawMessage, bool, error) {
	out, err := json.Marshal([]map[string]any{{"type": "Noop", "sector_id": sectorID, "tick": tick}})
	return out, true, err
}
