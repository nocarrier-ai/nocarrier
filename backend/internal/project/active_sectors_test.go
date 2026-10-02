package project

import (
	"maps"
	"testing"
	"time"

	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/sector"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

func TestRoute(t *testing.T) {
	cases := []struct {
		name     string
		data     string
		wantKey  string
		wantKeep bool
		wantLast int64
	}{
		{"pending tick", `{"type":"TickResolved","sector":"s1","tick":4,"pending":true}`, "s1", true, 4},
		{"idle tick", `{"type":"TickResolved","sector":"s1","tick":4,"pending":false}`, "s1", false, 4},
		{"doctrine with sector", `{"type":"DoctrineUpdated","avatar":"a1","sector":"s9"}`, "s9", true, -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			key, f := route([]byte(c.data))
			if f == nil {
				t.Fatal("not routed")
			}
			if key != c.wantKey {
				t.Errorf("key %q, want %q", key, c.wantKey)
			}
			e := ActiveEntry{LastResolved: -1}
			if keep := f(&e); keep != c.wantKeep {
				t.Errorf("keep %v, want %v", keep, c.wantKeep)
			}
			if e.LastResolved != c.wantLast {
				t.Errorf("last resolved %d, want %d", e.LastResolved, c.wantLast)
			}
		})
	}
}

func TestRouteSkips(t *testing.T) {
	for name, data := range map[string]string{
		"doctrine without sector": `{"type":"DoctrineUpdated","avatar":"a1"}`,
		"tick without sector":     `{"type":"TickResolved","tick":4,"pending":true}`,
		"plan revised":            `{"type":"PlanRevised","avatar":"a1"}`,
		"unknown type":            `{"type":"Nope"}`,
		"bad json":                `{`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, f := route([]byte(data)); f != nil {
				t.Error("routed, want skip")
			}
		})
	}
}

func TestStep(t *testing.T) {
	keep := func(last int64) fold {
		return func(e *ActiveEntry) bool { e.LastResolved = max(e.LastResolved, last); return true }
	}
	drop := func(e *ActiveEntry) bool { return false }

	cases := []struct {
		name   string
		e      ActiveEntry
		exists bool
		seq    uint64
		f      fold
		want   ActiveEntry
		wantOp op
	}{
		{"create", ActiveEntry{LastResolved: -1}, false, 3, keep(0), ActiveEntry{LastResolved: 0, Seq: 3}, opPut},
		{"advance", ActiveEntry{LastResolved: 4, Seq: 3}, true, 9, keep(5), ActiveEntry{LastResolved: 5, Seq: 9}, opPut},
		{"never regress", ActiveEntry{LastResolved: 4, Seq: 3}, true, 9, keep(2), ActiveEntry{LastResolved: 4, Seq: 9}, opPut},
		{"delete", ActiveEntry{LastResolved: 4, Seq: 3}, true, 9, drop, ActiveEntry{LastResolved: 4, Seq: 9}, opDelete},
		{"nothing to delete", ActiveEntry{LastResolved: -1}, false, 9, drop, ActiveEntry{LastResolved: -1, Seq: 9}, opSkip},
		{"redelivery", ActiveEntry{LastResolved: 4, Seq: 9}, true, 9, keep(5), ActiveEntry{LastResolved: 4, Seq: 9}, opSkip},
		{"older event", ActiveEntry{LastResolved: 4, Seq: 9}, true, 7, keep(5), ActiveEntry{LastResolved: 4, Seq: 9}, opSkip},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, o := step(c.e, c.exists, c.seq, c.f)
			if got != c.want || o != c.wantOp {
				t.Errorf("step = %+v, %v; want %+v, %v", got, o, c.want, c.wantOp)
			}
		})
	}
}

func TestActiveSectorsProjection(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	active, err := NewActiveSectors(ctx, js, natstest.Logger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	natstest.Run(t, NewLoop(js, natstest.Logger(), active))

	waitFor := func(want map[string]int64) {
		t.Helper()
		natstest.Eventually(t, 5*time.Second, func() bool {
			got, err := active.ActiveSectors(ctx)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			return maps.Equal(got, want)
		})
	}

	natstest.Publish(t, js, streams.SectorSubject("s1"), sector.TickResolved{Type: "TickResolved", Sector: "s1", Tick: 0, Pending: true})
	waitFor(map[string]int64{"s1": 0})

	natstest.Publish(t, js, streams.AvatarSubject("a1"), map[string]string{"type": "DoctrineUpdated", "avatar": "a1", "sector": "s9"})
	waitFor(map[string]int64{"s1": 0, "s9": -1})

	natstest.Publish(t, js, streams.PlanSubject("a1"), map[string]string{"type": "PlanRevised", "avatar": "a1"})
	natstest.Publish(t, js, streams.SectorSubject("s1"), sector.TickResolved{Type: "TickResolved", Sector: "s1", Tick: 1, Pending: true})
	waitFor(map[string]int64{"s1": 1, "s9": -1})

	natstest.Publish(t, js, streams.SectorSubject("s1"), sector.TickResolved{Type: "TickResolved", Sector: "s1", Tick: 2, Pending: false})
	waitFor(map[string]int64{"s9": -1})

	natstest.Publish(t, js, streams.SectorSubject("s1"), sector.TickResolved{Type: "TickResolved", Sector: "s1", Tick: 3, Pending: true})
	waitFor(map[string]int64{"s1": 3, "s9": -1})

	cons, err := js.Consumer(ctx, streams.StreamEvents, "proj-active-sectors")
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	if info.NumPending != 0 || info.AckFloor.Consumer != 5 {
		t.Errorf("consumer pending %d, acked %d; want 0 pending and 5 acked (plan event filtered out)",
			info.NumPending, info.AckFloor.Consumer)
	}
}
