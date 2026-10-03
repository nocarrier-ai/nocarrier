package projector

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/nocarrier-ai/nocarrier/internal/decide"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

func planRevised(avatarID string, tick int64) decide.PlanRevised {
	return decide.PlanRevised{
		Type:     decide.PlanRevisedType,
		AvatarID: avatarID,
		Tick:     tick,
		Intents:  json.RawMessage(`[]`),
	}
}

func TestRouteDue(t *testing.T) {
	cases := []struct {
		name     string
		data     any
		exists   bool
		e        DueEntry
		wantID   string
		wantWrit bool
		want     DueEntry
	}{
		{
			name: "commission schedules on its own tick",
			data: commissioned("a1", 4), wantID: "a1", wantWrit: true,
			want: DueEntry{NextTick: 4},
		},
		{
			name: "commission never resets an existing schedule",
			data: commissioned("a1", 4), exists: true,
			e:      DueEntry{NextTick: 40, Seq: 9},
			wantID: "a1", wantWrit: false, want: DueEntry{NextTick: 40, Seq: 9},
		},
		{
			name: "decision pushes the schedule out by the cadence",
			data: planRevised("a1", 10), exists: true,
			e:      DueEntry{NextTick: 10},
			wantID: "a1", wantWrit: true, want: DueEntry{NextTick: 10 + cadence},
		},
		{
			name: "decision for an uncommissioned avatar schedules nothing",
			data: planRevised("a1", 10), exists: false,
			wantID: "a1", wantWrit: false,
		},
		{
			name: "schedule never regresses",
			data: planRevised("a1", 2), exists: true,
			e:      DueEntry{NextTick: 50},
			wantID: "a1", wantWrit: true, want: DueEntry{NextTick: 50},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data, err := json.Marshal(c.data)
			if err != nil {
				t.Fatal(err)
			}
			avatarID, f := routeDue(data)
			if f == nil {
				t.Fatal("not routed")
			}
			if avatarID != c.wantID {
				t.Errorf("avatar ID %q, want %q", avatarID, c.wantID)
			}
			e := c.e
			if write := f(&e, c.exists); write != c.wantWrit {
				t.Errorf("write %v, want %v", write, c.wantWrit)
			}
			if c.wantWrit && e != c.want {
				t.Errorf("entry = %+v, want %+v", e, c.want)
			}
		})
	}
}

func TestRouteDueSkips(t *testing.T) {
	for name, data := range map[string]string{
		"tick resolved":    `{"type":"TickResolved","sector_id":"s1","tick":4}`,
		"doctrine updated": `{"type":"DoctrineUpdated","avatar_id":"a1","revision":1}`,
		"plan without ID":  `{"type":"PlanRevised","tick":4}`,
		"commission no ID": `{"type":"AdmiralCommissioned","home_sector":"0"}`,
		"unknown type":     `{"type":"Nope"}`,
		"bad json":         `{`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, f := routeDue([]byte(data)); f != nil {
				t.Error("routed, want skip")
			}
		})
	}
}

func TestDueAvatarsProjection(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	due, err := NewDueAvatars(ctx, js)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	natstest.Run(t, NewLoop(js, natstest.Logger(), due))

	waitFor := func(tick int64, want []string) {
		t.Helper()
		natstest.Eventually(t, 5*time.Second, func() bool {
			got, err := due.DueAvatars(ctx, tick)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			return slices.Equal(got, want)
		})
	}

	// A commissioned admiral is due immediately: it has no plan yet.
	natstest.Publish(t, js, streams.AvatarSubject("a1"), commissioned("a1", 4))
	waitFor(4, []string{"a1"})

	// ...and not due before the tick it was commissioned on.
	if got, err := due.DueAvatars(ctx, 3); err != nil || len(got) != 0 {
		t.Errorf("due at tick 3 = %v, err %v; want none", got, err)
	}

	// A decision pushes it out by one tick, the default cadence.
	natstest.Publish(t, js, streams.PlanSubject("a1"), planRevised("a1", 4))
	waitFor(4, nil)
	waitFor(5, []string{"a1"})

	// Sorted, so every instance fans a tick out in the same order.
	natstest.Publish(t, js, streams.AvatarSubject("a3"), commissioned("a3", 5))
	natstest.Publish(t, js, streams.AvatarSubject("a2"), commissioned("a2", 5))
	waitFor(5, []string{"a1", "a2", "a3"})

	// Events this projection does not fold leave the schedule alone.
	natstest.Publish(t, js, streams.DoctrineSubject("a1"), map[string]any{"type": "DoctrineUpdated", "avatar_id": "a1", "revision": 1})
	natstest.Publish(t, js, streams.PlanSubject("a2"), planRevised("a2", 5))
	waitFor(5, []string{"a1", "a3"})
}

// A decision for an avatar that was never commissioned must not invent a
// schedule: the dispatcher would then decide for an admiral who does not exist.
func TestDueAvatarsIgnoresUncommissionedPlans(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	due, err := NewDueAvatars(ctx, js)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	natstest.Run(t, NewLoop(js, natstest.Logger(), due))

	natstest.Publish(t, js, streams.PlanSubject("ghost"), planRevised("ghost", 1))
	natstest.Publish(t, js, streams.AvatarSubject("a1"), commissioned("a1", 1))

	// a1 landing proves the ghost's event was processed and dropped.
	natstest.Eventually(t, 5*time.Second, func() bool {
		got, err := due.DueAvatars(ctx, 1)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return slices.Equal(got, []string{"a1"})
	})
}

func TestDueAvatarsEmptyBucket(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	due, err := NewDueAvatars(ctx, js)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	got, err := due.DueAvatars(ctx, 99)
	if err != nil || len(got) != 0 {
		t.Errorf("due = %v, err = %v; want none", got, err)
	}
}
