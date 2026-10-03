package projector

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/avatar"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

func commissioned(avatarID string, tick int64) avatar.AdmiralCommissioned {
	return avatar.AdmiralCommissioned{
		AvatarID:   avatarID,
		Name:       "Akbar",
		ShipName:   "USS Cheesewheel",
		HomeSector: avatar.StartingSector,
		Tick:       tick,
		Type:       avatar.AdmiralCommissionedType,
	}
}

func TestDecodeAdmiralCommissioned(t *testing.T) {
	data, err := json.Marshal(commissioned("a1", 7))
	if err != nil {
		t.Fatal(err)
	}
	ev, ok := decodeAdmiralCommissioned(data)
	if !ok || ev.AvatarID != "a1" || ev.Tick != 7 || ev.HomeSector != avatar.StartingSector {
		t.Fatalf("decode = %+v, %v", ev, ok)
	}
}

func TestDecodeAdmiralCommissionedSkips(t *testing.T) {
	for name, data := range map[string]string{
		"other type":          `{"type":"PlanRevised","avatar_id":"a1"}`,
		"doctrine":            `{"type":"DoctrineUpdated","avatar_id":"a1","revision":1}`,
		"missing avatar ID":   `{"type":"AdmiralCommissioned","home_sector":"0"}`,
		"missing home sector": `{"type":"AdmiralCommissioned","avatar_id":"a1"}`,
		"bad json":            `{`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := decodeAdmiralCommissioned([]byte(data)); ok {
				t.Error("decoded, want skip")
			}
		})
	}
}

func TestAvatarStatusProjection(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	proj, err := NewAvatarStatus(ctx, js)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	natstest.Run(t, NewLoop(js, natstest.Logger(), proj))

	waitFor := func(avatarID string, want StatusEntry) StatusEntry {
		t.Helper()
		var got StatusEntry
		natstest.Eventually(t, 5*time.Second, func() bool {
			e, ok, err := proj.Status(ctx, avatarID)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			got = e
			want.Seq = e.Seq
			return ok && e == want
		})
		return got
	}

	natstest.Publish(t, js, streams.AvatarSubject("a1"), commissioned("a1", 4))
	got := waitFor("a1", StatusEntry{
		AvatarID:         "a1",
		Name:             "Akbar",
		ShipName:         "USS Cheesewheel",
		Sector:           avatar.StartingSector,
		CommissionedTick: 4,
	})
	if got.Seq == 0 {
		t.Error("entry recorded no stream sequence")
	}

	// Events this projection does not fold must leave the entry alone.
	natstest.Publish(t, js, streams.AvatarSubject("a1"), map[string]any{"type": "Nope", "avatar_id": "a1"})
	natstest.Publish(t, js, streams.AvatarSubject("a2"), commissioned("a2", 9))
	waitFor("a2", StatusEntry{
		AvatarID:         "a2",
		Name:             "Akbar",
		ShipName:         "USS Cheesewheel",
		Sector:           avatar.StartingSector,
		CommissionedTick: 9,
	})

	if again, _, err := proj.Status(ctx, "a1"); err != nil || again != got {
		t.Errorf("a1 = %+v, want unchanged %+v (err %v)", again, got, err)
	}
}

func TestAvatarStatusMissingAvatar(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	proj, err := NewAvatarStatus(ctx, js)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, found, err := proj.Status(ctx, "nobody"); err != nil || found {
		t.Errorf("found = %v, err = %v; want not found", found, err)
	}
}

// A replay delivers the same event at the same stream sequence, which the
// stored Seq has to skip rather than rewrite.
func TestAvatarStatusSkipsRedelivery(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	proj, err := NewAvatarStatus(ctx, js)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	kv, err := js.KeyValue(ctx, streams.BucketAvatarStatus)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	natstest.Run(t, NewLoop(js, natstest.Logger(), proj))

	natstest.Publish(t, js, streams.AvatarSubject("a1"), commissioned("a1", 4))
	natstest.Eventually(t, 5*time.Second, func() bool {
		_, found, err := proj.Status(ctx, "a1")
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return found
	})
	entry, err := kv.Get(ctx, "a1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	// Replay the stream through a fresh Apply: same sequence, so no write.
	cons, err := js.OrderedConsumer(ctx, streams.StreamEvents, jetstream.OrderedConsumerConfig{
		FilterSubjects: proj.FilterSubjects(),
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	msg, err := cons.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if err := proj.Apply(ctx, msg); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	after, err := kv.Get(ctx, "a1")
	if err != nil {
		t.Fatalf("get after: %v", err)
	}
	if after.Revision() != entry.Revision() {
		t.Errorf("revision %d, want unchanged %d", after.Revision(), entry.Revision())
	}
}
