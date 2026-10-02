package projector

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/doctrine"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

func doctrineEvent(avatarID string, revision int64, orders ...string) doctrine.DoctrineUpdated {
	return doctrine.DoctrineUpdated{
		Type:         doctrine.DoctrineUpdatedType,
		AvatarID:     avatarID,
		Revision:     revision,
		DoctrineData: doctrine.DoctrineData{Orders: orders},
	}
}

func TestDecodeDoctrineUpdated(t *testing.T) {
	data, err := json.Marshal(doctrineEvent("a1", 3, "trade first"))
	if err != nil {
		t.Fatal(err)
	}
	ev, ok := decodeDoctrineUpdated(data)
	if !ok || ev.AvatarID != "a1" || ev.Revision != 3 || ev.Orders[0] != "trade first" {
		t.Fatalf("decode = %+v, %v", ev, ok)
	}
}

func TestDecodeDoctrineUpdatedSkips(t *testing.T) {
	for name, data := range map[string]string{
		"other type":        `{"type":"PlanRevised","avatar_id":"a1"}`,
		"missing avatar ID": `{"type":"DoctrineUpdated","revision":1}`,
		"bad json":          `{`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := decodeDoctrineUpdated([]byte(data)); ok {
				t.Error("decoded, want skip")
			}
		})
	}
}

type doctrineFixture struct {
	js   jetstream.JetStream
	kv   jetstream.KeyValue
	proj *Doctrine
}

func startDoctrine(t *testing.T) doctrineFixture {
	t.Helper()
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	proj, err := NewDoctrine(ctx, js)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	kv, err := js.KeyValue(ctx, streams.BucketDoctrine)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	natstest.Run(t, NewLoop(js, natstest.Logger(), proj))
	return doctrineFixture{js: js, kv: kv, proj: proj}
}

func (f doctrineFixture) stored(t *testing.T, avatarID string) (doctrine.DoctrineUpdated, bool) {
	t.Helper()
	entry, err := f.kv.Get(natstest.Context(t), avatarID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return doctrine.DoctrineUpdated{}, false
	}
	if err != nil {
		t.Fatalf("get %s: %v", avatarID, err)
	}
	var ev doctrine.DoctrineUpdated
	if err := json.Unmarshal(entry.Value(), &ev); err != nil {
		t.Fatalf("decode %s: %v", avatarID, err)
	}
	return ev, true
}

func (f doctrineFixture) waitForRevision(t *testing.T, avatarID string, revision int64) doctrine.DoctrineUpdated {
	t.Helper()
	var ev doctrine.DoctrineUpdated
	natstest.Eventually(t, 5*time.Second, func() bool {
		var ok bool
		ev, ok = f.stored(t, avatarID)
		return ok && ev.Revision == revision
	})
	return ev
}

func (f doctrineFixture) waitIdle(t *testing.T) {
	t.Helper()
	ctx := natstest.Context(t)
	natstest.Eventually(t, 5*time.Second, func() bool {
		cons, err := f.js.Consumer(ctx, streams.StreamEvents, "proj-doctrine")
		if err != nil {
			return false
		}
		info, err := cons.Info(ctx)
		return err == nil && info.NumPending == 0 && info.NumAckPending == 0
	})
}

func TestDoctrineProjectionKeepsLatestRevision(t *testing.T) {
	f := startDoctrine(t)

	natstest.Publish(t, f.js, streams.DoctrineSubject("a1"), doctrineEvent("a1", 1, "trade first"))
	f.waitForRevision(t, "a1", 1)

	natstest.Publish(t, f.js, streams.DoctrineSubject("a1"), doctrineEvent("a1", 2, "fight only when cornered"))
	got := f.waitForRevision(t, "a1", 2)
	if got.Orders[0] != "fight only when cornered" {
		t.Errorf("stored orders %v", got.Orders)
	}
}

func TestDoctrineProjectionSkipsStaleRevisions(t *testing.T) {
	f := startDoctrine(t)

	natstest.Publish(t, f.js, streams.DoctrineSubject("a1"), doctrineEvent("a1", 2, "newer"))
	f.waitForRevision(t, "a1", 2)
	natstest.Publish(t, f.js, streams.DoctrineSubject("a1"), doctrineEvent("a1", 1, "older"))
	natstest.Publish(t, f.js, streams.DoctrineSubject("a1"), doctrineEvent("a1", 2, "duplicate"))
	f.waitIdle(t)

	got, _ := f.stored(t, "a1")
	if got.Revision != 2 || got.Orders[0] != "newer" {
		t.Errorf("stored = revision %d %v; want revision 2 [newer]", got.Revision, got.Orders)
	}
}

func TestDoctrineProjectionKeysByAvatar(t *testing.T) {
	f := startDoctrine(t)

	natstest.Publish(t, f.js, streams.DoctrineSubject("a1"), doctrineEvent("a1", 3, "a1 orders"))
	natstest.Publish(t, f.js, streams.DoctrineSubject("a2"), doctrineEvent("a2", 1, "a2 orders"))

	if got := f.waitForRevision(t, "a1", 3); got.Orders[0] != "a1 orders" {
		t.Errorf("a1 = %+v", got)
	}
	if got := f.waitForRevision(t, "a2", 1); got.Orders[0] != "a2 orders" {
		t.Errorf("a2 = %+v", got)
	}
}

func TestDoctrineProjectionIgnoresOtherEvents(t *testing.T) {
	f := startDoctrine(t)

	natstest.Publish(t, f.js, streams.DoctrineSubject("a1"), map[string]string{"type": "Unknown", "avatar_id": "a1"})
	natstest.Publish(t, f.js, streams.PlanSubject("a2"), doctrineEvent("a2", 1, "wrong subject"))
	natstest.Publish(t, f.js, streams.DoctrineSubject("a3"), doctrineEvent("a3", 1, "x"))
	f.waitForRevision(t, "a3", 1)
	f.waitIdle(t)

	for _, avatarID := range []string{"a1", "a2"} {
		if _, ok := f.stored(t, avatarID); ok {
			t.Errorf("%s projected", avatarID)
		}
	}
}

func TestDoctrineProjectionFromUpdate(t *testing.T) {
	f := startDoctrine(t)
	ctx := natstest.Context(t)
	if _, err := streams.Append(ctx, f.js, streams.SubjectClock, clock.UniverseCreated{Type: "UniverseCreated"}, 0); err != nil {
		t.Fatalf("universe: %v", err)
	}

	cmd := doctrine.UpdateDoctrine{
		AvatarID: "a1",
		DoctrineData: doctrine.DoctrineData{
			Orders: []string{"trade first"},
			Hooks:  map[string]string{doctrine.HookAttacked: "run"},
		},
	}
	ev, err := doctrine.NewHandler(f.js).Update(ctx, cmd)
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	if got := f.waitForRevision(t, "a1", ev.Revision); !reflect.DeepEqual(got, ev) {
		t.Errorf("stored %+v, want %+v", got, ev)
	}
}
