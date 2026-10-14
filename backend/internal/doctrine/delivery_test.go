package doctrine_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/doctrine"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/projector"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type deliveryFixture struct {
	js       jetstream.JetStream
	clockSeq uint64
}

func startDelivery(t *testing.T) *deliveryFixture {
	t.Helper()
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	seq, err := streams.Append(ctx, js, streams.SubjectClock,
		clock.UniverseCreated{Type: clock.UniverseCreatedType}, 0)
	if err != nil {
		t.Fatalf("universe: %v", err)
	}
	natstest.Run(t, projector.NewLoop(js, natstest.Logger(), doctrine.NewDelivery(js)))
	return &deliveryFixture{js: js, clockSeq: seq}
}

func (f *deliveryFixture) advance(t *testing.T, tick int64) {
	t.Helper()
	seq, err := streams.Append(natstest.Context(t), f.js, streams.SubjectClock,
		clock.TickAdvanced{Type: clock.TickAdvancedType, Tick: tick}, f.clockSeq)
	if err != nil {
		t.Fatalf("advance to %d: %v", tick, err)
	}
	f.clockSeq = seq
}

func (f *deliveryFixture) update(t *testing.T, avatarID string, tick int64, orders ...string) doctrine.DoctrineUpdated {
	t.Helper()
	ev := doctrine.DoctrineUpdated{
		Type:         doctrine.DoctrineUpdatedType,
		AvatarID:     avatarID,
		Revision:     1,
		Tick:         tick,
		DoctrineData: doctrine.DoctrineData{Orders: orders},
	}
	natstest.Publish(t, f.js, streams.DoctrineSubject(avatarID), ev)
	return ev
}

func (f *deliveryFixture) last(t *testing.T, avatarID string) doctrine.DoctrineUpdated {
	t.Helper()
	var last doctrine.DoctrineUpdated
	natstest.Last(t, f.js, streams.StreamEvents, streams.DoctrineSubject(avatarID), &last)
	return last
}

func (f *deliveryFixture) waitForDelivery(t *testing.T, avatarID string, tick int64) doctrine.DoctrineUpdated {
	t.Helper()
	var last doctrine.DoctrineUpdated
	natstest.Eventually(t, 5*time.Second, func() bool {
		last = f.last(t, avatarID)
		return last.Type == doctrine.DoctrineDeliveredType && last.Tick == tick
	})
	return last
}

func (f *deliveryFixture) eventCount(t *testing.T, avatarID string) uint64 {
	t.Helper()
	return natstest.SubjectCounts(t, f.js, streams.StreamEvents,
		streams.DoctrineSubject(avatarID))[streams.DoctrineSubject(avatarID)]
}

func TestDeliveryArrivesOnALaterTick(t *testing.T) {
	f := startDelivery(t)
	f.update(t, "a1", 0, "trade first")

	f.advance(t, 1)
	got := f.waitForDelivery(t, "a1", 1)
	if got.Revision != 1 || got.Orders[0] != "trade first" {
		t.Errorf("delivered = %+v", got)
	}

	f.advance(t, 2)
	f.advance(t, 3)
	natstest.Eventually(t, 5*time.Second, func() bool { return f.deliveryIdle(t) })
	if n := f.eventCount(t, "a1"); n != 2 {
		t.Errorf("doctrine.a1 has %d events, want update + one delivery", n)
	}
}

func TestDeliveryWaitsForTheNextTick(t *testing.T) {
	f := startDelivery(t)
	f.advance(t, 1)
	f.update(t, "a1", 1, "trade first")
	f.advance(t, 2)

	got := f.waitForDelivery(t, "a1", 2)
	if got.Tick != 2 {
		t.Errorf("delivered at tick %d, want 2", got.Tick)
	}
	if n := f.eventCount(t, "a1"); n != 2 {
		t.Errorf("doctrine.a1 has %d events", n)
	}
}

func (f *deliveryFixture) deliveryIdle(t *testing.T) bool {
	t.Helper()
	ctx := natstest.Context(t)
	cons, err := f.js.Consumer(ctx, streams.StreamClock, "proj-doctrine-delivery")
	if err != nil {
		return false
	}
	info, err := cons.Info(ctx)
	return err == nil && info.NumPending == 0 && info.NumAckPending == 0
}
