package doctrine

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

func startUniverse(t *testing.T) jetstream.JetStream {
	t.Helper()
	js := natstest.Start(t)
	u := clock.UniverseCreated{Type: "UniverseCreated", TickPeriod: clock.MinTickPeriod}
	if _, err := streams.Append(natstest.Context(t), js, streams.SubjectClock, u, 0); err != nil {
		t.Fatalf("universe: %v", err)
	}
	return js
}

func advance(t *testing.T, js jetstream.JetStream, tick int64, expected uint64) {
	t.Helper()
	ev := clock.TickAdvanced{Type: "TickAdvanced", Tick: tick, WinnerInstanceID: "test"}
	if _, err := streams.Append(natstest.Context(t), js, streams.SubjectClock, ev, expected); err != nil {
		t.Fatalf("advance to %d: %v", tick, err)
	}
}

func TestUpdateStartsAtRevisionOne(t *testing.T) {
	js := startUniverse(t)
	ev, err := NewHandler(js).Update(natstest.Context(t), command("a1", []string{"trade first"}, nil))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if ev.Type != "DoctrineUpdated" || ev.AvatarID != "a1" || ev.Revision != 1 || ev.Tick != -1 {
		t.Errorf("event = %+v", ev)
	}
	if ev.Hash != ev.hash() || len(ev.Hash) != 64 {
		t.Errorf("hash %q does not match content", ev.Hash)
	}

	var stored DoctrineUpdated
	if !natstest.Last(t, js, streams.StreamEvents, streams.DoctrineSubject("a1"), &stored) {
		t.Fatal("nothing appended")
	}
	if stored.Revision != 1 || stored.Hash != ev.Hash || stored.Orders[0] != "trade first" {
		t.Errorf("stored = %+v", stored)
	}
}

func TestUpdateIncrementsRevisionAndStampsTick(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js)

	if _, err := h.Update(ctx, command("a1", []string{"trade first"}, nil)); err != nil {
		t.Fatalf("first: %v", err)
	}
	advance(t, js, 0, 1)
	advance(t, js, 1, 2)

	ev, err := h.Update(ctx, command("a1", []string{"fight only when cornered"}, map[string]string{HookAttacked: "run"}))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if ev.Revision != 2 || ev.Tick != 1 {
		t.Errorf("second = revision %d tick %d; want 2, 1", ev.Revision, ev.Tick)
	}
}

func TestUpdateUnchangedReturnsExistingRevision(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js)

	first, err := h.Update(ctx, command("a1", []string{"trade first"}, map[string]string{HookMoved: "scan"}))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	again, err := h.Update(ctx, command("a1", []string{"trade first "}, map[string]string{HookMoved: "scan"}))
	if err != nil {
		t.Fatalf("again: %v", err)
	}
	if again.Revision != first.Revision || again.Hash != first.Hash {
		t.Errorf("again = revision %d; want existing revision %d", again.Revision, first.Revision)
	}
	if n := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.DoctrineEvents)["doctrine.a1"]; n != 1 {
		t.Errorf("doctrine.a1 has %d events, want 1", n)
	}
}

func TestUpdateKeepsAvatarsSeparate(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js)

	a1, err := h.Update(ctx, command("a1", []string{"trade first"}, nil))
	if err != nil {
		t.Fatalf("a1: %v", err)
	}
	a2, err := h.Update(ctx, command("a2", []string{"trade first"}, nil))
	if err != nil {
		t.Fatalf("a2: %v", err)
	}
	if a2.Revision != 1 || a2.Hash != a1.Hash {
		t.Errorf("a2 = revision %d hash %s; want revision 1 with a1's hash", a2.Revision, a2.Hash)
	}
}

func TestUpdateRejectsInvalidWithoutAppending(t *testing.T) {
	js := startUniverse(t)
	_, err := NewHandler(js).Update(natstest.Context(t), command("a1", nil, nil))
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if got := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.DoctrineEvents); len(got) != 0 {
		t.Errorf("appended %v", got)
	}
}

func TestUpdateFailsWithoutUniverse(t *testing.T) {
	js := natstest.Start(t)
	if _, err := NewHandler(js).Update(natstest.Context(t), command("a1", []string{"x"}, nil)); err == nil {
		t.Fatal("update succeeded with no clock")
	}
}
