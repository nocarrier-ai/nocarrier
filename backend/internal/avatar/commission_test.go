package avatar

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

func TestCommissionAppendsStartingState(t *testing.T) {
	js := startUniverse(t)
	advance(t, js, 0, 1)
	advance(t, js, 1, 2)

	ev, err := NewHandler(js).Commission(natstest.Context(t), command("a1", "Akbar", "USS Cheesewheel"))
	if err != nil {
		t.Fatalf("commission: %v", err)
	}
	want := AdmiralCommissioned{
		AvatarID:   "a1",
		Name:       "Akbar",
		ShipName:   "USS Cheesewheel",
		HomeSector: StartingSector,
		Tick:       1,
		Type:       AdmiralCommissionedType,
	}
	if ev != want {
		t.Errorf("event = %+v, want %+v", ev, want)
	}

	var stored AdmiralCommissioned
	if !natstest.Last(t, js, streams.StreamEvents, streams.AvatarSubject("a1"), &stored) {
		t.Fatal("nothing appended")
	}
	if stored != want {
		t.Errorf("stored = %+v, want %+v", stored, want)
	}
}

// Commissioning in the window between UniverseCreated and the first
// TickAdvanced must not stamp CurrentTick's -1: that tick activates the home
// sector, and a negative one replays every tick from 0.
func TestCommissionBeforeFirstTickStampsZero(t *testing.T) {
	js := startUniverse(t)
	ev, err := NewHandler(js).Commission(natstest.Context(t), command("a1", "Akbar", "Cheesewheel"))
	if err != nil {
		t.Fatalf("commission: %v", err)
	}
	if ev.Tick != 0 {
		t.Errorf("tick = %d, want 0", ev.Tick)
	}
}

func TestCommissionTrimsNames(t *testing.T) {
	js := startUniverse(t)
	ev, err := NewHandler(js).Commission(natstest.Context(t), command("a1", "  Akbar\t", " USS Cheesewheel "))
	if err != nil {
		t.Fatalf("commission: %v", err)
	}
	if ev.Name != "Akbar" || ev.ShipName != "USS Cheesewheel" {
		t.Errorf("event name %q, ship %q", ev.Name, ev.ShipName)
	}
}

// A name that is only whitespace is empty once trimmed, so it must be
// rejected rather than stored blank.
func TestCommissionRejectsBlankNames(t *testing.T) {
	js := startUniverse(t)
	_, err := NewHandler(js).Commission(natstest.Context(t), command("a1", "   ", "Cheesewheel"))
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
}

func TestCommissionIsOncePerAvatar(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js)

	if _, err := h.Commission(ctx, command("a1", "Akbar", "Cheesewheel")); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := h.Commission(ctx, command("a1", "Imposter", "Dial Tone")); !errors.Is(err, ErrAlreadyCommissioned) {
		t.Fatalf("second err = %v, want ErrAlreadyCommissioned", err)
	}
	if n := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.AvatarEvents)["avatar.a1"]; n != 1 {
		t.Errorf("avatar.a1 has %d events, want 1", n)
	}

	var stored AdmiralCommissioned
	natstest.Last(t, js, streams.StreamEvents, streams.AvatarSubject("a1"), &stored)
	if stored.Name != "Akbar" {
		t.Errorf("stored name %q; the rejected commission overwrote the original", stored.Name)
	}
}

// ErrAlreadyCommissioned is a permanent rejection, not the "resubmit" that a
// raw ErrConflict means elsewhere, so it must not surface as one.
func TestCommissionConflictIsNotRetryable(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js)

	if _, err := h.Commission(ctx, command("a1", "Akbar", "Cheesewheel")); err != nil {
		t.Fatalf("first: %v", err)
	}
	_, err := h.Commission(ctx, command("a1", "Akbar", "Cheesewheel"))
	if errors.Is(err, streams.ErrConflict) {
		t.Errorf("err = %v, want it not to read as a lost race", err)
	}
}

func TestCommissionKeepsAvatarsSeparate(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js)

	if _, err := h.Commission(ctx, command("a1", "Akbar", "Cheesewheel")); err != nil {
		t.Fatalf("a1: %v", err)
	}
	a2, err := h.Commission(ctx, command("a2", "Ackbar", "Dial Tone"))
	if err != nil {
		t.Fatalf("a2: %v", err)
	}
	if a2.AvatarID != "a2" || a2.HomeSector != StartingSector {
		t.Errorf("a2 = %+v", a2)
	}
	counts := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.AvatarEvents)
	if counts["avatar.a1"] != 1 || counts["avatar.a2"] != 1 {
		t.Errorf("subject counts = %v", counts)
	}
}

func TestCommissionRejectsInvalidWithoutAppending(t *testing.T) {
	js := startUniverse(t)
	_, err := NewHandler(js).Commission(natstest.Context(t), command("a.1", "Akbar", "Cheesewheel"))
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if got := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.AvatarEvents); len(got) != 0 {
		t.Errorf("appended %v", got)
	}
}

func TestCommissionFailsWithoutUniverse(t *testing.T) {
	js := natstest.Start(t)
	if _, err := NewHandler(js).Commission(natstest.Context(t), command("a1", "Akbar", "Cheesewheel")); err == nil {
		t.Fatal("commission succeeded with no clock")
	}
}
