package streams_test

import (
	"errors"
	"testing"

	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type event struct {
	Type string `json:"type"`
	Tick int64  `json:"tick"`
}

func TestLastOnEmptySubject(t *testing.T) {
	js := natstest.Start(t)
	ev := event{Type: "untouched"}

	seq, err := streams.Last(natstest.Context(t), js, streams.StreamEvents, streams.SectorSubject("s1"), &ev)
	if err != nil || seq != 0 {
		t.Fatalf("Last = %d, %v; want 0, nil", seq, err)
	}
	if ev.Type != "untouched" {
		t.Errorf("value modified on empty subject: %+v", ev)
	}
}

func TestAppendThenLast(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	subject := streams.SectorSubject("s1")

	first, err := streams.Append(ctx, js, subject, event{Type: "TickResolved", Tick: 0}, 0)
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	second, err := streams.Append(ctx, js, subject, event{Type: "TickResolved", Tick: 1}, first)
	if err != nil {
		t.Fatalf("second append: %v", err)
	}

	var ev event
	seq, err := streams.Last(ctx, js, streams.StreamEvents, subject, &ev)
	if err != nil {
		t.Fatalf("last: %v", err)
	}
	if seq != second || ev != (event{Type: "TickResolved", Tick: 1}) {
		t.Errorf("Last = %d %+v; want %d tick 1", seq, ev, second)
	}
}

func TestAppendConflicts(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	subject := streams.SectorSubject("s1")

	seq, err := streams.Append(ctx, js, subject, event{Tick: 0}, 0)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	for name, expected := range map[string]uint64{"stale": seq - 1, "ahead": seq + 1} {
		t.Run(name, func(t *testing.T) {
			if _, err := streams.Append(ctx, js, subject, event{Tick: 1}, expected); !errors.Is(err, streams.ErrConflict) {
				t.Fatalf("append err = %v, want ErrConflict", err)
			}
		})
	}

	var ev event
	if last, _ := streams.Last(ctx, js, streams.StreamEvents, subject, &ev); last != seq || ev.Tick != 0 {
		t.Errorf("conflicting append landed: last %d %+v", last, ev)
	}
}

func TestGuardIsPerSubject(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)

	if _, err := streams.Append(ctx, js, streams.SectorSubject("s1"), event{}, 0); err != nil {
		t.Fatalf("s1: %v", err)
	}
	if _, err := streams.Append(ctx, js, streams.SectorSubject("s2"), event{}, 0); err != nil {
		t.Fatalf("s2 with expected 0 after s1 append: %v", err)
	}
}

func TestAppendReportsOtherErrors(t *testing.T) {
	js := natstest.Start(t)
	_, err := streams.Append(natstest.Context(t), js, "nostream.s1", event{}, 0)
	if err == nil || errors.Is(err, streams.ErrConflict) {
		t.Fatalf("append err = %v, want a non-conflict error", err)
	}
}

func TestLastReportsDecodeErrors(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	subject := streams.SectorSubject("s1")
	if _, err := js.Publish(ctx, subject, []byte(`not json`)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	var ev event
	if _, err := streams.Last(ctx, js, streams.StreamEvents, subject, &ev); err == nil {
		t.Fatal("Last decoded invalid JSON without error")
	}
}
