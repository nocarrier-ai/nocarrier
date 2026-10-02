package clock

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type recorder struct {
	mu    sync.Mutex
	ticks []int64
}

func (r *recorder) Dispatch(_ context.Context, tick int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ticks = append(r.ticks, tick)
	return nil
}

func (r *recorder) dispatched() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.ticks)
}

func bigBang(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	u := UniverseCreated{Type: "UniverseCreated", TickPeriod: MinTickPeriod, Seed: 1}
	if _, err := streams.Append(natstest.Context(t), js, streams.SubjectClock, u, 0); err != nil {
		t.Fatalf("universe: %v", err)
	}
}

func newPacer(t *testing.T, js jetstream.JetStream, id string, d Dispatcher) *Pacer {
	t.Helper()
	p, err := NewPacer(js, natstest.Logger(), id, MinTickPeriod, d)
	if err != nil {
		t.Fatalf("pacer: %v", err)
	}
	head, err := p.readHead(natstest.Context(t))
	if err != nil {
		t.Fatalf("read head: %v", err)
	}
	p.observe(head)
	return p
}

func TestReadHeadBeforeFirstTick(t *testing.T) {
	js := natstest.Start(t)
	bigBang(t, js)

	p := newPacer(t, js, "a", &recorder{})
	if p.lastTick != -1 || p.lastSeq != 1 || p.isDriver {
		t.Errorf("observed tick %d seq %d driver %v; want -1, 1, false", p.lastTick, p.lastSeq, p.isDriver)
	}
}

func TestReadHeadWithoutUniverse(t *testing.T) {
	js := natstest.Start(t)
	p, err := NewPacer(js, natstest.Logger(), "a", MinTickPeriod, &recorder{})
	if err != nil {
		t.Fatalf("pacer: %v", err)
	}
	if _, err := p.readHead(natstest.Context(t)); err == nil {
		t.Fatal("readHead succeeded on an empty clock")
	}
}

func TestAttemptAdvancesOncePerTick(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	bigBang(t, js)

	ra, rb := &recorder{}, &recorder{}
	a, b := newPacer(t, js, "a", ra), newPacer(t, js, "b", rb)

	steps := []struct {
		p          *Pacer
		wantTick   int64
		wantDriver bool
	}{
		{a, 0, true},
		{b, 0, false},
		{b, 1, true},
		{a, 1, false},
	}
	for i, s := range steps {
		if err := s.p.attempt(ctx); err != nil {
			t.Fatalf("step %d: attempt: %v", i, err)
		}
		if s.p.lastTick != s.wantTick || s.p.isDriver != s.wantDriver {
			t.Fatalf("step %d: %s at tick %d driver %v; want %d, %v",
				i, s.p.instanceID, s.p.lastTick, s.p.isDriver, s.wantTick, s.wantDriver)
		}
	}

	if got := ra.dispatched(); !slices.Equal(got, []int64{0}) {
		t.Errorf("a dispatched %v, want [0]", got)
	}
	if got := rb.dispatched(); !slices.Equal(got, []int64{1}) {
		t.Errorf("b dispatched %v, want [1]", got)
	}
	if got := natstest.SubjectCounts(t, js, streams.StreamClock, streams.SubjectClock); got[streams.SubjectClock] != 3 {
		t.Errorf("clock events %d, want 3", got[streams.SubjectClock])
	}
	var head TickAdvanced
	natstest.Last(t, js, streams.StreamClock, streams.SubjectClock, &head)
	if head.Tick != 1 || head.WinnerInstanceID != "b" {
		t.Errorf("head = %+v, want tick 1 won by b", head)
	}
}

func TestArmDuration(t *testing.T) {
	p := &Pacer{period: MinTickPeriod, isDriver: true}
	if got := p.armDuration(); got != MinTickPeriod {
		t.Errorf("driver armed for %s, want %s", got, MinTickPeriod)
	}

	p.isDriver = false
	for range 100 {
		got := p.armDuration()
		if got < MinTickPeriod+graceInterval || got >= MinTickPeriod+graceInterval+maxJitter {
			t.Fatalf("standby armed for %s, want within [%s, %s)", got,
				MinTickPeriod+graceInterval, MinTickPeriod+graceInterval+maxJitter)
		}
	}
}

func TestCurrentTick(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)

	if _, err := CurrentTick(ctx, js); err == nil {
		t.Fatal("CurrentTick succeeded on an empty clock")
	}

	bigBang(t, js)
	if tick, err := CurrentTick(ctx, js); err != nil || tick != -1 {
		t.Fatalf("before first tick = %d, %v; want -1", tick, err)
	}

	p := newPacer(t, js, "a", &recorder{})
	for want := range int64(3) {
		if err := p.attempt(ctx); err != nil {
			t.Fatalf("attempt: %v", err)
		}
		if tick, err := CurrentTick(ctx, js); err != nil || tick != want {
			t.Fatalf("after tick %d = %d, %v", want, tick, err)
		}
	}
}
