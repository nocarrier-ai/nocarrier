package sector

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

// blockingResolver hangs until its context is cancelled, which only happens if
// the handler bounded it.
type blockingResolver struct {
	entered chan struct{}
}

func (b blockingResolver) Resolve(ctx context.Context, _ string, _ int64) (json.RawMessage, bool, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, false, ctx.Err()
}

// A resolver that hangs must lose its command rather than hold it. Holding it
// is the only way two commands for one sector are ever in flight at once, and
// an idle sector resolves whichever of those it is given first.
func TestResolveTimeoutNaksWithoutAppending(t *testing.T) {
	resolveTimeout = 100 * time.Millisecond
	t.Cleanup(func() { resolveTimeout = 5 * time.Second })

	js := natstest.Start(t)
	ctx := natstest.Context(t)
	r := blockingResolver{entered: make(chan struct{}, 1)}
	natstest.Run(t, NewPool(js, natstest.Logger(), 1, r))

	natstest.Publish(t, js, streams.ExecuteSubject("s1"), ExecuteTick{SectorID: "s1", Tick: 0})

	// The resolve returning at all proves the handler gave it a deadline: with
	// an unbounded context this blocks until the test times out.
	select {
	case <-r.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("resolver never ran")
	}

	// The tick is not resolved, and the command comes back for another attempt.
	cons, err := js.Consumer(ctx, streams.StreamExecute, "execute")
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	natstest.Eventually(t, 10*time.Second, func() bool {
		info, err := cons.Info(ctx)
		if err != nil {
			t.Fatalf("consumer info: %v", err)
		}
		return info.NumRedelivered > 0
	})

	var ev TickResolved
	if natstest.Last(t, js, streams.StreamEvents, streams.SectorSubject("s1"), &ev) {
		t.Errorf("appended %+v after the resolve timed out", ev)
	}
}

// The bound has to leave room for a real resolve and stay clear of both the
// tick floor and the ack deadline.
func TestResolveTimeoutIsInsideTheTickFloor(t *testing.T) {
	if resolveTimeout >= ackWait {
		t.Errorf("resolveTimeout %s is not inside AckWait %s", resolveTimeout, ackWait)
	}
	if resolveTimeout*2 > clock.MinTickPeriod {
		t.Errorf("resolveTimeout %s leaves no headroom inside the %s tick floor", resolveTimeout, clock.MinTickPeriod)
	}
}
