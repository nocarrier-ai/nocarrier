package sector_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/sector"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type pendingResolver struct{}

func (pendingResolver) Resolve(context.Context, string, int64) (json.RawMessage, bool, error) {
	return json.RawMessage(`[]`), true, nil
}

func startPool(t *testing.T) jetstream.JetStream {
	t.Helper()
	js := natstest.Start(t)
	natstest.Run(t, sector.NewPool(js, natstest.Logger(), 2, pendingResolver{}))
	return js
}

func execute(t *testing.T, js jetstream.JetStream, id string, tick int64) {
	t.Helper()
	natstest.Publish(t, js, streams.ExecuteSubject(id), sector.ExecuteTick{Sector: id, Tick: tick})
}

func resolvedTicks(t *testing.T, js jetstream.JetStream, id string) []int64 {
	t.Helper()
	ctx := natstest.Context(t)
	cons, err := js.OrderedConsumer(ctx, streams.StreamEvents, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{streams.SectorSubject(id)},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	var ticks []int64
	for range info.NumPending {
		msg, err := cons.Next(jetstream.FetchMaxWait(time.Second))
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		var ev sector.TickResolved
		if err := json.Unmarshal(msg.Data(), &ev); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if ev.Type != "TickResolved" || ev.Sector != id {
			t.Fatalf("unexpected event %+v", ev)
		}
		ticks = append(ticks, ev.Tick)
	}
	return ticks
}

func waitDrained(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	natstest.Eventually(t, 5*time.Second, func() bool {
		return len(natstest.SubjectCounts(t, js, streams.StreamExecute, "execute.>")) == 0
	})
}

func TestPoolResolvesTicksInOrder(t *testing.T) {
	js := startPool(t)
	execute(t, js, "s1", 0)
	execute(t, js, "s1", 1)
	waitDrained(t, js)

	if got := resolvedTicks(t, js, "s1"); !slices.Equal(got, []int64{0, 1}) {
		t.Fatalf("resolved %v, want [0 1]", got)
	}
}

func TestPoolIgnoresAlreadyResolvedTick(t *testing.T) {
	js := startPool(t)
	execute(t, js, "s1", 0)
	waitDrained(t, js)
	execute(t, js, "s1", 0)
	waitDrained(t, js)

	if got := resolvedTicks(t, js, "s1"); !slices.Equal(got, []int64{0}) {
		t.Fatalf("resolved %v, want [0]", got)
	}
}

func TestPoolWaitsForPredecessor(t *testing.T) {
	js := startPool(t)
	execute(t, js, "s1", 1)
	execute(t, js, "s1", 0)
	waitDrained(t, js)

	if got := resolvedTicks(t, js, "s1"); !slices.Equal(got, []int64{0, 1}) {
		t.Fatalf("resolved %v, want [0 1]", got)
	}
}

func TestPoolKeepsSectorsIndependent(t *testing.T) {
	js := startPool(t)
	execute(t, js, "s1", 0)
	execute(t, js, "s2", 0)
	execute(t, js, "s2", 1)
	waitDrained(t, js)

	if got := resolvedTicks(t, js, "s1"); !slices.Equal(got, []int64{0}) {
		t.Errorf("s1 resolved %v, want [0]", got)
	}
	if got := resolvedTicks(t, js, "s2"); !slices.Equal(got, []int64{0, 1}) {
		t.Errorf("s2 resolved %v, want [0 1]", got)
	}
}
