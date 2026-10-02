package dispatch_test

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/nocarrier-ai/nocarrier/internal/decide"
	"github.com/nocarrier-ai/nocarrier/internal/dispatch"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/sector"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type readModels struct {
	active map[string]int64
	due    []string
	err    error
}

func (r readModels) ActiveSectors(context.Context) (map[string]int64, error) {
	return r.active, r.err
}

func (r readModels) DueAvatars(context.Context, int64) ([]string, error) {
	return r.due, nil
}

func TestDispatchPublishesUnresolvedRangeAndDueAvatars(t *testing.T) {
	js := natstest.Start(t)
	d := dispatch.New(js, readModels{
		active: map[string]int64{"s1": 2, "s2": 4, "s3": -1},
		due:    []string{"a1"},
	}, natstest.Logger())

	if err := d.Dispatch(natstest.Context(t), 5); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	wantExec := map[string]uint64{"execute.s1": 3, "execute.s2": 1, "execute.s3": 6}
	if got := natstest.SubjectCounts(t, js, streams.StreamExecute, "execute.>"); !maps.Equal(got, wantExec) {
		t.Errorf("execute counts %v, want %v", got, wantExec)
	}
	wantDecide := map[string]uint64{"decide.a1": 1}
	if got := natstest.SubjectCounts(t, js, streams.StreamDecide, "decide.>"); !maps.Equal(got, wantDecide) {
		t.Errorf("decide counts %v, want %v", got, wantDecide)
	}

	var exec sector.ExecuteTick
	if !natstest.Last(t, js, streams.StreamExecute, "execute.s1", &exec) || exec != (sector.ExecuteTick{Sector: "s1", Tick: 5}) {
		t.Errorf("last execute.s1 = %+v", exec)
	}
	var dec decide.DecideNow
	if !natstest.Last(t, js, streams.StreamDecide, "decide.a1", &dec) || dec != (decide.DecideNow{Avatar: "a1", Tick: 5}) {
		t.Errorf("last decide.a1 = %+v", dec)
	}
}

func TestRedispatchIsDeduplicated(t *testing.T) {
	js := natstest.Start(t)
	d := dispatch.New(js, readModels{
		active: map[string]int64{"s1": 2},
		due:    []string{"a1"},
	}, natstest.Logger())

	for range 2 {
		if err := d.Dispatch(natstest.Context(t), 5); err != nil {
			t.Fatalf("dispatch: %v", err)
		}
	}

	if got := natstest.SubjectCounts(t, js, streams.StreamExecute, "execute.>"); got["execute.s1"] != 3 {
		t.Errorf("execute.s1 count %d, want 3", got["execute.s1"])
	}
	if got := natstest.SubjectCounts(t, js, streams.StreamDecide, "decide.>"); got["decide.a1"] != 1 {
		t.Errorf("decide.a1 count %d, want 1", got["decide.a1"])
	}
}

func TestDispatchFailsWhenReadModelFails(t *testing.T) {
	js := natstest.Start(t)
	boom := errors.New("boom")
	d := dispatch.New(js, readModels{err: boom}, natstest.Logger())

	if err := d.Dispatch(natstest.Context(t), 5); !errors.Is(err, boom) {
		t.Fatalf("dispatch err = %v, want %v", err, boom)
	}
}
