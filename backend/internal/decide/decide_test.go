package decide_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nocarrier-ai/nocarrier/internal/decide"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type model struct{}

func (model) Decide(_ context.Context, avatarID string, tick int64) (json.RawMessage, json.RawMessage, error) {
	return json.RawMessage(`[{"intent":"move"}]`), json.RawMessage(`{"avatar_id":"` + avatarID + `"}`), nil
}

func TestDecideAppendsPlanAndDecisionRecord(t *testing.T) {
	js := natstest.Start(t)
	natstest.Run(t, decide.NewPool(js, natstest.Logger(), 1, model{}))

	natstest.Publish(t, js, streams.DecideSubject("a1"), decide.DecideNow{AvatarID: "a1", Tick: 7})

	var plan decide.PlanRevised
	natstest.Eventually(t, 5*time.Second, func() bool {
		return natstest.Last(t, js, streams.StreamEvents, streams.PlanSubject("a1"), &plan)
	})
	if plan.Type != "PlanRevised" || plan.AvatarID != "a1" || plan.Tick != 7 || string(plan.Intents) != `[{"intent":"move"}]` {
		t.Errorf("plan = %+v", plan)
	}

	var record map[string]string
	natstest.Eventually(t, 5*time.Second, func() bool {
		return natstest.Last(t, js, streams.StreamDecisions, streams.DecisionsSubject("a1"), &record)
	})
	if record["avatar_id"] != "a1" {
		t.Errorf("record = %v", record)
	}

	if got := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.AvatarEvents); len(got) != 0 {
		t.Errorf("avatar subjects written: %v", got)
	}
}
