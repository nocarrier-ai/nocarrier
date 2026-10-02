package streams_test

import (
	"testing"

	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

func TestSubjectsRouteToStreams(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)

	cases := []struct {
		subject string
		stream  string
	}{
		{streams.SubjectClock, streams.StreamClock},
		{streams.SectorSubject("s1"), streams.StreamEvents},
		{streams.AvatarSubject("a1"), streams.StreamEvents},
		{streams.PlanSubject("a1"), streams.StreamEvents},
		{streams.DecisionsSubject("a1"), streams.StreamDecisions},
		{streams.ExecuteSubject("s1"), streams.StreamExecute},
		{streams.DecideSubject("a1"), streams.StreamDecide},
	}
	for _, c := range cases {
		ack, err := js.Publish(ctx, c.subject, []byte(`{}`))
		if err != nil {
			t.Fatalf("publish %s: %v", c.subject, err)
		}
		if ack.Stream != c.stream {
			t.Errorf("%s stored in %s, want %s", c.subject, ack.Stream, c.stream)
		}
	}
}

func TestEventsRejectsMultiTokenIDs(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)

	for _, subject := range []string{
		streams.SectorSubject("a.b"),
		streams.AvatarSubject("a.b"),
		streams.PlanSubject("a.b"),
	} {
		if _, err := js.Publish(ctx, subject, []byte(`{}`)); err == nil {
			t.Errorf("publish %s succeeded, want no stream", subject)
		}
	}
}

func TestEnsureIsIdempotent(t *testing.T) {
	js := natstest.Start(t)
	if err := streams.Ensure(natstest.Context(t), js, 1, natstest.TickPeriod); err != nil {
		t.Fatalf("second ensure: %v", err)
	}
}

func TestMsgIDs(t *testing.T) {
	if got := streams.ExecuteMsgID("s1", 7); got != "s1@7" {
		t.Errorf("ExecuteMsgID = %q", got)
	}
	if got := streams.DecideMsgID("a1", 7); got != "a1@7" {
		t.Errorf("DecideMsgID = %q", got)
	}
}
