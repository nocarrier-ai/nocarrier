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
		{streams.PortSubject("7"), streams.StreamEvents},
		{streams.AvatarSubject("a1"), streams.StreamEvents},
		{streams.PlanSubject("a1"), streams.StreamEvents},
		{streams.DoctrineSubject("a1"), streams.StreamEvents},
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
		streams.DoctrineSubject("a.b"),
	} {
		if _, err := js.Publish(ctx, subject, []byte(`{}`)); err == nil {
			t.Errorf("publish %s succeeded, want no stream", subject)
		}
	}
}

func TestValidID(t *testing.T) {
	for _, id := range []string{"0", "a1", "A-1", "fleet_admiral-akbar", "3f2b9c1e-7d4a-4f8e-9a6b-1c2d3e4f5a6b"} {
		if !streams.ValidID(id) {
			t.Errorf("ValidID(%q) = false, want true", id)
		}
	}
	for _, id := range []string{"", "a.b", "a*", "a>", "a 1", "a/1", "kevin@relay", "a:1", "$a1", "ünit-7"} {
		if streams.ValidID(id) {
			t.Errorf("ValidID(%q) = true, want false", id)
		}
	}
}

// Every id ValidID admits must survive the round trip to a subject and back
// as a single token; the rule exists only to protect that.
func TestValidIDsPublishToEvents(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)

	for _, id := range []string{"0", "a1", "A-1", "fleet_admiral-akbar"} {
		for _, subject := range []string{
			streams.SectorSubject(id), streams.AvatarSubject(id),
			streams.PlanSubject(id), streams.DoctrineSubject(id),
		} {
			if _, err := js.Publish(ctx, subject, []byte(`{}`)); err != nil {
				t.Errorf("publish %s: %v", subject, err)
			}
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
