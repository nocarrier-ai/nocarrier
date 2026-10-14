package projector

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nocarrier-ai/nocarrier/internal/doctrine"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

func startNewsfeed(t *testing.T) *nats.Subscription {
	t.Helper()
	js := natstest.Start(t)
	sub, err := js.Conn().SubscribeSync("news.>")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	natstest.Run(t, NewLoop(js, natstest.Logger(), NewNewsfeed(js.Conn())))

	natstest.Publish(t, js, streams.DoctrineSubject("a1"), map[string]string{"type": "Unknown", "avatar_id": "a1"})
	updated := doctrineEvent("a1", 15, "fight only when cornered")
	updated.Tick = 41883
	natstest.Publish(t, js, streams.DoctrineSubject("a1"), updated)

	delivered := updated
	delivered.Type = doctrine.DoctrineDeliveredType
	delivered.Tick = 41884
	natstest.Publish(t, js, streams.DoctrineSubject("a1"), delivered)
	return sub
}

func TestNewsfeedPublishesDoctrineArrival(t *testing.T) {
	sub := startNewsfeed(t)

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if msg.Subject != streams.NewsSubject("a1") {
		t.Errorf("subject = %s", msg.Subject)
	}
	var item NewsItem
	if err := json.Unmarshal(msg.Data, &item); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := NewsItem{PlayerID: "a1", Tick: 41884, Text: "DOCTRINE rev 15 RECEIVED"}
	if item != want {
		t.Errorf("item = %+v, want %+v", item, want)
	}
}

func TestNewsfeedSkipsEverythingButDelivery(t *testing.T) {
	sub := startNewsfeed(t)

	if _, err := sub.NextMsg(5 * time.Second); err != nil {
		t.Fatalf("next: %v", err)
	}
	if msg, err := sub.NextMsg(200 * time.Millisecond); err == nil {
		t.Errorf("unexpected news on %s: %s", msg.Subject, msg.Data)
	}
}
