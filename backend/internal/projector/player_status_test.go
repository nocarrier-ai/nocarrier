package projector

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/player"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

func created(playerID, uid string, tick int64) player.PlayerCreated {
	return player.PlayerCreated{
		PlayerID:    playerID,
		OAuthID:     player.OAuthID{Provider: "github", UID: uid},
		DisplayName: "ada",
		DialInCode:  "7719-0402-5518",
		Tick:        tick,
		Type:        player.PlayerCreatedType,
	}
}

func TestDecodePlayerCreated(t *testing.T) {
	data, err := json.Marshal(created("p1", "42", 7))
	if err != nil {
		t.Fatal(err)
	}
	ev, ok := decodePlayerCreated(data)
	if !ok || ev.PlayerID != "p1" || ev.OAuthID.UID != "42" || ev.Tick != 7 {
		t.Fatalf("decode = %+v, %v", ev, ok)
	}
}

func TestDecodePlayerCreatedSkips(t *testing.T) {
	for name, data := range map[string]string{
		"other type":        `{"type":"AdmiralCommissioned","player_id":"p1"}`,
		"missing player ID": `{"type":"PlayerCreated","oauth_id":{"provider":"github","uid":"42"}}`,
		"missing oauth ID":  `{"type":"PlayerCreated","player_id":"p1"}`,
		"missing uid":       `{"type":"PlayerCreated","player_id":"p1","oauth_id":{"provider":"github"}}`,
		"missing dial-in":   `{"type":"PlayerCreated","player_id":"p1","oauth_id":{"provider":"github","uid":"42"}}`,
		"bad json":          `{`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := decodePlayerCreated([]byte(data)); ok {
				t.Error("decoded, want skip")
			}
		})
	}
}

func startPlayerStatus(t *testing.T) (jetstream.JetStream, *PlayerStatus) {
	t.Helper()
	js := natstest.Start(t)
	proj, err := NewPlayerStatus(natstest.Context(t), js)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	natstest.Run(t, NewLoop(js, natstest.Logger(), proj))
	return js, proj
}

func waitForPlayer(t *testing.T, proj *PlayerStatus, playerID string) PlayerEntry {
	t.Helper()
	ctx := natstest.Context(t)
	var got PlayerEntry
	natstest.Eventually(t, 5*time.Second, func() bool {
		e, ok, err := proj.Player(ctx, playerID)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		got = e
		return ok
	})
	return got
}

func TestPlayerStatusProjection(t *testing.T) {
	js, proj := startPlayerStatus(t)
	ctx := natstest.Context(t)

	natstest.Publish(t, js, streams.PlayerSubject("p1"), created("p1", "42", 4))
	got := waitForPlayer(t, proj, "p1")
	want := PlayerEntry{
		PlayerID:    "p1",
		OAuthID:     player.OAuthID{Provider: "github", UID: "42"},
		DisplayName: "ada",
		DialInCode:  "7719-0402-5518",
		CreatedTick: 4,
	}
	if got != want {
		t.Errorf("entry = %+v, want %+v", got, want)
	}

	playerID, found, err := proj.PlayerIDByOAuthID(ctx, player.OAuthID{Provider: "github", UID: "42"})
	if err != nil || !found || playerID != "p1" {
		t.Errorf("by oauth = %q, %v, %v; want p1", playerID, found, err)
	}
	if _, found, err := proj.PlayerIDByOAuthID(ctx, player.OAuthID{Provider: "github", UID: "43"}); err != nil || found {
		t.Errorf("unknown oauth found = %v, err = %v", found, err)
	}
	if _, found, err := proj.Player(ctx, "nobody"); err != nil || found {
		t.Errorf("unknown player found = %v, err = %v", found, err)
	}

	var byCode IndexEntry
	if found, err := proj.read(ctx, player.DialInKey("7719-0402-5518"), &byCode); err != nil || !found || byCode.PlayerID != "p1" {
		t.Errorf("by dial-in code = %+v, %v, %v; want p1", byCode, found, err)
	}
}

func TestPlayerStatusIgnoresOtherEvents(t *testing.T) {
	js, proj := startPlayerStatus(t)

	natstest.Publish(t, js, streams.PlayerSubject("p1"), created("p1", "42", 4))
	first := waitForPlayer(t, proj, "p1")
	natstest.Publish(t, js, streams.PlayerSubject("p1"), map[string]any{"type": "Nope", "player_id": "p1"})
	natstest.Publish(t, js, streams.PlayerSubject("p2"), created("p2", "43", 9))
	waitForPlayer(t, proj, "p2")

	if again, _, err := proj.Player(natstest.Context(t), "p1"); err != nil || again != first {
		t.Errorf("p1 = %+v, want unchanged %+v (err %v)", again, first, err)
	}
}

func TestPlayerStatusIndexKeepsTheFirstPlayer(t *testing.T) {
	js, proj := startPlayerStatus(t)
	ctx := natstest.Context(t)

	natstest.Publish(t, js, streams.PlayerSubject("p1"), created("p1", "42", 4))
	waitForPlayer(t, proj, "p1")
	natstest.Publish(t, js, streams.PlayerSubject("p2"), created("p2", "42", 5))
	waitForPlayer(t, proj, "p2")

	playerID, found, err := proj.PlayerIDByOAuthID(ctx, player.OAuthID{Provider: "github", UID: "42"})
	if err != nil || !found || playerID != "p1" {
		t.Errorf("by oauth = %q, %v, %v; want the first player p1", playerID, found, err)
	}
	var byCode IndexEntry
	if found, err := proj.read(ctx, player.DialInKey("7719-0402-5518"), &byCode); err != nil || !found || byCode.PlayerID != "p1" {
		t.Errorf("by dial-in code = %+v, %v, %v; want the first player p1", byCode, found, err)
	}
}

func TestPlayerStatusSkipsRedelivery(t *testing.T) {
	js, proj := startPlayerStatus(t)
	ctx := natstest.Context(t)

	natstest.Publish(t, js, streams.PlayerSubject("p1"), created("p1", "42", 4))
	waitForPlayer(t, proj, "p1")
	entry, err := proj.kv.Get(ctx, "p1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	index, err := proj.kv.Get(ctx, "oauth.github.42")
	if err != nil {
		t.Fatalf("get index: %v", err)
	}

	cons, err := js.OrderedConsumer(ctx, streams.StreamEvents, jetstream.OrderedConsumerConfig{
		FilterSubjects: proj.FilterSubjects(),
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	msg, err := cons.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if err := proj.Apply(ctx, msg); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	after, err := proj.kv.Get(ctx, "p1")
	if err != nil {
		t.Fatalf("get after: %v", err)
	}
	if after.Revision() != entry.Revision() {
		t.Errorf("entry revision %d, want unchanged %d", after.Revision(), entry.Revision())
	}
	indexAfter, err := proj.kv.Get(ctx, "oauth.github.42")
	if err != nil {
		t.Fatalf("get index after: %v", err)
	}
	if indexAfter.Revision() != index.Revision() {
		t.Errorf("index revision %d, want unchanged %d", indexAfter.Revision(), index.Revision())
	}
}
