package player

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

func github(uid string) OAuthID { return OAuthID{Provider: "github", UID: uid} }

func command(playerID string, id OAuthID, displayName, dialInCode string) CreatePlayer {
	return CreatePlayer{PlayerID: playerID, OAuthID: id, DisplayName: displayName, DialInCode: dialInCode}
}

func TestValidateAccepts(t *testing.T) {
	cases := map[string]CreatePlayer{
		"plain":         command("p1", github("42"), "ada", "7719-0402-5518"),
		"url-safe ID":   command("Qm3_x-9TzLk0", github("42"), "ada", "7719-0402-5518"),
		"spaced name":   command("p1", github("42"), "Ada, the 2nd", "7719-0402-5518"),
		"unicode name":  command("p1", github("42"), "Ümit", "7719-0402-5518"),
		"at name limit": command("p1", github("42"), strings.Repeat("n", displayNameLimit), "7719-0402-5518"),
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if err := c.validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
		})
	}
}

func TestValidateRejects(t *testing.T) {
	cases := map[string]CreatePlayer{
		"missing player ID":     command("", github("42"), "ada", "7719-0402-5518"),
		"dotted player ID":      command("p.1", github("42"), "ada", "7719-0402-5518"),
		"missing provider":      command("p1", OAuthID{UID: "42"}, "ada", "7719-0402-5518"),
		"missing uid":           command("p1", OAuthID{Provider: "github"}, "ada", "7719-0402-5518"),
		"dotted uid":            command("p1", OAuthID{Provider: "github", UID: "a.1"}, "ada", "7719-0402-5518"),
		"missing name":          command("p1", github("42"), "", "7719-0402-5518"),
		"name over limit":       command("p1", github("42"), strings.Repeat("n", displayNameLimit+1), "7719-0402-5518"),
		"newline in name":       command("p1", github("42"), "ada\nlovelace", "7719-0402-5518"),
		"escape in name":        command("p1", github("42"), "ada\x1b[31m", "7719-0402-5518"),
		"missing dial-in code":  command("p1", github("42"), "ada", ""),
		"short dial-in code":    command("p1", github("42"), "ada", "7719-0402"),
		"lettered dial-in code": command("p1", github("42"), "ada", "7719-O4O2-5518"),
		"undashed dial-in code": command("p1", github("42"), "ada", "771904025518"),
		"spaced dial-in code":   command("p1", github("42"), "ada", "7719 0402 5518"),
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if err := c.validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("validate err = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestOAuthIDKeyIsOneKVKey(t *testing.T) {
	if got := github("13748641").Key(); got != "oauth.github.13748641" {
		t.Errorf("Key = %q", got)
	}
}

type fakeIndex struct {
	mu      sync.Mutex
	players map[string]string
	err     error
}

func (f *fakeIndex) PlayerIDByOAuthID(_ context.Context, id OAuthID) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return "", false, f.err
	}
	playerID, ok := f.players[id.Key()]
	return playerID, ok, nil
}

func startUniverse(t *testing.T) (*Handler, *fakeIndex) {
	t.Helper()
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	if _, err := streams.Append(ctx, js, streams.SubjectClock, clock.UniverseCreated{Type: "UniverseCreated"}, 0); err != nil {
		t.Fatalf("universe: %v", err)
	}
	index := &fakeIndex{players: map[string]string{}}
	return NewHandler(js, index), index
}

func TestCreateAppendsPlayerCreated(t *testing.T) {
	h, _ := startUniverse(t)
	ctx := natstest.Context(t)

	ev, err := h.Create(ctx, command("p1", github("42"), "  ada  ", "7719-0402-5518"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	want := PlayerCreated{
		PlayerID:    "p1",
		OAuthID:     github("42"),
		DisplayName: "ada",
		DialInCode:  "7719-0402-5518",
		Tick:        0,
		Type:        PlayerCreatedType,
	}
	if ev != want {
		t.Errorf("event = %+v, want %+v", ev, want)
	}

	var stored PlayerCreated
	if !natstest.Last(t, h.js, streams.StreamEvents, streams.PlayerSubject("p1"), &stored) {
		t.Fatal("no event appended")
	}
	if stored != want {
		t.Errorf("stored = %+v, want %+v", stored, want)
	}
}

func TestCreateRejectsInvalidWithoutAppending(t *testing.T) {
	h, _ := startUniverse(t)
	ctx := natstest.Context(t)

	if _, err := h.Create(ctx, command("p1", github("42"), "", "7719-0402-5518")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if counts := natstest.SubjectCounts(t, h.js, streams.StreamEvents, streams.PlayerEvents); len(counts) != 0 {
		t.Errorf("subjects = %v, want none", counts)
	}
}

func TestCreateRejectsRegisteredOAuthID(t *testing.T) {
	h, index := startUniverse(t)
	ctx := natstest.Context(t)
	index.players[github("42").Key()] = "p0"

	if _, err := h.Create(ctx, command("p1", github("42"), "ada", "7719-0402-5518")); !errors.Is(err, ErrAlreadyRegistered) {
		t.Fatalf("err = %v, want ErrAlreadyRegistered", err)
	}
	if counts := natstest.SubjectCounts(t, h.js, streams.StreamEvents, streams.PlayerEvents); len(counts) != 0 {
		t.Errorf("subjects = %v, want none", counts)
	}
}

func TestCreateRejectsReusedPlayerID(t *testing.T) {
	h, _ := startUniverse(t)
	ctx := natstest.Context(t)

	if _, err := h.Create(ctx, command("p1", github("42"), "ada", "7719-0402-5518")); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := h.Create(ctx, command("p1", github("43"), "bob", "0000-1111-2222")); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("err = %v, want ErrAlreadyExists", err)
	}
}

func TestCreateDeduplicatesARacingCreate(t *testing.T) {
	h, _ := startUniverse(t)
	ctx := natstest.Context(t)

	if _, err := h.Create(ctx, command("p1", github("42"), "ada", "7719-0402-5518")); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := h.Create(ctx, command("p2", github("42"), "ada again", "0000-1111-2222")); !errors.Is(err, ErrAlreadyRegistered) {
		t.Fatalf("err = %v, want ErrAlreadyRegistered", err)
	}
	counts := natstest.SubjectCounts(t, h.js, streams.StreamEvents, streams.PlayerEvents)
	if counts[streams.PlayerSubject("p1")] != 1 || counts[streams.PlayerSubject("p2")] != 0 {
		t.Errorf("subjects = %v, want only p1", counts)
	}
}

func TestCreateReportsIndexErrors(t *testing.T) {
	h, index := startUniverse(t)
	ctx := natstest.Context(t)
	index.err = errors.New("bucket unavailable")

	_, err := h.Create(ctx, command("p1", github("42"), "ada", "7719-0402-5518"))
	if err == nil || errors.Is(err, ErrInvalid) || errors.Is(err, ErrAlreadyRegistered) {
		t.Fatalf("err = %v, want the index failure", err)
	}
}
