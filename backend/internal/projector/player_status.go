package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/player"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type PlayerEntry struct {
	PlayerID    string         `json:"player_id"`
	OAuthID     player.OAuthID `json:"oauth_id"`
	DisplayName string         `json:"display_name"`
	DialInCode  string         `json:"dial_in_code"`
	CreatedTick int64          `json:"created_tick"`
}

type OAuthEntry struct {
	PlayerID string `json:"player_id"`
}

type PlayerStatus struct {
	kv jetstream.KeyValue
}

func NewPlayerStatus(ctx context.Context, js jetstream.JetStream) (*PlayerStatus, error) {
	kv, err := js.KeyValue(ctx, streams.BucketPlayerStatus)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", streams.BucketPlayerStatus, err)
	}
	return &PlayerStatus{kv: kv}, nil
}

func (p *PlayerStatus) Name() string { return "player-status" }

func (p *PlayerStatus) FilterSubjects() []string {
	return []string{streams.PlayerEvents}
}

func (p *PlayerStatus) Player(ctx context.Context, playerID string) (PlayerEntry, bool, error) {
	var e PlayerEntry
	found, err := p.read(ctx, playerID, &e)
	return e, found, err
}

func (p *PlayerStatus) PlayerIDByOAuthID(ctx context.Context, id player.OAuthID) (string, bool, error) {
	var e OAuthEntry
	found, err := p.read(ctx, id.Key(), &e)
	return e.PlayerID, found, err
}

func (p *PlayerStatus) Apply(ctx context.Context, msg jetstream.Msg) error {
	ev, ok := decodePlayerCreated(msg.Data())
	if !ok {
		return nil
	}
	if err := p.create(ctx, ev.PlayerID, PlayerEntry{
		PlayerID:    ev.PlayerID,
		OAuthID:     ev.OAuthID,
		DisplayName: ev.DisplayName,
		DialInCode:  ev.DialInCode,
		CreatedTick: ev.Tick,
	}); err != nil {
		return err
	}
	return p.create(ctx, ev.OAuthID.Key(), OAuthEntry{PlayerID: ev.PlayerID})
}

func (p *PlayerStatus) read(ctx context.Context, key string, v any) (bool, error) {
	entry, err := p.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(entry.Value(), v); err != nil {
		return false, fmt.Errorf("entry %s: %w", key, err)
	}
	return true, nil
}

func (p *PlayerStatus) create(ctx context.Context, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = p.kv.Create(ctx, key, data)
	if errors.Is(err, jetstream.ErrKeyExists) {
		return nil
	}
	return err
}

func decodePlayerCreated(data []byte) (player.PlayerCreated, bool) {
	var head struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &head) != nil || head.Type != player.PlayerCreatedType {
		return player.PlayerCreated{}, false
	}
	var ev player.PlayerCreated
	if json.Unmarshal(data, &ev) != nil || ev.PlayerID == "" || ev.OAuthID.Provider == "" || ev.OAuthID.UID == "" {
		return player.PlayerCreated{}, false
	}
	return ev, true
}
