package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/avatar"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

// StatusEntry is the Phoenix-facing current state of one fleet admiral.
type StatusEntry struct {
	AvatarID         string `json:"avatar_id"`
	Name             string `json:"name"`
	ShipName         string `json:"ship_name"`
	Sector           string `json:"sector"`
	CommissionedTick int64  `json:"commissioned_tick"`
}

// AvatarStatus folds avatar lifecycle events into the avatar-status bucket.
// Commissioning creates the entry: identity and where the flagship started.
// What a player watches change — position, cargo, condition, credits — is the
// ship aggregate's state, which this projection does not fold yet.
type AvatarStatus struct {
	kv jetstream.KeyValue
}

func NewAvatarStatus(ctx context.Context, js jetstream.JetStream) (*AvatarStatus, error) {
	kv, err := js.KeyValue(ctx, streams.BucketAvatarStatus)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", streams.BucketAvatarStatus, err)
	}
	return &AvatarStatus{kv: kv}, nil
}

func (a *AvatarStatus) Name() string { return "avatar-status" }

func (a *AvatarStatus) Stream() string { return streams.StreamEvents }

func (a *AvatarStatus) FilterSubjects() []string {
	return []string{streams.AvatarEvents}
}

// Status reads one admiral's current state. Reports false when no admiral has
// been commissioned under that id.
func (a *AvatarStatus) Status(ctx context.Context, avatarID string) (StatusEntry, bool, error) {
	entry, err := a.kv.Get(ctx, avatarID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return StatusEntry{}, false, nil
	}
	if err != nil {
		return StatusEntry{}, false, err
	}
	var e StatusEntry
	if err := json.Unmarshal(entry.Value(), &e); err != nil {
		return StatusEntry{}, false, fmt.Errorf("entry %s: %w", avatarID, err)
	}
	return e, true, nil
}

func (a *AvatarStatus) Apply(ctx context.Context, msg jetstream.Msg) error {
	ev, ok := decodeAdmiralCommissioned(msg.Data())
	if !ok {
		return nil
	}

	data, err := json.Marshal(StatusEntry{
		AvatarID:         ev.AvatarID,
		Name:             ev.Name,
		ShipName:         ev.ShipName,
		Sector:           ev.HomeSector,
		CommissionedTick: ev.Tick,
	})
	if err != nil {
		return err
	}
	_, err = a.kv.Create(ctx, ev.AvatarID, data)
	if errors.Is(err, jetstream.ErrKeyExists) {
		return nil
	}
	return err
}

func decodeAdmiralCommissioned(data []byte) (avatar.AdmiralCommissioned, bool) {
	var head struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &head) != nil || head.Type != avatar.AdmiralCommissionedType {
		return avatar.AdmiralCommissioned{}, false
	}
	var ev avatar.AdmiralCommissioned
	if json.Unmarshal(data, &ev) != nil || ev.AvatarID == "" || ev.HomeSector == "" {
		return avatar.AdmiralCommissioned{}, false
	}
	return ev, true
}
