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

// StatusEntry is the Phoenix-facing current state of one fleet admiral. Seq
// is the stream sequence of the last event folded in, so a redelivery or a
// replay at or below it is skipped.
type StatusEntry struct {
	AvatarID         string `json:"avatar_id"`
	Name             string `json:"name"`
	ShipName         string `json:"ship_name"`
	Sector           string `json:"sector"`
	CommissionedTick int64  `json:"commissioned_tick"`
	Seq              uint64 `json:"seq"`
}

// AvatarStatus folds avatar lifecycle events into the avatar-status bucket.
// Commissioning creates the entry: identity and where the flagship started.
// Everything a player watches change — hull, fuel, credits, position — arrives
// from TickResolved, which this projection does not read yet.
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
	md, err := msg.Metadata()
	if err != nil {
		return err
	}

	var (
		stored StatusEntry
		rev    uint64
	)
	entry, err := a.kv.Get(ctx, ev.AvatarID)
	switch {
	case err == nil:
		if err := json.Unmarshal(entry.Value(), &stored); err != nil {
			return fmt.Errorf("entry %s: %w", ev.AvatarID, err)
		}
		rev = entry.Revision()
	case !errors.Is(err, jetstream.ErrKeyNotFound):
		return err
	}
	if md.Sequence.Stream <= stored.Seq {
		return nil
	}

	data, err := json.Marshal(StatusEntry{
		AvatarID:         ev.AvatarID,
		Name:             ev.Name,
		ShipName:         ev.ShipName,
		Sector:           ev.HomeSector,
		CommissionedTick: ev.Tick,
		Seq:              md.Sequence.Stream,
	})
	if err != nil {
		return err
	}
	if rev == 0 {
		_, err = a.kv.Create(ctx, ev.AvatarID, data)
	} else {
		_, err = a.kv.Update(ctx, ev.AvatarID, data, rev)
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
