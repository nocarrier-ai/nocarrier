package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/decide"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

// cadence is the number of ticks between decision passes. Subscription tiers
// will make this per avatar (free-tier admirals decide less often, thematically
// from slower communications); until tiers exist it is the same for everyone and
// has no business being stored per entry.
const cadence = 1

// DueEntry is one admiral's decision schedule. NextTick is the first tick at
// which the admiral is due to decide again.
//
// Nothing records that a decide command was dispatched, only that a decision
// landed: PlanRevised is what pushes NextTick forward. So a DecideNow that is
// lost, or whose model call fails, leaves the admiral due and the next tick's
// winner dispatches it again. That is the same straggler recovery the execute
// side gets from active-sectors.
type DueEntry struct {
	NextTick int64  `json:"next_tick"`
	Seq      uint64 `json:"seq"`
}

// DueAvatars folds avatar lifecycle and plan events into the due-avatars
// bucket, and reads it back as the dispatcher's decide-side input.
type DueAvatars struct {
	kv jetstream.KeyValue
}

func NewDueAvatars(ctx context.Context, js jetstream.JetStream) (*DueAvatars, error) {
	kv, err := js.KeyValue(ctx, streams.BucketDue)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", streams.BucketDue, err)
	}
	return &DueAvatars{kv: kv}, nil
}

func (d *DueAvatars) Name() string { return "due-avatars" }

func (d *DueAvatars) FilterSubjects() []string {
	return []string{streams.AvatarEvents, streams.PlanEvents}
}

// DueAvatars implements dispatch.DueAvatars. Sorted so a tick's fan-out order
// is stable across instances and runs.
func (d *DueAvatars) DueAvatars(ctx context.Context, tick int64) ([]string, error) {
	var out []string
	lister, err := d.kv.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	for avatarID := range lister.Keys() {
		entry, err := d.kv.Get(ctx, avatarID)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue // deleted between list and get
			}
			return nil, err
		}
		var e DueEntry
		if err := json.Unmarshal(entry.Value(), &e); err != nil {
			return nil, fmt.Errorf("entry %s: %w", avatarID, err)
		}
		if e.NextTick <= tick {
			out = append(out, avatarID)
		}
	}
	slices.Sort(out)
	return out, nil
}

func (d *DueAvatars) Apply(ctx context.Context, msg jetstream.Msg) error {
	avatarID, f := routeDue(msg.Data())
	if f == nil {
		return nil
	}
	md, err := msg.Metadata()
	if err != nil {
		return err
	}

	var (
		e      DueEntry
		rev    uint64
		exists bool
	)
	entry, err := d.kv.Get(ctx, avatarID)
	switch {
	case err == nil:
		if err := json.Unmarshal(entry.Value(), &e); err != nil {
			return fmt.Errorf("entry %s: %w", avatarID, err)
		}
		rev = entry.Revision()
		exists = true
	case !errors.Is(err, jetstream.ErrKeyNotFound):
		return err
	}
	if md.Sequence.Stream <= e.Seq {
		return nil
	}
	if !f(&e, exists) {
		return nil
	}
	e.Seq = md.Sequence.Stream

	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if !exists {
		_, err = d.kv.Create(ctx, avatarID, data)
	} else {
		_, err = d.kv.Update(ctx, avatarID, data, rev)
	}
	return err
}

// dueFold folds one event into an admiral's schedule. It reports whether the
// entry should be written; exists tells it whether there is an entry at all.
type dueFold func(e *DueEntry, exists bool) (write bool)

func routeDue(data []byte) (string, dueFold) {
	var head struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &head) != nil {
		return "", nil
	}
	switch head.Type {
	case decide.PlanRevisedType:
		var ev decide.PlanRevised
		if json.Unmarshal(data, &ev) != nil || ev.AvatarID == "" {
			return "", nil
		}
		return ev.AvatarID, func(e *DueEntry, exists bool) bool {
			if !exists {
				// Only a commissioned admiral has a schedule. The aggregate
				// orders commissioning first, so this means the event is for
				// an avatar this projection has no business scheduling.
				return false
			}
			e.NextTick = max(e.NextTick, ev.Tick+cadence)
			return true
		}
	}
	if ev, ok := decodeAdmiralCommissioned(data); ok {
		return ev.AvatarID, func(e *DueEntry, exists bool) bool {
			if exists {
				// Commissioning happens once. Never reset a schedule that
				// has already moved on.
				return false
			}
			// Due on the commissioning tick itself: the admiral needs a
			// first plan before the flagship can do anything but fall back
			// to doctrine.
			*e = DueEntry{NextTick: ev.Tick}
			return true
		}
	}
	return "", nil
}
