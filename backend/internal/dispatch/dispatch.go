// Package dispatch fans a won tick out as commands: one execute command per
// active sector, one decide command per avatar that is due. Commands carry
// deterministic Nats-Msg-Ids so re-dispatch after a crash is deduplicated.
package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/decide"
	"github.com/nocarrier-ai/nocarrier/internal/sector"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type Dispatcher struct {
	js  jetstream.JetStream
	kv  ReadModels
	log *slog.Logger
}

// ReadModels supplies the two projections the dispatcher reads. Implemented
// over the active-sectors and due-avatars KV buckets; stubbed in tests.
type ReadModels interface {
	// ActiveSectors returns each sector with unresolved work and the last
	// tick it resolved, so stragglers can be re-dispatched.
	ActiveSectors(ctx context.Context) (map[string]int64, error)
	// DueAvatars returns the avatars whose cadence or triggers make them
	// due to decide at tick.
	DueAvatars(ctx context.Context, tick int64) ([]string, error)
}

func New(js jetstream.JetStream, kv ReadModels, log *slog.Logger) *Dispatcher {
	return &Dispatcher{js: js, kv: kv, log: log.With("loop", "dispatch")}
}

// Dispatch publishes the fans for tick. Called only by the pacer that won
// the tick. Idempotent: every publish carries a deterministic Msg-Id, so a
// partial fan re-published by the next winner deduplicates.
func (d *Dispatcher) Dispatch(ctx context.Context, tick int64) error {
	active, err := d.kv.ActiveSectors(ctx)
	if err != nil {
		return fmt.Errorf("active sectors: %w", err)
	}
	var published int
	for id, lastResolved := range active {
		// Straggler recovery: publish every unresolved tick up to and
		// including this one, in order. Normally that is just `tick`.
		for t := lastResolved + 1; t <= tick; t++ {
			if err := d.publish(ctx, streams.ExecuteSubject(id),
				streams.ExecuteMsgID(id, t), sector.ExecuteTick{Sector: id, Tick: t}); err != nil {
				return fmt.Errorf("dispatch execute %s@%d: %w", id, t, err)
			}
			published++
		}
	}

	due, err := d.kv.DueAvatars(ctx, tick)
	if err != nil {
		return fmt.Errorf("due avatars: %w", err)
	}
	for _, avatar := range due {
		if err := d.publish(ctx, streams.DecideSubject(avatar),
			streams.DecideMsgID(avatar, tick), decide.DecideNow{Avatar: avatar, Tick: tick}); err != nil {
			return fmt.Errorf("dispatch decide %s@%d: %w", avatar, tick, err)
		}
	}
	d.log.Info("dispatched", "tick", tick, "execute", published, "decide", len(due))
	return nil
}

func (d *Dispatcher) publish(ctx context.Context, subject, msgID string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	msg := nats.NewMsg(subject)
	msg.Data = data
	msg.Header.Set("Nats-Msg-Id", msgID)
	_, err = d.js.PublishMsg(ctx, msg)
	return err
}
