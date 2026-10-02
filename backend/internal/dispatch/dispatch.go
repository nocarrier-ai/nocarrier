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
	js      jetstream.JetStream
	sectors ActiveSectors
	avatars DueAvatars
	log     *slog.Logger
}

type ActiveSectors interface {
	// ActiveSectors returns each sector with unresolved work and the last
	// tick it resolved, so stragglers can be re-dispatched.
	ActiveSectors(ctx context.Context) (map[string]int64, error)
}

type DueAvatars interface {
	// DueAvatars returns the avatars whose cadence or triggers make them
	// due to decide at tick.
	DueAvatars(ctx context.Context, tick int64) ([]string, error)
}

func New(js jetstream.JetStream, sectors ActiveSectors, avatars DueAvatars, log *slog.Logger) *Dispatcher {
	return &Dispatcher{js: js, sectors: sectors, avatars: avatars, log: log.With("loop", "dispatch")}
}

// Dispatch publishes the fans for tick. Called only by the pacer that won
// the tick. Idempotent: every publish carries a deterministic Msg-Id, so a
// partial fan re-published by the next winner deduplicates.
func (d *Dispatcher) Dispatch(ctx context.Context, tick int64) error {
	active, err := d.sectors.ActiveSectors(ctx)
	if err != nil {
		return fmt.Errorf("active sectors: %w", err)
	}
	var published int
	for sectorID, lastResolved := range active {
		// Straggler recovery: publish every unresolved tick up to and
		// including this one, in order. Normally that is just `tick`.
		for t := lastResolved + 1; t <= tick; t++ {
			if err := d.publish(ctx, streams.ExecuteSubject(sectorID),
				streams.ExecuteMsgID(sectorID, t), sector.ExecuteTick{SectorID: sectorID, Tick: t}); err != nil {
				return fmt.Errorf("dispatch execute %s@%d: %w", sectorID, t, err)
			}
			published++
		}
	}

	due, err := d.avatars.DueAvatars(ctx, tick)
	if err != nil {
		return fmt.Errorf("due avatars: %w", err)
	}
	for _, avatarID := range due {
		if err := d.publish(ctx, streams.DecideSubject(avatarID),
			streams.DecideMsgID(avatarID, tick), decide.DecideNow{AvatarID: avatarID, Tick: tick}); err != nil {
			return fmt.Errorf("dispatch decide %s@%d: %w", avatarID, tick, err)
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
