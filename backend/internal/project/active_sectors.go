// The active-sectors projection maintains the read model the dispatcher
// fans execute commands from. An entry exists for a sector while it has
// unresolved work; the entry records the last tick the sector resolved and
// the last stream sequence applied per source stream.
//
// Two streams feed it. WORLD advances last_resolved and clears entries for
// sectors whose TickResolved reports nothing pending. AVATAR activates a
// sector when a DoctrineUpdated needs delivering there.
package project

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

// ActiveEntry is the KV value for one active sector, JSON-encoded under the
// sector ID key in the active-sectors bucket.
type ActiveEntry struct {
	// LastResolved is the last tick this sector resolved; -1 before its
	// first TickResolved. The dispatcher publishes ExecuteTick for every
	// tick from LastResolved+1 through the current tick.
	LastResolved int64 `json:"last_resolved"`
	// WorldSeq and AvatarSeq are the last applied stream sequences from
	// each source stream. Sequences from different streams are never
	// compared with each other.
	WorldSeq  uint64 `json:"world_seq"`
	AvatarSeq uint64 `json:"avatar_seq"`
}

// worldEvent is the projection's read of TickResolved. Pending is the
// resolver's own report of whether the sector still has work: unfinished
// plans, undelivered doctrine, or ships present.
type worldEvent struct {
	Type    string `json:"type"`
	Sector  string `json:"sector"`
	Tick    int64  `json:"tick"`
	Pending bool   `json:"pending"`
}

// avatarEvent is the projection's read of AVATAR stream events. Sector is
// where the avatar currently is, stamped by the command service at append
// time from the avatar-status read model.
type avatarEvent struct {
	Type   string `json:"type"`
	Avatar string `json:"avatar"`
	Sector string `json:"sector"`
}

// ActiveSectors runs both consumers and serves the dispatcher's reads.
// Run one instance of the projection loops per fleet (they register durable
// consumers, so extra instances share them; MaxAckPending 1 keeps order).
type ActiveSectors struct {
	kv  jetstream.KeyValue
	log *slog.Logger
}

func NewActiveSectors(ctx context.Context, js jetstream.JetStream, log *slog.Logger) (*ActiveSectors, error) {
	kv, err := js.KeyValue(ctx, streams.BucketActive)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", streams.BucketActive, err)
	}
	return &ActiveSectors{kv: kv, log: log.With("projection", "active-sectors")}, nil
}

// Loops returns the two projection loops to hand to the supervisor.
func (a *ActiveSectors) Loops(js jetstream.JetStream, log *slog.Logger) []*Loop {
	return []*Loop{
		NewLoop(js, log, &worldFold{a}),
		NewLoop(js, log, &avatarFold{a}),
	}
}

// ActiveSectors implements dispatch.ReadModels.
func (a *ActiveSectors) ActiveSectors(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	lister, err := a.kv.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	for key := range lister.Keys() {
		entry, err := a.kv.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue // deleted between list and get
			}
			return nil, err
		}
		var e ActiveEntry
		if err := json.Unmarshal(entry.Value(), &e); err != nil {
			return nil, fmt.Errorf("entry %s: %w", key, err)
		}
		out[key] = e.LastResolved
	}
	return out, nil
}

// DueAvatars stays stubbed until the due-avatars projection exists.
func (a *ActiveSectors) DueAvatars(context.Context, int64) ([]string, error) {
	return nil, nil
}

// worldFold applies TickResolved events.
type worldFold struct{ a *ActiveSectors }

func (w *worldFold) Name() string   { return "active-sectors-world" }
func (w *worldFold) Stream() string { return streams.StreamWorld }

func (w *worldFold) Apply(ctx context.Context, msg jetstream.Msg) error {
	var ev worldEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil || ev.Type != "TickResolved" {
		return nil // not ours; ack and move on
	}
	md, err := msg.Metadata()
	if err != nil {
		return err
	}
	seq := md.Sequence.Stream

	return w.a.update(ctx, ev.Sector, func(e *ActiveEntry) (keep bool, apply bool) {
		if seq <= e.WorldSeq {
			return true, false // redelivery, already applied
		}
		e.WorldSeq = seq
		if ev.Tick > e.LastResolved {
			e.LastResolved = ev.Tick
		}
		// The resolver's own pending report decides whether the sector
		// stays in the read model. No pending work: drop the entry, and
		// the sector costs nothing until an event reactivates it.
		return ev.Pending, true
	})
}

// avatarFold applies DoctrineUpdated events.
type avatarFold struct{ a *ActiveSectors }

func (f *avatarFold) Name() string   { return "active-sectors-avatar" }
func (f *avatarFold) Stream() string { return streams.StreamAvatar }

func (f *avatarFold) Apply(ctx context.Context, msg jetstream.Msg) error {
	var ev avatarEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil || ev.Type != "DoctrineUpdated" || ev.Sector == "" {
		return nil
	}
	md, err := msg.Metadata()
	if err != nil {
		return err
	}
	seq := md.Sequence.Stream

	return f.a.update(ctx, ev.Sector, func(e *ActiveEntry) (keep bool, apply bool) {
		if seq <= e.AvatarSeq {
			return true, false
		}
		e.AvatarSeq = seq
		// Activation only. Delivery happens inside execute; the world
		// fold clears the entry once the resolver reports nothing pending.
		return true, true
	})
}

// update runs a revision-checked read-modify-write on one entry. fold
// returns keep (entry should exist afterward) and apply (fold changed it).
// A new sector starts at LastResolved -1 so its first ExecuteTick is tick 0
// if activation precedes any TickResolved; the execute handler's
// predecessor check tolerates the dispatcher over-publishing early ticks.
func (a *ActiveSectors) update(ctx context.Context, sector string, fold func(*ActiveEntry) (bool, bool)) error {
	for {
		var (
			e   ActiveEntry
			rev uint64
		)
		entry, err := a.kv.Get(ctx, sector)
		switch {
		case err == nil:
			if jerr := json.Unmarshal(entry.Value(), &e); jerr != nil {
				return fmt.Errorf("entry %s: %w", sector, jerr)
			}
			rev = entry.Revision()
		case errors.Is(err, jetstream.ErrKeyNotFound):
			e = ActiveEntry{LastResolved: -1}
			rev = 0
		default:
			return err
		}

		keep, apply := fold(&e)
		if !apply {
			return nil
		}

		if !keep {
			if rev == 0 {
				return nil // never existed; nothing to delete
			}
			err = a.kv.Delete(ctx, sector, jetstream.LastRevision(rev))
			if isRevisionConflict(err) {
				continue // concurrent fold from the other stream; re-read
			}
			return err
		}

		data, merr := json.Marshal(e)
		if merr != nil {
			return merr
		}
		if rev == 0 {
			_, err = a.kv.Create(ctx, sector, data)
		} else {
			_, err = a.kv.Update(ctx, sector, data, rev)
		}
		if isRevisionConflict(err) {
			continue
		}
		return err
	}
}

func isRevisionConflict(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
		return true
	}
	return errors.Is(err, jetstream.ErrKeyExists)
}
