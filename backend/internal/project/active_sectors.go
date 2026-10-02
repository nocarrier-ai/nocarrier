package project

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/sector"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type ActiveEntry struct {
	// LastResolved is the last tick this sector resolved; -1 before its
	// first TickResolved. The dispatcher publishes ExecuteTick for every
	// tick from LastResolved+1 through the current tick.
	LastResolved int64  `json:"last_resolved"`
	Seq          uint64 `json:"seq"`
}

type doctrineUpdated struct {
	SectorID string `json:"sector_id"`
}

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

func (a *ActiveSectors) Name() string { return "active-sectors" }

func (a *ActiveSectors) FilterSubjects() []string {
	return []string{streams.SectorEvents, streams.AvatarEvents}
}

// ActiveSectors implements dispatch.ReadModels.
func (a *ActiveSectors) ActiveSectors(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	lister, err := a.kv.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	for sectorID := range lister.Keys() {
		entry, err := a.kv.Get(ctx, sectorID)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue // deleted between list and get
			}
			return nil, err
		}
		var e ActiveEntry
		if err := json.Unmarshal(entry.Value(), &e); err != nil {
			return nil, fmt.Errorf("entry %s: %w", sectorID, err)
		}
		out[sectorID] = e.LastResolved
	}
	return out, nil
}

// DueAvatars stays stubbed until the due-avatars projection exists.
func (a *ActiveSectors) DueAvatars(context.Context, int64) ([]string, error) {
	return nil, nil
}

func (a *ActiveSectors) Apply(ctx context.Context, msg jetstream.Msg) error {
	sectorID, f := route(msg.Data())
	if f == nil {
		return nil
	}
	md, err := msg.Metadata()
	if err != nil {
		return err
	}

	var (
		e   = ActiveEntry{LastResolved: -1}
		rev uint64
	)
	entry, err := a.kv.Get(ctx, sectorID)
	switch {
	case err == nil:
		if err := json.Unmarshal(entry.Value(), &e); err != nil {
			return fmt.Errorf("entry %s: %w", sectorID, err)
		}
		rev = entry.Revision()
	case !errors.Is(err, jetstream.ErrKeyNotFound):
		return err
	}

	next, o := step(e, rev != 0, md.Sequence.Stream, f)
	switch o {
	case opPut:
		data, err := json.Marshal(next)
		if err != nil {
			return err
		}
		if rev == 0 {
			_, err = a.kv.Create(ctx, sectorID, data)
		} else {
			_, err = a.kv.Update(ctx, sectorID, data, rev)
		}
		return err
	case opDelete:
		return a.kv.Delete(ctx, sectorID, jetstream.LastRevision(rev))
	}
	return nil
}

type fold func(e *ActiveEntry) (keep bool)

type op int

const (
	opSkip op = iota
	opPut
	opDelete
)

func route(data []byte) (string, fold) {
	var head struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &head) != nil {
		return "", nil
	}
	switch head.Type {
	case "TickResolved":
		var ev sector.TickResolved
		if json.Unmarshal(data, &ev) != nil || ev.SectorID == "" {
			return "", nil
		}
		return ev.SectorID, func(e *ActiveEntry) bool {
			e.LastResolved = max(e.LastResolved, ev.Tick)
			return ev.Pending
		}
	case "DoctrineUpdated":
		var ev doctrineUpdated
		if json.Unmarshal(data, &ev) != nil || ev.SectorID == "" {
			return "", nil
		}
		return ev.SectorID, func(*ActiveEntry) bool { return true }
	}
	return "", nil
}

func step(e ActiveEntry, exists bool, seq uint64, f fold) (ActiveEntry, op) {
	if seq <= e.Seq {
		return e, opSkip
	}
	e.Seq = seq
	if f(&e) {
		return e, opPut
	}
	if exists {
		return e, opDelete
	}
	return e, opSkip
}
