package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/planet"
	"github.com/nocarrier-ai/nocarrier/internal/player"
	"github.com/nocarrier-ai/nocarrier/internal/port"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

const (
	clockKey  = "clock"
	countsKey = "counts"
)

type ClockEntry struct {
	Seed         int64 `json:"seed"`
	Sectors      int   `json:"sectors"`
	Tick         int64 `json:"tick"`
	TickPeriodMS int64 `json:"tick_period_ms"`
}

type CountsEntry struct {
	Ports   int `json:"ports"`
	Planets int `json:"planets"`
	Players int `json:"players"`
}

type UniverseClock struct {
	kv jetstream.KeyValue
}

func NewUniverseClock(ctx context.Context, js jetstream.JetStream) (*UniverseClock, error) {
	kv, err := js.KeyValue(ctx, streams.BucketUniverseStatus)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", streams.BucketUniverseStatus, err)
	}
	return &UniverseClock{kv: kv}, nil
}

func (c *UniverseClock) Name() string { return "universe-clock" }

func (c *UniverseClock) Stream() string { return streams.StreamClock }

func (c *UniverseClock) FilterSubjects() []string { return []string{streams.SubjectClock} }

func (c *UniverseClock) Clock(ctx context.Context) (ClockEntry, bool, error) {
	var e ClockEntry
	_, found, err := readEntry(ctx, c.kv, clockKey, &e)
	return e, found, err
}

func (c *UniverseClock) Apply(ctx context.Context, msg jetstream.Msg) error {
	switch eventType(msg.Data()) {
	case clock.UniverseCreatedType:
		var ev clock.UniverseCreated
		if json.Unmarshal(msg.Data(), &ev) != nil {
			return nil
		}
		data, err := json.Marshal(ClockEntry{Seed: ev.Seed, Sectors: ev.Sectors, TickPeriodMS: ev.TickPeriod.Milliseconds()})
		if err != nil {
			return err
		}
		_, err = c.kv.Create(ctx, clockKey, data)
		if errors.Is(err, jetstream.ErrKeyExists) {
			return nil
		}
		return err
	case clock.TickAdvancedType:
		var ev clock.TickAdvanced
		if json.Unmarshal(msg.Data(), &ev) != nil {
			return nil
		}
		var e ClockEntry
		rev, found, err := readEntry(ctx, c.kv, clockKey, &e)
		if err != nil || !found || ev.Tick <= e.Tick {
			return err
		}
		e.Tick = ev.Tick
		data, err := json.Marshal(e)
		if err != nil {
			return err
		}
		_, err = c.kv.Update(ctx, clockKey, data, rev)
		return err
	}
	return nil
}

type UniverseCounts struct {
	js jetstream.JetStream
	kv jetstream.KeyValue
}

func NewUniverseCounts(ctx context.Context, js jetstream.JetStream) (*UniverseCounts, error) {
	kv, err := js.KeyValue(ctx, streams.BucketUniverseStatus)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", streams.BucketUniverseStatus, err)
	}
	return &UniverseCounts{js: js, kv: kv}, nil
}

func (c *UniverseCounts) Name() string { return "universe-counts" }

func (c *UniverseCounts) Stream() string { return streams.StreamEvents }

func (c *UniverseCounts) FilterSubjects() []string {
	return []string{streams.PortEvents, streams.PlanetEvents, streams.PlayerEvents}
}

func (c *UniverseCounts) Counts(ctx context.Context) (CountsEntry, bool, error) {
	var e CountsEntry
	_, found, err := readEntry(ctx, c.kv, countsKey, &e)
	return e, found, err
}

func (c *UniverseCounts) Apply(ctx context.Context, msg jetstream.Msg) error {
	var (
		filter string
		set    func(*CountsEntry, int)
	)
	switch eventType(msg.Data()) {
	case port.PortCreatedType:
		filter, set = streams.PortEvents, func(e *CountsEntry, n int) { e.Ports = n }
	case planet.PlanetCreatedType:
		filter, set = streams.PlanetEvents, func(e *CountsEntry, n int) { e.Planets = n }
	case player.PlayerCreatedType:
		filter, set = streams.PlayerEvents, func(e *CountsEntry, n int) { e.Players = n }
	default:
		return nil
	}
	n, err := streams.SubjectCount(ctx, c.js, filter)
	if err != nil {
		return err
	}
	var e CountsEntry
	rev, found, err := readEntry(ctx, c.kv, countsKey, &e)
	if err != nil {
		return err
	}
	set(&e, n)
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if found {
		_, err = c.kv.Update(ctx, countsKey, data, rev)
	} else {
		_, err = c.kv.Create(ctx, countsKey, data)
	}
	return err
}

func readEntry(ctx context.Context, kv jetstream.KeyValue, key string, v any) (uint64, bool, error) {
	entry, err := kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if err := json.Unmarshal(entry.Value(), v); err != nil {
		return 0, false, fmt.Errorf("entry %s: %w", key, err)
	}
	return entry.Revision(), true, nil
}

func eventType(data []byte) string {
	var head struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &head) != nil {
		return ""
	}
	return head.Type
}
