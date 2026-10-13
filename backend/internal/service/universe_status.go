package service

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

type UniverseStatusReader struct {
	js         jetstream.JetStream
	u          *universe.Universe
	tickPeriod time.Duration
}

func NewUniverseStatusReader(js jetstream.JetStream, u *universe.Universe, tickPeriod time.Duration) *UniverseStatusReader {
	return &UniverseStatusReader{js: js, u: u, tickPeriod: tickPeriod}
}

func (r *UniverseStatusReader) Status(ctx context.Context) (UniverseStatus, error) {
	tick, err := clock.CurrentTick(ctx, r.js)
	if err != nil {
		return UniverseStatus{}, fmt.Errorf("read tick: %w", err)
	}
	ports, err := streams.SubjectCount(ctx, r.js, streams.PortEvents)
	if err != nil {
		return UniverseStatus{}, err
	}
	planets, err := streams.SubjectCount(ctx, r.js, streams.PlanetEvents)
	if err != nil {
		return UniverseStatus{}, err
	}
	players, err := streams.SubjectCount(ctx, r.js, streams.PlayerEvents)
	if err != nil {
		return UniverseStatus{}, err
	}
	return UniverseStatus{
		Seed:         r.u.Seed,
		Sectors:      len(r.u.Sectors),
		Tick:         max(tick, 0),
		TickPeriodMS: r.tickPeriod.Milliseconds(),
		Ports:        ports,
		Planets:      planets,
		Players:      players,
	}, nil
}
