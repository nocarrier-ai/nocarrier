package sector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

// resolveTimeout bounds one resolve, so a hung resolver cannot hold its command
// across a tick boundary: the handler naks and the command is redelivered
// instead. Far inside MinTickPeriod and inside AckWait. A var so tests can
// lower it.
var resolveTimeout = 5 * time.Second

// ackWait is how long a command may sit unacked before redelivery. A handler
// that dies gets its command back well inside one tick period.
const ackWait = 30 * time.Second

// Resolver is the pure game-rules function. Given the sector's replayed
// state and the tick, it returns the outcome events. Deterministic: same
// inputs, same bytes.
type Resolver interface {
	Resolve(ctx context.Context, sectorID string, tick int64) (events json.RawMessage, pending bool, err error)
}

type Pool struct {
	js      jetstream.JetStream
	log     *slog.Logger
	workers int
	resolve Resolver
}

func NewPool(js jetstream.JetStream, log *slog.Logger, workers int, r Resolver) *Pool {
	return &Pool{js: js, log: log.With("loop", "execute"), workers: workers, resolve: r}
}

func (p *Pool) Name() string { return "execute" }

func (p *Pool) Run(ctx context.Context) error {
	cons, err := p.js.CreateOrUpdateConsumer(ctx, streams.StreamExecute, jetstream.ConsumerConfig{
		Durable:       "execute",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxAckPending: 4 * p.workers,
	})
	if err != nil {
		return fmt.Errorf("execute consumer: %w", err)
	}

	sem := make(chan struct{}, p.workers)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			p.handle(ctx, msg)
		}()
	})
	if err != nil {
		return fmt.Errorf("consume execute: %w", err)
	}
	p.log.Info("started", "workers", p.workers)
	<-ctx.Done()
	cc.Stop()
	return nil
}

func (p *Pool) handle(ctx context.Context, msg jetstream.Msg) {
	var cmd ExecuteTick
	if err := json.Unmarshal(msg.Data(), &cmd); err != nil {
		p.log.Error("bad execute command, terminating", "err", err)
		_ = msg.Term()
		return
	}
	log := p.log.With("sector_id", cmd.SectorID, "tick", cmd.Tick)

	stop := keepAlive(ctx, msg, 10*time.Second)
	defer stop()

	subject := streams.SectorSubject(cmd.SectorID)
	lastTick, lastSeq, err := p.head(ctx, subject)
	if err != nil {
		log.Warn("read sector head", "err", err)
		_ = msg.NakWithDelay(2 * time.Second)
		return
	}

	if lastTick >= cmd.Tick {
		// Already resolved (a duplicate command, or a redelivery after a
		// stalled handler's append landed). Nothing to record.
		_ = msg.Ack()
		return
	}

	// Is this tick's predecessor resolved? Every sector exists from the big
	// bang, but an idle one has never resolved anything, so its stream is
	// empty and there is no predecessor to be out of order with: it resolves
	// whatever tick first gives it work. A sector with history resolves
	// strictly in order.
	if lastSeq != 0 && lastTick != cmd.Tick-1 {
		// The predecessor's command is in the queue (straggler re-dispatch
		// covers loss). Wait for it.
		_ = msg.NakWithDelay(time.Second)
		return
	}

	rctx, cancel := context.WithTimeout(ctx, resolveTimeout)
	outcomes, pending, err := p.resolve.Resolve(rctx, cmd.SectorID, cmd.Tick)
	cancel()
	if err != nil {
		log.Error("resolve", "err", err)
		_ = msg.NakWithDelay(2 * time.Second)
		return
	}

	ev := TickResolved{Type: "TickResolved", SectorID: cmd.SectorID, Tick: cmd.Tick, Events: outcomes, Pending: pending}
	if _, err := streams.Append(ctx, p.js, subject, ev, lastSeq); err != nil {
		if errors.Is(err, streams.ErrConflict) {
			// Another handler appended first; the tick is resolved.
			_ = msg.Ack()
			return
		}
		log.Warn("append TickResolved", "err", err)
		_ = msg.NakWithDelay(2 * time.Second)
		return
	}
	_ = msg.Ack()
}

// head returns the last resolved tick and stream sequence for a sector
// subject. A sector with no events yet reports tick -1, sequence 0.
func (p *Pool) head(ctx context.Context, subject string) (int64, uint64, error) {
	var ev TickResolved
	seq, err := streams.Last(ctx, p.js, streams.StreamEvents, subject, &ev)
	if err != nil {
		return 0, 0, err
	}
	if seq == 0 {
		return -1, 0, nil
	}
	return ev.Tick, seq, nil
}

// keepAlive extends the ack deadline while a handler works, so a live
// handler never loses its command to redelivery.
func keepAlive(ctx context.Context, msg jetstream.Msg, every time.Duration) (stop func()) {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				_ = msg.InProgress()
			}
		}
	}()
	return func() { close(done) }
}
