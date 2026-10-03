package decide

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

const PlanRevisedType = "PlanRevised"

// DecideNow commands a decision pass for one avatar at tick Tick.
type DecideNow struct {
	AvatarID string `json:"avatar_id"`
	Tick     int64  `json:"tick"`
}

// PlanRevised carries an avatar's full new intent queue. The current plan is
// the last PlanRevised plus completions recorded in TickResolved events.
type PlanRevised struct {
	Type     string          `json:"type"` // PlanRevisedType
	AvatarID string          `json:"avatar_id"`
	Tick     int64           `json:"tick"`
	Intents  json.RawMessage `json:"intents"`
}

// Model is the decision-model seam: Noul/Choice/Score questions in, typed
// answers out. Implemented against Jev; swappable for a self-hosted server.
type Model interface {
	Decide(ctx context.Context, avatarID string, tick int64) (intents, record json.RawMessage, err error)
}

type Pool struct {
	js      jetstream.JetStream
	log     *slog.Logger
	workers int
	model   Model
}

func NewPool(js jetstream.JetStream, log *slog.Logger, workers int, m Model) *Pool {
	return &Pool{js: js, log: log.With("loop", "decide"), workers: workers, model: m}
}

func (p *Pool) Name() string { return "decide" }

func (p *Pool) Run(ctx context.Context) error {
	cons, err := p.js.CreateOrUpdateConsumer(ctx, streams.StreamDecide, jetstream.ConsumerConfig{
		Durable:   "decide",
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   2 * time.Minute,
		// Fleet-wide in-flight model call cap: shared across every
		// instance's workers pulling from this durable.
		MaxAckPending: 1000,
	})
	if err != nil {
		return fmt.Errorf("decide consumer: %w", err)
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
		return fmt.Errorf("consume decide: %w", err)
	}
	p.log.Info("started", "workers", p.workers)
	<-ctx.Done()
	cc.Stop()
	return nil
}

func (p *Pool) handle(ctx context.Context, msg jetstream.Msg) {
	var cmd DecideNow
	if err := json.Unmarshal(msg.Data(), &cmd); err != nil {
		p.log.Error("bad decide command, terminating", "err", err)
		_ = msg.Term()
		return
	}
	log := p.log.With("avatar_id", cmd.AvatarID, "tick", cmd.Tick)

	intents, record, err := p.model.Decide(ctx, cmd.AvatarID, cmd.Tick)
	if err != nil {
		log.Warn("model", "err", err)
		// The plan keeps executing without a revision; retry via redelivery.
		_ = msg.NakWithDelay(5 * time.Second)
		return
	}

	rev := PlanRevised{Type: PlanRevisedType, AvatarID: cmd.AvatarID, Tick: cmd.Tick, Intents: intents}
	if err := p.append(ctx, streams.PlanSubject(cmd.AvatarID), rev); err != nil {
		log.Warn("append PlanRevised", "err", err)
		_ = msg.NakWithDelay(2 * time.Second)
		return
	}
	if err := p.append(ctx, streams.DecisionsSubject(cmd.AvatarID), json.RawMessage(record)); err != nil {
		// The plan revision landed; the log record is best-effort enough to
		// retry inline rather than redeliver and double-revise.
		log.Warn("append decision record", "err", err)
	}
	_ = msg.Ack()
}

func (p *Pool) append(ctx context.Context, subject string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = p.js.Publish(ctx, subject, data)
	return err
}
