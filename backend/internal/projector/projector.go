// Package projector runs the projections: sequential consumers over the event
// streams writing KV read models.
package projector

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"
)

// Projection folds events from one stream into read models.
type Projection interface {
	Name() string
	Stream() string
	FilterSubjects() []string
	Apply(ctx context.Context, msg jetstream.Msg) error
}

// Loop runs one projection as a sequential durable consumer.
type Loop struct {
	js   jetstream.JetStream
	log  *slog.Logger
	proj Projection
}

func NewLoop(js jetstream.JetStream, log *slog.Logger, p Projection) *Loop {
	return &Loop{js: js, log: log.With("loop", "projector", "projection", p.Name()), proj: p}
}

func (l *Loop) Name() string { return "projector-" + l.proj.Name() }

func (l *Loop) Run(ctx context.Context) error {
	cons, err := l.js.CreateOrUpdateConsumer(ctx, l.proj.Stream(), jetstream.ConsumerConfig{
		Durable:        "proj-" + l.proj.Name(),
		FilterSubjects: l.proj.FilterSubjects(),
		AckPolicy:      jetstream.AckExplicitPolicy,
		// Sequential by design: one message in flight keeps stream order.
		MaxAckPending: 1,
	})
	if err != nil {
		return fmt.Errorf("consumer: %w", err)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		if err := l.proj.Apply(ctx, msg); err != nil {
			l.log.Warn("apply", "err", err)
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	l.log.Info("started")
	<-ctx.Done()
	cc.Stop()
	return nil
}
