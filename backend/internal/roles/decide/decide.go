package decide

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/roles/role"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type decideRole struct {
	deps role.RoleOptions
}

func New(deps role.RoleOptions) (role.Role, error) {
	return &decideRole{deps: deps}, nil
}

func (d *decideRole) Name() string { return role.Decide }

func (d *decideRole) Run(ctx context.Context) error {
	log := d.deps.Logger.With("role", role.Decide)

	// One durable consumer shared by every instance running this role spreads
	// requests across them. MaxAckPending caps in-flight model calls across
	// the whole fleet, which doubles as a global inference guard.
	cons, err := d.deps.JS.CreateOrUpdateConsumer(ctx, streams.StreamDecide, jetstream.ConsumerConfig{
		Durable:       "decide",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       2 * time.Minute,
		MaxAckPending: 1000,
	})
	if err != nil {
		return fmt.Errorf("decide consumer: %w", err)
	}

	// The handler runs sequentially per consume context. Hand requests to a
	// worker pool here once model calls are real.
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		// TODO: decode the request, build state and questions, call the model
		// through the decision interface, enforce the tier budget, and publish
		// the decision record and intents.
		log.Debug("decision request", "subject", msg.Subject())
		if err := msg.Ack(); err != nil {
			log.Warn("ack failed", "err", err)
		}
	})
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	log.Info("started")

	<-ctx.Done()
	cc.Stop()
	log.Info("stopped")
	return nil
}
