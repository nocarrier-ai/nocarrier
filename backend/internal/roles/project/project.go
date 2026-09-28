package project

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/roles/role"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type projectRole struct {
	deps role.RoleOptions
}

func New(deps role.RoleOptions) (role.Role, error) {
	return &projectRole{deps: deps}, nil
}

func (p *projectRole) Name() string { return role.Project }

func (p *projectRole) Run(ctx context.Context) error {
	log := p.deps.Logger.With("role", role.Project)

	var consumers []jetstream.ConsumeContext
	defer func() {
		for _, cc := range consumers {
			cc.Stop()
		}
	}()

	for part := range p.deps.Partitions {
		stream := streams.WorldStream(part)
		// MaxAckPending of 1 keeps each partition in order even when several
		// instances share the durable. Raise it once partitions are assigned
		// to specific instances with leases, the same way sim assigns sectors.
		cons, err := p.deps.JS.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
			Durable:       "project-read-models",
			AckPolicy:     jetstream.AckExplicitPolicy,
			MaxAckPending: 1,
		})
		if err != nil {
			return fmt.Errorf("consumer on %s: %w", stream, err)
		}
		cc, err := cons.Consume(func(msg jetstream.Msg) {
			// TODO: apply the committed tick to avatar status, leaderboards,
			// and stats rollups, then publish avatar.<id>.feed messages.
			if err := msg.Ack(); err != nil {
				log.Warn("ack failed", "stream", stream, "err", err)
			}
		})
		if err != nil {
			return fmt.Errorf("consume %s: %w", stream, err)
		}
		consumers = append(consumers, cc)
	}
	log.Info("started", "partitions", p.deps.Partitions)

	<-ctx.Done()
	log.Info("stopped")
	return nil
}
