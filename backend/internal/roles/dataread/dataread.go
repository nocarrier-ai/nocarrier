package dataread

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/micro"

	"github.com/nocarrier-ai/nocarrier/internal/roles/role"
)

type dataReadRole struct {
	opts role.RoleOptions
}

func New(deps role.RoleOptions) (role.Role, error) {
	return &dataReadRole{opts: deps}, nil
}

func (q *dataReadRole) Name() string { return role.DataRead }

func (q *dataReadRole) Run(ctx context.Context) error {
	log := q.opts.Logger.With("role", role.DataRead)

	svc, err := micro.AddService(q.opts.NC, micro.Config{
		Name:        "nocarrier-read",
		Version:     "0.1.0",
		Description: "Serves decision log pages and other history queries",
	})
	if err != nil {
		return fmt.Errorf("add service: %w", err)
	}
	defer func() {
		if err := svc.Stop(); err != nil {
			log.Warn("service stop", "err", err)
		}
	}()

	grp := svc.AddGroup("read")
	if err := grp.AddEndpoint("decision-log", micro.HandlerFunc(q.decisionLog),
		micro.WithEndpointSubject("decisions.page")); err != nil {
		return fmt.Errorf("endpoint decision-log: %w", err)
	}

	log.Info("started")
	<-ctx.Done()
	log.Info("stopped")
	return nil
}

// decisionLog handles query.decisions.page.
func (q *dataReadRole) decisionLog(req micro.Request) {
	// TODO: decode the page request (avatar, time window), read DECISIONS with
	// an ordered consumer or direct get, upcast old events, and reply with
	// proto/api read models.
	_ = req.Error("501", "not implemented", nil)
}
