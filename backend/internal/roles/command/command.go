package command

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/micro"
	"github.com/nocarrier-ai/nocarrier/internal/roles/role"
)

type commandRole struct {
	opts role.RoleOptions
}

func New(opts role.RoleOptions) (role.Role, error) {
	return &commandRole{opts: opts}, nil
}

func (c *commandRole) Name() string { return role.Command }

func (c *commandRole) Run(ctx context.Context) error {
	log := c.opts.Logger.With("role", role.Command)

	// Every instance registers the same service, so requests are load
	// balanced across instances through the service's queue group.
	svc, err := micro.AddService(c.opts.NC, micro.Config{
		Name:        "nocarrier-command",
		Version:     "0.1.0",
		Description: "Validates player commands and appends events",
	})
	if err != nil {
		return fmt.Errorf("add service: %w", err)
	}
	defer func() {
		if err := svc.Stop(); err != nil {
			log.Warn("service stop", "err", err)
		}
	}()

	cmd := svc.AddGroup("cmd")
	if err := cmd.AddEndpoint("doctrine-update", micro.HandlerFunc(c.doctrineUpdate),
		micro.WithEndpointSubject("doctrine.update")); err != nil {
		return fmt.Errorf("endpoint doctrine-update: %w", err)
	}

	log.Info("started")
	<-ctx.Done()

	return nil
}

// doctrineUpdate handles cmd.doctrine.update.
func (c *commandRole) doctrineUpdate(req micro.Request) {
	// TODO: decode the proto/api command, validate it against the avatar,
	// append it to the DOCTRINE stream, and reply with the new revision.
	_ = req.Error("501", "not implemented", nil)
}
