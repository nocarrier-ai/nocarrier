// Package service hosts the command and query micro services. Both use core
// NATS queue groups, so requests load-balance across instances. Commands are
// ephemeral request/reply: they validate and append events, or reject.
package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

type Services struct {
	nc  *nats.Conn
	log *slog.Logger
}

func New(nc *nats.Conn, log *slog.Logger) *Services {
	return &Services{nc: nc, log: log.With("loop", "services")}
}

func (s *Services) Name() string { return "services" }

func (s *Services) Run(ctx context.Context) error {
	cmd, err := micro.AddService(s.nc, micro.Config{
		Name:        "nocarrier-command",
		Version:     "0.1.0",
		Description: "Validates player commands and appends events",
	})
	if err != nil {
		return fmt.Errorf("command service: %w", err)
	}
	defer func() { _ = cmd.Stop() }()

	grp := cmd.AddGroup("cmd")
	if err := grp.AddEndpoint("doctrine-update", micro.HandlerFunc(s.doctrineUpdate),
		micro.WithEndpointSubject("doctrine.update")); err != nil {
		return fmt.Errorf("endpoint: %w", err)
	}

	qry, err := micro.AddService(s.nc, micro.Config{
		Name:        "nocarrier-query",
		Version:     "0.1.0",
		Description: "Serves decision log pages and other history reads",
	})
	if err != nil {
		return fmt.Errorf("query service: %w", err)
	}
	defer func() { _ = qry.Stop() }()

	qgrp := qry.AddGroup("query")
	if err := qgrp.AddEndpoint("decision-log", micro.HandlerFunc(s.decisionLog),
		micro.WithEndpointSubject("decisions.page")); err != nil {
		return fmt.Errorf("endpoint: %w", err)
	}

	s.log.Info("started")
	<-ctx.Done()
	return nil
}

func (s *Services) doctrineUpdate(req micro.Request) {
	// TODO: decode proto/api command, validate, append DoctrineUpdated to
	// avatar.<id>, reply with the accepted revision.
	_ = req.Error("501", "not implemented", nil)
}

func (s *Services) decisionLog(req micro.Request) {
	// TODO: decode page request, read decisions.<avatar> by time window
	// with an ordered consumer, reply with proto/api read models.
	_ = req.Error("501", "not implemented", nil)
}
