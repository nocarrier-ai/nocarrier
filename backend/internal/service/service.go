// Package service hosts the command and query micro services. Both use core
// NATS queue groups, so requests load-balance across instances. Commands are
// ephemeral request/reply: they validate and append events, or reject.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/nocarrier-ai/nocarrier/internal/doctrine"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

const requestTimeout = 5 * time.Second

type DoctrineUpdater interface {
	Update(ctx context.Context, cmd doctrine.UpdateDoctrine) (doctrine.DoctrineUpdated, error)
}

type DoctrineUpdateReply struct {
	Revision int64  `json:"revision"`
	Tick     int64  `json:"tick"`
	Hash     string `json:"hash"`
}

type Services struct {
	nc       *nats.Conn
	log      *slog.Logger
	doctrine DoctrineUpdater
}

func New(nc *nats.Conn, log *slog.Logger, d DoctrineUpdater) *Services {
	return &Services{nc: nc, log: log.With("loop", "services"), doctrine: d}
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
	doctrineUpdate := func(req micro.Request) { s.doctrineUpdate(ctx, req) }
	if err := grp.AddEndpoint("doctrine-update", micro.HandlerFunc(doctrineUpdate),
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

func (s *Services) doctrineUpdate(ctx context.Context, req micro.Request) {
	var cmd doctrine.UpdateDoctrine
	if err := json.Unmarshal(req.Data(), &cmd); err != nil {
		_ = req.Error("400", "malformed doctrine update: "+err.Error(), nil)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	ev, err := s.doctrine.Update(ctx, cmd)
	switch {
	case err == nil:
		_ = req.RespondJSON(DoctrineUpdateReply{Revision: ev.Revision, Tick: ev.Tick, Hash: ev.Hash})
	case errors.Is(err, doctrine.ErrInvalid):
		_ = req.Error("400", err.Error(), nil)
	case errors.Is(err, streams.ErrConflict):
		_ = req.Error("409", "doctrine changed concurrently, resubmit", nil)
	default:
		s.log.Error("doctrine update", "avatar_id", cmd.AvatarID, "err", err)
		_ = req.Error("500", "doctrine update failed", nil)
	}
}

func (s *Services) decisionLog(req micro.Request) {
	// TODO: decode page request, read decisions.<avatar> by time window
	// with an ordered consumer, reply with proto/api read models.
	_ = req.Error("501", "not implemented", nil)
}
