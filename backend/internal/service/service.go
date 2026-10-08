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

	"github.com/nocarrier-ai/nocarrier/internal/avatar"
	"github.com/nocarrier-ai/nocarrier/internal/doctrine"
	"github.com/nocarrier-ai/nocarrier/internal/port"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

const requestTimeout = 5 * time.Second

type DoctrineUpdater interface {
	Update(ctx context.Context, cmd doctrine.UpdateDoctrine) (doctrine.DoctrineUpdated, error)
}

type AdmiralCommissioner interface {
	Commission(ctx context.Context, cmd avatar.CommissionAdmiral) (avatar.AdmiralCommissioned, error)
}

type PortTrader interface {
	Trade(ctx context.Context, cmd port.Trade) (port.TradeCompleted, error)
}

type DoctrineUpdateReply struct {
	Revision int64  `json:"revision"`
	Tick     int64  `json:"tick"`
	Hash     string `json:"hash"`
}

// AvatarCommissionReply tells the player where their flagship appeared and
// when. Accepted means the admiral exists, not that it has done anything yet.
type AvatarCommissionReply struct {
	AvatarID   string `json:"avatar_id"`
	ShipName   string `json:"ship_name"`
	HomeSector string `json:"home_sector"`
	Tick       int64  `json:"tick"`
}

// PortTradeReply tells the ship what it traded and what the port has left of
// that commodity.
type PortTradeReply struct {
	SectorID  string             `json:"sector_id"`
	ShipID    string             `json:"ship_id"`
	Commodity universe.Commodity `json:"commodity"`
	Units     int                `json:"units"`
	Available int                `json:"available"`
	Tick      int64              `json:"tick"`
}

type Services struct {
	nc       *nats.Conn
	log      *slog.Logger
	doctrine DoctrineUpdater
	avatars  AdmiralCommissioner
	ports    PortTrader
}

func New(nc *nats.Conn, log *slog.Logger, d DoctrineUpdater, a AdmiralCommissioner, p PortTrader) *Services {
	return &Services{nc: nc, log: log.With("loop", "services"), doctrine: d, avatars: a, ports: p}
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
	avatarCommission := func(req micro.Request) { s.avatarCommission(ctx, req) }
	if err := grp.AddEndpoint("avatar-commission", micro.HandlerFunc(avatarCommission),
		micro.WithEndpointSubject("avatar.commission")); err != nil {
		return fmt.Errorf("endpoint: %w", err)
	}
	portTrade := func(req micro.Request) { s.portTrade(ctx, req) }
	if err := grp.AddEndpoint("port-trade", micro.HandlerFunc(portTrade),
		micro.WithEndpointSubject("port.trade")); err != nil {
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

func (s *Services) avatarCommission(ctx context.Context, req micro.Request) {
	var cmd avatar.CommissionAdmiral
	if err := json.Unmarshal(req.Data(), &cmd); err != nil {
		_ = req.Error("400", "malformed admiral commission: "+err.Error(), nil)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	ev, err := s.avatars.Commission(ctx, cmd)
	switch {
	case err == nil:
		_ = req.RespondJSON(AvatarCommissionReply{
			AvatarID:   ev.AvatarID,
			ShipName:   ev.ShipName,
			HomeSector: ev.HomeSector,
			Tick:       ev.Tick,
		})
	case errors.Is(err, avatar.ErrInvalid):
		_ = req.Error("400", err.Error(), nil)
	case errors.Is(err, avatar.ErrAlreadyCommissioned):
		// Permanent, unlike the doctrine 409: resubmitting never helps.
		_ = req.Error("409", "admiral already commissioned", nil)
	default:
		s.log.Error("avatar commission", "avatar_id", cmd.AvatarID, "err", err)
		_ = req.Error("500", "admiral commission failed", nil)
	}
}

func (s *Services) portTrade(ctx context.Context, req micro.Request) {
	var cmd port.Trade
	if err := json.Unmarshal(req.Data(), &cmd); err != nil {
		_ = req.Error("400", "malformed trade: "+err.Error(), nil)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	ev, err := s.ports.Trade(ctx, cmd)
	switch {
	case err == nil:
		_ = req.RespondJSON(PortTradeReply{
			SectorID:  ev.SectorID,
			ShipID:    ev.ShipID,
			Commodity: ev.Commodity,
			Units:     ev.Units,
			Available: ev.Available[ev.Commodity],
			Tick:      ev.Tick,
		})
	case errors.Is(err, port.ErrInvalid), errors.Is(err, port.ErrNoPort):
		_ = req.Error("400", err.Error(), nil)
	case errors.Is(err, port.ErrInsufficient):
		_ = req.Error("422", err.Error(), nil)
	case errors.Is(err, streams.ErrConflict):
		_ = req.Error("409", "port traded concurrently, resubmit", nil)
	default:
		s.log.Error("port trade", "sector_id", cmd.SectorID, "err", err)
		_ = req.Error("500", "trade failed", nil)
	}
}

func (s *Services) decisionLog(req micro.Request) {
	// TODO: decode page request, read decisions.<avatar> by time window
	// with an ordered consumer, reply with proto/api read models.
	_ = req.Error("501", "not implemented", nil)
}
