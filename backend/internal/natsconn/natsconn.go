package natsconn

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type Config struct {
	URL        string
	Creds      string
	Name       string
	Replicas   int
	TickPeriod time.Duration
}

func Open(ctx context.Context, cfg Config) (*nats.Conn, jetstream.JetStream, error) {
	opts := []nats.Option{nats.Name(cfg.Name), nats.MaxReconnects(-1)}
	if cfg.Creds != "" {
		opts = append(opts, nats.UserCredentials(cfg.Creds))
	}
	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("connect nats: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("jetstream: %w", err)
	}
	if err := streams.Ensure(ctx, js, cfg.Replicas, cfg.TickPeriod); err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("ensure streams: %w", err)
	}
	return nc, js, nil
}
