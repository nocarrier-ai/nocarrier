package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nocarrier-ai/nocarrier/internal/devnats"
	"github.com/nocarrier-ai/nocarrier/internal/roles/command"
	"github.com/nocarrier-ai/nocarrier/internal/roles/dataread"
	"github.com/nocarrier-ai/nocarrier/internal/roles/decide"
	"github.com/nocarrier-ai/nocarrier/internal/roles/project"
	"github.com/nocarrier-ai/nocarrier/internal/roles/role"
	"github.com/nocarrier-ai/nocarrier/internal/roles/sim"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

var factories = map[string]role.Factory{
	role.Sim:      sim.New,
	role.Decide:   decide.New,
	role.Project:  project.New,
	role.Command:  command.New,
	role.DataRead: dataread.New,
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "nocarrierd:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := parseFlags()
	if err := validate(cfg); err != nil {
		return err
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.logLevel)); err != nil {
		return fmt.Errorf("--log-level: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	// Fail fast on a bad --roles value before touching the network.
	names, err := role.Parse(cfg.roles)
	if err != nil {
		return fmt.Errorf("--roles: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	natsURL := cfg.natsURL
	if cfg.embeddedNATS {
		ns, err := devnats.Start("127.0.0.1", 4222, cfg.embeddedStore)
		if err != nil {
			return fmt.Errorf("embedded nats: %w", err)
		}
		defer func() {
			ns.Shutdown()
			ns.WaitForShutdown()
		}()
		natsURL = ns.ClientURL()
		log.Info("embedded nats started", "url", natsURL, "store", cfg.embeddedStore)
	}

	nc, err := connect(natsURL, cfg, log)
	if err != nil {
		return err
	}
	// Deferred after the embedded server, so it runs first on the way out.
	defer func() {
		if err := nc.FlushTimeout(2 * time.Second); err != nil {
			log.Warn("nats flush", "err", err)
		}
		nc.Close()
	}()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}

	if cfg.ensureStreams {
		ectx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := streams.Ensure(ectx, js, cfg.partitions, cfg.replicas)
		cancel()
		if err != nil {
			return fmt.Errorf("ensure streams: %w", err)
		}
	}

	opts := role.RoleOptions{
		Logger:     log,
		NC:         nc,
		JS:         js,
		InstanceID: cfg.instanceID,
		Partitions: cfg.partitions,
		TickPeriod: cfg.tick,
	}
	roles := make([]role.Role, 0, len(names))
	for _, name := range names {
		factory, ok := factories[name]
		if !ok {
			return fmt.Errorf("role %q has no factory", name)
		}
		r, err := factory(opts)
		if err != nil {
			return fmt.Errorf("build role %s: %w", name, err)
		}
		roles = append(roles, r)
	}
	log.Info("starting roles", "roles", names)
	return supervise(ctx, roles, cfg.shutdownGrace, log)
}

func supervise(ctx context.Context, roles []role.Role, grace time.Duration, log *slog.Logger) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type exit struct {
		name string
		err  error
	}
	exits := make(chan exit, len(roles))
	for _, r := range roles {
		go func() {
			defer func() {
				if p := recover(); p != nil {
					exits <- exit{r.Name(), fmt.Errorf("panic: %v", p)}
				}
			}()
			exits <- exit{r.Name(), r.Run(runCtx)}
		}()
	}

	var failure error
	remaining := len(roles)
	select {
	case <-ctx.Done():
		log.Info("shutdown requested")
	case e := <-exits:
		remaining--
		err := e.err
		if err == nil {
			err = errors.New("exited unexpectedly")
		}
		failure = fmt.Errorf("role %s: %w", e.name, err)
		log.Error("role stopped, shutting down", "role", e.name, "err", err)
	}
	cancel()

	deadline := time.After(grace)
	for remaining > 0 {
		select {
		case e := <-exits:
			remaining--
			if e.err != nil && !errors.Is(e.err, context.Canceled) {
				log.Error("role stopped with error", "role", e.name, "err", e.err)
				failure = errors.Join(failure, fmt.Errorf("role %s: %w", e.name, e.err))
			} else {
				log.Info("role stopped", "role", e.name)
			}
		case <-deadline:
			log.Error("shutdown grace period expired", "still_running", remaining)
			return errors.Join(failure, errors.New("shutdown timed out"))
		}
	}
	return failure
}

func connect(url string, cfg config, log *slog.Logger) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name("nocarrierd-" + cfg.instanceID),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("nats disconnected", "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("nats reconnected", "url", nc.ConnectedUrl())
		}),
	}
	if cfg.natsCreds != "" {
		opts = append(opts, nats.UserCredentials(cfg.natsCreds))
	}
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to nats at %s: %w", url, err)
	}
	return nc, nil
}
