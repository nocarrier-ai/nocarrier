// Command nocarrierd is the whole NoCarrier backend. Every instance runs the
// same loops: pacer, execute pool, decide pool, projections, and the
// command/query services. Run more instances to automatically
// scale
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nocarrier-ai/nocarrier/internal/avatar"
	"github.com/nocarrier-ai/nocarrier/internal/bigbang"
	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/decide"
	"github.com/nocarrier-ai/nocarrier/internal/devnats"
	"github.com/nocarrier-ai/nocarrier/internal/dispatch"
	"github.com/nocarrier-ai/nocarrier/internal/doctrine"
	"github.com/nocarrier-ai/nocarrier/internal/loop"
	"github.com/nocarrier-ai/nocarrier/internal/natsconn"
	"github.com/nocarrier-ai/nocarrier/internal/player"
	"github.com/nocarrier-ai/nocarrier/internal/port"
	"github.com/nocarrier-ai/nocarrier/internal/projector"
	"github.com/nocarrier-ai/nocarrier/internal/sector"
	"github.com/nocarrier-ai/nocarrier/internal/service"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "nocarrierd:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	log := newLogger(cfg.logLevel, cfg.instanceID)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	natsURL := cfg.natsURL
	if cfg.embeddedNATS {
		ns, err := devnats.Start("127.0.0.1", 4222, cfg.embeddedStore)
		if err != nil {
			return fmt.Errorf("embedded nats: %w", err)
		}
		defer func() { ns.Shutdown(); ns.WaitForShutdown() }()
		natsURL = ns.ClientURL()
		log.Info("embedded nats started", "url", natsURL)
	}

	nc, js, err := natsconn.Open(ctx, natsconn.Config{
		URL:        natsURL,
		Creds:      cfg.natsCreds,
		Name:       "nocarrierd-" + cfg.instanceID,
		Replicas:   cfg.replicas,
		TickPeriod: cfg.tickPeriod,
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = nc.FlushTimeout(2 * time.Second)
		nc.Close()
	}()

	rep := newReporter(os.Stdout, !cfg.noColor && isTerminal(os.Stdout))
	bang, err := bigbang.Ensure(ctx, js, bigbang.Options{Sectors: cfg.sectors, TickPeriod: cfg.tickPeriod}, rep)
	if err != nil {
		return err
	}
	u := bang.Map
	rep.universe(bang)

	active, err := projector.NewActiveSectors(ctx, js, log)
	if err != nil {
		return err
	}
	doctrineProjection, err := projector.NewDoctrine(ctx, js)
	if err != nil {
		return err
	}
	avatarStatus, err := projector.NewAvatarStatus(ctx, js)
	if err != nil {
		return err
	}
	dueAvatars, err := projector.NewDueAvatars(ctx, js)
	if err != nil {
		return err
	}
	portStatus, err := projector.NewPortStatus(ctx, js)
	if err != nil {
		return err
	}
	playerStatus, err := projector.NewPlayerStatus(ctx, js)
	if err != nil {
		return err
	}
	universeClock, err := projector.NewUniverseClock(ctx, js)
	if err != nil {
		return err
	}
	universeCounts, err := projector.NewUniverseCounts(ctx, js)
	if err != nil {
		return err
	}
	dispatcher := dispatch.New(js, active, dueAvatars, log)
	pacer, err := clock.NewPacer(js, log, cfg.instanceID, bang.Created.TickPeriod, dispatcher)
	if err != nil {
		return err
	}
	execPool := sector.NewPool(js, log, cfg.executeWorkers, sector.ToyResolver{})
	decidePool := decide.NewPool(js, log, cfg.decideWorkers, decide.NotImplementedModel{})
	services := service.New(nc, log, doctrine.NewHandler(js), avatar.NewHandler(js), port.NewHandler(js, u),
		player.NewHandler(js, playerStatus))

	health := startHealth(cfg.httpAddr, nc, log)
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = health.Shutdown(sctx)
	}()

	loops := []loop.Loop{
		pacer, execPool, decidePool, services,
		projector.NewLoop(js, log, active),
		projector.NewLoop(js, log, doctrineProjection),
		projector.NewLoop(js, log, avatarStatus),
		projector.NewLoop(js, log, dueAvatars),
		projector.NewLoop(js, log, portStatus),
		projector.NewLoop(js, log, playerStatus),
		projector.NewLoop(js, log, universeClock),
		projector.NewLoop(js, log, universeCounts),
	}
	return loop.Supervise(ctx, log, loops...)
}

func startHealth(addr string, nc *nats.Conn, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !nc.IsConnected() {
			http.Error(w, "nats disconnected", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server", "err", err)
		}
	}()
	return srv
}

type config struct {
	natsURL        string
	natsCreds      string
	embeddedNATS   bool
	embeddedStore  string
	replicas       int
	tickPeriod     time.Duration
	executeWorkers int
	decideWorkers  int
	jevURL         string
	sectors        int
	httpAddr       string
	logLevel       string
	instanceID     string
	noColor        bool
}

func loadConfig() (config, error) {
	c := config{
		noColor:        os.Getenv("NO_COLOR") != "",
		natsURL:        envOr("NATS_URL", nats.DefaultURL),
		natsCreds:      os.Getenv("NATS_CREDS"),
		embeddedStore:  envOr("NOCARRIER_EMBEDDED_STORE", ".data/nats"),
		jevURL:         os.Getenv("NOCARRIER_JEV_URL"),
		httpAddr:       envOr("NOCARRIER_HTTP_ADDR", ":8080"),
		logLevel:       envOr("NOCARRIER_LOG_LEVEL", "info"),
		instanceID:     newInstanceID(),
		executeWorkers: runtime.NumCPU(),
		decideWorkers:  32,
		replicas:       1,
		tickPeriod:     time.Minute,
		sectors:        1000,
	}
	var err error
	if c.embeddedNATS, err = envBool("NOCARRIER_EMBEDDED_NATS", false); err != nil {
		return c, err
	}
	if c.replicas, err = envInt("NOCARRIER_REPLICAS", c.replicas); err != nil {
		return c, err
	}
	if c.executeWorkers, err = envInt("NOCARRIER_EXECUTE_WORKERS", c.executeWorkers); err != nil {
		return c, err
	}
	if c.decideWorkers, err = envInt("NOCARRIER_DECIDE_WORKERS", c.decideWorkers); err != nil {
		return c, err
	}
	if c.sectors, err = envInt("NOCARRIER_SECTORS", c.sectors); err != nil {
		return c, err
	}
	if c.tickPeriod, err = envDuration("NOCARRIER_TICK_PERIOD", c.tickPeriod); err != nil {
		return c, err
	}
	if c.tickPeriod < clock.MinTickPeriod {
		return c, fmt.Errorf("NOCARRIER_TICK_PERIOD %s is below the %s floor", c.tickPeriod, clock.MinTickPeriod)
	}
	if c.replicas < 1 || c.replicas > 5 {
		return c, errors.New("NOCARRIER_REPLICAS must be between 1 and 5")
	}
	return c, nil
}

// newInstanceID is random per process start, so restarts and cloned machines
// never collide. Used only to recognize this instance's own TickAdvanced.
func newInstanceID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("pid-%d-%d", os.Getpid(), time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func newLogger(level, instanceID string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: l,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.String(a.Key, a.Value.Time().Format(time.TimeOnly))
			}
			return a
		},
	})).With("instance", instanceID)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def, fmt.Errorf("%s: %w", key, err)
	}
	return b, nil
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
