// Command nocarrierd is the whole NoCarrier backend. Every instance runs the
// same loops: pacer, execute pool, decide pool, projections, and the
// command/query services. Run more instances to automatically
// scale
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/decide"
	"github.com/nocarrier-ai/nocarrier/internal/devnats"
	"github.com/nocarrier-ai/nocarrier/internal/dispatch"
	"github.com/nocarrier-ai/nocarrier/internal/execute"
	"github.com/nocarrier-ai/nocarrier/internal/loop"
	"github.com/nocarrier-ai/nocarrier/internal/project"
	"github.com/nocarrier-ai/nocarrier/internal/service"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
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

	opts := []nats.Option{
		nats.Name("nocarrierd-" + cfg.instanceID),
		nats.MaxReconnects(-1),
	}
	if cfg.natsCreds != "" {
		opts = append(opts, nats.UserCredentials(cfg.natsCreds))
	}
	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer func() {
		_ = nc.FlushTimeout(2 * time.Second)
		nc.Close()
	}()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}

	universe, err := ensureUniverse(ctx, js, cfg, log)
	if err != nil {
		return err
	}
	log.Info("universe", "tick_period", universe.TickPeriod.String())

	// Wiring. Resolver, Model, and ReadModels get real implementations as
	// the game rules land; the seams keep the loops testable meanwhile.

	active, err := project.NewActiveSectors(ctx, js, log)
	if err != nil {
		return err
	}
	dispatcher := dispatch.New(js, active, log)
	pacer, err := clock.NewPacer(js, log, cfg.instanceID, universe.TickPeriod, dispatcher)
	if err != nil {
		return err
	}
	execPool := execute.NewPool(js, log, cfg.executeWorkers, toyResolver{})
	decidePool := decide.NewPool(js, log, cfg.decideWorkers, notImplementedModel{})
	services := service.New(nc, log)

	health := startHealth(cfg.httpAddr, nc, log)
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = health.Shutdown(sctx)
	}()

	loops := []loop.Loop{pacer, execPool, decidePool, services}
	for _, l := range active.Loops(js, log) {
		loops = append(loops, l)
	}

	// TODO: append project.NewLoop(js, log, <projection>) per projection
	// once the concrete projections exist.
	return loop.Supervise(ctx, log, loops...)
}

// ensureUniverse creates streams and buckets, and appends UniverseCreated if
// the CLOCK stream is empty. The guarded first append means concurrent
// instances race safely: one wins, the rest read the winner's event.
func ensureUniverse(ctx context.Context, js jetstream.JetStream, cfg config, log *slog.Logger) (clock.UniverseCreated, error) {
	var u clock.UniverseCreated
	if err := streams.Ensure(ctx, js, cfg.replicas, cfg.tickPeriod); err != nil {
		return u, fmt.Errorf("ensure streams: %w", err)
	}
	s, err := js.Stream(ctx, streams.StreamClock)
	if err != nil {
		return u, err
	}
	raw, err := s.GetLastMsgForSubject(ctx, streams.SubjectClock)
	switch {
	case err == nil:
		// Universe exists. UniverseCreated is always the first message.
		first, ferr := s.GetMsg(ctx, 1)
		if ferr != nil {
			return u, fmt.Errorf("read UniverseCreated: %w", ferr)
		}
		if jerr := json.Unmarshal(first.Data, &u); jerr != nil || u.Type != "UniverseCreated" {
			return u, fmt.Errorf("first clock event is not UniverseCreated")
		}
		_ = raw
		return u, nil
	case errors.Is(err, jetstream.ErrMsgNotFound):
		u = clock.UniverseCreated{Type: "UniverseCreated", TickPeriod: cfg.tickPeriod, Seed: time.Now().UnixNano()}
		data, merr := json.Marshal(u)
		if merr != nil {
			return u, merr
		}
		msg := nats.NewMsg(streams.SubjectClock)
		msg.Data = data
		msg.Header.Set("Nats-Expected-Last-Subject-Sequence", "0")
		if _, perr := js.PublishMsg(ctx, msg); perr != nil {
			// Lost the race: another instance created it. Re-read.
			log.Info("universe already created by another instance")
			return ensureUniverse(ctx, js, cfg, log)
		}
		log.Info("universe created")
		return u, nil
	default:
		return u, err
	}
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
	httpAddr       string
	logLevel       string
	instanceID     string
}

func loadConfig() (config, error) {
	c := config{
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
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})).
		With("instance", instanceID)
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

// Seams awaiting real implementations.

type toyResolver struct{}

func (toyResolver) Resolve(_ context.Context, sector string, tick int64) (json.RawMessage, bool, error) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(sector))
	lifetime := int64(3 + h.Sum32()%8) // sector stays active 3-10 ticks
	out, err := json.Marshal([]map[string]any{{
		"type":   "ToyCounterIncremented",
		"sector": sector,
		"tick":   tick,
	}})
	if err != nil {
		return nil, false, err
	}
	return out, tick%lifetime != lifetime-1, nil
}

type notImplementedModel struct{}

func (notImplementedModel) Decide(context.Context, string, int64) (json.RawMessage, json.RawMessage, error) {
	return nil, nil, errors.New("decision model not implemented")
}
