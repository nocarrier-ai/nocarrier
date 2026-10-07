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

	"github.com/nocarrier-ai/nocarrier/internal/avatar"
	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/decide"
	"github.com/nocarrier-ai/nocarrier/internal/devnats"
	"github.com/nocarrier-ai/nocarrier/internal/dispatch"
	"github.com/nocarrier-ai/nocarrier/internal/doctrine"
	"github.com/nocarrier-ai/nocarrier/internal/loop"
	"github.com/nocarrier-ai/nocarrier/internal/projector"
	"github.com/nocarrier-ai/nocarrier/internal/sector"
	"github.com/nocarrier-ai/nocarrier/internal/service"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
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

	bigBang, sectorMap, err := ensureUniverse(ctx, js, cfg, log)
	if err != nil {
		return err
	}
	log.Info("universe", "tick_period", bigBang.TickPeriod.String(),
		"sectors", sectorMap.Count(), "lanes", len(sectorMap.Lanes),
		"ports", len(sectorMap.Ports), "spawn", sectorMap.Spawn)

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
	dispatcher := dispatch.New(js, active, dueAvatars, log)
	pacer, err := clock.NewPacer(js, log, cfg.instanceID, bigBang.TickPeriod, dispatcher)
	if err != nil {
		return err
	}
	execPool := sector.NewPool(js, log, cfg.executeWorkers, toyResolver{})
	decidePool := decide.NewPool(js, log, cfg.decideWorkers, notImplementedModel{})
	services := service.New(nc, log, doctrine.NewHandler(js), avatar.NewHandler(js))

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
	}
	return loop.Supervise(ctx, log, loops...)
}

// ensureUniverse brings the universe into being, or finds the one that already
// exists. Two things have to be created exactly once: the sector map and the
// UniverseCreated event.
//
// The map goes first. Its KV create is the election — concurrent instances all
// generate a candidate, exactly one lands, and the losers read the winner's
// rather than their own. Only then is UniverseCreated appended, carrying the
// seed of a map that is already durable, so there is never a moment where the
// clock says a universe exists but its geography does not.
func ensureUniverse(ctx context.Context, js jetstream.JetStream, cfg config, log *slog.Logger) (clock.UniverseCreated, *universe.Universe, error) {
	fail := func(err error) (clock.UniverseCreated, *universe.Universe, error) {
		return clock.UniverseCreated{}, nil, err
	}
	if err := streams.Ensure(ctx, js, cfg.replicas, cfg.tickPeriod); err != nil {
		return fail(fmt.Errorf("ensure streams: %w", err))
	}
	store, err := universe.NewStore(ctx, js)
	if err != nil {
		return fail(err)
	}

	stored, err := store.Load(ctx)
	switch {
	case err == nil:
	case errors.Is(err, universe.ErrNoMap):
		candidate, gerr := universe.Generate(time.Now().UnixNano(), cfg.sectors)
		if gerr != nil {
			return fail(fmt.Errorf("generate universe: %w", gerr))
		}
		switch cerr := store.Create(ctx, candidate); {
		case cerr == nil:
			log.Info("universe map created", "sectors", candidate.Count(),
				"lanes", len(candidate.Lanes), "ports", len(candidate.Ports),
				"map_version", candidate.Version)
			stored = candidate
		case errors.Is(cerr, universe.ErrMapExists):
			// Another instance won the big bang; its map is the real one.
			if stored, err = store.Load(ctx); err != nil {
				return fail(fmt.Errorf("read the winning universe map: %w", err))
			}
		default:
			return fail(cerr)
		}
	default:
		return fail(err)
	}

	u := clock.UniverseCreated{Type: "UniverseCreated", TickPeriod: cfg.tickPeriod, Seed: stored.Seed}
	_, err = streams.Append(ctx, js, streams.SubjectClock, u, 0)
	switch {
	case err == nil:
		log.Info("universe created", "seed", u.Seed)
		return u, stored, nil
	case !errors.Is(err, streams.ErrConflict):
		return fail(fmt.Errorf("create universe: %w", err))
	}
	s, err := js.Stream(ctx, streams.StreamClock)
	if err != nil {
		return fail(err)
	}
	first, err := s.GetMsg(ctx, 1)
	if err != nil {
		return fail(fmt.Errorf("read UniverseCreated: %w", err))
	}
	var existing clock.UniverseCreated
	if err := json.Unmarshal(first.Data, &existing); err != nil || existing.Type != "UniverseCreated" {
		return fail(errors.New("first clock event is not UniverseCreated"))
	}
	// The clock and the map must agree about which universe this is. A
	// mismatch means one of them was wiped independently, and starting anyway
	// would move every ship.
	if existing.Seed != stored.Seed {
		return fail(fmt.Errorf("clock says universe seed %d but the stored map is seed %d",
			existing.Seed, stored.Seed))
	}
	return existing, stored, nil
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

func (toyResolver) Resolve(_ context.Context, sectorID string, tick int64) (json.RawMessage, bool, error) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(sectorID))
	lifetime := int64(3 + h.Sum32()%8) // sector stays active 3-10 ticks
	out, err := json.Marshal([]map[string]any{{
		"type":      "ToyCounterIncremented",
		"sector_id": sectorID,
		"tick":      tick,
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
