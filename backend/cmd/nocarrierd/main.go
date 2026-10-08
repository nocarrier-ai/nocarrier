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
	"github.com/nocarrier-ai/nocarrier/internal/planet"
	"github.com/nocarrier-ai/nocarrier/internal/port"
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

	rep := newReporter(os.Stdout, !cfg.noColor && isTerminal(os.Stdout))
	universeCreated, u, err := ensureUniverse(ctx, js, cfg, rep)
	if err != nil {
		return err
	}

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
	dispatcher := dispatch.New(js, active, dueAvatars, log)
	pacer, err := clock.NewPacer(js, log, cfg.instanceID, universeCreated.TickPeriod, dispatcher)
	if err != nil {
		return err
	}
	execPool := sector.NewPool(js, log, cfg.executeWorkers, toyResolver{})
	decidePool := decide.NewPool(js, log, cfg.decideWorkers, notImplementedModel{})
	services := service.New(nc, log, doctrine.NewHandler(js), avatar.NewHandler(js), port.NewHandler(js, u))

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
	}
	return loop.Supervise(ctx, log, loops...)
}

// ensureUniverse brings the universe into being, or finds the one that already
// exists. The big bang is three steps, each done exactly once: store the map,
// create every port and planet, append UniverseCreated.
//
// The map goes first. Its KV create is the election — concurrent instances all
// generate a candidate, exactly one lands, and the losers read the winner's
// rather than their own. Ports and planets come next, from the roster rolled
// beside the map, each as the first event on its own subject. UniverseCreated
// goes last, carrying the seed of a map that is already durable, so there is
// never a moment where the clock says a universe exists but its geography does
// not. Until it lands, any instance that finds the map completes the rest.
func ensureUniverse(ctx context.Context, js jetstream.JetStream, cfg config, rep reporter) (clock.UniverseCreated, *universe.Universe, error) {
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

	var (
		bb      *universe.BigBang // the roster, when this instance rolled the map
		bigBang bool
	)
	stored, err := store.Load(ctx)
	switch {
	case err == nil:
	case errors.Is(err, universe.ErrNoMap):
		rep.bigBangStarting(cfg.sectors)
		if bb, err = universe.Generate(time.Now().UnixNano(), cfg.sectors); err != nil {
			return fail(fmt.Errorf("generate universe: %w", err))
		}
		switch cerr := store.Create(ctx, bb.Map); {
		case cerr == nil:
			bigBang = true
			stored = bb.Map
		case errors.Is(cerr, universe.ErrMapExists):
			rep.bigBangLost()
			bb = nil
			if stored, err = store.Load(ctx); err != nil {
				return fail(fmt.Errorf("read the winning universe map: %w", err))
			}
		default:
			return fail(cerr)
		}
	default:
		return fail(err)
	}

	created, found, err := universeCreated(ctx, js)
	if err != nil {
		return fail(err)
	}
	if !found {
		if created, err = completeBigBang(ctx, js, cfg, rep, stored, bb); err != nil {
			return fail(err)
		}
	}
	// The clock and the map must agree about which universe this is. A
	// mismatch means one of them was wiped independently, and starting anyway
	// would move every ship.
	if created.Seed != stored.Seed {
		return fail(fmt.Errorf("clock says universe seed %d but the stored map is seed %d",
			created.Seed, stored.Seed))
	}
	ports, err := subjectCount(ctx, js, streams.PortEvents)
	if err != nil {
		return fail(err)
	}
	planets, err := subjectCount(ctx, js, streams.PlanetEvents)
	if err != nil {
		return fail(err)
	}
	rep.universe(stored, created.TickPeriod, bigBang, ports, planets)
	return created, stored, nil
}

// completeBigBang creates every port and planet in the roster, then appends
// UniverseCreated. Every step is idempotent — a port or planet that exists is
// skipped, and the clock append expects an empty subject — so an instance that
// crashed part way, or several arriving together, converge on one universe.
// An instance without the roster rolls it again from the stored seed.
func completeBigBang(ctx context.Context, js jetstream.JetStream, cfg config, rep reporter, stored *universe.Universe, bb *universe.BigBang) (clock.UniverseCreated, error) {
	if bb == nil {
		rep.bigBangResuming()
		var err error
		if bb, err = universe.Regenerate(stored); err != nil {
			return clock.UniverseCreated{}, fmt.Errorf("regenerate universe: %w", err)
		}
	}
	ports := port.NewHandler(js, stored)
	for _, p := range bb.Ports {
		_, err := ports.Create(ctx, port.CreatePort{SectorID: strconv.Itoa(p.Sector), Commodities: p.Commodities})
		if err != nil && !errors.Is(err, port.ErrAlreadyExists) {
			return clock.UniverseCreated{}, fmt.Errorf("create port in sector %d: %w", p.Sector, err)
		}
	}
	planets := planet.NewHandler(js, stored)
	for i, p := range bb.Planets {
		_, err := planets.Create(ctx, planet.CreatePlanet{
			PlanetID:  strconv.Itoa(i + 1),
			SectorID:  strconv.Itoa(p.Sector),
			Class:     p.Class,
			Colonists: p.InitialColonists,
		})
		if err != nil && !errors.Is(err, planet.ErrAlreadyExists) {
			return clock.UniverseCreated{}, fmt.Errorf("create planet %d: %w", i+1, err)
		}
	}

	u := clock.UniverseCreated{Type: "UniverseCreated", TickPeriod: cfg.tickPeriod, Seed: stored.Seed}
	_, err := streams.Append(ctx, js, streams.SubjectClock, u, 0)
	switch {
	case err == nil:
		return u, nil
	case errors.Is(err, streams.ErrConflict):
		// Another instance finished first.
		existing, found, err := universeCreated(ctx, js)
		if err != nil {
			return clock.UniverseCreated{}, err
		}
		if !found {
			return clock.UniverseCreated{}, errors.New("clock has events but no UniverseCreated")
		}
		return existing, nil
	default:
		return clock.UniverseCreated{}, fmt.Errorf("create universe: %w", err)
	}
}

// universeCreated reads the clock's first event. Not found means the big bang
// has not finished.
func universeCreated(ctx context.Context, js jetstream.JetStream) (clock.UniverseCreated, bool, error) {
	s, err := js.Stream(ctx, streams.StreamClock)
	if err != nil {
		return clock.UniverseCreated{}, false, err
	}
	first, err := s.GetMsg(ctx, 1)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return clock.UniverseCreated{}, false, nil
	}
	if err != nil {
		return clock.UniverseCreated{}, false, fmt.Errorf("read UniverseCreated: %w", err)
	}
	var existing clock.UniverseCreated
	if err := json.Unmarshal(first.Data, &existing); err != nil || existing.Type != "UniverseCreated" {
		return clock.UniverseCreated{}, false, errors.New("first clock event is not UniverseCreated")
	}
	return existing, true, nil
}

// subjectCount counts the aggregates of one kind: the subjects on EVENTS that
// match the filter.
func subjectCount(ctx context.Context, js jetstream.JetStream, filter string) (int, error) {
	s, err := js.Stream(ctx, streams.StreamEvents)
	if err != nil {
		return 0, err
	}
	info, err := s.Info(ctx, jetstream.WithSubjectFilter(filter))
	if err != nil {
		return 0, fmt.Errorf("count %s: %w", filter, err)
	}
	return len(info.State.Subjects), nil
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
