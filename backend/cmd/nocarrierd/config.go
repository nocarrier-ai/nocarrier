package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nocarrier-ai/nocarrier/internal/roles/role"
)

type config struct {
	roles         string
	natsURL       string
	natsCreds     string
	embeddedNATS  bool
	embeddedStore string
	ensureStreams bool
	instanceID    string
	replicas      int
	partitions    int
	tick          time.Duration
	httpAddr      string
	shutdownGrace time.Duration
	logLevel      string
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.roles, "roles", envOr("NOCARRIER_ROLES", "all"),
		"comma-separated roles to run ("+strings.Join(role.Order, ", ")+") or all")
	flag.StringVar(&c.natsURL, "nats-url", envOr("NATS_URL", nats.DefaultURL), "NATS server URL")
	flag.StringVar(&c.natsCreds, "nats-creds", os.Getenv("NATS_CREDS"), "path to a NATS credentials file")
	flag.BoolVar(&c.embeddedNATS, "embedded-nats", envBool("NOCARRIER_EMBEDDED_NATS", false),
		"run an embedded NATS server on 127.0.0.1:4222 for local development")
	flag.StringVar(&c.embeddedStore, "embedded-store", envOr("NOCARRIER_EMBEDDED_STORE", ".data/nats"),
		"JetStream storage directory for the embedded server")
	flag.BoolVar(&c.ensureStreams, "ensure-streams", envBool("NOCARRIER_ENSURE_STREAMS", true),
		"create or update streams and KV buckets at startup")
	flag.IntVar(&c.replicas, "replicas", envInt("NOCARRIER_REPLICAS", 1), "replica count for streams and buckets")
	flag.IntVar(&c.partitions, "partitions", envInt("NOCARRIER_PARTITIONS", 16), "number of world stream partitions")
	flag.DurationVar(&c.tick, "tick", envDuration("NOCARRIER_TICK", time.Minute), "tick period")
	flag.StringVar(&c.httpAddr, "http-addr", envOr("NOCARRIER_HTTP_ADDR", ":8080"), "address for health endpoints")
	flag.DurationVar(&c.shutdownGrace, "shutdown-grace", envDuration("NOCARRIER_SHUTDOWN_GRACE", 20*time.Second),
		"time allowed for roles to stop after shutdown begins")
	flag.StringVar(&c.logLevel, "log-level", envOr("NOCARRIER_LOG_LEVEL", "info"), "debug, info, warn, or error")
	flag.StringVar(&c.instanceID, "instance-id", defaultInstanceID(), "unique instance id, used for leases")
	flag.Parse()
	return c
}

func validate(c config) error {
	switch {
	case c.partitions < 1:
		return errors.New("--partitions must be at least 1")
	case c.replicas < 1 || c.replicas > 5:
		return errors.New("--replicas must be between 1 and 5")
	case c.embeddedNATS && c.replicas != 1:
		return errors.New("--embedded-nats only supports --replicas=1")
	case c.tick <= 0:
		return errors.New("--tick must be positive")
	case c.shutdownGrace <= 0:
		return errors.New("--shutdown-grace must be positive")
	}
	return nil
}

func defaultInstanceID() string {
	if id := os.Getenv("FLY_MACHINE_ID"); id != "" {
		return id
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "local"
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		exitf("%s: %v", key, err)
	}
	return b
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		exitf("%s: %v", key, err)
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		exitf("%s: %v", key, err)
	}
	return d
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "nocarrierd: "+format+"\n", args...)
	os.Exit(2)
}
