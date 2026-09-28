package role

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Canonical role names.
const (
	Sim      = "sim"
	Decide   = "decide"
	Project  = "project"
	Command  = "command"
	DataRead = "read"
)

// Order is the canonical start order. Parse always returns roles in this order.
var Order = []string{Sim, Decide, Project, Command, DataRead}

type Role interface {
	Name() string
	Run(ctx context.Context) error
}

type RoleOptions struct {
	Logger     *slog.Logger
	NC         *nats.Conn
	JS         jetstream.JetStream
	InstanceID string
	Partitions int
	TickPeriod time.Duration
}

type Factory func(RoleOptions) (Role, error)

// Parse turns a comma-separated list such as "sim,decide" or "all" into a
// deduplicated list of roles in canonical order. Names are case-insensitive
// and surrounding whitespace is ignored.
func Parse(s string) ([]string, error) {
	want := make(map[string]bool)
	for _, part := range strings.Split(s, ",") {
		name := strings.ToLower(strings.TrimSpace(part))
		switch {
		case name == "":
			continue
		case name == "all":
			for _, n := range Order {
				want[n] = true
			}
		case slices.Contains(Order, name):
			want[name] = true
		default:
			return nil, fmt.Errorf("unknown role %q (valid: %s, all)", name, strings.Join(Order, ", "))
		}
	}
	if len(want) == 0 {
		return nil, errors.New("no roles specified")
	}
	out := make([]string, 0, len(want))
	for _, n := range Order {
		if want[n] {
			out = append(out, n)
		}
	}
	return out, nil
}
