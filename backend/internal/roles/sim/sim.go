package sim

import (
	"context"
	"time"

	"github.com/nocarrier-ai/nocarrier/internal/roles/role"
)

// epoch is the universe start time.
// TODO: read it from the Big Bang event instead of hard-coding it.
var epoch = time.Unix(0, 0).UTC()

type simRole struct {
	deps role.RoleOptions
}

func New(deps role.RoleOptions) (role.Role, error) {
	return &simRole{deps: deps}, nil
}

func (s *simRole) Name() string { return role.Sim }

func (s *simRole) Run(ctx context.Context) error {
	log := s.deps.Logger.With("role", role.Sim)
	log.Info("started", "tick", s.deps.TickPeriod.String(), "partitions", s.deps.Partitions)

	// TODO: claim sector leases in the leases bucket, load snapshots, and
	// replay each owned sector's tail from its WORLD partition.

	for {
		next := NextTickStart(epoch, s.deps.TickPeriod, time.Now())
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			// TODO: release leases so other instances pick up these sectors at
			// the next boundary instead of waiting for the TTL to expire.
			log.Info("stopped")
			return nil
		case <-timer.C:
			tick := TickAt(epoch, s.deps.TickPeriod, next)
			log.Debug("tick", "n", tick)
			// TODO: for each owned sector, gather intents for this tick, run the
			// resolver, commit with an expected last subject sequence, publish
			// boundary summaries, and enqueue decision requests.
		}
	}
}

// TickAt returns the number of the tick containing t.
func TickAt(epoch time.Time, period time.Duration, t time.Time) int64 {
	return int64(t.Sub(epoch) / period)
}

// NextTickStart returns the start time of the tick after the one containing t.
func NextTickStart(epoch time.Time, period time.Duration, t time.Time) time.Time {
	return epoch.Add(time.Duration(TickAt(epoch, period, t)+1) * period)
}
