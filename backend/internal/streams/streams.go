package streams

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamClock     = "CLOCK"
	StreamEvents    = "EVENTS"
	StreamDecisions = "DECISIONS"
	StreamExecute   = "EXECUTE"
	StreamDecide    = "DECIDE"

	SubjectClock = "clock.universe"

	SectorEvents   = "sector.*"
	PortEvents     = "port.*"
	AvatarEvents   = "avatar.*"
	PlanEvents     = "plan.*"
	DoctrineEvents = "doctrine.*"

	BucketSectorState  = "sector-state"
	BucketActive       = "active-sectors"
	BucketDue          = "due-avatars"
	BucketAvatarStatus = "avatar-status"
	BucketPortStatus   = "port-status"
	BucketLeaderboards = "leaderboards"
	BucketDoctrine     = "doctrine"
	// BucketUniverse holds the one packed sector map, written once at the big
	// bang. Binary rather than JSON keeps even a very large universe inside
	// the default max payload, so a plain KV value does the job.
	BucketUniverse = "universe"
)

var validID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// ValidID reports whether id can be the single token in a `<kind>.<id>`
// subject. EVENTS binds `<kind>.*`, so an id carrying a dot, a wildcard or a
// space has nowhere to land; aggregates validate their ids with this before
// building a subject.
func ValidID(id string) bool { return validID.MatchString(id) }

func SectorSubject(sectorID string) string    { return "sector." + sectorID }
func PortSubject(sectorID string) string      { return "port." + sectorID }
func AvatarSubject(avatarID string) string    { return "avatar." + avatarID }
func PlanSubject(avatarID string) string      { return "plan." + avatarID }
func DoctrineSubject(avatarID string) string  { return "doctrine." + avatarID }
func DecisionsSubject(avatarID string) string { return "decisions." + avatarID }
func ExecuteSubject(sectorID string) string   { return "execute." + sectorID }
func DecideSubject(avatarID string) string    { return "decide." + avatarID }

// ExecuteMsgID and DecideMsgID are the Nats-Msg-Id values that deduplicate
// dispatch. The duplicate window on those streams must exceed two tick
// periods for these to hold across a re-dispatch.
func ExecuteMsgID(sectorID string, tick int64) string { return fmt.Sprintf("%s@%d", sectorID, tick) }
func DecideMsgID(avatarID string, tick int64) string  { return fmt.Sprintf("%s@%d", avatarID, tick) }

// Ensure creates or updates all streams and buckets. Safe to run from every
// instance concurrently.
func Ensure(ctx context.Context, js jetstream.JetStream, replicas int, tickPeriod time.Duration) error {
	dupWindow := 3 * tickPeriod
	cfgs := []jetstream.StreamConfig{
		{
			Name:        StreamClock,
			Subjects:    []string{SubjectClock},
			Storage:     jetstream.FileStorage,
			Replicas:    replicas,
			AllowDirect: true,
		},
		{
			Name:        StreamEvents,
			Subjects:    []string{SectorEvents, PortEvents, AvatarEvents, PlanEvents, DoctrineEvents},
			Storage:     jetstream.FileStorage,
			Compression: jetstream.S2Compression,
			Replicas:    replicas,
			AllowDirect: true,
		},
		{
			Name:              StreamDecisions,
			Subjects:          []string{"decisions.>"},
			Storage:           jetstream.FileStorage,
			Compression:       jetstream.S2Compression,
			MaxMsgsPerSubject: 5000,
			Replicas:          replicas,
			AllowDirect:       true,
		},
		{
			Name:       StreamExecute,
			Subjects:   []string{"execute.>"},
			Retention:  jetstream.WorkQueuePolicy,
			Storage:    jetstream.FileStorage,
			Duplicates: dupWindow,
			Replicas:   replicas,
		},
		{
			Name:       StreamDecide,
			Subjects:   []string{"decide.>"},
			Retention:  jetstream.WorkQueuePolicy,
			Storage:    jetstream.FileStorage,
			Duplicates: dupWindow,
			Replicas:   replicas,
		},
	}
	for _, cfg := range cfgs {
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("stream %s: %w", cfg.Name, err)
		}
	}
	for _, b := range []string{BucketSectorState, BucketActive, BucketDue, BucketAvatarStatus, BucketPortStatus, BucketLeaderboards, BucketDoctrine, BucketUniverse} {
		cfg := jetstream.KeyValueConfig{Bucket: b, Storage: jetstream.FileStorage, Replicas: replicas}
		if _, err := js.CreateOrUpdateKeyValue(ctx, cfg); err != nil {
			return fmt.Errorf("bucket %s: %w", b, err)
		}
	}
	return nil
}
