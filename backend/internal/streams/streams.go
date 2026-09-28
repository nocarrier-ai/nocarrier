package streams

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamClock     = "CLOCK"
	StreamWorld     = "WORLD"
	StreamAvatar    = "AVATAR"
	StreamDecisions = "DECISIONS"
	StreamExecute   = "EXECUTE"
	StreamDecide    = "DECIDE"

	SubjectClock = "clock.universe"

	BucketSectorState  = "sector-state"
	BucketActive       = "active-sectors"
	BucketDue          = "due-avatars"
	BucketAvatarStatus = "avatar-status"
	BucketLeaderboards = "leaderboards"
)

func WorldSubject(sector string) string     { return "world." + sector }
func AvatarSubject(avatar string) string    { return "avatar." + avatar }
func DecisionsSubject(avatar string) string { return "decisions." + avatar }
func ExecuteSubject(sector string) string   { return "execute." + sector }
func DecideSubject(avatar string) string    { return "decide." + avatar }

// ExecuteMsgID and DecideMsgID are the Nats-Msg-Id values that deduplicate
// dispatch. The duplicate window on those streams must exceed two tick
// periods for these to hold across a re-dispatch.
func ExecuteMsgID(sector string, tick int64) string { return fmt.Sprintf("%s@%d", sector, tick) }
func DecideMsgID(avatar string, tick int64) string  { return fmt.Sprintf("%s@%d", avatar, tick) }

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
			Name:        StreamWorld,
			Subjects:    []string{"world.>"},
			Storage:     jetstream.FileStorage,
			Compression: jetstream.S2Compression,
			Replicas:    replicas,
			AllowDirect: true,
		},
		{
			Name:        StreamAvatar,
			Subjects:    []string{"avatar.>"},
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
	for _, b := range []string{BucketSectorState, BucketActive, BucketDue, BucketAvatarStatus, BucketLeaderboards} {
		cfg := jetstream.KeyValueConfig{Bucket: b, Storage: jetstream.FileStorage, Replicas: replicas}
		if _, err := js.CreateOrUpdateKeyValue(ctx, cfg); err != nil {
			return fmt.Errorf("bucket %s: %w", b, err)
		}
	}
	return nil
}
