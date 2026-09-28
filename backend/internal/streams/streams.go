// Package streams declares the JetStream streams and KV buckets the backend
// relies on and creates or updates them idempotently.
package streams

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamDecide    = "DECIDE"
	StreamIntents   = "INTENTS"
	StreamDecisions = "DECISIONS"
	StreamDoctrine  = "DOCTRINE"
	StreamBoundary  = "BOUNDARY"

	BucketLeases       = "leases"
	BucketAvatarStatus = "avatar-status"
	BucketLeaderboards = "leaderboards"
)

// WorldStream returns the name of the world stream for partition p.
func WorldStream(p int) string { return fmt.Sprintf("WORLD_%d", p) }

// WorldSubject returns the commit subject for a sector. The publisher picks
// the partition, so no server-side subject mapping is needed.
func WorldSubject(p int, sector string) string { return fmt.Sprintf("world.%d.%s", p, sector) }

// Ensure creates or updates every stream and bucket. It's safe to run from
// several instances at once; in production, running it once per deploy with
// --ensure-streams=false everywhere else keeps config changes deliberate.
func Ensure(ctx context.Context, js jetstream.JetStream, partitions, replicas int) error {
	cfgs := make([]jetstream.StreamConfig, 0, partitions+5)
	for p := range partitions {
		cfgs = append(cfgs, jetstream.StreamConfig{
			Name:        WorldStream(p),
			Subjects:    []string{fmt.Sprintf("world.%d.>", p)},
			Storage:     jetstream.FileStorage,
			Compression: jetstream.S2Compression,
			Replicas:    replicas,
			AllowDirect: true,
		})
	}
	cfgs = append(cfgs,
		jetstream.StreamConfig{
			Name:      StreamDecide,
			Subjects:  []string{"decide.requests"},
			Retention: jetstream.WorkQueuePolicy,
			Storage:   jetstream.FileStorage,
			Replicas:  replicas,
		},
		jetstream.StreamConfig{
			Name:     StreamIntents,
			Subjects: []string{"intents.>"},
			MaxAge:   15 * time.Minute,
			Storage:  jetstream.FileStorage,
			Replicas: replicas,
		},
		jetstream.StreamConfig{
			Name:              StreamDecisions,
			Subjects:          []string{"decisions.>"},
			MaxMsgsPerSubject: 5000,
			Storage:           jetstream.FileStorage,
			Compression:       jetstream.S2Compression,
			Replicas:          replicas,
			AllowDirect:       true,
		},
		jetstream.StreamConfig{
			Name:        StreamDoctrine,
			Subjects:    []string{"doctrine.>"},
			Storage:     jetstream.FileStorage,
			Replicas:    replicas,
			AllowDirect: true,
		},
		jetstream.StreamConfig{
			Name:              StreamBoundary,
			Subjects:          []string{"boundary.>"},
			MaxMsgsPerSubject: 4,
			Storage:           jetstream.FileStorage,
			Replicas:          replicas,
		},
	)
	for _, cfg := range cfgs {
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("stream %s: %w", cfg.Name, err)
		}
	}

	buckets := []jetstream.KeyValueConfig{
		{Bucket: BucketLeases, TTL: 15 * time.Second, Storage: jetstream.FileStorage, Replicas: replicas},
		{Bucket: BucketAvatarStatus, Storage: jetstream.FileStorage, Replicas: replicas},
		{Bucket: BucketLeaderboards, Storage: jetstream.FileStorage, Replicas: replicas},
	}
	for _, cfg := range buckets {
		if _, err := js.CreateOrUpdateKeyValue(ctx, cfg); err != nil {
			return fmt.Errorf("bucket %s: %w", cfg.Bucket, err)
		}
	}
	return nil
}
