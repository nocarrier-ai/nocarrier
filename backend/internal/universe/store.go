package universe

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

// mapKey is the one key in the universe bucket.
const mapKey = "map"

var (
	// ErrNoMap: the big bang has not happened.
	ErrNoMap = errors.New("no universe map stored")
	// ErrMapExists: another instance won the big bang; Load its map.
	ErrMapExists = errors.New("universe map already stored")
	errCorrupt   = errors.New("corrupt universe map")
)

// Store reads and writes the one sector map, gob-encoded. Written once at the
// big bang.
type Store struct {
	kv jetstream.KeyValue
}

func NewStore(ctx context.Context, js jetstream.JetStream) (*Store, error) {
	kv, err := js.KeyValue(ctx, streams.BucketUniverse)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", streams.BucketUniverse, err)
	}
	return &Store{kv: kv}, nil
}

// Load reads the stored map and checks its integrity. ErrNoMap if none.
func (s *Store) Load(ctx context.Context) (*Universe, error) {
	entry, err := s.kv.Get(ctx, mapKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, ErrNoMap
	}
	if err != nil {
		return nil, fmt.Errorf("read universe map: %w", err)
	}
	u, err := decode(entry.Value())
	if err != nil {
		return nil, err
	}
	if err := u.validate(); err != nil {
		return nil, fmt.Errorf("stored universe map is unusable: %w", err)
	}
	return u, nil
}

// Create stores a map only if none exists; the KV create is the big-bang
// election. Called before UniverseCreated is appended, so the event names a
// map that is already durable.
func (s *Store) Create(ctx context.Context, u *Universe) error {
	data, err := encode(u)
	if err != nil {
		return err
	}
	if _, err := s.kv.Create(ctx, mapKey, data); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return ErrMapExists
		}
		return fmt.Errorf("store universe map: %w", err)
	}
	return nil
}

func encode(u *Universe) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(u); err != nil {
		return nil, fmt.Errorf("encode universe map: %w", err)
	}
	return buf.Bytes(), nil
}

// decode unpacks a map and rebuilds its lookups.
func decode(data []byte) (*Universe, error) {
	var u Universe
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&u); err != nil {
		return nil, fmt.Errorf("%w: %v", errCorrupt, err)
	}
	u.index()
	return &u, nil
}
