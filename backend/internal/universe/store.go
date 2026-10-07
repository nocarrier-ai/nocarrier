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

// mapKey is the single key in the universe bucket. One deployment hosts one
// universe, so there is nothing to name.
const mapKey = "map"

var (
	// ErrNoMap means the big bang has not happened yet.
	ErrNoMap = errors.New("no universe map stored")
	// ErrMapExists means another instance won the big bang first; its map is
	// the real one and the caller should Load it rather than keep its own.
	ErrMapExists = errors.New("universe map already stored")
	// errCorrupt means the stored bytes could not be decoded at all.
	errCorrupt = errors.New("corrupt universe map")
)

// Store reads and writes the one sector map. The map is written exactly once,
// at the big bang, and only read afterwards.
//
// The encoding is encoding/gob. Nothing outside Go ever reads the map, the
// bucket is written only by us, and a 1000-sector universe is under 50 KB —
// well inside a KV value. A field added for a later pass decodes as its zero
// value on a map stored before it existed, which is the property that matters
// most: the format will change as ports and planets land, and a universe that
// already exists has to keep loading.
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

// Load reads the stored map. Reports ErrNoMap when the universe has not been
// created yet.
//
// The decoded map is checked for structural integrity, because a map that
// fails its own invariants is worse than no map: it would quietly strand
// ships. Better to refuse to start. Shape is not judged here; see validate.
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

// Create writes a map only if none exists. ErrMapExists means another instance
// got there first. The guard is the KV create, so concurrent instances racing
// to hold the big bang settle it here: exactly one map is ever stored, and the
// loser reads the winner's rather than its own.
//
// This is why the map is written before UniverseCreated is appended. The event
// records the seed of a map that is already durable, so there is no window
// where the universe exists on the clock but its geography does not.
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

// decode unpacks a stored map and rebuilds its in-memory lookups, which gob
// does not carry because they are unexported.
func decode(data []byte) (*Universe, error) {
	var u Universe
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&u); err != nil {
		return nil, fmt.Errorf("%w: %v", errCorrupt, err)
	}
	u.index()
	return &u, nil
}
