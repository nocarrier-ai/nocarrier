package universe

import (
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/natstest"
)

func startStore(t *testing.T) (*Store, jetstream.JetStream) {
	t.Helper()
	js := natstest.Start(t)
	s, err := NewStore(natstest.Context(t), js)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return s, js
}

// Load returns ErrNoMap on an empty bucket.
func TestStoreLoadBeforeTheBigBang(t *testing.T) {
	s, _ := startStore(t)
	_, err := s.Load(natstest.Context(t))
	if !errors.Is(err, ErrNoMap) {
		t.Fatalf("err = %v, want ErrNoMap", err)
	}
}

// A stored map loads back equal, with lookups rebuilt.
func TestStoreCreateThenLoad(t *testing.T) {
	s, _ := startStore(t)
	ctx := natstest.Context(t)
	u, err := Generate(11, 200)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, u); err != nil {
		t.Fatalf("create: %v", err)
	}

	back, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if back.Version != u.Version || back.Seed != u.Seed || back.Spawn != u.Spawn {
		t.Errorf("header loaded as %+v", back)
	}
	if !slices.Equal(back.Sectors, u.Sectors) || !slices.Equal(back.Lanes, u.Lanes) ||
		!slices.Equal(back.Ports, u.Ports) || !slices.Equal(back.PublicAtBigBang, u.PublicAtBigBang) {
		t.Error("sectors, lanes, ports or the public set changed in the round trip")
	}
	// lookups are rebuilt on load
	for _, sec := range u.Sectors {
		if !slices.Equal(back.Exits(sec.ID), u.Exits(sec.ID)) {
			t.Fatalf("exits of %d differ after load", sec.ID)
		}
		if back.HasPort(sec.ID) != u.HasPort(sec.ID) {
			t.Fatalf("port of %d differs after load", sec.ID)
		}
	}
}

// A second Create fails with ErrMapExists and the first map stays.
func TestStoreCreateIsOnce(t *testing.T) {
	s, _ := startStore(t)
	ctx := natstest.Context(t)
	first, err := Generate(1, 200)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Generate(2, 200)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, first); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := s.Create(ctx, second); !errors.Is(err, ErrMapExists) {
		t.Fatalf("second create err = %v, want ErrMapExists", err)
	}
	back, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if back.Seed != first.Seed {
		t.Errorf("stored seed %d, want the first writer's %d", back.Seed, first.Seed)
	}
}

// Concurrent Creates: exactly one wins, and Load returns its map.
func TestStoreCreateRacesSafely(t *testing.T) {
	s, _ := startStore(t)
	ctx := natstest.Context(t)
	const instances = 5

	maps := make([]*Universe, instances)
	for i := range instances {
		u, err := Generate(int64(i+1), 100)
		if err != nil {
			t.Fatal(err)
		}
		maps[i] = u
	}

	errs := make([]error, instances)
	var wg sync.WaitGroup
	for i := range instances {
		wg.Go(func() { errs[i] = s.Create(ctx, maps[i]) })
	}
	wg.Wait()

	winner := -1
	for i, err := range errs {
		switch {
		case err == nil:
			if winner != -1 {
				t.Fatalf("instances %d and %d both won the big bang", winner, i)
			}
			winner = i
		case !errors.Is(err, ErrMapExists):
			t.Fatalf("instance %d: %v", i, err)
		}
	}
	if winner == -1 {
		t.Fatal("nobody won the big bang")
	}

	back, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if back.Seed != maps[winner].Seed {
		t.Errorf("stored seed %d, want the winner's %d", back.Seed, maps[winner].Seed)
	}
}

// Load refuses a stored map that fails validate.
func TestStoreLoadRejectsAnInvalidMap(t *testing.T) {
	s, js := startStore(t)
	ctx := natstest.Context(t)
	broken := mutate(t, func(u *Universe) {
		u.Lanes = slices.DeleteFunc(u.Lanes, func(l Lane) bool { return l.From == 40 })
	})
	data, err := encode(broken)
	if err != nil {
		t.Fatal(err)
	}
	kv, err := js.KeyValue(ctx, "universe")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, mapKey, data); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx); err == nil {
		t.Fatal("loaded a map that fails validation")
	}
}

// Load reports errCorrupt for bytes that do not decode.
func TestStoreLoadRejectsGarbage(t *testing.T) {
	s, js := startStore(t)
	ctx := natstest.Context(t)
	kv, err := js.KeyValue(ctx, "universe")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, mapKey, []byte("not a universe")); err != nil {
		t.Fatal(err)
	}
	_, err = s.Load(ctx)
	if !errors.Is(err, errCorrupt) {
		t.Fatalf("err = %v, want errCorrupt", err)
	}
}

// chainUniverse: a two-way line. Structurally sound, no hubs.
func chainUniverse(n int) *Universe {
	u := &Universe{Version: version, Seed: 1, Spawn: 1}
	for i := 1; i <= n; i++ {
		u.Sectors = append(u.Sectors, Sector{ID: i, Core: i <= 2})
	}
	link := func(a, b int) {
		u.Lanes = append(u.Lanes, Lane{From: a, To: b}, Lane{From: b, To: a})
	}
	for i := 1; i < n; i++ {
		link(i, i+1)
	}
	slices.SortFunc(u.Lanes, cmpLane)
	u.PublicAtBigBang = slices.Clone(u.Lanes)
	u.Ports = []Port{{Sector: 1}, {Sector: 2}}
	u.index()
	return u
}

// Load accepts a map that fails wellShaped; shape is not an integrity rule.
func TestStoreLoadAcceptsASoundButPoorlyShapedMap(t *testing.T) {
	u := chainUniverse(25)
	if err := u.validate(); err != nil {
		t.Fatalf("chain should be structurally sound: %v", err)
	}
	if err := u.wellShaped(); err == nil {
		t.Fatal("chain should fail the quality gate; the test proves nothing otherwise")
	}

	s, _ := startStore(t)
	ctx := natstest.Context(t)
	if err := s.Create(ctx, u); err != nil {
		t.Fatal(err)
	}
	back, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("load refused a sound map over its shape: %v", err)
	}
	if back.Count() != 25 {
		t.Errorf("loaded %d sectors, want 25", back.Count())
	}
}

// Everything Generate returns passes wellShaped.
func TestGenerateRequiresGoodShape(t *testing.T) {
	for _, u := range testUniverses(t) {
		if err := u.wellShaped(); err != nil {
			t.Errorf("%s: %v", name(u), err)
		}
	}
}
