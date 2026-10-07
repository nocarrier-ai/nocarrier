package universe

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

// testUniverses covers a few shapes. Generation validates, so anything that
// comes back is already well formed; these cases exist so the invariant tests
// below run against more than one universe. Generated once for the whole
// package, since generation is the slow part and the result is immutable.
func testUniverses(t *testing.T) []*Universe {
	t.Helper()
	return sharedUniverses()
}

var sharedUniverses = sync.OnceValue(func() []*Universe {
	cases := []struct {
		sectors int
		seeds   []int64
	}{
		{64, []int64{1, 2, 3, 7, 99}},
		{200, []int64{1, 2, 3}},
		{1000, []int64{1, 42}},
	}
	var out []*Universe
	for _, c := range cases {
		for _, seed := range c.seeds {
			u, err := Generate(seed, c.sectors)
			if err != nil {
				panic(fmt.Sprintf("generate %d sectors seed %d: %v", c.sectors, seed, err))
			}
			out = append(out, u)
		}
	}
	return out
})

func name(u *Universe) string {
	b, _ := json.Marshal(struct {
		Sectors int   `json:"sectors"`
		Seed    int64 `json:"seed"`
	}{len(u.Sectors), u.Seed})
	return string(b)
}

func TestGenerateIsDeterministic(t *testing.T) {
	// Twice in the same process: a dependency on Go's randomised map
	// iteration order shows up here and nowhere else.
	for range 4 {
		a, err := Generate(7, 300)
		if err != nil {
			t.Fatal(err)
		}
		b, err := Generate(7, 300)
		if err != nil {
			t.Fatal(err)
		}
		ja, _ := json.Marshal(a)
		jb, _ := json.Marshal(b)
		if string(ja) != string(jb) {
			t.Fatal("same seed produced two different universes")
		}
	}
}

func TestGenerateDiffersBySeed(t *testing.T) {
	a, err := Generate(1, 300)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Generate(2, 300)
	if err != nil {
		t.Fatal(err)
	}
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if string(ja) == string(jb) {
		t.Fatal("different seeds produced the same universe")
	}
}

// The rule the whole design rests on. No traps, ever.
func TestNobodyIsStranded(t *testing.T) {
	for _, u := range testUniverses(t) {
		if !u.adjacency().stronglyConnected() {
			t.Errorf("%s: not strongly connected", name(u))
		}
		for _, s := range u.Sectors {
			if len(u.Exits(s.ID)) == 0 {
				t.Errorf("%s: sector %d has no way out", name(u), s.ID)
			}
		}
	}
}

// A sector joined only to one neighbour must be able to go back out that way.
func TestSpursAreTwoWay(t *testing.T) {
	spurs := 0
	for _, u := range testUniverses(t) {
		for _, s := range u.Sectors {
			n := u.neighbours(s.ID)
			if len(n) != 1 {
				continue
			}
			spurs++
			var out, back bool
			for _, l := range u.Exits(s.ID) {
				out = out || l.To == n[0]
			}
			for _, in := range u.inboundFrom(s.ID) {
				back = back || in == n[0]
			}
			if !out || !back {
				t.Errorf("%s: spur %d joined only to %d, one-way", name(u), s.ID, n[0])
			}
		}
	}
	t.Logf("checked %d spurs", spurs)
}

// publicAtBigBang indexes the initial public set for a test.
func publicAtBigBang(u *Universe) map[Lane]bool {
	m := make(map[Lane]bool, len(u.PublicAtBigBang))
	for _, l := range u.PublicAtBigBang {
		m[l] = true
	}
	return m
}

// Pockets carry no label, so they are found the way a player finds them: by
// shape. One way in from one sector, one way out to a different sector, and
// neither lane in the public map.
func TestPocketsAreConcealedAndEscapable(t *testing.T) {
	pockets := 0
	for _, u := range testUniverses(t) {
		public := publicAtBigBang(u)
		for _, s := range u.Sectors {
			exits := u.Exits(s.ID)
			inbound := u.inboundFrom(s.ID)
			if len(exits) != 1 || len(inbound) != 1 || exits[0].To == inbound[0] {
				continue
			}
			if public[exits[0]] || public[Lane{From: inbound[0], To: s.ID}] {
				continue
			}
			pockets++
			// Escapable: the exit is a real outbound lane of the pocket itself,
			// which is what makes it discoverable by scanning from inside, and
			// strong connectivity (checked elsewhere) means it leads home.
			if _, ok := u.Sector(exits[0].To); !ok {
				t.Errorf("%s: pocket %d exits to nowhere", name(u), s.ID)
			}
		}
	}
	if pockets == 0 {
		t.Fatal("no pocket-shaped sectors in any universe")
	}
	t.Logf("found %d pockets by shape", pockets)
}

func TestLaneCapRespected(t *testing.T) {
	for _, u := range testUniverses(t) {
		for _, s := range u.Sectors {
			if n := u.laneCount(s.ID); n > laneCap {
				t.Errorf("%s: sector %d has %d lanes, cap %d", name(u), s.ID, n, laneCap)
			}
		}
	}
}

// Proves the trunk structure emerged rather than being hoped for: some sectors
// are busy junctions and most are backwaters.
func TestHubsAndBackwatersBothExist(t *testing.T) {
	for _, u := range testUniverses(t) {
		busy, quiet := 0, 0
		for _, s := range u.Sectors {
			if u.laneCount(s.ID) >= 4 {
				busy++
			} else {
				quiet++
			}
		}
		if busy == 0 {
			t.Errorf("%s: no busy sectors; the map is uniform", name(u))
		}
		if quiet*2 < len(u.Sectors) {
			t.Errorf("%s: only %d of %d sectors quiet; a lattice, not a mesh",
				name(u), quiet, len(u.Sectors))
		}
	}
}

// The core takes the lowest IDs, following TW2002's FedSpace, and is protected
// space. Protected does not mean ported: the sectors around the spawn are part
// of the core whether or not anyone trades there.
func TestCoreTakesLowestIDs(t *testing.T) {
	for _, u := range testUniverses(t) {
		core := coreSize(len(u.Sectors))
		for i := range core {
			s := u.Sectors[i]
			if !s.Core {
				t.Errorf("%s: sector %d is not protected space", name(u), s.ID)
			}
		}
		for _, s := range u.Sectors[core:] {
			if s.Core {
				t.Errorf("%s: sector %d is protected but outside the lowest IDs", name(u), s.ID)
			}
		}
		if s, _ := u.Sector(u.Spawn); !s.Core {
			t.Errorf("%s: spawn %d is not protected space", name(u), u.Spawn)
		}
	}
}

// Protected space is fully public from the start; the rest of the map has
// something to find.
func TestPublicationRules(t *testing.T) {
	for _, u := range testUniverses(t) {
		public := publicAtBigBang(u)
		for _, l := range u.Lanes {
			from, _ := u.Sector(l.From)
			to, _ := u.Sector(l.To)
			if from.Core && to.Core && !public[l] {
				t.Errorf("%s: core lane %d->%d is not public at the big bang", name(u), l.From, l.To)
			}
		}
		if len(u.PublicAtBigBang) == len(u.Lanes) {
			t.Errorf("%s: every lane is public; nothing to discover", name(u))
		}
		if len(u.PublicAtBigBang) == 0 {
			t.Errorf("%s: nothing is public; the trunk should be", name(u))
		}
	}
}

// If the character pass converts nothing, the map is a road atlas.
func TestOneWayLanesExist(t *testing.T) {
	for _, u := range testUniverses(t) {
		pairs := make(map[[2]int]bool, len(u.Lanes))
		for _, l := range u.Lanes {
			pairs[[2]int{l.From, l.To}] = true
		}
		oneWay := 0
		for _, l := range u.Lanes {
			if !pairs[[2]int{l.To, l.From}] {
				oneWay++
			}
		}
		if oneWay == 0 {
			t.Errorf("%s: no one-way lanes", name(u))
		}
	}
}

// Lanes and ports come out in canonical order so two instances can compare
// universes byte for byte.
func TestCanonicalOrdering(t *testing.T) {
	for _, u := range testUniverses(t) {
		for i := 1; i < len(u.Lanes); i++ {
			prev, cur := u.Lanes[i-1], u.Lanes[i]
			if prev.From > cur.From || (prev.From == cur.From && prev.To >= cur.To) {
				t.Fatalf("%s: lanes out of order at %d: %v then %v", name(u), i, prev, cur)
			}
		}
		for i := 1; i < len(u.PublicAtBigBang); i++ {
			prev, cur := u.PublicAtBigBang[i-1], u.PublicAtBigBang[i]
			if prev.From > cur.From || (prev.From == cur.From && prev.To >= cur.To) {
				t.Fatalf("%s: public set out of order at %d", name(u), i)
			}
		}
		for i := 1; i < len(u.Ports); i++ {
			if u.Ports[i-1].Sector >= u.Ports[i].Sector {
				t.Fatalf("%s: ports out of order at %d", name(u), i)
			}
		}
	}
}

func TestPortsRoughlyHitTarget(t *testing.T) {
	for _, u := range testUniverses(t) {
		got := len(u.Ports) * 100 / len(u.Sectors)
		want := portPercent
		if got < want-15 || got > want+20 {
			t.Errorf("%s: %d%% of sectors have ports, want near %d%%", name(u), got, want)
		}
	}
}

func TestRejectsTooSmallAUniverse(t *testing.T) {
	if _, err := Generate(1, minSectors-1); err == nil {
		t.Fatal("generated a universe too small for its own shape rules")
	}
}

func TestAccessorsOutOfRange(t *testing.T) {
	u, err := Generate(1, 64)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{-1, 0, len(u.Sectors) + 1} {
		if _, ok := u.Sector(id); ok {
			t.Errorf("Sector(%d) reported a hit", id)
		}
		if got := u.Exits(id); got != nil {
			t.Errorf("Exits(%d) = %v, want nil", id, got)
		}
		if u.HasPort(id) {
			t.Errorf("HasPort(%d) = true", id)
		}
	}
	if u.Count() != len(u.Sectors) {
		t.Errorf("Count = %d, want %d", u.Count(), len(u.Sectors))
	}
}
