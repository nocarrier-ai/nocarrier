package universe

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

// testUniverses is generated once per package run.
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

// Same seed and size give byte-identical output.
func TestGenerateIsDeterministic(t *testing.T) {
	// twice in one process catches map-iteration-order dependence
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

// Different seeds give different universes.
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

// Every sector reaches every other and has at least one exit.
func TestNobodyIsStranded(t *testing.T) {
	for _, u := range testUniverses(t) {
		if !u.adjacency().stronglyConnected() {
			t.Errorf("%s: not strongly connected", name(u))
		}
		for _, s := range u.Sectors {
			if len(u.ExitsFromSector(s.ID)) == 0 {
				t.Errorf("%s: sector %d has no way out", name(u), s.ID)
			}
		}
	}
}

// A sector with exactly one neighbour has a lane to it in both directions.
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
			for _, l := range u.ExitsFromSector(s.ID) {
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

func publicAtBigBang(u *Universe) map[Lane]bool {
	m := make(map[Lane]bool, len(u.PublicAtBigBang))
	for _, l := range u.PublicAtBigBang {
		m[l] = true
	}
	return m
}

// Pockets have no label; find them by shape: one lane in, one lane out to
// somewhere else, neither public.
func TestPocketsAreConcealedAndEscapable(t *testing.T) {
	pockets := 0
	for _, u := range testUniverses(t) {
		public := publicAtBigBang(u)
		for _, s := range u.Sectors {
			exits := u.ExitsFromSector(s.ID)
			inbound := u.inboundFrom(s.ID)
			if len(exits) != 1 || len(inbound) != 1 || exits[0].To == inbound[0] {
				continue
			}
			if public[exits[0]] || public[Lane{From: inbound[0], To: s.ID}] {
				continue
			}
			pockets++
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

// No sector has more than laneCap distinct neighbours.
func TestLaneCapRespected(t *testing.T) {
	for _, u := range testUniverses(t) {
		for _, s := range u.Sectors {
			if n := u.laneCount(s.ID); n > laneCap {
				t.Errorf("%s: sector %d has %d lanes, cap %d", name(u), s.ID, n, laneCap)
			}
		}
	}
}

// Some sectors have 4+ lanes and at least half have fewer.
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

// Core is the lowest IDs and does not imply a port.
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

// All core lanes are public at creation; the public set is neither empty nor everything.
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

// Public lanes sit nearer the spawn than unpublished ones: charted around the
// hubs, dark at the frontier.
func TestPublicMapThinsWithDepth(t *testing.T) {
	u, err := Generate(1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	dist := hopsFromAny(u.adjacency().out, []int{u.Spawn - 1})
	public := publicAtBigBang(u)
	pubSum, pubN, darkSum, darkN := 0, 0, 0, 0
	for _, l := range u.Lanes {
		d := max(dist[l.From-1], dist[l.To-1])
		if public[l] {
			pubSum, pubN = pubSum+d, pubN+1
		} else {
			darkSum, darkN = darkSum+d, darkN+1
		}
	}
	if float64(pubSum)/float64(pubN) >= float64(darkSum)/float64(darkN) {
		t.Errorf("public lanes average %.1f hops out, unpublished %.1f", float64(pubSum)/float64(pubN), float64(darkSum)/float64(darkN))
	}
	share := len(u.PublicAtBigBang) * 100 / len(u.Lanes)
	if share < 20 || share > 40 {
		t.Errorf("%d%% of lanes public at creation; want a frontier, not a charted map or a blank one", share)
	}
}

// At least one lane has no reverse.
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

// Lanes, PublicAtBigBang and Ports are sorted.
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

// The port share lands near portPercent.
func TestPortsRoughlyHitTarget(t *testing.T) {
	for _, u := range testUniverses(t) {
		got := len(u.Ports) * 100 / len(u.Sectors)
		want := portPercent
		if got < want-15 || got > want+20 {
			t.Errorf("%s: %d%% of sectors have ports, want near %d%%", name(u), got, want)
		}
	}
}

// Generate refuses fewer than minSectors.
func TestRejectsTooSmallAUniverse(t *testing.T) {
	if _, err := Generate(1, minSectors-1); err == nil {
		t.Fatal("generated a universe too small for its own shape rules")
	}
}

// Out-of-range IDs return zero values, not panics.
func TestAccessorsOutOfRange(t *testing.T) {
	u, err := Generate(1, 64)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{-1, 0, len(u.Sectors) + 1} {
		if _, ok := u.Sector(id); ok {
			t.Errorf("Sector(%d) reported a hit", id)
		}
		if got := u.ExitsFromSector(id); got != nil {
			t.Errorf("Exits(%d) = %v, want nil", id, got)
		}
		if u.HasPort(id) {
			t.Errorf("HasPort(%d) = true", id)
		}
	}
	if u.SectorCount() != len(u.Sectors) {
		t.Errorf("Count = %d, want %d", u.SectorCount(), len(u.Sectors))
	}
}

// Every port trades all three commodities with capacity in range and regen set.
func TestPortsTradeAllThreeCommodities(t *testing.T) {
	for _, u := range testUniverses(t) {
		for _, p := range u.Ports {
			for c, g := range p.Goods {
				if g.Capacity < capacityMin || g.Capacity > capacityMax {
					t.Errorf("%s: port %d commodity %d capacity %d", name(u), p.Sector, c, g.Capacity)
				}
				if g.Regen < 1 {
					t.Errorf("%s: port %d commodity %d regen %d", name(u), p.Sector, c, g.Regen)
				}
			}
		}
	}
}

// All eight buy/sell classes appear in a large universe.
func TestAllPortClassesAppear(t *testing.T) {
	u, err := Generate(1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[[3]bool]bool{}
	for _, p := range u.Ports {
		seen[[3]bool{p.Goods[FuelOre].Sells, p.Goods[Organics].Sells, p.Goods[Equipment].Sells}] = true
	}
	if len(seen) != 8 {
		t.Errorf("%d of 8 port classes present", len(seen))
	}
}

// Fuel Ore sellers sit farther from the spawn than buyers on average, and
// Equipment the reverse.
func TestStancesLeanWithDistanceFromCore(t *testing.T) {
	u, err := Generate(1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	dist := hopsFromAny(u.adjacency().out, []int{u.Spawn - 1})
	mean := func(c Commodity, sells bool) float64 {
		sum, n := 0, 0
		for _, p := range u.Ports {
			if p.Goods[c].Sells == sells {
				sum += dist[p.Sector-1]
				n++
			}
		}
		return float64(sum) / float64(n)
	}
	if mean(FuelOre, true) <= mean(FuelOre, false) {
		t.Errorf("Fuel Ore sellers at %.1f hops, buyers at %.1f", mean(FuelOre, true), mean(FuelOre, false))
	}
	if mean(Equipment, true) >= mean(Equipment, false) {
		t.Errorf("Equipment sellers at %.1f hops, buyers at %.1f", mean(Equipment, true), mean(Equipment, false))
	}
}

// plantShortcuts adds unpublished lanes.
func TestPlantShortcutsAddsLanes(t *testing.T) {
	b := newBuilder(3, 0, 1000)
	b.buildCore()
	b.buildTrunk()
	b.buildRegions()
	b.convertOneWay()
	b.carvePockets()
	b.placePorts()
	b.assignStances()
	before := len(b.lanes)
	b.plantShortcuts()
	added := 0
	for _, l := range b.lanes[before:] {
		if l.published {
			t.Errorf("shortcut %d->%d is published", l.from, l.to)
		}
		added++
	}
	if added == 0 {
		t.Fatal("no shortcuts planted")
	}
	if !b.graphCorrect() {
		t.Fatal("shortcuts broke soundness")
	}
}

// Terra is the spawn's only planet and the only one in the core; it holds the
// colonist source.
func TestTerraAtSpawn(t *testing.T) {
	for _, u := range testUniverses(t) {
		var atSpawn []Planet
		for _, p := range u.Planets {
			if s, _ := u.Sector(p.Sector); s.Core && p.Sector != u.Spawn {
				t.Errorf("%s: planet in core sector %d", name(u), p.Sector)
			}
			if p.Sector == u.Spawn {
				atSpawn = append(atSpawn, p)
			}
		}
		if len(atSpawn) != 1 || atSpawn[0].Class != ClassM || atSpawn[0].InitialColonists != terraColonists {
			t.Errorf("%s: spawn planets = %+v", name(u), atSpawn)
		}
	}
}

// No sector exceeds the cap, and sectors with more planets are no more common.
func TestPlanetsPerSectorTaperOff(t *testing.T) {
	u, err := Generate(1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	per := map[int]int{}
	for _, p := range u.Planets {
		per[p.Sector]++
	}
	with := [maxPlanetsPerSector + 2]int{}
	for _, n := range per {
		if n > maxPlanetsPerSector {
			t.Fatalf("sector with %d planets", n)
		}
		with[n]++
	}
	if !(with[1] > with[2] && with[2] >= with[3]) {
		t.Errorf("sectors with 1/2/3 planets: %d/%d/%d, want non-increasing", with[1], with[2], with[3])
	}
	// clustering makes doubles a feature, not a fluke
	if with[2]*20 < with[1]+with[2]+with[3] {
		t.Errorf("only %d of %d planet sectors hold two; clustering is not taking", with[2], with[1]+with[2]+with[3])
	}
}

// Planet sectors sit deeper than average, and pockets hold planets far more
// often than other sectors.
func TestPlanetsFavourDepthAndPockets(t *testing.T) {
	u, err := Generate(1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	dist := hopsFromAny(u.adjacency().out, []int{u.Spawn - 1})
	hasPlanet := map[int]bool{}
	for _, p := range u.Planets {
		hasPlanet[p.Sector] = true
	}
	allSum, planetSum := 0, 0
	for _, s := range u.Sectors {
		allSum += dist[s.ID-1]
		if hasPlanet[s.ID] {
			planetSum += dist[s.ID-1]
		}
	}
	if float64(planetSum)/float64(len(hasPlanet)) <= float64(allSum)/float64(len(u.Sectors)) {
		t.Error("planet sectors are not deeper than average")
	}

	public := publicAtBigBang(u)
	pockets, pocketsWith, others, othersWith := 0, 0, 0, 0
	for _, s := range u.Sectors {
		exits, inbound := u.ExitsFromSector(s.ID), u.inboundFrom(s.ID)
		pocket := len(exits) == 1 && len(inbound) == 1 && exits[0].To != inbound[0] &&
			!public[exits[0]] && !public[Lane{From: inbound[0], To: s.ID}]
		if pocket {
			pockets++
			if hasPlanet[s.ID] {
				pocketsWith++
			}
		} else {
			others++
			if hasPlanet[s.ID] {
				othersWith++
			}
		}
	}
	if pockets == 0 {
		t.Fatal("no pockets to measure")
	}
	if float64(pocketsWith)/float64(pockets) <= float64(othersWith)/float64(others) {
		t.Errorf("pockets with planets %d/%d, others %d/%d", pocketsWith, pockets, othersWith, others)
	}
}

// A few planets start with a small colony; the rest are empty.
func TestSeededColoniesAreFew(t *testing.T) {
	for _, u := range testUniverses(t) {
		seeded := 0
		for _, p := range u.Planets {
			if p.Sector == u.Spawn {
				continue
			}
			if p.InitialColonists == 0 {
				continue
			}
			if p.InitialColonists < colonyMin || p.InitialColonists > colonyMax {
				t.Errorf("%s: colony of %d outside %d..%d", name(u), p.InitialColonists, colonyMin, colonyMax)
			}
			seeded++
		}
		if want := seededColonies(len(u.Sectors)); seeded != want {
			t.Errorf("%s: %d seeded colonies, want %d", name(u), seeded, want)
		}
	}
}

// All seven classes appear in a large universe.
func TestAllPlanetClassesAppear(t *testing.T) {
	u, err := Generate(1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[PlanetClass]bool{}
	for _, p := range u.Planets {
		seen[p.Class] = true
	}
	if len(seen) != int(planetClassCount) {
		t.Errorf("%d of %d planet classes present", len(seen), planetClassCount)
	}
}
