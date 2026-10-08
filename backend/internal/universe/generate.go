package universe

import (
	"fmt"
	"math/rand/v2"
	"slices"
)

// maxAttempts bounds the reseed loop.
const maxAttempts = 16

// treeDepthBound caps how deep a region tree grows from its hub, leaving slack
// under trunkReach for one-way detours.
const treeDepthBound = 10

// extraLaneReach is how many tree hops away an extra lane may join, so that it
// closes a short cycle.
const extraLaneReach = 3

// minSectors is the smallest universe the shape rules can be met in.
const minSectors = 16

// trunkReach is the most hops any sector may be from a trunk lane.
const trunkReach = 16

// Generation knobs. Constants until there is a universe to tune them against.
const (
	oneWayPercent = 15 // share of eligible lanes made one-way
	portPercent   = 33 // share of sectors with a port
)

// A regional lane's chance of being public at creation falls with tree depth:
// the area around a hub is charted, the frontier is not.
const (
	publicNearHubPercent = 80
	publicDepthStep      = 10 // percent lost per tree hop from the hub
	publicFloorPercent   = 10
)

func coreSize(n int) int { return max(4, min(10, n/8)) }
func hubCount(n int) int { return max(3, n/100) }

// extraRegionalLanes per region. Half the region size lifts average degree
// from about 2 to about 3.
func extraRegionalLanes(n int) int { return max(2, n/hubCount(n)/2) }

func pocketCount(n int) int { return max(1, n/100) }

// Economy knobs.
const (
	capacityMin  = 1000
	capacityMax  = 5000
	regenDivisor = 200 // regen = capacity / regenDivisor per tick
	sellRawBase  = 25  // percent chance a depth-0 port sells Fuel Ore or Organics
	sellRawSlope = 60  // added at full tree depth
	minPairHops  = 8   // a planted seller/buyer pair is at least this far apart
	shortcutGain = 3   // hops an unpublished route must save to count
)

// shortcutCount is how many trade routes must be shortened by unpublished
// lanes.
func shortcutCount(n int) int { return max(2, n/100) }

// Planet knobs.
const (
	maxPlanetsPerSector = 3
	pocketPlanetWeight  = 4  // multiplier on a pocket's chance of a planet
	clusterPercent      = 25 // chance a placement targets a sector that already has a planet
	colonyMin           = 100
	colonyMax           = 500
	terraColonists      = 1_000_000
)

// extraPlanetPercent[k-1] is the chance a sector holding k planets takes one
// more.
var extraPlanetPercent = [maxPlanetsPerSector - 1]int{40, 5}

func planetCount(n int) int    { return max(4, n/10) }
func seededColonies(n int) int { return max(2, n/200) }

// Generate builds a universe of sectors for the given seed. Deterministic.
// A bad result indicates a new seed is required.
func Generate(seed int64, sectors int) (*BigBang, error) {
	if sectors < minSectors {
		return nil, fmt.Errorf("universe needs at least %d sectors, got %d", minSectors, sectors)
	}
	var last error
	for attempt := range maxAttempts {
		bb := generate(seed, attempt, sectors)
		if bb == nil {
			last = fmt.Errorf("%w: final graph is not sound", errPoorlyShaped)
			continue
		}
		if err := bb.Map.validate(); err != nil {
			last = err
			continue
		}
		if err := bb.validate(); err != nil {
			last = err
			continue
		}
		if err := bb.wellShaped(); err != nil {
			last = err
			continue
		}
		return bb, nil
	}
	return nil, fmt.Errorf("no valid universe in %d attempts: %w", maxAttempts, last)
}

// Regenerate reproduces the big bang that built a stored map, for an instance
// that has to finish creating its ports and planets. It fails if the generator
// no longer produces that map.
func Regenerate(stored *Universe) (*BigBang, error) {
	bb, err := Generate(stored.Seed, stored.SectorCount())
	if err != nil {
		return nil, err
	}
	if !bb.Map.Equal(stored) {
		return nil, fmt.Errorf("generator v%d no longer reproduces the stored map (v%d, seed %d)",
			version, stored.Version, stored.Seed)
	}
	return bb, nil
}

// sectorKind and laneKind drive the passes and are not recorded in the map.
type sectorKind uint8

const (
	sectorRegional sectorKind = iota
	sectorCore
	sectorHub
	sectorPocket
)

type laneKind uint8

const (
	laneRegional laneKind = iota
	laneCore
	laneTrunk
	lanePocket
	laneShortcut
)

func newRNG(seed int64, attempt int) *rand.Rand {
	return rand.New(rand.NewPCG(uint64(seed), uint64(attempt)))
}

// workLane is a directed edge in the working set. A two-way lane is two of
// them.
type workLane struct {
	from, to  int
	kind      laneKind
	published bool
}

// builder holds the working state. Indices are 0-based; IDs are index+1. The
// core takes the first indices and the hubs the next.
type builder struct {
	rng *rand.Rand
	n   int

	kind  []sectorKind
	depth []int // tree hops from the region's hub; 0 for core and hubs
	core  []int
	hubs  []int

	lanes   []workLane
	hasPort []bool
	terms   []Terms
	planets []Planet
}

func newBuilder(seed int64, attempt, n int) *builder {
	return &builder{
		rng:     newRNG(seed, attempt),
		n:       n,
		kind:    make([]sectorKind, n),
		depth:   make([]int, n),
		hasPort: make([]bool, n),
		terms:   make([]Terms, n),
	}
}

func generate(seed int64, attempt, n int) *BigBang {
	b := newBuilder(seed, attempt, n)

	b.buildCore()
	b.buildTrunk()
	b.buildRegions()

	b.convertOneWay()
	b.carvePockets()
	b.placePorts()
	b.assignStances()
	b.plantShortcuts()
	b.placePlanets()

	// Every mutating pass checks sound; asserted once more on the final set.
	if !b.graphCorrect() {
		return nil
	}
	return b.emit(seed)
}

// buildCore wires the first coreSize(n) sectors: a ring, then shuffled chords
// while both ends have room.
func (b *builder) buildCore() {
	for i := range coreSize(b.n) {
		b.core = append(b.core, i)
		b.kind[i] = sectorCore
	}
	c := len(b.core)
	for i := range c {
		b.addTwoWay(b.core[i], b.core[(i+1)%c], laneCore, true)
	}
	var pairs [][2]int
	for i := range c {
		for j := i + 2; j < c; j++ {
			pairs = append(pairs, [2]int{b.core[i], b.core[j]})
		}
	}
	b.rng.Shuffle(len(pairs), func(x, y int) { pairs[x], pairs[y] = pairs[y], pairs[x] })
	for _, pr := range pairs {
		// leave room for the trunk splice
		if b.degreeOf(pr[0]) >= laneCap-2 || b.degreeOf(pr[1]) >= laneCap-2 {
			continue
		}
		b.addTwoWay(pr[0], pr[1], laneCore, true)
	}
}

// buildTrunk joins the hubs in a ring and links it to the core
func (b *builder) buildTrunk() {
	for i := range hubCount(b.n) {
		h := coreSize(b.n) + i
		b.hubs = append(b.hubs, h)
		b.kind[h] = sectorHub
	}
	n := len(b.hubs)
	for i := range n {
		b.addTwoWay(b.hubs[i], b.hubs[(i+1)%n], laneTrunk, true)
	}
	// open one segment and route it through the core
	b.removeTwoWay(b.hubs[0], b.hubs[1])
	b.addTwoWay(b.hubs[0], b.core[0], laneTrunk, true)
	b.addTwoWay(b.hubs[1], b.core[1], laneTrunk, true)
}

// buildRegions deals the remaining sectors to the hubs in contiguous runs and
// grows a tree per region.
func (b *builder) buildRegions() {
	first := coreSize(b.n) + hubCount(b.n)
	members := make([][]int, len(b.hubs))
	for i := first; i < b.n; i++ {
		r := (i - first) * len(b.hubs) / (b.n - first)
		b.kind[i] = sectorRegional
		members[r] = append(members[r], i)
	}
	for r, hub := range b.hubs {
		b.growTree(hub, members[r])
	}
}

// growTree attaches each member to a random tree node under the degree and
// depth bounds, then adds the region's extra lanes a few tree hops apart.
func (b *builder) growTree(hub int, members []int) {
	inTree := []int{hub}
	treeAdj := make([][]int, b.n)
	for _, m := range members {
		var admissible []int
		for _, t := range inTree {
			if b.degreeOf(t) < laneCap-2 && b.depth[t] < treeDepthBound {
				admissible = append(admissible, t)
			}
		}
		if len(admissible) == 0 {
			admissible = inTree // all saturated; validate catches an over-cap sector
		}
		parent := admissible[b.rng.IntN(len(admissible))]
		b.depth[m] = b.depth[parent] + 1
		b.addTwoWay(parent, m, laneRegional, b.publicAt(b.depth[m]))
		treeAdj[parent] = append(treeAdj[parent], m)
		treeAdj[m] = append(treeAdj[m], parent)
		inTree = append(inTree, m)
	}

	want := extraRegionalLanes(b.n)
	added := 0
	for attempt := 0; attempt < want*4 && added < want; attempt++ {
		a := inTree[b.rng.IntN(len(inTree))]
		candidates := b.extraLaneCandidates(treeAdj, a)
		if len(candidates) == 0 {
			continue
		}
		z := candidates[b.rng.IntN(len(candidates))]
		if b.degreeOf(a) >= laneCap || b.degreeOf(z) >= laneCap || b.joined(a, z) {
			continue
		}
		b.addTwoWay(a, z, laneRegional, b.publicAt(max(b.depth[a], b.depth[z])))
		added++
	}
}

// extraLaneCandidates returns the sectors 2..extraLaneReach tree hops from
// start.
func (b *builder) extraLaneCandidates(treeAdj [][]int, start int) []int {
	dist := make([]int, b.n)
	for i := range dist {
		dist[i] = -1
	}
	dist[start] = 0
	queue := []int{start}
	var out []int
	for len(queue) > 0 {
		v := queue[0]
		queue = queue[1:]
		if dist[v] >= extraLaneReach {
			continue
		}
		for _, w := range treeAdj[v] {
			if dist[w] != -1 {
				continue
			}
			dist[w] = dist[v] + 1
			if dist[w] >= 2 {
				out = append(out, w)
			}
			queue = append(queue, w)
		}
	}
	return out
}

// convertOneWay makes a share of regional lanes one-directional, reverting any
// that break correctness rules. A spur's only lane is never converted.
func (b *builder) convertOneWay() {
	var eligible [][2]int
	for _, l := range b.lanes {
		if l.from > l.to || l.kind != laneRegional || !b.hasLane(l.to, l.from) {
			continue // each two-way regional pair once
		}
		if b.degreeOf(l.from) < 2 || b.degreeOf(l.to) < 2 {
			continue // sole lane of a spur
		}
		eligible = append(eligible, [2]int{l.from, l.to})
	}
	b.rng.Shuffle(len(eligible), func(x, y int) { eligible[x], eligible[y] = eligible[y], eligible[x] })

	target := len(eligible) * oneWayPercent / 100
	converted := 0
	for _, pr := range eligible {
		if converted >= target {
			break
		}
		from, to := pr[0], pr[1]
		if b.rng.IntN(2) == 0 {
			from, to = to, from
		}
		removed := b.takeLane(from, to)
		if b.graphCorrect() {
			converted++
			continue
		}
		b.lanes = append(b.lanes, removed)
	}
}

// carvePockets turns leaves into pockets: one unpublished one-way lane in, one
// out to somewhere else.
func (b *builder) carvePockets() {
	var leaves []int
	for i := range b.n {
		if b.kind[i] == sectorRegional && b.degreeOf(i) == 1 {
			leaves = append(leaves, i)
		}
	}
	b.rng.Shuffle(len(leaves), func(x, y int) { leaves[x], leaves[y] = leaves[y], leaves[x] })

	carved := 0
	for _, p := range leaves {
		if carved >= pocketCount(b.n) {
			break
		}
		nb := b.neighboursOf(p)
		if len(nb) != 1 {
			continue
		}
		in := nb[0]
		exit := b.pocketExit(p, in)
		if exit == -1 {
			continue
		}

		saved := slices.Clone(b.lanes)
		b.takeLane(p, in)
		for i := range b.lanes {
			if b.lanes[i].from == in && b.lanes[i].to == p {
				b.lanes[i].kind, b.lanes[i].published = lanePocket, false
			}
		}
		b.addOneWay(p, exit, lanePocket, false)

		if b.graphCorrect() {
			b.kind[p] = sectorPocket
			carved++
			continue
		}
		b.lanes = saved
	}
}

// pocketExit picks any sector with room that is not p, not the way in, and not
// already joined to p.
func (b *builder) pocketExit(p, in int) int {
	var candidates []int
	for i := range b.n {
		if i == p || i == in || b.degreeOf(i) >= laneCap || b.joined(p, i) {
			continue
		}
		candidates = append(candidates, i)
	}
	if len(candidates) == 0 {
		return -1
	}
	return candidates[b.rng.IntN(len(candidates))]
}

// placePorts: the spawn always, hubs usually, pockets rarely, everything else
// at portPercent.
func (b *builder) placePorts() {
	for i := range b.n {
		var chance int
		switch b.kind[i] {
		case sectorCore:
			chance = portPercent
			if i == b.core[0] {
				chance = 100
			}
		case sectorHub:
			chance = 90
		case sectorPocket:
			chance = 10
		default:
			chance = portPercent
		}
		b.hasPort[i] = b.rng.IntN(100) < chance
	}
}

// assignStances rolls each port's terms per commodity. Deeper sectors lean
// toward selling Fuel Ore and Organics and buying Equipment; hubs and the core
// lean the other way.
func (b *builder) assignStances() {
	for i := range b.n {
		if !b.hasPort[i] {
			continue
		}
		raw := sellRawBase + sellRawSlope*b.depth[i]/treeDepthBound
		for _, c := range Commodities {
			pct := raw
			if c == Equipment {
				pct = 100 - raw
			}
			capacity := capacityMin + b.rng.IntN(capacityMax-capacityMin+1)
			b.terms[i][c] = CommodityTerms{
				Sells:    b.rng.IntN(100) < pct,
				Capacity: capacity,
				Regen:    max(1, capacity/regenDivisor),
			}
		}
	}
}

// plantShortcuts adds unpublished lanes that shorten the route between a
// seller and a distant buyer of the same commodity.
func (b *builder) plantShortcuts() {
	public := b.publicGraph()
	want := shortcutCount(b.n)
	planted := 0
	for attempt := 0; attempt < want*20 && planted < want; attempt++ {
		c := Commodities[b.rng.IntN(len(Commodities))]
		var sellers, buyers []int
		for i := range b.n {
			if !b.hasPort[i] {
				continue
			}
			if b.terms[i][c].Sells {
				sellers = append(sellers, i)
			} else {
				buyers = append(buyers, i)
			}
		}
		if len(sellers) == 0 || len(buyers) == 0 {
			continue
		}
		s := sellers[b.rng.IntN(len(sellers))]
		t := buyers[b.rng.IntN(len(buyers))]
		if d := hopsFromAny(public.out, []int{s})[t]; d != -1 && d < minPairHops {
			continue
		}
		all := b.graph()
		a := b.pick(b.nearby(all, s))
		z := b.pick(b.nearby(all, t))
		if a == -1 || z == -1 || a == z || b.joined(a, z) {
			continue
		}
		saved := slices.Clone(b.lanes)
		b.addTwoWay(a, z, laneShortcut, false)
		if !b.graphCorrect() {
			b.lanes = saved
			continue
		}
		planted++
	}
}

// nearby: sectors within two hops of x that can take another lane.
func (b *builder) nearby(g *graph, x int) []int {
	var out []int
	for i, d := range hopsFromAny(g.out, []int{x}) {
		if d == -1 || d > 2 || b.kind[i] == sectorPocket || b.degreeOf(i) >= laneCap {
			continue
		}
		out = append(out, i)
	}
	return out
}

func (b *builder) pick(xs []int) int {
	if len(xs) == 0 {
		return -1
	}
	return xs[b.rng.IntN(len(xs))]
}

func (b *builder) publicGraph() *graph {
	g := newGraph(b.n)
	for _, l := range b.lanes {
		if l.published {
			g.add(l.from, l.to)
		}
	}
	return g
}

// placePlanets puts Terra at the spawn point, then scatters planets weighted toward
// tree depth and heavily toward pockets, never in the core. Some placements
// deliberately target a sector that already has a planet, so planets cluster;
// a sector takes an extra planet at extraPlanetPercent. A few planets start
// with a small colony.
func (b *builder) placePlanets() {
	b.planets = append(b.planets, Planet{Sector: b.core[0], Class: ClassM, InitialColonists: terraColonists})

	weights := make([]int, b.n)
	total := 0
	for i := range b.n {
		if b.kind[i] == sectorCore {
			continue
		}
		w := 1 + b.depth[i]
		if b.kind[i] == sectorPocket {
			w *= pocketPlanetWeight
		}
		weights[i] = w
		total += w
	}
	count := make([]int, b.n)
	placed := 0
	for attempt := 0; placed < planetCount(b.n) && attempt < planetCount(b.n)*10; attempt++ {
		i := -1
		if b.rng.IntN(100) < clusterPercent {
			var open []int
			for j := range b.n {
				if count[j] > 0 && count[j] < maxPlanetsPerSector {
					open = append(open, j)
				}
			}
			i = b.pick(open)
		}
		if i == -1 {
			r := b.rng.IntN(total)
			for i = 0; r >= weights[i]; i++ {
				r -= weights[i]
			}
		}
		if count[i] >= maxPlanetsPerSector {
			continue
		}
		if count[i] > 0 && b.rng.IntN(100) >= extraPlanetPercent[count[i]-1] {
			continue
		}
		b.planets = append(b.planets, Planet{Sector: i, Class: PlanetClasses[b.rng.IntN(len(PlanetClasses))]})
		count[i]++
		placed++
	}

	for _, i := range b.rng.Perm(len(b.planets) - 1)[:min(seededColonies(b.n), len(b.planets)-1)] {
		b.planets[i+1].InitialColonists = colonyMin + b.rng.IntN(colonyMax-colonyMin+1)
	}
}

// emit freezes the universe in canonical order. The kinds and classes are discarded
// because they are only used as hints during generation.
func (b *builder) emit(seed int64) *BigBang {
	u := &Universe{
		Version: version,
		Seed:    seed,
		Spawn:   b.core[0] + 1,
		Sectors: make([]Sector, b.n),
		Lanes:   make([]Lane, 0, len(b.lanes)),
	}
	bb := &BigBang{Map: u}
	for i := range b.n {
		u.Sectors[i] = Sector{ID: i + 1, Core: b.kind[i] == sectorCore}
		if b.hasPort[i] {
			bb.Ports = append(bb.Ports, Port{Sector: i + 1, Commodities: b.terms[i]})
		}
	}
	for _, l := range b.lanes {
		lane := Lane{From: l.from + 1, To: l.to + 1}
		u.Lanes = append(u.Lanes, lane)
		if l.published {
			u.PublicAtBigBang = append(u.PublicAtBigBang, lane)
		}
	}
	for _, p := range b.planets {
		p.Sector++
		bb.Planets = append(bb.Planets, p)
	}
	slices.SortFunc(u.Lanes, cmpLane)
	slices.SortFunc(u.PublicAtBigBang, cmpLane)
	slices.SortStableFunc(bb.Planets, func(x, y Planet) int { return x.Sector - y.Sector })
	u.index()
	return bb
}

func cmpLane(x, y Lane) int {
	if x.From != y.From {
		return x.From - y.From
	}
	return x.To - y.To
}

func (b *builder) hasLane(from, to int) bool {
	for _, l := range b.lanes {
		if l.from == from && l.to == to {
			return true
		}
	}
	return false
}

// joined: any lane between a and z, either direction.
func (b *builder) joined(a, z int) bool {
	return b.hasLane(a, z) || b.hasLane(z, a)
}

// neighboursOf: distinct sectors joined to i. Its length is what laneCap
// counts.
func (b *builder) neighboursOf(i int) []int {
	var out []int
	for _, l := range b.lanes {
		switch {
		case l.from == i && !slices.Contains(out, l.to):
			out = append(out, l.to)
		case l.to == i && !slices.Contains(out, l.from):
			out = append(out, l.from)
		}
	}
	return out
}

func (b *builder) degreeOf(i int) int { return len(b.neighboursOf(i)) }

func (b *builder) addOneWay(from, to int, kind laneKind, published bool) {
	if from == to || b.hasLane(from, to) {
		return
	}
	b.lanes = append(b.lanes, workLane{from: from, to: to, kind: kind, published: published})
}

// addTwoWay adds both directions; false if already joined.
func (b *builder) addTwoWay(a, z int, kind laneKind, published bool) bool {
	if a == z || b.joined(a, z) {
		return false
	}
	b.addOneWay(a, z, kind, published)
	b.addOneWay(z, a, kind, published)
	return true
}

// takeLane removes one directed lane and returns it.
func (b *builder) takeLane(from, to int) workLane {
	for i, l := range b.lanes {
		if l.from == from && l.to == to {
			b.lanes = slices.Delete(b.lanes, i, i+1)
			return l
		}
	}
	return workLane{}
}

func (b *builder) removeTwoWay(a, z int) {
	b.takeLane(a, z)
	b.takeLane(z, a)
}

func (b *builder) publicAt(depth int) bool {
	return b.rng.IntN(100) < max(publicFloorPercent, publicNearHubPercent-publicDepthStep*depth)
}

func (b *builder) graph() *graph {
	g := newGraph(b.n)
	for _, l := range b.lanes {
		g.add(l.from, l.to)
	}
	return g
}

// graphCorrect reports whether the working graph is strongly connected and every
// sector is within trunkReach of a trunk lane.
func (b *builder) graphCorrect() bool {
	g := b.graph()
	if !g.stronglyConnected() {
		return false
	}
	var starts []int
	for _, l := range b.lanes {
		if l.kind == laneTrunk || l.kind == laneCore {
			starts = append(starts, l.from, l.to)
		}
	}
	if len(starts) == 0 {
		return false
	}
	for _, n := range hopsFromAny(g.in, starts) {
		if n == -1 || n > trunkReach {
			return false
		}
	}
	return true
}
