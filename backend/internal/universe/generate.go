package universe

import (
	"fmt"
	"math/rand/v2"
	"slices"
)

// maxAttempts bounds the reseed loop. Validation failures should be rare; a
// universe that cannot be generated in this many tries means the parameters
// are wrong, not that we were unlucky.
const maxAttempts = 16

// treeDepthBound is how deep a region tree may grow from its hub. It leaves
// room inside trunkReach for the detours one-way lanes force, since reaching
// the trunk is a directed question and a tree depth is not.
const treeDepthBound = 10

// extraLaneReach is how far along the tree an extra lane may reach. Joining a
// sector to something two or three tree hops away makes a short cycle, which
// is what "more than one way through" means in a graph. Hops are the only
// distance this universe has.
const extraLaneReach = 3

// minSectors is the smallest universe the shape rules can be satisfied in.
const minSectors = 16

// Generation knobs. None of these values are defended by anything but taste
// yet; the design doc says they need a universe to look at before they can be
// tuned. Until that happens they are constants, and the whole interface to
// generation is a seed and a size.
const (
	// oneWayPercent is the share of eligible lanes made one-directional.
	oneWayPercent = 15
	// portPercent is roughly what share of sectors hold a port.
	portPercent = 33
	// regionalPublishedPercent is the share of regional lanes in the public
	// map at the big bang. Core and trunk lanes are always published and
	// pocket lanes never are.
	regionalPublishedPercent = 90
)

// coreSize is how many sectors make up protected space.
func coreSize(n int) int { return max(4, min(10, n/8)) }

// hubCount is how many sectors anchor the trunk.
func hubCount(n int) int { return max(3, n/100) }

// extraRegionalLanes is how many non-tree lanes each region gets. A region
// tree alone averages two lanes per sector, which is a chain, not a mesh; half
// the region again in extra lanes lifts that to a little under three.
func extraRegionalLanes(n int) int { return max(2, n/hubCount(n)/2) }

// pocketCount is how many pockets to carve.
func pocketCount(n int) int { return max(1, n/100) }

// Generate builds a universe of the given size for a seed. Pure: the same
// arguments always produce the same universe, which matters for tests and for
// reproducing a report, but is no longer load-bearing now that the map is
// stored rather than recomputed.
//
// It validates and reseeds rather than repairing a bad graph in place.
func Generate(seed int64, sectors int) (*Universe, error) {
	if sectors < minSectors {
		return nil, fmt.Errorf("universe needs at least %d sectors, got %d", minSectors, sectors)
	}
	var last error
	for attempt := range maxAttempts {
		u := generate(seed, attempt, sectors)
		if u == nil {
			last = fmt.Errorf("%w: final graph is not sound", errPoorlyShaped)
			continue
		}
		if err := u.validate(); err != nil {
			last = err
			continue
		}
		if err := u.wellShaped(); err != nil {
			last = err
			continue
		}
		return u, nil
	}
	return nil, fmt.Errorf("no valid universe in %d attempts: %w", maxAttempts, last)
}

// trunkReach is the furthest any sector may be from a trunk lane. The highway
// has to be usable from wherever you are, or the trunk structure is
// decoration. Region trees are built to treeDepthBound; the slack above it
// absorbs the detours one-way lanes force, because reaching the trunk is a
// directed question and a tree depth is not.
const trunkReach = 16

// sectorKind and laneKind are the generator's own vocabulary for what it is
// building. They drive the passes and are never recorded in the map: the
// world knows only sectors, lanes, publication and protected space.
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
)

func newRNG(seed int64, attempt int) *rand.Rand {
	return rand.New(rand.NewPCG(uint64(seed), uint64(attempt)))
}

// workLane is a directed edge in the working set, the same shape the output
// has. A two-way lane is two of them; making a lane one-way deletes one. There
// is nothing here but from and to: no position, no length, no orientation.
// This is a graph, and the generator only ever reasons about nodes and edges.
type workLane struct {
	from, to  int
	kind      laneKind
	published bool
}

// builder holds the working state. Sector indices are 0-based internally and
// become 1-based IDs on output. The core takes the first indices and the hubs
// the next, so sector 1 is in the core by construction.
type builder struct {
	rng *rand.Rand
	n   int

	kind []sectorKind
	core []int
	hubs []int

	lanes   []workLane
	hasPort []bool
}

func generate(seed int64, attempt, n int) *Universe {
	b := &builder{
		rng:     newRNG(seed, attempt),
		n:       n,
		kind:    make([]sectorKind, n),
		hasPort: make([]bool, n),
	}

	b.buildCore()
	b.buildTrunk()
	b.buildRegions()

	b.convertOneWay()
	b.carvePockets()
	b.placePorts()

	// Every lane mutation above was tested against sound, so this holds; it is
	// asserted once more here so the guarantee is checked, not argued. The lane
	// cap is not repaired here either: every pass checks it before adding, and
	// if one slips past, validate rejects the map and the loop reseeds.
	if !b.sound() {
		return nil
	}
	return b.emit(seed)
}

// buildCore takes the first coreSize(n) sectors as protected space and wires them
// densely but inside the lane cap: a ring so they are certainly connected, then
// chords in a shuffled order wherever both ends still have room.
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
		for j := i + 2; j < c; j++ { // ring neighbours are already joined
			pairs = append(pairs, [2]int{b.core[i], b.core[j]})
		}
	}
	b.rng.Shuffle(len(pairs), func(x, y int) { pairs[x], pairs[y] = pairs[y], pairs[x] })
	for _, pr := range pairs {
		// Leave room on every core sector for the trunk splice.
		if b.degreeOf(pr[0]) >= laneCap-2 || b.degreeOf(pr[1]) >= laneCap-2 {
			continue
		}
		b.addTwoWay(pr[0], pr[1], laneCore, true)
	}
}

// buildTrunk designates the next Hubs sectors as hubs, joins them in a loop,
// and splices the core into it. A loop rather than a line means the highway
// has no dead end and there are always two ways around a blockade. Which hubs
// sit next to each other on the ring is arbitrary: a ring is a ring.
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
	// Splice: open one ring segment and route both its ends through the core.
	b.removeTwoWay(b.hubs[0], b.hubs[1])
	b.addTwoWay(b.hubs[0], b.core[0], laneTrunk, true)
	b.addTwoWay(b.hubs[1], b.core[1], laneTrunk, true)
}

// buildRegions deals the remaining sectors out to the hubs in contiguous runs,
// then wires each region as a tree rooted at its hub. The tree is what
// guarantees every sector can reach the trunk; the extra lanes give more than
// one way through.
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

// growTree attaches each member to a random sector already in the tree that
// still has room, under both a degree bound and a depth bound. The degree
// bound stops the tree pushing a sector past the lane cap on its own. The
// depth bound stops it growing into a long thin chain, which is how a sector
// ends up a dozen hops from the hub that anchors it. Bounding depth makes "the
// trunk is always nearby" true by construction instead of by luck.
//
// Then it adds the region's extra lanes, each joining a sector to another a
// few tree hops away, so the region has short cycles rather than only a tree.
func (b *builder) growTree(hub int, members []int) {
	inTree := []int{hub}
	depth := make([]int, b.n)
	treeAdj := make([][]int, b.n)
	for _, m := range members {
		var admissible []int
		for _, t := range inTree {
			if b.degreeOf(t) < laneCap-2 && depth[t] < treeDepthBound {
				admissible = append(admissible, t)
			}
		}
		// Every tree node is saturated: attach anywhere rather than orphan
		// the sector, and let the cap pass and validation sort it out.
		if len(admissible) == 0 {
			admissible = inTree
		}
		parent := admissible[b.rng.IntN(len(admissible))]
		b.addTwoWay(parent, m, laneRegional, b.publishRegional())
		depth[m] = depth[parent] + 1
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
		b.addTwoWay(a, z, laneRegional, b.publishRegional())
		added++
	}
}

// extraLaneCandidates returns the sectors two to extraLaneReach tree hops from
// start: close enough that joining one makes a short cycle, not so close that
// it is already a neighbour.
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

// convertOneWay makes a share of ordinary lanes one-directional, which is what
// makes the map feel like TW2002 rather than a road atlas. Every conversion is
// tested against strong connectivity and reverted if it would strand anyone.
//
// A sector's only lane is never converted: per the design, a spur has to let
// the player back out the way they came.
func (b *builder) convertOneWay() {
	var eligible [][2]int
	for _, l := range b.lanes {
		if l.from > l.to || l.kind != laneRegional || !b.hasLane(l.to, l.from) {
			continue // visit each two-way regional pair once
		}
		if b.degreeOf(l.from) < 2 || b.degreeOf(l.to) < 2 {
			continue // sole lane of a spur; must stay two-way
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
		if b.sound() {
			converted++
			continue
		}
		b.lanes = append(b.lanes, removed)
	}
}

// carvePockets builds the structures players hunt for: a sector entered by one
// unpublished one-way lane and left by another to somewhere else. Concealed,
// and never a trap — the exit is a real lane, discoverable by scanning from
// inside.
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

		// Try it against a snapshot. Restoring the whole working set is both
		// simpler and more obviously correct than unpicking each change.
		saved := slices.Clone(b.lanes)
		b.takeLane(p, in) // the way in becomes one-way, towards the pocket
		for i := range b.lanes {
			if b.lanes[i].from == in && b.lanes[i].to == p {
				b.lanes[i].kind, b.lanes[i].published = lanePocket, false
			}
		}
		b.addOneWay(p, exit, lanePocket, false)

		if b.sound() {
			b.kind[p] = sectorPocket
			carved++
			continue
		}
		b.lanes = saved
	}
}

// pocketExit picks where a pocket leads: any sector with room for another lane
// that is not the pocket itself, not the way in, and not already joined to it.
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

// placePorts seeds trading posts. Roughly a third of sectors overall, weighted
// so hubs almost always have one and the periphery rarely does. The spawn
// always has one, because a new admiral has to be able to buy fuel; the rest
// of protected space is protected, not ported, and rolls like anywhere else.
// Pockets mostly stay empty: a pocket with no port is what a player wants to
// build their own in.
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

// emit freezes the universe. IDs are indices plus one, so the core is sectors
// 1..coreSize(n) and the hubs follow it. Only what is true in the world is
// recorded; the generator's kinds stay behind, and which lanes start public
// goes to PublicAtBigBang rather than onto the lanes. Everything comes out in
// canonical order so the whole value compares byte for byte.
func (b *builder) emit(seed int64) *Universe {
	u := &Universe{
		Version: version,
		Seed:    seed,
		Spawn:   b.core[0] + 1,
		Sectors: make([]Sector, b.n),
		Lanes:   make([]Lane, 0, len(b.lanes)),
	}
	for i := range b.n {
		u.Sectors[i] = Sector{ID: i + 1, Core: b.kind[i] == sectorCore}
		if b.hasPort[i] {
			u.Ports = append(u.Ports, Port{Sector: i + 1})
		}
	}
	for _, l := range b.lanes {
		lane := Lane{From: l.from + 1, To: l.to + 1}
		u.Lanes = append(u.Lanes, lane)
		if l.published {
			u.PublicAtBigBang = append(u.PublicAtBigBang, lane)
		}
	}
	slices.SortFunc(u.Lanes, cmpLane)
	slices.SortFunc(u.PublicAtBigBang, cmpLane)
	u.index()
	return u
}

func cmpLane(x, y Lane) int {
	if x.From != y.From {
		return x.From - y.From
	}
	return x.To - y.To
}

// --- working-set helpers ---

func (b *builder) hasLane(from, to int) bool {
	for _, l := range b.lanes {
		if l.from == from && l.to == to {
			return true
		}
	}
	return false
}

// joined reports whether any lane runs between two sectors in either direction.
func (b *builder) joined(a, z int) bool {
	return b.hasLane(a, z) || b.hasLane(z, a)
}

// neighboursOf returns the distinct sectors joined to this one in either
// direction. Its length is the number the lane cap applies to.
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

// addTwoWay adds a lane in both directions. Reports false if the pair is
// already joined.
func (b *builder) addTwoWay(a, z int, kind laneKind, published bool) bool {
	if a == z || b.joined(a, z) {
		return false
	}
	b.addOneWay(a, z, kind, published)
	b.addOneWay(z, a, kind, published)
	return true
}

// takeLane removes one directed lane and returns it, so a caller trying a
// conversion can put it back.
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

func (b *builder) publishRegional() bool {
	return b.rng.IntN(100) < regionalPublishedPercent
}

// graph builds the directed adjacency from the working set.
func (b *builder) graph() *graph {
	g := newGraph(b.n)
	for _, l := range b.lanes {
		g.add(l.from, l.to)
	}
	return g
}

// sound reports whether the working graph still satisfies the two structural
// invariants every conversion has to preserve: nobody is stranded, and the
// trunk is reachable from everywhere inside trunkReach hops.
//
// Both are checked here rather than left to validation because a conversion
// that breaks either is simply reverted, and reverting one is far cheaper than
// throwing away a whole universe. Reaching the trunk is a directed question,
// so a one-way lane pointing the wrong way can push a sector well past its
// tree depth.
func (b *builder) sound() bool {
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
