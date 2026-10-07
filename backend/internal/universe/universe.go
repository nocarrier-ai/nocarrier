// Package universe generates the sector map at the big bang: a pure function
// of a seed, run once, whose output is then stored and read back by every
// instance.
//
// The map is generated once and stored in a KV bucket, then loaded on startup.
// It is not recomputed per process: generation is pure so that it is
// reproducible, not so that it can be repeated in production.
//
// The stored map holds only what is true in the world and never changes:
// sectors, which of them are protected space, the lanes between them, where
// the ports are, and which lanes were public at the moment of creation. The generator's own vocabulary — hubs, trunk,
// regions, pockets — describes how the map was built and is deliberately not
// recorded. A player finds a pocket by its topology and learns the highway by
// its traffic; a label would hand over the thing they are meant to discover.
//
// See docs/game/universe.md. This package implements the skeleton and
// character passes — sectors, lanes and port placement. Port economies and
// planets are separate passes and are not here yet.
package universe

// version records which generator built a map. It is provenance only: the map
// is stored, so changing the generator cannot disturb a universe that already
// exists, and nothing dispatches on this.
const version = 1

// laneCap is the maximum number of lanes a sector may have, inherited from
// TradeWars 2002. It keeps the mesh sparse enough that topology is worth
// learning, and it applies to the core too.
const laneCap = 6

// Sector is a node. Sectors are numbered from 1; the core takes the lowest
// numbers, following TW2002's FedSpace, so sector 1 is always protected space.
type Sector struct {
	ID int
	// Core marks protected space: densely connected, fully published, holding
	// the spawn. It is the one generation-time classification with meaning
	// after the big bang, because the rules of the game differ inside it.
	// Protected does not mean ported.
	Core bool
}

// Lane is a directed edge. A two-way lane is two Lane records.
//
// A lane has no length, no capacity, and no publication flag. This is a graph,
// not Euclidean space: warp lanes exist precisely because there are no
// coordinates, and the only distance between sectors is a count of hops.
// Whether a lane is public is not a property of the lane, because it changes
// over time by player action; see PublicAtBigBang.
type Lane struct {
	From int
	To   int
}

// Port is a trading post. Placement is all this carries for now; per-commodity
// stances, stock and regeneration come with the economy pass.
type Port struct {
	Sector int
}

// Universe is the generated map. Lanes are sorted by (From, To) and Ports by
// Sector, so the whole value serializes canonically and two instances can be
// compared byte for byte.
type Universe struct {
	// Version is which generator built this map. Provenance, nothing more.
	Version int
	Seed    int64
	// Spawn is the core sector new admirals are commissioned into.
	Spawn   int
	Sectors []Sector
	Lanes   []Lane
	Ports   []Port
	// PublicAtBigBang is the subset of Lanes that were common knowledge at
	// the moment of creation, in the same canonical order as Lanes. It is
	// immutable, like everything else here: it records a fact about the big
	// bang. The public map as it stands *now* — this set plus every lane an
	// admiral has since published — is a projection seeded from it, folding
	// LanePublished events, and lives in its own store.
	PublicAtBigBang []Lane

	// exits[i] holds indices into Lanes for lanes leaving sector i+1.
	exits [][]int
	// portAt[i] is true when sector i+1 has a port.
	portAt []bool
}

// Count returns the number of sectors.
func (u *Universe) Count() int { return len(u.Sectors) }

// Exits returns the lanes leaving a sector, in canonical order. Empty for an
// unknown sector; never empty for a known one, because nobody gets stranded.
func (u *Universe) Exits(sectorID int) []Lane {
	if sectorID < 1 || sectorID > len(u.exits) {
		return nil
	}
	out := make([]Lane, 0, len(u.exits[sectorID-1]))
	for _, i := range u.exits[sectorID-1] {
		out = append(out, u.Lanes[i])
	}
	return out
}

// Sector looks a sector up by ID.
func (u *Universe) Sector(sectorID int) (Sector, bool) {
	if sectorID < 1 || sectorID > len(u.Sectors) {
		return Sector{}, false
	}
	return u.Sectors[sectorID-1], true
}

// HasPort reports whether a sector holds a port.
func (u *Universe) HasPort(sectorID int) bool {
	if sectorID < 1 || sectorID > len(u.portAt) {
		return false
	}
	return u.portAt[sectorID-1]
}

// index builds the derived lookups. Called once, after Lanes is final and
// sorted.
//
// Out-of-range references are skipped rather than panicking, so a malformed
// universe — one decoded from somewhere rather than generated — reaches
// validate and gets a proper error instead of taking the process down.
func (u *Universe) index() {
	n := len(u.Sectors)
	u.exits = make([][]int, n)
	for i := range u.Lanes {
		if from := u.Lanes[i].From; from >= 1 && from <= n {
			u.exits[from-1] = append(u.exits[from-1], i)
		}
	}
	u.portAt = make([]bool, n)
	for _, p := range u.Ports {
		if p.Sector >= 1 && p.Sector <= n {
			u.portAt[p.Sector-1] = true
		}
	}
}
