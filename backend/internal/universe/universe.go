// Package universe generates the sector map at the big bang. Generation is a
// pure function of a seed, run once; the result is stored and loaded by every
// instance.
//
// The map holds only what is true in the world and never changes: sectors,
// which of them are protected space, lanes, ports, and which lanes were public
// at creation. Hubs, trunk, regions and pockets are generator vocabulary and
// are not recorded. See docs/game/universe.md.
package universe

// version is which generator built a map. Provenance only.
const version = 1

// laneCap is the most lanes a sector may have (TW2002's limit).
const laneCap = 6

// Sector is a node. IDs start at 1; the core takes the lowest.
type Sector struct {
	ID int
	// Core marks protected space. It does not imply a port.
	Core bool
}

// Lane is a directed edge; a two-way lane is two records. Distance is hops.
// Whether a lane is public changes over time and is not stored here; see
// PublicAtBigBang.
type Lane struct {
	From int
	To   int
}

// Commodity indexes a port's Goods.
type Commodity uint8

const (
	FuelOre Commodity = iota
	Organics
	Equipment
	commodityCount
)

// Good is a port's terms for one commodity. Stock is runtime state on the
// port aggregate, not here.
type Good struct {
	Sells    bool // sells to ships; otherwise buys from them
	Capacity int  // TW2002's max
	Regen    int  // per tick, toward Capacity
}

// Port is a trading post. Every port trades all three commodities.
type Port struct {
	Sector int
	Goods  [commodityCount]Good
}

// Universe is the generated map. Lanes and PublicAtBigBang are sorted by
// (From, To) and Ports by Sector, so the value is canonical.
type Universe struct {
	Version int
	Seed    int64
	// Spawn is the core sector new admirals start in.
	Spawn   int
	Sectors []Sector
	Lanes   []Lane
	Ports   []Port
	// PublicAtBigBang is the subset of Lanes that were common knowledge at
	// creation. The current public map is a projection seeded from it.
	PublicAtBigBang []Lane

	exits  [][]int // indices into Lanes, by sector
	portAt []bool
}

// Count returns the number of sectors.
func (u *Universe) Count() int { return len(u.Sectors) }

// Exits returns the lanes leaving a sector, in canonical order.
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

// index builds the lookups. Out-of-range references are skipped so a bad
// stored map reaches validate instead of panicking.
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
