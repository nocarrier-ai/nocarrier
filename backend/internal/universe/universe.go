// Package universe generates the sector map at the big bang. Generation is a
// pure function of a seed. The result is stored and loaded by every
// instance.
//
// The map holds only what no aggregate owns and what never changes: sectors,
// which of them are protected space, lanes, and which lanes were public at
// creation. Ports and planets are aggregates: Generate rolls them beside the
// map, the big bang creates each one with a command, and from then on its
// facts live on its own subject. Hubs, trunk, regions and pockets are
// generator vocabulary and are not recorded. See docs/game/universe.md.
package universe

import (
	"fmt"
	"slices"
)

// version is which generator built a map. Provenance only.
const version = 1

// laneCap is the most lanes a sector may have (TW2002's limit).
const laneCap = 6

// Sector is a node. IDs start at 1; the core takes the lowest.
type Sector struct {
	ID int
	// Core marks protected space.
	Core bool
}

// Lane is a directed edge. 2-way lanes are represented with 2 records. Distance is hops,
// so every lane transit is the same thematic distance.
type Lane struct {
	From int
	To   int
}

// Commodity indexes a port's Terms.
type Commodity uint8

const (
	FuelOre Commodity = iota
	Organics
	Equipment
)

// Commodities lists every commodity, in index order.
var Commodities = [...]Commodity{FuelOre, Organics, Equipment}

// String is the TW2002 name.
func (c Commodity) String() string {
	switch c {
	case FuelOre:
		return "Fuel Ore"
	case Organics:
		return "Organics"
	case Equipment:
		return "Equipment"
	}
	return fmt.Sprintf("Commodity(%d)", uint8(c))
}

// CommodityTerms is how a port trades one commodity, as rolled at the big
// bang.
type CommodityTerms struct {
	Sells    bool `json:"sells"`    // sells to ships; otherwise buys from them
	Capacity int  `json:"capacity"` // TW2002's max
	Regen    int  `json:"regen"`    // per tick, toward Capacity
}

// Terms is a port's CommodityTerms for every commodity, indexed by Commodity.
type Terms [len(Commodities)]CommodityTerms

// Port is a trading post to create at the big bang. Every port trades all
// three commodities.
type Port struct {
	Sector      int
	Commodities Terms
}

// PlanetClass is TW2002's planet type. What a class does — production per
// colonist, maximum population, habitability — is the planet aggregate's
// table, not stored here.
type PlanetClass uint8

const (
	ClassM PlanetClass = iota // Earth-like
	ClassK                    // desert
	ClassO                    // oceanic
	ClassL                    // mountainous
	ClassC                    // glacial
	ClassH                    // volcanic
	ClassU                    // gaseous
)

// PlanetClasses lists every class, in index order.
var PlanetClasses = [...]PlanetClass{ClassM, ClassK, ClassO, ClassL, ClassC, ClassH, ClassU}

// Planet is a colonisable world to create at the big bang. Up to
// maxPlanetsPerSector share a sector.
type Planet struct {
	Sector int
	Class  PlanetClass
	// InitialColonists is the population at creation. Zero for almost every
	// planet; a handful start with a small colony, and Terra at the spawn is
	// the colonist source.
	InitialColonists int
}

// Universe is the generated map. Lanes and PublicAtBigBang are sorted by
// (From, To), so the value is canonical.
type Universe struct {
	Version int
	Seed    int64
	// Spawn is the core sector new admirals start in.
	Spawn   int
	Sectors []Sector
	Lanes   []Lane
	// PublicAtBigBang is the subset of Lanes that were common knowledge at
	// creation. The current public map is a projection seeded from it.
	PublicAtBigBang []Lane

	exits [][]int // indices into Lanes, by sector
}

// BigBang is what Generate produces: the map, stored once, and the ports and
// planets to create, each becoming the first event on its own subject. Ports
// and Planets are sorted by sector; a planet's ID is its position here, from 1.
type BigBang struct {
	Map     *Universe
	Ports   []Port
	Planets []Planet
}

// Equal reports whether two maps are the same universe.
func (u *Universe) Equal(v *Universe) bool {
	return u.Version == v.Version && u.Seed == v.Seed && u.Spawn == v.Spawn &&
		slices.Equal(u.Sectors, v.Sectors) && slices.Equal(u.Lanes, v.Lanes) &&
		slices.Equal(u.PublicAtBigBang, v.PublicAtBigBang)
}

// SectorCount returns the number of sectors.
func (u *Universe) SectorCount() int { return len(u.Sectors) }

// ExitsFromSector returns the lanes leaving a sector, in canonical order.
func (u *Universe) ExitsFromSector(sectorID int) []Lane {
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
}
