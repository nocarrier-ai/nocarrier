package universe

import (
	"errors"
	"fmt"
	"slices"
)

// errInvalid: structural integrity failed.
var errInvalid = errors.New("invalid universe")

// errPoorlyShaped: sound, but no hub structure. Generate reseeds.
var errPoorlyShaped = errors.New("poorly shaped universe")

// validate checks structural integrity: references resolve, nobody is
// stranded, the lane cap and spur rule hold. Runs on load. Does not judge
// shape; see wellShaped.
func (u *Universe) validate() error {
	for _, check := range []func() error{
		u.checkSectors,
		u.checkLanes,
		u.checkPublicAtBigBang,
		u.checkNobodyStranded,
		u.checkLaneCap,
		u.checkSpurs,
		u.checkPorts,
		u.checkPlanets,
	} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

// wellShaped checks generation quality. Run only by Generate.
func (u *Universe) wellShaped() error {
	if err := u.checkHubsEmerged(); err != nil {
		return err
	}
	return u.checkShortcutsExist()
}

func (u *Universe) checkSectors() error {
	for i, s := range u.Sectors {
		if s.ID != i+1 {
			return fmt.Errorf("%w: sector at index %d has ID %d", errInvalid, i, s.ID)
		}
	}
	s, ok := u.Sector(u.Spawn)
	if !ok {
		return fmt.Errorf("%w: spawn sector %d does not exist", errInvalid, u.Spawn)
	}
	if !s.Core {
		return fmt.Errorf("%w: spawn sector %d is not protected space", errInvalid, u.Spawn)
	}
	return nil
}

func (u *Universe) checkLanes() error {
	seen := make(map[[2]int]bool, len(u.Lanes))
	for _, l := range u.Lanes {
		if _, ok := u.Sector(l.From); !ok {
			return fmt.Errorf("%w: lane from unknown sector %d", errInvalid, l.From)
		}
		if _, ok := u.Sector(l.To); !ok {
			return fmt.Errorf("%w: lane to unknown sector %d", errInvalid, l.To)
		}
		if l.From == l.To {
			return fmt.Errorf("%w: sector %d has a lane to itself", errInvalid, l.From)
		}
		key := [2]int{l.From, l.To}
		if seen[key] {
			return fmt.Errorf("%w: duplicate lane %d->%d", errInvalid, l.From, l.To)
		}
		seen[key] = true
	}
	return nil
}

// checkPublicAtBigBang: every initially-public lane exists.
func (u *Universe) checkPublicAtBigBang() error {
	exists := make(map[Lane]bool, len(u.Lanes))
	for _, l := range u.Lanes {
		exists[l] = true
	}
	for _, l := range u.PublicAtBigBang {
		if !exists[l] {
			return fmt.Errorf("%w: public lane %d->%d does not exist", errInvalid, l.From, l.To)
		}
	}
	return nil
}

// checkNobodyStranded: every sector reaches every other. No traps.
func (u *Universe) checkNobodyStranded() error {
	if !u.adjacency().stronglyConnected() {
		return fmt.Errorf("%w: graph is not strongly connected; some sector is a trap", errInvalid)
	}
	for _, s := range u.Sectors {
		if len(u.ExitsFromSector(s.ID)) == 0 {
			return fmt.Errorf("%w: sector %d has no way out", errInvalid, s.ID)
		}
	}
	return nil
}

func (u *Universe) checkLaneCap() error {
	for _, s := range u.Sectors {
		if n := u.laneCount(s.ID); n > laneCap {
			return fmt.Errorf("%w: sector %d has %d lanes, cap is %d", errInvalid, s.ID, n, laneCap)
		}
	}
	return nil
}

// checkHubsEmerged: some sectors are busy, most are quiet.
func (u *Universe) checkHubsEmerged() error {
	busy, quiet := 0, 0
	for _, s := range u.Sectors {
		if u.laneCount(s.ID) >= 4 {
			busy++
		} else {
			quiet++
		}
	}
	if busy == 0 {
		return fmt.Errorf("%w: no sector has 4 or more lanes; the map is uniform", errPoorlyShaped)
	}
	if quiet*2 < len(u.Sectors) {
		return fmt.Errorf("%w: only %d of %d sectors are quiet; the map is a lattice, not a mesh",
			errPoorlyShaped, quiet, len(u.Sectors))
	}
	return nil
}

// checkSpurs: a sector with one neighbour must have a two-way lane to it. One
// exit alone is a pass-through, not a spur.
func (u *Universe) checkSpurs() error {
	for _, s := range u.Sectors {
		n := u.neighbours(s.ID)
		if len(n) != 1 {
			continue
		}
		var out, back bool
		for _, l := range u.ExitsFromSector(s.ID) {
			out = out || l.To == n[0]
		}
		for _, in := range u.inboundFrom(s.ID) {
			back = back || in == n[0]
		}
		if !out || !back {
			return fmt.Errorf("%w: spur %d is joined only to %d and that lane is one-way",
				errInvalid, s.ID, n[0])
		}
	}
	return nil
}

func (u *Universe) checkPorts() error {
	for i, p := range u.Ports {
		if _, ok := u.Sector(p.Sector); !ok {
			return fmt.Errorf("%w: port in unknown sector %d", errInvalid, p.Sector)
		}
		if i > 0 && p.Sector <= u.Ports[i-1].Sector {
			return fmt.Errorf("%w: ports out of order at sector %d", errInvalid, p.Sector)
		}
		for c, g := range p.Goods {
			if g.Capacity <= 0 || g.Regen <= 0 {
				return fmt.Errorf("%w: port %d commodity %d has capacity %d regen %d",
					errInvalid, p.Sector, c, g.Capacity, g.Regen)
			}
		}
	}
	if len(u.Ports) == 0 {
		return fmt.Errorf("%w: universe has no ports", errInvalid)
	}
	return nil
}

// checkPlanets: references resolve, the per-sector cap holds, classes are
// known, and the spawn has a planet for colonists to come from.
func (u *Universe) checkPlanets() error {
	per := make(map[int]int, len(u.Planets))
	for _, p := range u.Planets {
		if _, ok := u.Sector(p.Sector); !ok {
			return fmt.Errorf("%w: planet in unknown sector %d", errInvalid, p.Sector)
		}
		if p.Class >= planetClassCount {
			return fmt.Errorf("%w: planet in sector %d has class %d", errInvalid, p.Sector, p.Class)
		}
		if p.InitialColonists < 0 {
			return fmt.Errorf("%w: planet in sector %d has %d colonists", errInvalid, p.Sector, p.InitialColonists)
		}
		per[p.Sector]++
		if per[p.Sector] > maxPlanetsPerSector {
			return fmt.Errorf("%w: sector %d has more than %d planets", errInvalid, p.Sector, maxPlanetsPerSector)
		}
	}
	if per[u.Spawn] == 0 {
		return fmt.Errorf("%w: spawn sector %d has no planet", errInvalid, u.Spawn)
	}
	return nil
}

// checkShortcutsExist: enough seller-to-buyer routes are shorter over all
// lanes than over public ones.
func (u *Universe) checkShortcutsExist() error {
	all := u.adjacency()
	public := u.publicAdjacency()
	target := shortcutCount(len(u.Sectors))
	found := 0
	for _, c := range Commodities {
		var sellers, buyers []int
		for _, p := range u.Ports {
			if p.Goods[c].Sells {
				sellers = append(sellers, p.Sector-1)
			} else {
				buyers = append(buyers, p.Sector-1)
			}
		}
		for _, s := range sellers {
			da := hopsFromAny(all.out, []int{s})
			dp := hopsFromAny(public.out, []int{s})
			for _, t := range buyers {
				if da[t] == -1 {
					continue
				}
				if dp[t] == -1 || dp[t]-da[t] >= shortcutGain {
					found++
					if found >= target {
						return nil
					}
				}
			}
		}
	}
	return fmt.Errorf("%w: %d trade routes shortened by unpublished lanes, want %d",
		errPoorlyShaped, found, target)
}

// laneCount is distinct neighbours, which is what laneCap applies to.
func (u *Universe) laneCount(sectorID int) int {
	return len(u.neighbours(sectorID))
}

// neighbours: distinct sectors joined in either direction, ascending.
func (u *Universe) neighbours(sectorID int) []int {
	seen := map[int]bool{}
	for _, l := range u.ExitsFromSector(sectorID) {
		seen[l.To] = true
	}
	for _, n := range u.inboundFrom(sectorID) {
		seen[n] = true
	}
	out := make([]int, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	slices.Sort(out)
	return out
}

// inboundFrom returns the sectors with a lane into this one.
func (u *Universe) inboundFrom(sectorID int) []int {
	var out []int
	for _, l := range u.Lanes {
		if l.To == sectorID {
			out = append(out, l.From)
		}
	}
	return out
}
