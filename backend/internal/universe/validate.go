package universe

import (
	"errors"
	"fmt"
	"slices"
)

// errInvalid marks a universe that failed its structural invariants.
var errInvalid = errors.New("invalid universe")

// errPoorlyShaped marks a universe that is sound but came out badly: no hubs
// emerged. Generate reseeds on it.
var errPoorlyShaped = errors.New("poorly shaped universe")

// validate checks structural integrity: the things that, if false, mean the
// map cannot be played at all. References resolve, nobody is stranded, the
// lane cap and the spur rule hold. A stored map that fails this is refused on
// load, because starting anyway would strand ships.
//
// It checks only what is true in the world. The generator's own rules — that
// trunk lanes start public, that a pocket's exit does not — are not recorded
// as such and are not checked here; they are enforced where the generator
// makes them.
//
// It deliberately does not judge quality. A universe that already exists with
// a bland degree distribution is still the universe; refusing to boot over it
// would be strictly worse than running it. Quality is wellShaped's job and is
// only asked of a map at generation time.
func (u *Universe) validate() error {
	for _, check := range []func() error{
		u.checkSectors,
		u.checkLanes,
		u.checkPublicAtBigBang,
		u.checkNobodyStranded,
		u.checkLaneCap,
		u.checkSpurs,
		u.checkPorts,
	} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

// wellShaped checks generation quality: that the trunk structure actually
// emerged. A map that fails this is sound but not worth keeping, so Generate
// reseeds. It is never run against a stored map.
func (u *Universe) wellShaped() error {
	return u.checkHubsEmerged()
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

// checkPublicAtBigBang requires every initially-public lane to be a lane that
// exists; the set is a subset of Lanes, not a second list of them.
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

// checkNobodyStranded is the rule the whole design rests on: every sector can
// reach every other sector. No traps, ever.
func (u *Universe) checkNobodyStranded() error {
	if !u.adjacency().stronglyConnected() {
		return fmt.Errorf("%w: graph is not strongly connected; some sector is a trap", errInvalid)
	}
	for _, s := range u.Sectors {
		if len(u.Exits(s.ID)) == 0 {
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

// checkHubsEmerged proves the trunk structure actually happened rather than
// being hoped for: a few sectors are busy junctions and most are not.
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

// checkSpurs enforces the rule that a sector joined only to one neighbour must
// be able to go back out the way it came. A one-way spur is a trap by another
// name.
//
// Having a single *exit* is not a spur. A sector entered from one neighbour and
// left towards a different one is an ordinary one-way pass-through, which is
// exactly the texture the character pass is for.
func (u *Universe) checkSpurs() error {
	for _, s := range u.Sectors {
		n := u.neighbours(s.ID)
		if len(n) != 1 {
			continue
		}
		var out, back bool
		for _, l := range u.Exits(s.ID) {
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
	seen := make(map[int]bool, len(u.Ports))
	for _, p := range u.Ports {
		if _, ok := u.Sector(p.Sector); !ok {
			return fmt.Errorf("%w: port in unknown sector %d", errInvalid, p.Sector)
		}
		if seen[p.Sector] {
			return fmt.Errorf("%w: sector %d has two ports", errInvalid, p.Sector)
		}
		seen[p.Sector] = true
	}
	if len(u.Ports) == 0 {
		return fmt.Errorf("%w: universe has no ports", errInvalid)
	}
	return nil
}

// laneCount is how many distinct neighbours a sector is joined to, counting a
// two-way lane once. This is the number the lane cap applies to.
func (u *Universe) laneCount(sectorID int) int {
	return len(u.neighbours(sectorID))
}

// neighbours returns the distinct sectors joined to this one in either
// direction, in ascending order.
func (u *Universe) neighbours(sectorID int) []int {
	seen := map[int]bool{}
	for _, l := range u.Exits(sectorID) {
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
