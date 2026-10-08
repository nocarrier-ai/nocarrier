package universe

import (
	"errors"
	"slices"
	"testing"
)

// mutate copies a good big bang, applies a change, and re-indexes the map.
func mutate(t *testing.T, change func(*BigBang)) *BigBang {
	t.Helper()
	src, err := Generate(5, 64)
	if err != nil {
		t.Fatal(err)
	}
	u := &Universe{
		Version: src.Map.Version, Seed: src.Map.Seed, Spawn: src.Map.Spawn,
		Sectors:         slices.Clone(src.Map.Sectors),
		Lanes:           slices.Clone(src.Map.Lanes),
		PublicAtBigBang: slices.Clone(src.Map.PublicAtBigBang),
	}
	bb := &BigBang{Map: u, Ports: slices.Clone(src.Ports), Planets: slices.Clone(src.Planets)}
	change(bb)
	u.index()
	return bb
}

// A generated universe passes both validations.
func TestValidateAcceptsAGeneratedUniverse(t *testing.T) {
	bb := mutate(t, func(*BigBang) {})
	if err := bb.Map.validate(); err != nil {
		t.Fatalf("a freshly generated map failed validation: %v", err)
	}
	if err := bb.validate(); err != nil {
		t.Fatalf("a freshly generated roster failed validation: %v", err)
	}
}

// Each structural fault in the map is caught and reported as errInvalid.
func TestValidateRejects(t *testing.T) {
	cases := map[string]func(*Universe){
		"renumbered sector": func(u *Universe) { u.Sectors[5].ID = 999 },
		"wrong sector count": func(u *Universe) {
			u.Sectors = u.Sectors[:len(u.Sectors)-1]
		},
		"spawn outside protected space": func(u *Universe) {
			for _, s := range u.Sectors {
				if !s.Core {
					u.Spawn = s.ID
					return
				}
			}
		},
		"spawn does not exist": func(u *Universe) { u.Spawn = 100000 },
		"lane to nowhere":      func(u *Universe) { u.Lanes[3].To = 100000 },
		"lane from nowhere":    func(u *Universe) { u.Lanes[3].From = 0 },
		"lane to itself":       func(u *Universe) { u.Lanes[3].To = u.Lanes[3].From },
		"duplicate lane":       func(u *Universe) { u.Lanes = append(u.Lanes, u.Lanes[0]) },
		"public lane that does not exist": func(u *Universe) {
			u.PublicAtBigBang = append(u.PublicAtBigBang, Lane{From: 1, To: 100000})
		},
		"stranded sector": func(u *Universe) { // no way out
			u.Lanes = slices.DeleteFunc(u.Lanes, func(l Lane) bool { return l.From == 40 })
		},
		"unreachable sector": func(u *Universe) { // no way in
			u.Lanes = slices.DeleteFunc(u.Lanes, func(l Lane) bool { return l.To == 41 })
		},
	}
	for label, change := range cases {
		t.Run(label, func(t *testing.T) {
			bb := mutate(t, func(bb *BigBang) { change(bb.Map) })
			err := bb.Map.validate()
			if err == nil {
				t.Fatal("validation passed")
			}
			if !errors.Is(err, errInvalid) {
				t.Fatalf("err = %v, want errInvalid", err)
			}
			t.Log(err)
		})
	}
}

// Each fault in the roster is caught at generation and reported as errInvalid.
func TestBigBangValidateRejects(t *testing.T) {
	cases := map[string]func(*BigBang){
		"port in a sector that does not exist": func(bb *BigBang) {
			bb.Ports = append(bb.Ports, Port{Sector: 100000})
		},
		"two ports in one sector": func(bb *BigBang) {
			bb.Ports = append(bb.Ports, Port{Sector: bb.Ports[0].Sector})
		},
		"ports out of order": func(bb *BigBang) {
			bb.Ports[0], bb.Ports[1] = bb.Ports[1], bb.Ports[0]
		},
		"no ports at all":         func(bb *BigBang) { bb.Ports = nil },
		"no port at the spawn":    func(bb *BigBang) { bb.Ports = bb.Ports[1:] },
		"port with zero capacity": func(bb *BigBang) { bb.Ports[0].Commodities[FuelOre].Capacity = 0 },
		"port with zero regen":    func(bb *BigBang) { bb.Ports[0].Commodities[Organics].Regen = 0 },
		"planet in a sector that does not exist": func(bb *BigBang) {
			bb.Planets = append(bb.Planets, Planet{Sector: 100000})
		},
		"planet with unknown class":      func(bb *BigBang) { bb.Planets[0].Class = PlanetClass(len(PlanetClasses)) },
		"planet with negative colonists": func(bb *BigBang) { bb.Planets[0].InitialColonists = -1 },
		"too many planets in one sector": func(bb *BigBang) {
			for range maxPlanetsPerSector + 1 {
				bb.Planets = append(bb.Planets, Planet{Sector: 30})
			}
		},
		"no planet at the spawn": func(bb *BigBang) {
			bb.Planets = slices.DeleteFunc(bb.Planets, func(p Planet) bool { return p.Sector == bb.Map.Spawn })
		},
	}
	for label, change := range cases {
		t.Run(label, func(t *testing.T) {
			bb := mutate(t, change)
			err := bb.validate()
			if err == nil {
				t.Fatal("validation passed")
			}
			if !errors.Is(err, errInvalid) {
				t.Fatalf("err = %v, want errInvalid", err)
			}
			t.Log(err)
		})
	}
}

// A one-neighbour sector whose only lane is one-way is rejected.
func TestValidateRejectsOneWaySpur(t *testing.T) {
	bb := mutate(t, func(bb *BigBang) {
		u := bb.Map
		for _, s := range u.Sectors {
			if len(u.neighbours(s.ID)) != 1 {
				continue
			}
			only := u.neighbours(s.ID)[0]
			u.Lanes = slices.DeleteFunc(u.Lanes, func(l Lane) bool {
				return l.From == only && l.To == s.ID
			})
			return
		}
	})
	err := bb.Map.validate()
	if err == nil || !errors.Is(err, errInvalid) {
		t.Fatalf("err = %v, want errInvalid", err)
	}
	t.Log(err)
}
