package universe

import (
	"errors"
	"slices"
	"testing"
)

// mutate copies a good universe, applies a change, and re-indexes.
func mutate(t *testing.T, change func(*Universe)) *Universe {
	t.Helper()
	src, err := Generate(5, 64)
	if err != nil {
		t.Fatal(err)
	}
	u := &Universe{
		Version: src.Version, Seed: src.Seed, Spawn: src.Spawn,
		Sectors:         slices.Clone(src.Sectors),
		Lanes:           slices.Clone(src.Lanes),
		Ports:           slices.Clone(src.Ports),
		Planets:         slices.Clone(src.Planets),
		PublicAtBigBang: slices.Clone(src.PublicAtBigBang),
	}
	change(u)
	u.index()
	return u
}

// A generated universe passes validate.
func TestValidateAcceptsAGeneratedUniverse(t *testing.T) {
	u := mutate(t, func(*Universe) {})
	if err := u.validate(); err != nil {
		t.Fatalf("a freshly generated universe failed validation: %v", err)
	}
}

// Each structural fault is caught and reported as errInvalid.
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
		"port in a sector that does not exist": func(u *Universe) {
			u.Ports = append(u.Ports, Port{Sector: 100000})
		},
		"two ports in one sector": func(u *Universe) {
			u.Ports = append(u.Ports, Port{Sector: u.Ports[0].Sector})
		},
		"ports out of order": func(u *Universe) {
			u.Ports[0], u.Ports[1] = u.Ports[1], u.Ports[0]
		},
		"no ports at all":         func(u *Universe) { u.Ports = nil },
		"port with zero capacity": func(u *Universe) { u.Ports[0].Goods[FuelOre].Capacity = 0 },
		"port with zero regen":    func(u *Universe) { u.Ports[0].Goods[Organics].Regen = 0 },
		"planet in a sector that does not exist": func(u *Universe) {
			u.Planets = append(u.Planets, Planet{Sector: 100000})
		},
		"planet with unknown class":      func(u *Universe) { u.Planets[0].Class = planetClassCount },
		"planet with negative colonists": func(u *Universe) { u.Planets[0].InitialColonists = -1 },
		"too many planets in one sector": func(u *Universe) {
			for range maxPlanetsPerSector + 1 {
				u.Planets = append(u.Planets, Planet{Sector: 30})
			}
		},
		"no planet at the spawn": func(u *Universe) {
			u.Planets = slices.DeleteFunc(u.Planets, func(p Planet) bool { return p.Sector == u.Spawn })
		},
	}
	for label, change := range cases {
		t.Run(label, func(t *testing.T) {
			u := mutate(t, change)
			err := u.validate()
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
	u := mutate(t, func(u *Universe) {
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
	err := u.validate()
	if err == nil || !errors.Is(err, errInvalid) {
		t.Fatalf("err = %v, want errInvalid", err)
	}
	t.Log(err)
}
