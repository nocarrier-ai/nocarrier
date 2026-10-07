package universe

import (
	"errors"
	"slices"
	"testing"
)

// mutate copies a known-good universe, applies a change, and re-indexes. Used
// to prove each invariant actually fires, rather than trusting that the
// generator happens never to break it.
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
		PublicAtBigBang: slices.Clone(src.PublicAtBigBang),
	}
	change(u)
	u.index()
	return u
}

func TestValidateAcceptsAGeneratedUniverse(t *testing.T) {
	u := mutate(t, func(*Universe) {})
	if err := u.validate(); err != nil {
		t.Fatalf("a freshly generated universe failed validation: %v", err)
	}
}

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
		// Sever every way out of one sector: the trap case the whole design
		// forbids.
		"stranded sector": func(u *Universe) {
			u.Lanes = slices.DeleteFunc(u.Lanes, func(l Lane) bool { return l.From == 40 })
		},
		// Reachable by nobody: the trap case from the other direction.
		"unreachable sector": func(u *Universe) {
			u.Lanes = slices.DeleteFunc(u.Lanes, func(l Lane) bool { return l.To == 41 })
		},
		"port in a sector that does not exist": func(u *Universe) {
			u.Ports = append(u.Ports, Port{Sector: 100000})
		},
		"two ports in one sector": func(u *Universe) {
			u.Ports = append(u.Ports, Port{Sector: u.Ports[0].Sector})
		},
		"no ports at all": func(u *Universe) { u.Ports = nil },
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

// A spur whose single lane is one-way is a trap by another name, and the one
// change that is easy to make by accident.
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
