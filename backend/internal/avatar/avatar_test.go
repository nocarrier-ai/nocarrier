package avatar

import (
	"errors"
	"strings"
	"testing"

	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

func command(avatarID, name, shipName string) CommissionAdmiral {
	return CommissionAdmiral{AvatarID: avatarID, Name: name, ShipName: shipName}
}

func TestValidateAccepts(t *testing.T) {
	cases := map[string]CommissionAdmiral{
		"plain":            command("a1", "Akbar", "USS Cheesewheel"),
		"uuid avatar ID":   command("3f2b9c1e-7d4a-4f8e-9a6b-1c2d3e4f5a6b", "Akbar", "Cheesewheel"),
		"slug avatar ID":   command("fleet_admiral-akbar", "Akbar", "Cheesewheel"),
		"punctuated names": command("a1", "Akbar, the 2nd", "H.M.S. Dial-Tone"),
		"unicode names":    command("a1", "Ümit Akbar", "Ünité"),
		"at name limit":    command("a1", strings.Repeat("n", nameLimit), strings.Repeat("s", nameLimit)),
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if err := c.validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
		})
	}
}

func TestValidateRejects(t *testing.T) {
	cases := map[string]CommissionAdmiral{
		"missing avatar ID":       command("", "Akbar", "Cheesewheel"),
		"dotted avatar ID":        command("a.1", "Akbar", "Cheesewheel"),
		"wildcard avatar ID":      command("a*", "Akbar", "Cheesewheel"),
		"tail wildcard avatar ID": command("a>", "Akbar", "Cheesewheel"),
		"spaced avatar ID":        command("a 1", "Akbar", "Cheesewheel"),
		"email avatar ID":         command("kevin@relay", "Akbar", "Cheesewheel"),
		"slash avatar ID":         command("a/1", "Akbar", "Cheesewheel"),
		"unicode avatar ID":       command("ünit-7", "Akbar", "Cheesewheel"),
		"missing name":            command("a1", "", "Cheesewheel"),
		"missing ship name":       command("a1", "Akbar", ""),
		"name over limit":         command("a1", strings.Repeat("n", nameLimit+1), "Cheesewheel"),
		"ship name over limit":    command("a1", "Akbar", strings.Repeat("s", nameLimit+1)),
		"newline in name":         command("a1", "Akbar\nAkbar", "Cheesewheel"),
		"escape in ship name":     command("a1", "Akbar", "Cheese\x1b[31mwheel"),
		"null in ship name":       command("a1", "Akbar", "Cheese\x00wheel"),
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if err := c.validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("validate err = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestValidateNamesTheOffendingField(t *testing.T) {
	err := command("a1", "Akbar", strings.Repeat("s", nameLimit+1)).validate()
	if err == nil || !strings.Contains(err.Error(), "flagship name") {
		t.Errorf("err = %v, want it to name the flagship name", err)
	}
}

// The starting sector is a constant today but becomes a subject token the
// moment it is picked from a map, so it has to satisfy the same rule the
// aggregate enforces on avatar ids.
func TestStartingSectorIsASubjectToken(t *testing.T) {
	if !streams.ValidID(StartingSector) {
		t.Errorf("StartingSector %q is not a valid subject token", StartingSector)
	}
}
