package doctrine

import (
	"errors"
	"strings"
	"testing"
)

func command(avatarID string, orders []string, hooks map[string]string) UpdateDoctrine {
	return UpdateDoctrine{AvatarID: avatarID, DoctrineData: DoctrineData{Orders: orders, Hooks: hooks}}
}

func words(n int) string {
	return strings.TrimSpace(strings.Repeat("w ", n))
}

func TestValidateAccepts(t *testing.T) {
	cases := map[string]UpdateDoctrine{
		"orders and hooks": command("a1", []string{"trade first"}, map[string]string{HookAttacked: "run"}),
		"uuid avatar ID":   command("3f2b9c1e-7d4a-4f8e-9a6b-1c2d3e4f5a6b", []string{"x"}, nil),
		"slug avatar ID":   command("fleet_admiral-akbar", []string{"x"}, nil),
		"no hooks":         command("a1", []string{"trade first"}, nil),
		"multiline order":  command("a1", []string{"trade first\nfight only when cornered"}, nil),
		"at word limit":    command("a1", []string{words(doctrineWordLimit - 1)}, map[string]string{HookMoved: "x"}),
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
	cases := map[string]UpdateDoctrine{
		"missing avatar ID":         command("", []string{"x"}, nil),
		"dotted avatar ID":          command("a.1", []string{"x"}, nil),
		"wildcard avatar ID":        command("a*", []string{"x"}, nil),
		"tail wildcard avatar ID":   command("a>", []string{"x"}, nil),
		"spaced avatar ID":          command("a 1", []string{"x"}, nil),
		"email avatar ID":           command("kevin@relay", []string{"x"}, nil),
		"colon avatar ID":           command("a:1", []string{"x"}, nil),
		"slash avatar ID":           command("a/1", []string{"x"}, nil),
		"dollar avatar ID":          command("$a1", []string{"x"}, nil),
		"unicode avatar ID":         command("ünit-7", []string{"x"}, nil),
		"no orders":                 command("a1", nil, nil),
		"blank order":               command("a1", []string{"x", "  \n "}, nil),
		"unknown hook":              command("a1", []string{"x"}, map[string]string{"bribed": "pay"}),
		"blank hook":                command("a1", []string{"x"}, map[string]string{HookAttacked: ""}),
		"over limit in orders":      command("a1", []string{words(doctrineWordLimit + 1)}, nil),
		"over limit counting hooks": command("a1", []string{words(doctrineWordLimit - 1)}, map[string]string{HookMoved: "x y"}),
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if err := c.validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("validate err = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestValidateReportsFirstUnknownHookStably(t *testing.T) {
	c := command("a1", []string{"x"}, map[string]string{"zeta": "a", "alpha": "b", HookAttacked: "c"})
	for range 20 {
		err := c.validate()
		if err == nil || !strings.Contains(err.Error(), `"alpha"`) {
			t.Fatalf("validate err = %v, want it to name \"alpha\"", err)
		}
	}
}
