package port

import (
	"errors"
	"strings"
	"testing"

	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

func trade(sectorID, shipID string, c universe.Commodity, units int) Trade {
	return Trade{SectorID: sectorID, ShipID: shipID, Commodity: c, Units: units}
}

func TestValidateAccepts(t *testing.T) {
	cases := map[string]Trade{
		"fuel ore":     trade("1", "s1", universe.FuelOre, 100),
		"equipment":    trade("1000", "s1", universe.Equipment, 1),
		"slug ship ID": trade("42", "uss_cheesewheel-2", universe.Organics, 7),
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
	cases := map[string]Trade{
		"missing sector ID":  trade("", "s1", universe.FuelOre, 1),
		"sector zero":        trade("0", "s1", universe.FuelOre, 1),
		"negative sector":    trade("-3", "s1", universe.FuelOre, 1),
		"non-numeric sector": trade("hub", "s1", universe.FuelOre, 1),
		"signed sector":      trade("+5", "s1", universe.FuelOre, 1),
		"zero-padded sector": trade("05", "s1", universe.FuelOre, 1),
		"missing ship ID":    trade("1", "", universe.FuelOre, 1),
		"dotted ship ID":     trade("1", "s.1", universe.FuelOre, 1),
		"wildcard ship ID":   trade("1", "s*", universe.FuelOre, 1),
		"unknown commodity":  trade("1", "s1", universe.Commodity(3), 1),
		"zero units":         trade("1", "s1", universe.FuelOre, 0),
		"negative units":     trade("1", "s1", universe.FuelOre, -1),
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
	err := trade("1", "s 1", universe.FuelOre, 1).validate()
	if err == nil || !strings.Contains(err.Error(), "ship ID") {
		t.Errorf("err = %v, want it to name the ship ID", err)
	}
}

func TestAtRestIsEveryCommodityAtCapacity(t *testing.T) {
	p := universe.Port{Sector: 1, Goods: universe.Goods{
		{Sells: true, Capacity: 1000, Regen: 5},
		{Sells: false, Capacity: 2000, Regen: 10},
		{Sells: true, Capacity: 3000, Regen: 15},
	}}
	if got := AtRest(p); got != (Available{1000, 2000, 3000}) {
		t.Errorf("at rest = %v", got)
	}
}
