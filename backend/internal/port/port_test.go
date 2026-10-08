package port

import (
	"errors"
	"strings"
	"testing"

	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

// testUniverse has sectors 1 to 3 and nothing else; ports are events.
func testUniverse() *universe.Universe {
	return &universe.Universe{Sectors: []universe.Sector{{ID: 1, Core: true}, {ID: 2}, {ID: 3}}}
}

var (
	sellsTwo = universe.Terms{
		{Sells: true, Capacity: 1000, Regen: 5},
		{Sells: false, Capacity: 2000, Regen: 10},
		{Sells: true, Capacity: 3000, Regen: 15},
	}
	buysTwo = universe.Terms{
		{Sells: false, Capacity: 500, Regen: 2},
		{Sells: true, Capacity: 500, Regen: 2},
		{Sells: false, Capacity: 500, Regen: 2},
	}
)

func create(sectorID string, terms universe.Terms) CreatePort {
	return CreatePort{SectorID: sectorID, Commodities: terms}
}

func trade(sectorID, shipID string, c universe.Commodity, units int) Trade {
	return Trade{SectorID: sectorID, ShipID: shipID, Commodity: c, Units: units}
}

func TestCreateValidateAccepts(t *testing.T) {
	for name, c := range map[string]CreatePort{
		"spawn":       create("1", sellsTwo),
		"last sector": create("3", buysTwo),
	} {
		t.Run(name, func(t *testing.T) {
			if err := c.validate(testUniverse()); err != nil {
				t.Fatalf("validate: %v", err)
			}
		})
	}
}

func TestCreateValidateRejects(t *testing.T) {
	zeroCapacity, zeroRegen := sellsTwo, sellsTwo
	zeroCapacity[universe.FuelOre].Capacity = 0
	zeroRegen[universe.Organics].Regen = 0
	for name, c := range map[string]CreatePort{
		"missing sector ID":  create("", sellsTwo),
		"non-numeric sector": create("hub", sellsTwo),
		"signed sector":      create("+1", sellsTwo),
		"unknown sector":     create("99", sellsTwo),
		"zero capacity":      create("1", zeroCapacity),
		"zero regen":         create("1", zeroRegen),
		"no terms":           create("1", universe.Terms{}),
	} {
		t.Run(name, func(t *testing.T) {
			if err := c.validate(testUniverse()); !errors.Is(err, ErrInvalid) {
				t.Fatalf("validate err = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestTradeValidateAccepts(t *testing.T) {
	for name, c := range map[string]Trade{
		"fuel ore":     trade("1", "s1", universe.FuelOre, 100),
		"equipment":    trade("1000", "s1", universe.Equipment, 1),
		"slug ship ID": trade("42", "uss_cheesewheel-2", universe.Organics, 7),
	} {
		t.Run(name, func(t *testing.T) {
			if err := c.validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
		})
	}
}

func TestTradeValidateRejects(t *testing.T) {
	for name, c := range map[string]Trade{
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
	} {
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
	err = create("1", universe.Terms{}).validate(testUniverse())
	if err == nil || !strings.Contains(err.Error(), "Fuel Ore") {
		t.Errorf("err = %v, want it to name the commodity", err)
	}
}

func TestAtRestIsEveryCommodityAtCapacity(t *testing.T) {
	if got := atRest(sellsTwo); got != (Available{1000, 2000, 3000}) {
		t.Errorf("at rest = %v", got)
	}
}
