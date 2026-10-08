package projector

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/port"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

// portUniverse has ports in sectors 1 and 3 and none in sector 2.
func portUniverse() *universe.Universe {
	return &universe.Universe{
		Sectors: []universe.Sector{{ID: 1, Core: true}, {ID: 2}, {ID: 3}},
		Ports: []universe.Port{
			{Sector: 1, Goods: universe.Goods{
				{Sells: true, Capacity: 1000, Regen: 5},
				{Sells: false, Capacity: 2000, Regen: 10},
				{Sells: true, Capacity: 3000, Regen: 15},
			}},
			{Sector: 3, Goods: universe.Goods{
				{Sells: false, Capacity: 500, Regen: 2},
				{Sells: true, Capacity: 500, Regen: 2},
				{Sells: false, Capacity: 500, Regen: 2},
			}},
		},
	}
}

func traded(sectorID string, c universe.Commodity, units int, available port.Available) port.TradeCompleted {
	return port.TradeCompleted{
		Type:      port.TradeCompletedType,
		SectorID:  sectorID,
		ShipID:    "s1",
		Commodity: c,
		Units:     units,
		Available: available,
		Tick:      3,
	}
}

func TestDecodeTradeCompleted(t *testing.T) {
	data, err := json.Marshal(traded("1", universe.Organics, 50, port.Available{1000, 1950, 3000}))
	if err != nil {
		t.Fatal(err)
	}
	ev, ok := decodeTradeCompleted(data)
	if !ok || ev.SectorID != "1" || ev.Units != 50 || ev.Available != (port.Available{1000, 1950, 3000}) {
		t.Fatalf("decode = %+v, %v", ev, ok)
	}
}

func TestDecodeTradeCompletedSkips(t *testing.T) {
	for name, data := range map[string]string{
		"other type":        `{"type":"TickResolved","sector_id":"1"}`,
		"missing sector ID": `{"type":"TradeCompleted","units":5}`,
		"bad json":          `{`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := decodeTradeCompleted([]byte(data)); ok {
				t.Error("decoded, want skip")
			}
		})
	}
}

// Every port has a status before any trade: the map's terms, everything at
// capacity, no sequence. A sector without a port has none.
func TestPortStatusSeedsEveryPortAtRest(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	proj, err := NewPortStatus(ctx, js, portUniverse())
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	got, ok, err := proj.Status(ctx, "1")
	if err != nil || !ok {
		t.Fatalf("status = %v, %v", ok, err)
	}
	want := PortStatusEntry{SectorID: "1", Goods: [3]GoodStatus{
		{Sells: true, Capacity: 1000, Available: 1000},
		{Sells: false, Capacity: 2000, Available: 2000},
		{Sells: true, Capacity: 3000, Available: 3000},
	}}
	if got != want {
		t.Errorf("port 1 = %+v, want %+v", got, want)
	}
	if _, ok, err := proj.Status(ctx, "3"); err != nil || !ok {
		t.Errorf("port 3 status = %v, %v; want seeded", ok, err)
	}
	if _, ok, err := proj.Status(ctx, "2"); err != nil || ok {
		t.Errorf("sector 2 found = %v, err = %v; want no port", ok, err)
	}
}

func TestPortStatusProjection(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	proj, err := NewPortStatus(ctx, js, portUniverse())
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	natstest.Run(t, NewLoop(js, natstest.Logger(), proj))

	waitFor := func(sectorID string, want PortStatusEntry) PortStatusEntry {
		t.Helper()
		var got PortStatusEntry
		natstest.Eventually(t, 5*time.Second, func() bool {
			e, ok, err := proj.Status(ctx, sectorID)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			got = e
			want.Seq = e.Seq
			return ok && e == want
		})
		return got
	}

	natstest.Publish(t, js, streams.PortSubject("1"), traded("1", universe.FuelOre, 100, port.Available{900, 2000, 3000}))
	got := waitFor("1", PortStatusEntry{SectorID: "1", Goods: [3]GoodStatus{
		{Sells: true, Capacity: 1000, Available: 900},
		{Sells: false, Capacity: 2000, Available: 2000},
		{Sells: true, Capacity: 3000, Available: 3000},
	}})
	if got.Seq == 0 {
		t.Error("entry recorded no stream sequence")
	}

	// Events this projection does not fold must leave the entry alone.
	natstest.Publish(t, js, streams.PortSubject("1"), map[string]any{"type": "Nope", "sector_id": "1"})
	natstest.Publish(t, js, streams.PortSubject("3"), traded("3", universe.Organics, 500, port.Available{500, 0, 500}))
	waitFor("3", PortStatusEntry{SectorID: "3", Goods: [3]GoodStatus{
		{Sells: false, Capacity: 500, Available: 500},
		{Sells: true, Capacity: 500, Available: 0},
		{Sells: false, Capacity: 500, Available: 500},
	}})

	if again, _, err := proj.Status(ctx, "1"); err != nil || again != got {
		t.Errorf("port 1 = %+v, want unchanged %+v (err %v)", again, got, err)
	}

	// Seeding again, as a restart does, must not put a traded port back at rest.
	if _, err := NewPortStatus(ctx, js, portUniverse()); err != nil {
		t.Fatalf("reseed: %v", err)
	}
	if again, _, err := proj.Status(ctx, "1"); err != nil || again != got {
		t.Errorf("after reseed port 1 = %+v, want unchanged %+v (err %v)", again, got, err)
	}
}
