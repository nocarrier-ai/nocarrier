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

var sellsTwo = universe.Terms{
	{Sells: true, Capacity: 1000, Regen: 5},
	{Sells: false, Capacity: 2000, Regen: 10},
	{Sells: true, Capacity: 3000, Regen: 15},
}

func portCreated(sectorID string, terms universe.Terms) port.PortCreated {
	var available port.Available
	for c, g := range terms {
		available[c] = g.Capacity
	}
	return port.PortCreated{
		Type:     port.PortCreatedType,
		SectorID: sectorID,
		State:    port.State{Commodities: terms, Available: available},
	}
}

func traded(sectorID string, c universe.Commodity, units int, available port.Available) port.TradeCompleted {
	return port.TradeCompleted{
		Type:      port.TradeCompletedType,
		SectorID:  sectorID,
		ShipID:    "s1",
		Commodity: c,
		Units:     units,
		State:     port.State{Commodities: sellsTwo, Available: available},
		Tick:      3,
	}
}

func TestDecodePortState(t *testing.T) {
	for name, ev := range map[string]any{
		"created": portCreated("1", sellsTwo),
		"traded":  traded("1", universe.Organics, 50, port.Available{1000, 1950, 3000}),
	} {
		t.Run(name, func(t *testing.T) {
			data, err := json.Marshal(ev)
			if err != nil {
				t.Fatal(err)
			}
			sectorID, st, ok := decodePortState(data)
			if !ok || sectorID != "1" || st.Commodities != sellsTwo || st.Available[universe.FuelOre] != 1000 {
				t.Fatalf("decode = %q, %+v, %v", sectorID, st, ok)
			}
		})
	}
}

func TestDecodePortStateSkips(t *testing.T) {
	for name, data := range map[string]string{
		"other type":        `{"type":"TickResolved","sector_id":"1"}`,
		"missing sector ID": `{"type":"TradeCompleted","units":5}`,
		"bad json":          `{`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, ok := decodePortState([]byte(data)); ok {
				t.Error("decoded, want skip")
			}
		})
	}
}

func TestPortStatusProjection(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	proj, err := NewPortStatus(ctx, js)
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

	// PortCreated makes the entry: the terms, everything at capacity.
	natstest.Publish(t, js, streams.PortSubject("1"), portCreated("1", sellsTwo))
	created := waitFor("1", PortStatusEntry{SectorID: "1", Commodities: [3]CommodityStatus{
		{Sells: true, Capacity: 1000, Available: 1000},
		{Sells: false, Capacity: 2000, Available: 2000},
		{Sells: true, Capacity: 3000, Available: 3000},
	}})
	if created.Seq == 0 {
		t.Error("entry recorded no stream sequence")
	}

	natstest.Publish(t, js, streams.PortSubject("1"), traded("1", universe.FuelOre, 100, port.Available{900, 2000, 3000}))
	got := waitFor("1", PortStatusEntry{SectorID: "1", Commodities: [3]CommodityStatus{
		{Sells: true, Capacity: 1000, Available: 900},
		{Sells: false, Capacity: 2000, Available: 2000},
		{Sells: true, Capacity: 3000, Available: 3000},
	}})
	if got.Seq <= created.Seq {
		t.Errorf("seq %d after trade, want past the creation's %d", got.Seq, created.Seq)
	}

	// Events this projection does not fold must leave the entry alone.
	natstest.Publish(t, js, streams.PortSubject("1"), map[string]any{"type": "Nope", "sector_id": "1"})
	natstest.Publish(t, js, streams.PortSubject("3"), portCreated("3", sellsTwo))
	waitFor("3", PortStatusEntry{SectorID: "3", Commodities: [3]CommodityStatus{
		{Sells: true, Capacity: 1000, Available: 1000},
		{Sells: false, Capacity: 2000, Available: 2000},
		{Sells: true, Capacity: 3000, Available: 3000},
	}})
	if again, _, err := proj.Status(ctx, "1"); err != nil || again != got {
		t.Errorf("port 1 = %+v, want unchanged %+v (err %v)", again, got, err)
	}
}

func TestPortStatusMissingPort(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	proj, err := NewPortStatus(ctx, js)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, found, err := proj.Status(ctx, "2"); err != nil || found {
		t.Errorf("found = %v, err = %v; want not found", found, err)
	}
}
