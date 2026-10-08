package port

import (
	"errors"
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

// testUniverse has a port in sector 1 and sector 3, and none in sector 2.
func testUniverse() *universe.Universe {
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

func startUniverse(t *testing.T) jetstream.JetStream {
	t.Helper()
	js := natstest.Start(t)
	u := clock.UniverseCreated{Type: "UniverseCreated", TickPeriod: clock.MinTickPeriod}
	if _, err := streams.Append(natstest.Context(t), js, streams.SubjectClock, u, 0); err != nil {
		t.Fatalf("universe: %v", err)
	}
	return js
}

func advance(t *testing.T, js jetstream.JetStream, tick int64, expected uint64) {
	t.Helper()
	ev := clock.TickAdvanced{Type: "TickAdvanced", Tick: tick, WinnerInstanceID: "test"}
	if _, err := streams.Append(natstest.Context(t), js, streams.SubjectClock, ev, expected); err != nil {
		t.Fatalf("advance to %d: %v", tick, err)
	}
}

func TestTradeStartsFromCapacity(t *testing.T) {
	js := startUniverse(t)
	advance(t, js, 0, 1)
	advance(t, js, 1, 2)

	ev, err := NewHandler(js, testUniverse()).Trade(natstest.Context(t), trade("1", "s1", universe.FuelOre, 100))
	if err != nil {
		t.Fatalf("trade: %v", err)
	}
	want := TradeCompleted{
		Type:      TradeCompletedType,
		SectorID:  "1",
		ShipID:    "s1",
		Commodity: universe.FuelOre,
		Units:     100,
		Available: Available{900, 2000, 3000},
		Tick:      1,
	}
	if ev != want {
		t.Errorf("event = %+v, want %+v", ev, want)
	}

	var stored TradeCompleted
	if !natstest.Last(t, js, streams.StreamEvents, streams.PortSubject("1"), &stored) {
		t.Fatal("nothing appended")
	}
	if stored != want {
		t.Errorf("stored = %+v, want %+v", stored, want)
	}
}

// The second trade's books come from the first trade's event, not the map.
func TestTradeContinuesFromLastEvent(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js, testUniverse())

	if _, err := h.Trade(ctx, trade("1", "s1", universe.FuelOre, 100)); err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := h.Trade(ctx, trade("1", "s2", universe.Organics, 250))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second.Available != (Available{900, 1750, 3000}) {
		t.Errorf("available = %v, want [900 1750 3000]", second.Available)
	}
	if n := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.PortEvents)["port.1"]; n != 2 {
		t.Errorf("port.1 has %d events, want 2", n)
	}
}

func TestTradeRejectsMoreThanAvailable(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js, testUniverse())

	_, err := h.Trade(ctx, trade("1", "s1", universe.FuelOre, 1001))
	if !errors.Is(err, ErrInsufficient) {
		t.Fatalf("err = %v, want ErrInsufficient", err)
	}
	if !strings.Contains(err.Error(), "1000 Fuel Ore available") {
		t.Errorf("err = %v, want it to say what is available", err)
	}
	if _, err := h.Trade(ctx, trade("1", "s1", universe.FuelOre, 1000)); err != nil {
		t.Fatalf("exactly available: %v", err)
	}
	if _, err := h.Trade(ctx, trade("1", "s1", universe.FuelOre, 1)); !errors.Is(err, ErrInsufficient) {
		t.Fatalf("after draining err = %v, want ErrInsufficient", err)
	}
	if n := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.PortEvents)["port.1"]; n != 1 {
		t.Errorf("port.1 has %d events, want only the trade that was filled", n)
	}
}

func TestTradeKeepsPortsSeparate(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js, testUniverse())

	if _, err := h.Trade(ctx, trade("1", "s1", universe.FuelOre, 100)); err != nil {
		t.Fatalf("port 1: %v", err)
	}
	ev, err := h.Trade(ctx, trade("3", "s1", universe.FuelOre, 100))
	if err != nil {
		t.Fatalf("port 3: %v", err)
	}
	if ev.Available != (Available{400, 500, 500}) {
		t.Errorf("port 3 available = %v, want its own books", ev.Available)
	}
}

func TestTradeRejectsSectorWithoutPort(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js, testUniverse())
	for _, sector := range []string{"2", "99"} {
		if _, err := h.Trade(ctx, trade(sector, "s1", universe.FuelOre, 1)); !errors.Is(err, ErrNoPort) {
			t.Errorf("sector %s err = %v, want ErrNoPort", sector, err)
		}
	}
	if got := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.PortEvents); len(got) != 0 {
		t.Errorf("appended %v", got)
	}
}

func TestTradeRejectsInvalidWithoutAppending(t *testing.T) {
	js := startUniverse(t)
	_, err := NewHandler(js, testUniverse()).Trade(natstest.Context(t), trade("1", "s.1", universe.FuelOre, 1))
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if got := natstest.SubjectCounts(t, js, streams.StreamEvents, streams.PortEvents); len(got) != 0 {
		t.Errorf("appended %v", got)
	}
}

func TestTradeBeforeFirstTickStampsZero(t *testing.T) {
	js := startUniverse(t)
	ev, err := NewHandler(js, testUniverse()).Trade(natstest.Context(t), trade("1", "s1", universe.FuelOre, 1))
	if err != nil {
		t.Fatalf("trade: %v", err)
	}
	if ev.Tick != 0 {
		t.Errorf("tick = %d, want 0", ev.Tick)
	}
}
