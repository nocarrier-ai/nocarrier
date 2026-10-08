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

func createPort(t *testing.T, h *Handler, sectorID string, terms universe.Terms) {
	t.Helper()
	if _, err := h.Create(natstest.Context(t), create(sectorID, terms)); err != nil {
		t.Fatalf("create port in sector %s: %v", sectorID, err)
	}
}

func portEvents(t *testing.T, js jetstream.JetStream) map[string]uint64 {
	t.Helper()
	return natstest.SubjectCounts(t, js, streams.StreamEvents, streams.PortEvents)
}

// The big bang creates ports before the clock exists: PortCreated carries the
// terms, every commodity at capacity, tick 0.
func TestCreateBeforeTheClockIsTickZero(t *testing.T) {
	js := natstest.Start(t)
	ev, err := NewHandler(js, testUniverse()).Create(natstest.Context(t), create("1", sellsTwo))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	want := PortCreated{
		Type:     PortCreatedType,
		SectorID: "1",
		State:    State{Commodities: sellsTwo, Available: Available{1000, 2000, 3000}},
		Tick:     0,
	}
	if ev != want {
		t.Errorf("event = %+v, want %+v", ev, want)
	}
	var stored PortCreated
	if !natstest.Last(t, js, streams.StreamEvents, streams.PortSubject("1"), &stored) {
		t.Fatal("nothing appended")
	}
	if stored != want {
		t.Errorf("stored = %+v, want %+v", stored, want)
	}
}

func TestCreateStampsTheCurrentTick(t *testing.T) {
	js := startUniverse(t)
	advance(t, js, 0, 1)
	advance(t, js, 1, 2)
	ev, err := NewHandler(js, testUniverse()).Create(natstest.Context(t), create("1", sellsTwo))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if ev.Tick != 1 {
		t.Errorf("tick = %d, want 1", ev.Tick)
	}
}

func TestCreateIsOncePerSector(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	h := NewHandler(js, testUniverse())

	createPort(t, h, "1", sellsTwo)
	if _, err := h.Create(ctx, create("1", buysTwo)); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("second err = %v, want ErrAlreadyExists", err)
	}
	if n := portEvents(t, js)["port.1"]; n != 1 {
		t.Errorf("port.1 has %d events, want 1", n)
	}
	var stored PortCreated
	natstest.Last(t, js, streams.StreamEvents, streams.PortSubject("1"), &stored)
	if stored.Commodities != sellsTwo {
		t.Errorf("stored terms %+v; the rejected create overwrote the original", stored.Commodities)
	}
}

func TestCreateRejectsInvalidWithoutAppending(t *testing.T) {
	js := natstest.Start(t)
	_, err := NewHandler(js, testUniverse()).Create(natstest.Context(t), create("99", sellsTwo))
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if got := portEvents(t, js); len(got) != 0 {
		t.Errorf("appended %v", got)
	}
}

func TestTradeStartsFromCapacity(t *testing.T) {
	js := startUniverse(t)
	advance(t, js, 0, 1)
	advance(t, js, 1, 2)
	h := NewHandler(js, testUniverse())
	createPort(t, h, "1", sellsTwo)

	ev, err := h.Trade(natstest.Context(t), trade("1", "s1", universe.FuelOre, 100))
	if err != nil {
		t.Fatalf("trade: %v", err)
	}
	want := TradeCompleted{
		Type:      TradeCompletedType,
		SectorID:  "1",
		ShipID:    "s1",
		Commodity: universe.FuelOre,
		Units:     100,
		State:     State{Commodities: sellsTwo, Available: Available{900, 2000, 3000}},
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

// The second trade's books come from the first trade's event, not from
// PortCreated.
func TestTradeContinuesFromLastEvent(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js, testUniverse())
	createPort(t, h, "1", sellsTwo)

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
	if n := portEvents(t, js)["port.1"]; n != 3 {
		t.Errorf("port.1 has %d events, want created plus two trades", n)
	}
}

func TestTradeRejectsMoreThanAvailable(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js, testUniverse())
	createPort(t, h, "1", sellsTwo)

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
	if n := portEvents(t, js)["port.1"]; n != 2 {
		t.Errorf("port.1 has %d events, want created plus the one trade that was filled", n)
	}
}

func TestTradeKeepsPortsSeparate(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js, testUniverse())
	createPort(t, h, "1", sellsTwo)
	createPort(t, h, "3", buysTwo)

	if _, err := h.Trade(ctx, trade("1", "s1", universe.FuelOre, 100)); err != nil {
		t.Fatalf("port 1: %v", err)
	}
	ev, err := h.Trade(ctx, trade("3", "s1", universe.FuelOre, 100))
	if err != nil {
		t.Fatalf("port 3: %v", err)
	}
	if ev.Commodities != buysTwo || ev.Available != (Available{400, 500, 500}) {
		t.Errorf("port 3 = %+v, want its own terms and books", ev.State)
	}
}

// A sector with no PortCreated has no port, whether or not it exists.
func TestTradeRejectsSectorWithoutPort(t *testing.T) {
	js := startUniverse(t)
	ctx := natstest.Context(t)
	h := NewHandler(js, testUniverse())
	createPort(t, h, "1", sellsTwo)
	for _, sector := range []string{"2", "99"} {
		if _, err := h.Trade(ctx, trade(sector, "s1", universe.FuelOre, 1)); !errors.Is(err, ErrNoPort) {
			t.Errorf("sector %s err = %v, want ErrNoPort", sector, err)
		}
	}
	if n := portEvents(t, js)["port.1"]; n != 1 || len(portEvents(t, js)) != 1 {
		t.Errorf("port events = %v, want only the creation", portEvents(t, js))
	}
}

func TestTradeRejectsInvalidWithoutAppending(t *testing.T) {
	js := startUniverse(t)
	h := NewHandler(js, testUniverse())
	createPort(t, h, "1", sellsTwo)
	_, err := h.Trade(natstest.Context(t), trade("1", "s.1", universe.FuelOre, 1))
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if n := portEvents(t, js)["port.1"]; n != 1 {
		t.Errorf("port.1 has %d events, want only the creation", n)
	}
}

func TestTradeBeforeFirstTickStampsZero(t *testing.T) {
	js := startUniverse(t)
	h := NewHandler(js, testUniverse())
	createPort(t, h, "1", sellsTwo)
	ev, err := h.Trade(natstest.Context(t), trade("1", "s1", universe.FuelOre, 1))
	if err != nil {
		t.Fatalf("trade: %v", err)
	}
	if ev.Tick != 0 {
		t.Errorf("tick = %d, want 0", ev.Tick)
	}
}
