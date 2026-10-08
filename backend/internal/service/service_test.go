package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/nocarrier-ai/nocarrier/internal/avatar"
	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/doctrine"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/port"
	"github.com/nocarrier-ai/nocarrier/internal/service"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

const (
	doctrineUpdateSubject   = "cmd.doctrine.update"
	avatarCommissionSubject = "cmd.avatar.commission"
	portTradeSubject        = "cmd.port.trade"
)

type fakeUpdater struct {
	ev  doctrine.DoctrineUpdated
	err error

	mu    sync.Mutex
	calls []doctrine.UpdateDoctrine
}

func (f *fakeUpdater) Update(_ context.Context, cmd doctrine.UpdateDoctrine) (doctrine.DoctrineUpdated, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, cmd)
	return f.ev, f.err
}

func (f *fakeUpdater) received() []doctrine.UpdateDoctrine {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]doctrine.UpdateDoctrine(nil), f.calls...)
}

type fakeCommissioner struct {
	ev  avatar.AdmiralCommissioned
	err error

	mu    sync.Mutex
	calls []avatar.CommissionAdmiral
}

func (f *fakeCommissioner) Commission(_ context.Context, cmd avatar.CommissionAdmiral) (avatar.AdmiralCommissioned, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, cmd)
	return f.ev, f.err
}

func (f *fakeCommissioner) received() []avatar.CommissionAdmiral {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]avatar.CommissionAdmiral(nil), f.calls...)
}

type fakeTrader struct {
	ev  port.TradeCompleted
	err error

	mu    sync.Mutex
	calls []port.Trade
}

func (f *fakeTrader) Trade(_ context.Context, cmd port.Trade) (port.TradeCompleted, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, cmd)
	return f.ev, f.err
}

func (f *fakeTrader) received() []port.Trade {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]port.Trade(nil), f.calls...)
}

func startServices(t *testing.T, d service.DoctrineUpdater, a service.AdmiralCommissioner, p service.PortTrader) (jetstream.JetStream, *nats.Conn) {
	t.Helper()
	js := natstest.Start(t)
	natstest.Run(t, service.New(js.Conn(), natstest.Logger(), d, a, p))
	return js, js.Conn()
}

func requestOn(t *testing.T, nc *nats.Conn, subject string, data []byte) *nats.Msg {
	t.Helper()
	var reply *nats.Msg
	natstest.Eventually(t, 5*time.Second, func() bool {
		msg, err := nc.Request(subject, data, time.Second)
		if errors.Is(err, nats.ErrNoResponders) {
			return false
		}
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		reply = msg
		return true
	})
	return reply
}

func request(t *testing.T, nc *nats.Conn, data []byte) *nats.Msg {
	t.Helper()
	return requestOn(t, nc, doctrineUpdateSubject, data)
}

func requestJSON(t *testing.T, nc *nats.Conn, v any) *nats.Msg {
	t.Helper()
	return requestOn(t, nc, doctrineUpdateSubject, marshal(t, v))
}

func commissionJSON(t *testing.T, nc *nats.Conn, v any) *nats.Msg {
	t.Helper()
	return requestOn(t, nc, avatarCommissionSubject, marshal(t, v))
}

func tradeJSON(t *testing.T, nc *nats.Conn, v any) *nats.Msg {
	t.Helper()
	return requestOn(t, nc, portTradeSubject, marshal(t, v))
}

func trade(sectorID, shipID string, c universe.Commodity, units int) port.Trade {
	return port.Trade{SectorID: sectorID, ShipID: shipID, Commodity: c, Units: units}
}

func marshal(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

func commission(avatarID, name, shipName string) avatar.CommissionAdmiral {
	return avatar.CommissionAdmiral{AvatarID: avatarID, Name: name, ShipName: shipName}
}

func update(avatarID string, orders ...string) doctrine.UpdateDoctrine {
	return doctrine.UpdateDoctrine{AvatarID: avatarID, DoctrineData: doctrine.DoctrineData{Orders: orders}}
}

func errorCode(msg *nats.Msg) string {
	return msg.Header.Get(micro.ErrorCodeHeader)
}

func TestDoctrineUpdateReplies(t *testing.T) {
	fake := &fakeUpdater{ev: doctrine.DoctrineUpdated{Revision: 2, Tick: 7, Hash: "abc"}}
	_, nc := startServices(t, fake, &fakeCommissioner{}, &fakeTrader{})

	msg := requestJSON(t, nc, update("a1", "trade first"))
	if code := errorCode(msg); code != "" {
		t.Fatalf("error %s: %s", code, msg.Header.Get(micro.ErrorHeader))
	}
	var reply service.DoctrineUpdateReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if reply != (service.DoctrineUpdateReply{Revision: 2, Tick: 7, Hash: "abc"}) {
		t.Errorf("reply = %+v", reply)
	}

	calls := fake.received()
	if len(calls) != 1 || calls[0].AvatarID != "a1" || calls[0].Orders[0] != "trade first" {
		t.Errorf("handler received %+v", calls)
	}
}

func TestDoctrineUpdateErrorCodes(t *testing.T) {
	cases := map[string]struct {
		err  error
		code string
	}{
		"invalid":  {fmt.Errorf("%w: unknown hook \"bribed\"", doctrine.ErrInvalid), "400"},
		"conflict": {fmt.Errorf("append doctrine: %w", streams.ErrConflict), "409"},
		"other":    {errors.New("nats unavailable"), "500"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, nc := startServices(t, &fakeUpdater{err: c.err}, &fakeCommissioner{}, &fakeTrader{})
			msg := requestJSON(t, nc, update("a1", "x"))
			if code := errorCode(msg); code != c.code {
				t.Errorf("code %q, want %q", code, c.code)
			}
		})
	}
}

func TestDoctrineUpdateInvalidNamesReason(t *testing.T) {
	_, nc := startServices(t, &fakeUpdater{err: fmt.Errorf("%w: unknown hook \"bribed\"", doctrine.ErrInvalid)}, &fakeCommissioner{}, &fakeTrader{})
	msg := requestJSON(t, nc, update("a1", "x"))
	if desc := msg.Header.Get(micro.ErrorHeader); !strings.Contains(desc, "bribed") {
		t.Errorf("description %q does not name the reason", desc)
	}
}

func TestDoctrineUpdateMalformed(t *testing.T) {
	fake := &fakeUpdater{}
	_, nc := startServices(t, fake, &fakeCommissioner{}, &fakeTrader{})

	msg := request(t, nc, []byte(`{"avatar_id":`))
	if code := errorCode(msg); code != "400" {
		t.Errorf("code %q, want 400", code)
	}
	if calls := fake.received(); len(calls) != 0 {
		t.Errorf("handler called with %+v", calls)
	}
}

func TestDoctrineUpdateThroughHandler(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	if _, err := streams.Append(ctx, js, streams.SubjectClock, clock.UniverseCreated{Type: "UniverseCreated"}, 0); err != nil {
		t.Fatalf("universe: %v", err)
	}
	natstest.Run(t, service.New(js.Conn(), natstest.Logger(), doctrine.NewHandler(js), avatar.NewHandler(js), &fakeTrader{}))

	msg := requestJSON(t, js.Conn(), update("a1", "trade first"))
	var reply service.DoctrineUpdateReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil || errorCode(msg) != "" {
		t.Fatalf("reply %q, code %q, err %v", msg.Data, errorCode(msg), err)
	}

	var stored doctrine.DoctrineUpdated
	if !natstest.Last(t, js, streams.StreamEvents, streams.DoctrineSubject("a1"), &stored) {
		t.Fatal("no event appended")
	}
	if reply.Revision != 1 || reply.Tick != -1 || reply.Hash != stored.Hash || stored.Revision != 1 {
		t.Errorf("reply %+v, stored %+v", reply, stored)
	}

	msg = requestJSON(t, js.Conn(), update("a.1", "x"))
	if code := errorCode(msg); code != "400" {
		t.Errorf("invalid avatar ID code %q, want 400", code)
	}
}

func TestAvatarCommissionReplies(t *testing.T) {
	fake := &fakeCommissioner{ev: avatar.AdmiralCommissioned{
		AvatarID:   "a1",
		Name:       "Akbar",
		ShipName:   "USS Cheesewheel",
		HomeSector: avatar.StartingSector,
		Tick:       7,
	}}
	_, nc := startServices(t, &fakeUpdater{}, fake, &fakeTrader{})

	msg := commissionJSON(t, nc, commission("a1", "Akbar", "USS Cheesewheel"))
	if code := errorCode(msg); code != "" {
		t.Fatalf("error %s: %s", code, msg.Header.Get(micro.ErrorHeader))
	}
	var reply service.AvatarCommissionReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	want := service.AvatarCommissionReply{
		AvatarID:   "a1",
		ShipName:   "USS Cheesewheel",
		HomeSector: avatar.StartingSector,
		Tick:       7,
	}
	if reply != want {
		t.Errorf("reply = %+v, want %+v", reply, want)
	}

	calls := fake.received()
	if len(calls) != 1 || calls[0] != commission("a1", "Akbar", "USS Cheesewheel") {
		t.Errorf("handler received %+v", calls)
	}
}

func TestAvatarCommissionErrorCodes(t *testing.T) {
	cases := map[string]struct {
		err  error
		code string
	}{
		"invalid":        {fmt.Errorf("%w: missing admiral name", avatar.ErrInvalid), "400"},
		"already":        {avatar.ErrAlreadyCommissioned, "409"},
		"wrapped always": {fmt.Errorf("commission a1: %w", avatar.ErrAlreadyCommissioned), "409"},
		"other":          {errors.New("nats unavailable"), "500"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, nc := startServices(t, &fakeUpdater{}, &fakeCommissioner{err: c.err}, &fakeTrader{})
			msg := commissionJSON(t, nc, commission("a1", "Akbar", "Cheesewheel"))
			if code := errorCode(msg); code != c.code {
				t.Errorf("code %q, want %q", code, c.code)
			}
		})
	}
}

// A lost race on the avatar subject means the admiral already exists, so it
// must not surface as the doctrine endpoint's retryable 409.
func TestAvatarCommissionAlreadyDoesNotSayResubmit(t *testing.T) {
	_, nc := startServices(t, &fakeUpdater{}, &fakeCommissioner{err: avatar.ErrAlreadyCommissioned}, &fakeTrader{})
	msg := commissionJSON(t, nc, commission("a1", "Akbar", "Cheesewheel"))
	desc := msg.Header.Get(micro.ErrorHeader)
	if !strings.Contains(desc, "already commissioned") || strings.Contains(desc, "resubmit") {
		t.Errorf("description %q", desc)
	}
}

func TestAvatarCommissionInvalidNamesReason(t *testing.T) {
	_, nc := startServices(t, &fakeUpdater{}, &fakeCommissioner{
		err: fmt.Errorf("%w: flagship name is 60 characters, limit is 48", avatar.ErrInvalid),
	}, &fakeTrader{})
	msg := commissionJSON(t, nc, commission("a1", "Akbar", "Cheesewheel"))
	if desc := msg.Header.Get(micro.ErrorHeader); !strings.Contains(desc, "flagship name") {
		t.Errorf("description %q does not name the reason", desc)
	}
}

func TestAvatarCommissionMalformed(t *testing.T) {
	fake := &fakeCommissioner{}
	_, nc := startServices(t, &fakeUpdater{}, fake, &fakeTrader{})

	msg := requestOn(t, nc, avatarCommissionSubject, []byte(`{"avatar_id":`))
	if code := errorCode(msg); code != "400" {
		t.Errorf("code %q, want 400", code)
	}
	if calls := fake.received(); len(calls) != 0 {
		t.Errorf("handler called with %+v", calls)
	}
}

func TestAvatarCommissionThroughHandler(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	if _, err := streams.Append(ctx, js, streams.SubjectClock, clock.UniverseCreated{Type: "UniverseCreated"}, 0); err != nil {
		t.Fatalf("universe: %v", err)
	}
	natstest.Run(t, service.New(js.Conn(), natstest.Logger(), doctrine.NewHandler(js), avatar.NewHandler(js), &fakeTrader{}))

	msg := commissionJSON(t, js.Conn(), commission("a1", "Akbar", "USS Cheesewheel"))
	var reply service.AvatarCommissionReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil || errorCode(msg) != "" {
		t.Fatalf("reply %q, code %q, err %v", msg.Data, errorCode(msg), err)
	}
	if reply.HomeSector != avatar.StartingSector || reply.Tick != 0 {
		t.Errorf("reply = %+v", reply)
	}

	var stored avatar.AdmiralCommissioned
	if !natstest.Last(t, js, streams.StreamEvents, streams.AvatarSubject("a1"), &stored) {
		t.Fatal("no event appended")
	}
	if stored.Name != "Akbar" || stored.ShipName != "USS Cheesewheel" {
		t.Errorf("stored = %+v", stored)
	}

	// Second commission for the same admiral is a permanent 409.
	msg = commissionJSON(t, js.Conn(), commission("a1", "Imposter", "Dial Tone"))
	if code := errorCode(msg); code != "409" {
		t.Errorf("recommission code %q, want 409", code)
	}

	msg = commissionJSON(t, js.Conn(), commission("a.1", "Akbar", "Cheesewheel"))
	if code := errorCode(msg); code != "400" {
		t.Errorf("invalid avatar ID code %q, want 400", code)
	}
}

// Both command endpoints live on the same service; neither may shadow the other.
func TestCommandEndpointsAreIndependent(t *testing.T) {
	updater := &fakeUpdater{ev: doctrine.DoctrineUpdated{Revision: 1}}
	commissioner := &fakeCommissioner{ev: avatar.AdmiralCommissioned{AvatarID: "a1"}}
	trader := &fakeTrader{ev: port.TradeCompleted{SectorID: "1"}}
	_, nc := startServices(t, updater, commissioner, trader)

	if code := errorCode(requestJSON(t, nc, update("a1", "trade first"))); code != "" {
		t.Errorf("doctrine code %q", code)
	}
	if code := errorCode(commissionJSON(t, nc, commission("a1", "Akbar", "Cheesewheel"))); code != "" {
		t.Errorf("commission code %q", code)
	}
	if code := errorCode(tradeJSON(t, nc, trade("1", "s1", universe.FuelOre, 10))); code != "" {
		t.Errorf("trade code %q", code)
	}
	if len(updater.received()) != 1 || len(commissioner.received()) != 1 {
		t.Errorf("doctrine calls %d, commission calls %d; want 1 each",
			len(updater.received()), len(commissioner.received()))
	}
}

func TestPortTradeReplies(t *testing.T) {
	fake := &fakeTrader{ev: port.TradeCompleted{
		Type:      port.TradeCompletedType,
		SectorID:  "1",
		ShipID:    "s1",
		Commodity: universe.Organics,
		Units:     50,
		State:     port.State{Available: port.Available{1000, 1950, 3000}},
		Tick:      7,
	}}
	_, nc := startServices(t, &fakeUpdater{}, &fakeCommissioner{}, fake)

	msg := tradeJSON(t, nc, trade("1", "s1", universe.Organics, 50))
	if code := errorCode(msg); code != "" {
		t.Fatalf("error %s: %s", code, msg.Header.Get(micro.ErrorHeader))
	}
	var reply service.PortTradeReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	want := service.PortTradeReply{
		SectorID:  "1",
		ShipID:    "s1",
		Commodity: universe.Organics,
		Units:     50,
		Available: 1950,
		Tick:      7,
	}
	if reply != want {
		t.Errorf("reply = %+v, want %+v", reply, want)
	}

	calls := fake.received()
	if len(calls) != 1 || calls[0] != trade("1", "s1", universe.Organics, 50) {
		t.Errorf("handler received %+v", calls)
	}
}

func TestPortTradeErrorCodes(t *testing.T) {
	cases := map[string]struct {
		err  error
		code string
	}{
		"invalid":      {fmt.Errorf("%w: units must be positive", port.ErrInvalid), "400"},
		"no port":      {fmt.Errorf("%w: sector 2", port.ErrNoPort), "400"},
		"insufficient": {fmt.Errorf("%w: port 1 has 10 Fuel Ore available", port.ErrInsufficient), "422"},
		"lost race":    {fmt.Errorf("append trade: %w", streams.ErrConflict), "409"},
		"other":        {errors.New("nats unavailable"), "500"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, nc := startServices(t, &fakeUpdater{}, &fakeCommissioner{}, &fakeTrader{err: c.err})
			msg := tradeJSON(t, nc, trade("1", "s1", universe.FuelOre, 10))
			if code := errorCode(msg); code != c.code {
				t.Errorf("code %q, want %q", code, c.code)
			}
		})
	}
}

func TestPortTradeMalformed(t *testing.T) {
	fake := &fakeTrader{}
	_, nc := startServices(t, &fakeUpdater{}, &fakeCommissioner{}, fake)
	msg := requestOn(t, nc, portTradeSubject, []byte(`{"sector_id": 1`))
	if code := errorCode(msg); code != "400" {
		t.Errorf("code %q, want 400", code)
	}
	if calls := fake.received(); len(calls) != 0 {
		t.Errorf("handler received %+v for malformed JSON", calls)
	}
}

func TestPortTradeThroughHandler(t *testing.T) {
	js := natstest.Start(t)
	ctx := natstest.Context(t)
	if _, err := streams.Append(ctx, js, streams.SubjectClock, clock.UniverseCreated{Type: "UniverseCreated"}, 0); err != nil {
		t.Fatalf("universe: %v", err)
	}
	u := &universe.Universe{Sectors: []universe.Sector{{ID: 1, Core: true}, {ID: 2}}}
	ports := port.NewHandler(js, u)
	if _, err := ports.Create(ctx, port.CreatePort{SectorID: "1", Commodities: universe.Terms{
		{Sells: true, Capacity: 1000, Regen: 5},
		{Sells: false, Capacity: 2000, Regen: 10},
		{Sells: true, Capacity: 3000, Regen: 15},
	}}); err != nil {
		t.Fatalf("create port: %v", err)
	}
	natstest.Run(t, service.New(js.Conn(), natstest.Logger(), doctrine.NewHandler(js), avatar.NewHandler(js), ports))

	msg := tradeJSON(t, js.Conn(), trade("1", "s1", universe.FuelOre, 100))
	var reply service.PortTradeReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil || errorCode(msg) != "" {
		t.Fatalf("reply %q, code %q, err %v", msg.Data, errorCode(msg), err)
	}
	if reply.Available != 900 || reply.Tick != 0 {
		t.Errorf("reply = %+v", reply)
	}

	var stored port.TradeCompleted
	if !natstest.Last(t, js, streams.StreamEvents, streams.PortSubject("1"), &stored) {
		t.Fatal("no event appended")
	}
	if stored.Available != (port.Available{900, 2000, 3000}) {
		t.Errorf("stored = %+v", stored)
	}

	msg = tradeJSON(t, js.Conn(), trade("1", "s1", universe.FuelOre, 901))
	if code := errorCode(msg); code != "422" {
		t.Errorf("over-trade code %q, want 422", code)
	}
	msg = tradeJSON(t, js.Conn(), trade("2", "s1", universe.FuelOre, 1))
	if code := errorCode(msg); code != "400" {
		t.Errorf("no-port code %q, want 400", code)
	}
}
