package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/port"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

// PortStatusEntry is the Phoenix-facing current state of one port: the map's
// terms per commodity beside what is available. Seq is the stream sequence of
// the last event folded in; zero for a port that has never traded.
type PortStatusEntry struct {
	SectorID string                                `json:"sector_id"`
	Goods    [len(universe.Commodities)]GoodStatus `json:"goods"`
	Seq      uint64                                `json:"seq"`
}

// GoodStatus is one commodity's terms and availability at a port.
type GoodStatus struct {
	Sells     bool `json:"sells"`
	Capacity  int  `json:"capacity"`
	Available int  `json:"available"`
}

// PortStatus folds port.* into the port-status bucket, keyed by sector. Every
// port has an entry from the start, at rest, so a reader never needs the map.
type PortStatus struct {
	kv jetstream.KeyValue
	u  *universe.Universe
}

func NewPortStatus(ctx context.Context, js jetstream.JetStream, u *universe.Universe) (*PortStatus, error) {
	kv, err := js.KeyValue(ctx, streams.BucketPortStatus)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", streams.BucketPortStatus, err)
	}
	s := &PortStatus{kv: kv, u: u}
	if err := s.seed(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *PortStatus) Name() string { return "port-status" }

func (s *PortStatus) FilterSubjects() []string {
	return []string{streams.PortEvents}
}

// seed writes the at-rest entry for every port that has none. Create fails on
// a key that exists, so a restart or a second instance changes nothing.
func (s *PortStatus) seed(ctx context.Context) error {
	for _, p := range s.u.Ports {
		id := strconv.Itoa(p.Sector)
		data, err := json.Marshal(s.entry(id, p, port.AtRest(p), 0))
		if err != nil {
			return err
		}
		if _, err := s.kv.Create(ctx, id, data); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
			return fmt.Errorf("seed port %s: %w", id, err)
		}
	}
	return nil
}

// Status reads one port's current state. Reports false when the sector has no
// port.
func (s *PortStatus) Status(ctx context.Context, sectorID string) (PortStatusEntry, bool, error) {
	entry, err := s.kv.Get(ctx, sectorID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return PortStatusEntry{}, false, nil
	}
	if err != nil {
		return PortStatusEntry{}, false, err
	}
	var e PortStatusEntry
	if err := json.Unmarshal(entry.Value(), &e); err != nil {
		return PortStatusEntry{}, false, fmt.Errorf("entry %s: %w", sectorID, err)
	}
	return e, true, nil
}

func (s *PortStatus) Apply(ctx context.Context, msg jetstream.Msg) error {
	ev, ok := decodeTradeCompleted(msg.Data())
	if !ok {
		return nil
	}
	sector, err := strconv.Atoi(ev.SectorID)
	if err != nil {
		return nil
	}
	p, ok := s.u.PortInSector(sector)
	if !ok {
		return nil
	}
	md, err := msg.Metadata()
	if err != nil {
		return err
	}

	var (
		stored PortStatusEntry
		rev    uint64
	)
	entry, err := s.kv.Get(ctx, ev.SectorID)
	switch {
	case err == nil:
		if err := json.Unmarshal(entry.Value(), &stored); err != nil {
			return fmt.Errorf("entry %s: %w", ev.SectorID, err)
		}
		rev = entry.Revision()
	case !errors.Is(err, jetstream.ErrKeyNotFound):
		return err
	}
	if md.Sequence.Stream <= stored.Seq {
		return nil
	}

	data, err := json.Marshal(s.entry(ev.SectorID, p, ev.Available, md.Sequence.Stream))
	if err != nil {
		return err
	}
	if rev == 0 {
		_, err = s.kv.Create(ctx, ev.SectorID, data)
	} else {
		_, err = s.kv.Update(ctx, ev.SectorID, data, rev)
	}
	return err
}

func (s *PortStatus) entry(id string, p universe.Port, a port.Available, seq uint64) PortStatusEntry {
	e := PortStatusEntry{SectorID: id, Seq: seq}
	for c, g := range p.Goods {
		e.Goods[c] = GoodStatus{Sells: g.Sells, Capacity: g.Capacity, Available: a[c]}
	}
	return e
}

func decodeTradeCompleted(data []byte) (port.TradeCompleted, bool) {
	var head struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &head) != nil || head.Type != port.TradeCompletedType {
		return port.TradeCompleted{}, false
	}
	var ev port.TradeCompleted
	if json.Unmarshal(data, &ev) != nil || ev.SectorID == "" {
		return port.TradeCompleted{}, false
	}
	return ev, true
}
