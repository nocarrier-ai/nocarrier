package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/port"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
	"github.com/nocarrier-ai/nocarrier/internal/universe"
)

// PortStatusEntry is the Phoenix-facing current state of one port: its terms
// per commodity beside what is available. Seq is the stream sequence of the
// last event folded in.
type PortStatusEntry struct {
	SectorID    string                                     `json:"sector_id"`
	Commodities [len(universe.Commodities)]CommodityStatus `json:"commodities"`
	Seq         uint64                                     `json:"seq"`
}

// CommodityStatus is one commodity's terms and availability at a port.
type CommodityStatus struct {
	Sells     bool `json:"sells"`
	Capacity  int  `json:"capacity"`
	Available int  `json:"available"`
}

// PortStatus folds port.* into the port-status bucket, keyed by sector. Every
// port event carries the port's state after it, so the fold is a replace
// guarded by Seq; PortCreated makes the entry.
type PortStatus struct {
	kv jetstream.KeyValue
}

func NewPortStatus(ctx context.Context, js jetstream.JetStream) (*PortStatus, error) {
	kv, err := js.KeyValue(ctx, streams.BucketPortStatus)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", streams.BucketPortStatus, err)
	}
	return &PortStatus{kv: kv}, nil
}

func (s *PortStatus) Name() string { return "port-status" }

func (s *PortStatus) FilterSubjects() []string {
	return []string{streams.PortEvents}
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
	sectorID, st, ok := decodePortState(msg.Data())
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
	entry, err := s.kv.Get(ctx, sectorID)
	switch {
	case err == nil:
		if err := json.Unmarshal(entry.Value(), &stored); err != nil {
			return fmt.Errorf("entry %s: %w", sectorID, err)
		}
		rev = entry.Revision()
	case !errors.Is(err, jetstream.ErrKeyNotFound):
		return err
	}
	if md.Sequence.Stream <= stored.Seq {
		return nil
	}

	e := PortStatusEntry{SectorID: sectorID, Seq: md.Sequence.Stream}
	for c, g := range st.Commodities {
		e.Commodities[c] = CommodityStatus{Sells: g.Sells, Capacity: g.Capacity, Available: st.Available[c]}
	}
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if rev == 0 {
		_, err = s.kv.Create(ctx, sectorID, data)
	} else {
		_, err = s.kv.Update(ctx, sectorID, data, rev)
	}
	return err
}

// decodePortState reads the port's state off any event that carries it.
func decodePortState(data []byte) (string, port.State, bool) {
	var ev struct {
		Type     string `json:"type"`
		SectorID string `json:"sector_id"`
		port.State
	}
	if json.Unmarshal(data, &ev) != nil || ev.SectorID == "" {
		return "", port.State{}, false
	}
	switch ev.Type {
	case port.PortCreatedType, port.TradeCompletedType:
		return ev.SectorID, ev.State, true
	}
	return "", port.State{}, false
}
