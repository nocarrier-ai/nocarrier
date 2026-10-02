package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/doctrine"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type Doctrine struct {
	kv jetstream.KeyValue
}

func NewDoctrine(ctx context.Context, js jetstream.JetStream) (*Doctrine, error) {
	kv, err := js.KeyValue(ctx, streams.BucketDoctrine)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", streams.BucketDoctrine, err)
	}
	return &Doctrine{kv: kv}, nil
}

func (d *Doctrine) Name() string { return "doctrine" }

func (d *Doctrine) FilterSubjects() []string {
	return []string{streams.DoctrineEvents}
}

func (d *Doctrine) Apply(ctx context.Context, msg jetstream.Msg) error {
	ev, ok := decodeDoctrineUpdated(msg.Data())
	if !ok {
		return nil
	}

	var (
		stored doctrine.DoctrineUpdated
		rev    uint64
	)
	entry, err := d.kv.Get(ctx, ev.AvatarID)
	switch {
	case err == nil:
		if err := json.Unmarshal(entry.Value(), &stored); err != nil {
			return fmt.Errorf("entry %s: %w", ev.AvatarID, err)
		}
		rev = entry.Revision()
	case !errors.Is(err, jetstream.ErrKeyNotFound):
		return err
	}
	if rev != 0 && stored.Revision >= ev.Revision {
		return nil
	}

	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if rev == 0 {
		_, err = d.kv.Create(ctx, ev.AvatarID, data)
	} else {
		_, err = d.kv.Update(ctx, ev.AvatarID, data, rev)
	}
	return err
}

func decodeDoctrineUpdated(data []byte) (doctrine.DoctrineUpdated, bool) {
	var head struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &head) != nil || head.Type != doctrine.DoctrineUpdatedType {
		return doctrine.DoctrineUpdated{}, false
	}
	var ev doctrine.DoctrineUpdated
	if json.Unmarshal(data, &ev) != nil || ev.AvatarID == "" {
		return doctrine.DoctrineUpdated{}, false
	}
	return ev, true
}
