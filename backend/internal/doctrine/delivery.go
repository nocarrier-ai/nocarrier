package doctrine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type Delivery struct {
	js jetstream.JetStream
}

func NewDelivery(js jetstream.JetStream) *Delivery { return &Delivery{js: js} }

func (d *Delivery) Name() string { return "doctrine-delivery" }

func (d *Delivery) Stream() string { return streams.StreamClock }

func (d *Delivery) FilterSubjects() []string {
	return []string{streams.SubjectClock}
}

func (d *Delivery) Apply(ctx context.Context, msg jetstream.Msg) error {
	var tick clock.TickAdvanced
	if json.Unmarshal(msg.Data(), &tick) != nil || tick.Type != clock.TickAdvancedType {
		return nil
	}

	s, err := d.js.Stream(ctx, streams.StreamEvents)
	if err != nil {
		return err
	}
	info, err := s.Info(ctx, jetstream.WithSubjectFilter(streams.DoctrineEvents))
	if err != nil {
		return fmt.Errorf("list %s: %w", streams.DoctrineEvents, err)
	}

	for subject := range info.State.Subjects {
		var last DoctrineUpdated
		seq, err := streams.Last(ctx, d.js, streams.StreamEvents, subject, &last)
		if err != nil {
			return fmt.Errorf("read %s: %w", subject, err)
		}
		if seq == 0 || last.Type != DoctrineUpdatedType || last.Tick >= tick.Tick {
			continue
		}
		last.Type = DoctrineDeliveredType
		last.Tick = tick.Tick
		if _, err := streams.Append(ctx, d.js, subject, last, seq); err != nil && !errors.Is(err, streams.ErrConflict) {
			return fmt.Errorf("deliver %s: %w", subject, err)
		}
	}
	return nil
}
