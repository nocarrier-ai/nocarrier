package clock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

// ReadUniverseCreated reads the clock's first event. Not found means the big
// bang has not finished.
func ReadUniverseCreated(ctx context.Context, js jetstream.JetStream) (UniverseCreated, bool, error) {
	s, err := js.Stream(ctx, streams.StreamClock)
	if err != nil {
		return UniverseCreated{}, false, err
	}
	first, err := s.GetMsg(ctx, 1)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return UniverseCreated{}, false, nil
	}
	if err != nil {
		return UniverseCreated{}, false, fmt.Errorf("read UniverseCreated: %w", err)
	}
	var existing UniverseCreated
	if err := json.Unmarshal(first.Data, &existing); err != nil || existing.Type != "UniverseCreated" {
		return UniverseCreated{}, false, errors.New("first clock event is not UniverseCreated")
	}
	return existing, true, nil
}

func CreateUniverse(ctx context.Context, js jetstream.JetStream, tickPeriod time.Duration, seed int64) (UniverseCreated, error) {
	u := UniverseCreated{Type: "UniverseCreated", TickPeriod: tickPeriod, Seed: seed}
	_, err := streams.Append(ctx, js, streams.SubjectClock, u, 0)
	switch {
	case err == nil:
		return u, nil
	case errors.Is(err, streams.ErrConflict):
		// Another instance finished first.
		existing, found, err := ReadUniverseCreated(ctx, js)
		if err != nil {
			return UniverseCreated{}, err
		}
		if !found {
			return UniverseCreated{}, errors.New("clock has events but no UniverseCreated")
		}
		return existing, nil
	default:
		return UniverseCreated{}, fmt.Errorf("create universe: %w", err)
	}
}
