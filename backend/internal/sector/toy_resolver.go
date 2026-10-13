package sector

import (
	"context"
	"encoding/json"
	"hash/fnv"
)

// ToyResolver is the seam awaiting the real game rules.
type ToyResolver struct{}

func (ToyResolver) Resolve(_ context.Context, sectorID string, tick int64) (json.RawMessage, bool, error) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(sectorID))
	lifetime := int64(3 + h.Sum32()%8) // sector stays active 3-10 ticks
	out, err := json.Marshal([]map[string]any{{
		"type":      "ToyCounterIncremented",
		"sector_id": sectorID,
		"tick":      tick,
	}})
	if err != nil {
		return nil, false, err
	}
	return out, tick%lifetime != lifetime-1, nil
}
