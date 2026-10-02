package sector

import "encoding/json"

// ExecuteTick commands the sector aggregate to resolve tick Tick.
type ExecuteTick struct {
	Sector string `json:"sector"`
	Tick   int64  `json:"tick"`
}

// TickResolved is the one event a sector appends per resolved tick. Payload
// carries the tick's outcomes; Resolver fills them in.
type TickResolved struct {
	Type    string          `json:"type"` // "TickResolved"
	Sector  string          `json:"sector"`
	Tick    int64           `json:"tick"`
	Pending bool            `json:"pending"`
	Events  json.RawMessage `json:"events"`
}
