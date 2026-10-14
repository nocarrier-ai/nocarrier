package projector

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/doctrine"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type NewsItem struct {
	PlayerID string `json:"player_id"`
	Tick     int64  `json:"tick"`
	Text     string `json:"text"`
}

type Newsfeed struct {
	nc *nats.Conn
}

func NewNewsfeed(nc *nats.Conn) *Newsfeed { return &Newsfeed{nc: nc} }

func (n *Newsfeed) Name() string { return "newsfeed" }

func (n *Newsfeed) Stream() string { return streams.StreamEvents }

func (n *Newsfeed) FilterSubjects() []string {
	return []string{streams.DoctrineEvents}
}

func (n *Newsfeed) Apply(_ context.Context, msg jetstream.Msg) error {
	ev, ok := decodeDoctrineEvent(msg.Data(), doctrine.DoctrineDeliveredType)
	if !ok {
		return nil
	}
	item := NewsItem{
		PlayerID: ev.AvatarID,
		Tick:     ev.Tick,
		Text:     fmt.Sprintf("DOCTRINE rev %d RECEIVED", ev.Revision),
	}
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return n.nc.Publish(streams.NewsSubject(item.PlayerID), data)
}
