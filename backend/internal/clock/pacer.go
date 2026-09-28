package clock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

type UniverseCreated struct {
	Type       string        `json:"type"` // UniverseCreated
	TickPeriod time.Duration `json:"tick_period"`
	Seed       int64         `json:"seed"`
}

type TickAdvanced struct {
	Type             string `json:"type"` // TickAdvanced
	Tick             int64  `json:"tick"`
	WinnerInstanceID string `json:"winner_instance_id"`
}

const MinTickPeriod = 30 * time.Second

const graceInterval = 2 * time.Second

type Dispatcher interface {
	Dispatch(ctx context.Context, tick int64) error
}

type Pacer struct {
	js         jetstream.JetStream
	log        *slog.Logger
	instanceID string
	period     time.Duration
	dispatch   Dispatcher

	lastTick int64
	lastSeq  uint64
	isDriver bool
}

func NewPacer(js jetstream.JetStream, log *slog.Logger, instanceID string,
	period time.Duration, d Dispatcher) (*Pacer, error) {
	if period < MinTickPeriod {
		return nil, fmt.Errorf("tick period %s is below the %s min", period, MinTickPeriod)
	}
	return &Pacer{js: js, log: log.With("loop", "pacer"), instanceID: instanceID, period: period, dispatch: d}, nil
}

func (p *Pacer) Name() string { return "pacer" }

func (p *Pacer) Run(ctx context.Context) error {
	head, err := p.readHead(ctx)
	if err != nil {
		return fmt.Errorf("read clock head: %w", err)
	}
	p.observe(head)

	cons, err := p.js.OrderedConsumer(ctx, streams.StreamClock, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   p.lastSeq + 1,
	})
	if err != nil {
		return fmt.Errorf("clock consumer: %w", err)
	}
	events := make(chan advance, 8)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		var ev TickAdvanced
		if err := json.Unmarshal(msg.Data(), &ev); err != nil || ev.Type != "TickAdvanced" {
			return
		}
		md, err := msg.Metadata()
		if err != nil {
			return
		}
		select {
		case events <- advance{ev, md.Sequence.Stream}:
		case <-ctx.Done():
		}
	})
	if err != nil {
		return fmt.Errorf("consume clock: %w", err)
	}
	defer cc.Stop()

	timer := time.NewTimer(p.armDuration())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case a := <-events:
			p.observe(observed{a.ev.Tick, a.seq, a.ev.WinnerInstanceID})
			timer.Stop()
			timer.Reset(p.armDuration())
		case <-timer.C:
			if err := p.attempt(ctx); err != nil {
				p.log.Warn("tick attempt", "err", err)
			}

			timer.Reset(p.armDuration())
		}
	}
}

type advance struct {
	ev  TickAdvanced
	seq uint64
}

type observed struct {
	tick     int64
	seq      uint64
	winnerID string
}

func (p *Pacer) observe(o observed) {
	if o.seq <= p.lastSeq {
		return
	}
	p.lastTick, p.lastSeq = o.tick, o.seq
	p.isDriver = o.winnerID == p.instanceID
}

// armDuration is the heart of NTP-free pacing: the driver fires first, and
// standbys give it a head start plus jitter so at most a few instances race.
func (p *Pacer) armDuration() time.Duration {
	if p.isDriver {
		return p.period
	}
	return p.period + graceInterval + p.jitter()
}

func (p *Pacer) jitter() time.Duration {
	return time.Duration(rand.Int64N(int64(500 * time.Millisecond)))
}

// attempt tries to advance to lastTick+1 with the expected-sequence guard,
// and dispatches if this instance won.
func (p *Pacer) attempt(ctx context.Context) error {
	next := p.lastTick + 1
	payload, err := json.Marshal(TickAdvanced{Type: "TickAdvanced", Tick: next, WinnerInstanceID: p.instanceID})
	if err != nil {
		return err
	}
	msg := nats.NewMsg(streams.SubjectClock)
	msg.Data = payload
	msg.Header.Set("Nats-Expected-Last-Subject-Sequence", fmt.Sprintf("%d", p.lastSeq))

	ack, err := p.js.PublishMsg(ctx, msg)
	switch {
	case err == nil:
		p.observe(observed{next, ack.Sequence, p.instanceID})
		p.log.Info("tick advanced", "tick", next)
		return p.dispatch.Dispatch(ctx, next)
	case isWrongLastSequence(err):
		// Someone else advanced first; their event is coming on
		// the consumer, but sync state now so the re-armed timer
		// is compued from the new tick
		head, herr := p.readHead(ctx)
		if herr != nil {
			return nil
		}
		p.observe(head)
		return nil
	default:
		// Ambiguous (timeout, reconnect): the append may have landed. Read
		// the head; if it carries our ID we won and must dispatch.
		head, herr := p.readHead(ctx)
		if herr != nil {
			return errors.Join(err, herr)
		}
		won := head.tick == next && head.winnerID == p.instanceID
		p.observe(head)
		if won {
			p.log.Info("tick advanced (recovered ack)", "tick", next)
			return p.dispatch.Dispatch(ctx, next)
		}
		return err
	}
}

func (p *Pacer) readHead(ctx context.Context) (observed, error) {
	s, err := p.js.Stream(ctx, streams.StreamClock)
	if err != nil {
		return observed{}, err
	}
	raw, err := s.GetLastMsgForSubject(ctx, streams.SubjectClock)
	if err != nil {
		return observed{}, err
	}
	var ev TickAdvanced
	if err := json.Unmarshal(raw.Data, &ev); err != nil {
		return observed{}, err
	}
	if ev.Type != "TickAdvanced" {
		// Head is UniverseCreated: tick 0 hasn't happened yet.
		return observed{tick: -1, seq: raw.Sequence}, nil
	}
	return observed{ev.Tick, raw.Sequence, ev.WinnerInstanceID}, nil
}

func isWrongLastSequence(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
}
