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

	"github.com/nocarrier-ai/nocarrier/internal/clock"
	"github.com/nocarrier-ai/nocarrier/internal/doctrine"
	"github.com/nocarrier-ai/nocarrier/internal/natstest"
	"github.com/nocarrier-ai/nocarrier/internal/service"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

const doctrineUpdateSubject = "cmd.doctrine.update"

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

func startServices(t *testing.T, d service.DoctrineUpdater) (jetstream.JetStream, *nats.Conn) {
	t.Helper()
	js := natstest.Start(t)
	natstest.Run(t, service.New(js.Conn(), natstest.Logger(), d))
	return js, js.Conn()
}

func request(t *testing.T, nc *nats.Conn, data []byte) *nats.Msg {
	t.Helper()
	var reply *nats.Msg
	natstest.Eventually(t, 5*time.Second, func() bool {
		msg, err := nc.Request(doctrineUpdateSubject, data, time.Second)
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

func requestJSON(t *testing.T, nc *nats.Conn, v any) *nats.Msg {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return request(t, nc, data)
}

func update(avatarID string, orders ...string) doctrine.UpdateDoctrine {
	return doctrine.UpdateDoctrine{AvatarID: avatarID, DoctrineData: doctrine.DoctrineData{Orders: orders}}
}

func errorCode(msg *nats.Msg) string {
	return msg.Header.Get(micro.ErrorCodeHeader)
}

func TestDoctrineUpdateReplies(t *testing.T) {
	fake := &fakeUpdater{ev: doctrine.DoctrineUpdated{Revision: 2, Tick: 7, Hash: "abc"}}
	_, nc := startServices(t, fake)

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
			_, nc := startServices(t, &fakeUpdater{err: c.err})
			msg := requestJSON(t, nc, update("a1", "x"))
			if code := errorCode(msg); code != c.code {
				t.Errorf("code %q, want %q", code, c.code)
			}
		})
	}
}

func TestDoctrineUpdateInvalidNamesReason(t *testing.T) {
	_, nc := startServices(t, &fakeUpdater{err: fmt.Errorf("%w: unknown hook \"bribed\"", doctrine.ErrInvalid)})
	msg := requestJSON(t, nc, update("a1", "x"))
	if desc := msg.Header.Get(micro.ErrorHeader); !strings.Contains(desc, "bribed") {
		t.Errorf("description %q does not name the reason", desc)
	}
}

func TestDoctrineUpdateMalformed(t *testing.T) {
	fake := &fakeUpdater{}
	_, nc := startServices(t, fake)

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
	natstest.Run(t, service.New(js.Conn(), natstest.Logger(), doctrine.NewHandler(js)))

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
