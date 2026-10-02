package natstest

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nocarrier-ai/nocarrier/internal/devnats"
	"github.com/nocarrier-ai/nocarrier/internal/streams"
)

const TickPeriod = time.Minute

func Start(t *testing.T) jetstream.JetStream {
	t.Helper()
	ns, err := devnats.Start("127.0.0.1", -1, t.TempDir())
	if err != nil {
		t.Fatalf("start nats: %v", err)
	}
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := streams.Ensure(Context(t), js, 1, TickPeriod); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	return js
}

func Context(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func Logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type Runner interface {
	Run(ctx context.Context) error
}

func Run(t *testing.T, r Runner) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	})
}

func Eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func Publish(t *testing.T, js jetstream.JetStream, subject string, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ctx, cancel := timeout()
	defer cancel()
	if _, err := js.Publish(ctx, subject, data); err != nil {
		t.Fatalf("publish %s: %v", subject, err)
	}
}

func SubjectCounts(t *testing.T, js jetstream.JetStream, stream, filter string) map[string]uint64 {
	t.Helper()
	ctx, cancel := timeout()
	defer cancel()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	info, err := s.Info(ctx, jetstream.WithSubjectFilter(filter))
	if err != nil {
		t.Fatalf("stream info %s: %v", stream, err)
	}
	if info.State.Subjects == nil {
		return map[string]uint64{}
	}
	return info.State.Subjects
}

func Last(t *testing.T, js jetstream.JetStream, stream, subject string, v any) bool {
	t.Helper()
	ctx, cancel := timeout()
	defer cancel()
	seq, err := streams.Last(ctx, js, stream, subject, v)
	if err != nil {
		t.Fatalf("last %s: %v", subject, err)
	}
	return seq != 0
}

func timeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}
