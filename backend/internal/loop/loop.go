// Package loop supervises the long-running loops that make up a nocarrierd
// instance. Every loop runs until its context is cancelled; a loop that
// returns early is restarted with backoff, and a loop that keeps failing
// takes the process down so the platform replaces it.
package loop

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Loop is one long-running responsibility of the process.
type Loop interface {
	Name() string
	// Run blocks until ctx is cancelled or the loop fails. Returning nil
	// after cancellation is a clean stop; returning before cancellation,
	// with or without an error, triggers a restart.
	Run(ctx context.Context) error
}

const (
	maxRestarts    = 5
	restartWindow  = 5 * time.Minute
	initialBackoff = time.Second
	maxBackoff     = 30 * time.Second
)

// Supervise runs every loop until ctx is cancelled. It restarts individual
// loops with exponential backoff, and returns an error if any loop exceeds
// maxRestarts within restartWindow.
func Supervise(ctx context.Context, log *slog.Logger, loops ...Loop) error {
	type failure struct {
		name string
		err  error
	}
	fatal := make(chan failure, len(loops))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	running := len(loops)
	finished := make(chan string, len(loops))

	for _, l := range loops {
		go func() {
			defer func() { finished <- l.Name() }()
			backoff := initialBackoff
			var restarts []time.Time
			for {
				err := runOne(runCtx, l)
				if runCtx.Err() != nil {
					if err != nil && !errors.Is(err, context.Canceled) {
						log.Warn("loop stopped with error during shutdown", "loop", l.Name(), "err", err)
					}
					return
				}
				now := time.Now()
				restarts = append(restarts, now)
				for len(restarts) > 0 && now.Sub(restarts[0]) > restartWindow {
					restarts = restarts[1:]
				}
				if len(restarts) > maxRestarts {
					fatal <- failure{l.Name(), fmt.Errorf("restarted %d times in %s, last error: %w",
						len(restarts), restartWindow, errOrUnexpected(err))}
					return
				}
				log.Error("loop stopped, restarting", "loop", l.Name(),
					"err", errOrUnexpected(err), "backoff", backoff.String())
				select {
				case <-runCtx.Done():
					return
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, maxBackoff)
			}
		}()
	}

	go func() {
		for range loops {
			name := <-finished
			running--
			log.Debug("loop goroutine finished", "loop", name, "running", running)
		}
		close(done)
	}()

	var cause error
	select {
	case <-ctx.Done():
	case f := <-fatal:
		cause = fmt.Errorf("loop %s: %w", f.name, f.err)
		log.Error("fatal loop failure, shutting down", "loop", f.name, "err", f.err)
	}
	cancel()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		cause = errors.Join(cause, errors.New("shutdown timed out"))
	}
	return cause
}

func runOne(ctx context.Context, l Loop) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	return l.Run(ctx)
}

func errOrUnexpected(err error) error {
	if err == nil {
		return errors.New("exited unexpectedly")
	}
	return err
}
