// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package durableq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unilinq/durableq/internal/runtime"
	"github.com/unilinq/durableq/storage"
)

var fastRetry = Policy{MaxAttempts: 5, Schedule: []time.Duration{0}, Jitter: 0}

// The same worker claims attempt 2 while the abandoned goroutine of attempt 1
// is still running. The late commit of attempt 1 must add nothing.
func TestStreamAbandonedAttemptCannotCommitAfterSameWorkerReclaims(t *testing.T) {
	t.Parallel()
	app := newTestApp(t, func(c *Config) { c.HandlerTimeout = 500 * time.Millisecond })

	wedge := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(wedge) }) }
	t.Cleanup(release)

	late := make(chan error, 1)
	second := newGate()
	var attempts atomic.Int64
	job := app.Job("fence-same-worker").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			if attempts.Add(1) == 1 {
				if err := out.Add(crawlIn{URL: "before"}); err != nil {
					return err
				}
				if err := out.Flush(); err != nil {
					return err
				}
				<-wedge
				err := out.Add(crawlIn{URL: "late"})
				if err == nil {
					err = out.Flush()
				}
				late <- err
				return err
			}
			if err := out.Add(crawlIn{URL: "second"}); err != nil {
				return err
			}
			second.hit(context.Background()) // attempt 2 is running here
			return nil
		}, StepFlush(1000, time.Hour), StepPolicy(fastRetry))).
		Step("crawl", noop, After("discover"))

	pools := app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	pools["discover"].TestSignals.Stuck.WaitOrTimeout(t)
	second.waitReached(t) // the same worker holds attempt 2

	release()
	select {
	case err := <-late:
		if !errors.Is(err, storage.ErrLeaseLost) {
			t.Fatalf("late commit of attempt 1: got %v, want ErrLeaseLost", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("the abandoned handler never finished")
	}
	parent := app.parentOf(t, exec.ID)
	if parent.Attempt != 2 || parent.Produced != 1 {
		t.Fatalf("while attempt 2 runs: attempt %d produced %d, want attempt 2 produced 1", parent.Attempt, parent.Produced)
	}
	second.open(t)
	app.waitForExecution(t, exec.ID, 1+2)
	requireURLs(t, payloadURLs(t, app.itemsOf(t, exec.ID, "crawl")), sorted("before", "second"), "successor items")
	if p := app.parentOf(t, exec.ID); p.Produced != 2 {
		t.Fatalf("parent produced %d, want 2", p.Produced)
	}
}

// After the handler returns, Add and Flush commit nothing.
func TestStreamOutIsClosedWhenTheHandlerReturns(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	var kept Out[crawlIn]
	job := app.Job("closed-out").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			kept = out
			return out.Add(crawlIn{URL: "only"})
		}, StepFlush(1000, time.Hour))).
		Step("crawl", noop, After("discover"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1+1)

	if err := kept.Add(crawlIn{URL: "after"}); !errors.Is(err, ErrOutClosed) {
		t.Fatalf("Add after return: got %v, want ErrOutClosed", err)
	}
	if err := kept.Flush(); !errors.Is(err, ErrOutClosed) {
		t.Fatalf("Flush after return: got %v, want ErrOutClosed", err)
	}
	requireURLs(t, payloadURLs(t, app.itemsOf(t, exec.ID, "crawl")), []string{"only"}, "successor items")
	if p := app.parentOf(t, exec.ID); p.Produced != 1 {
		t.Fatalf("parent produced %d, want 1", p.Produced)
	}
}

// An item that flushed in attempt 1 and added nothing in attempt 2 produced
// output in total, so it is a success.
func TestStreamOutcomeCountsEveryAttempt(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	var attempts atomic.Int64
	job := app.Job("outcome-total").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			if attempts.Add(1) == 1 {
				if err := out.Add(urls("first", 2)...); err != nil {
					return err
				}
				if err := out.Flush(); err != nil {
					return err
				}
				return errors.New("transient")
			}
			return nil
		}, StepFlush(1000, time.Hour), StepPolicy(fastRetry))).
		Step("crawl", noop, After("discover"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1+2)
	p := app.parentOf(t, exec.ID)
	if p.Outcome != storage.OutcomeSuccess || p.Produced != 2 || p.Attempt != 2 {
		t.Fatalf("parent: outcome %s produced %d attempt %d, want success, 2, 2", p.Outcome, p.Produced, p.Attempt)
	}
}

type emitFake struct {
	storage.Store
	calls atomic.Int64
}

func (f *emitFake) Emit(ctx context.Context, req storage.EmitRequest) error {
	f.calls.Add(1)
	return storage.ErrLeaseLost
}

// After ErrLeaseLost the Out is dead: later calls return the error at once and
// the store is not asked again.
func TestStreamOutIsDeadAfterLeaseLost(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	fake := &emitFake{}
	ctx := runtime.WithEmitter(t.Context(), fake, "w", 1, 1)

	next := &Step{id: "crawl", queue: StepQueue("dead-out", "crawl")}

	live := Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
		first := out.Add(crawlIn{URL: "a"})
		if !errors.Is(first, storage.ErrLeaseLost) {
			t.Errorf("first Add: got %v, want ErrLeaseLost", first)
		}
		for i := 0; i < 3; i++ {
			if err := out.Add(crawlIn{URL: "b"}); !errors.Is(err, storage.ErrLeaseLost) {
				t.Errorf("later Add: got %v, want ErrLeaseLost", err)
			}
		}
		if err := out.Flush(); !errors.Is(err, storage.ErrLeaseLost) {
			t.Errorf("Flush: got %v, want ErrLeaseLost", err)
		}
		return nil
	})
	h2, err := makeStepHandler(live, []*Step{next},
		flushConfig{size: 1, interval: time.Hour}, app.cfg.Clock)
	if err != nil {
		t.Fatalf("makeStepHandler: %v", err)
	}
	_, _ = h2(ctx, storage.Item{ExecutionID: "e", ItemID: "i", Payload: []byte(`{}`)})
	if n := fake.calls.Load(); n != 1 {
		t.Fatalf("store Emit called %d times, want 1", n)
	}
}
