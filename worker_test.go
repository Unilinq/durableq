// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package durableq

import (
	"context"
	"errors"
	"fmt"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unilinq/durableq/internal/dqtest"
	"github.com/unilinq/durableq/internal/leasing"
	"github.com/unilinq/durableq/internal/runtime"
	"github.com/unilinq/durableq/storage"
	"github.com/unilinq/durableq/storage/postgres"
)

// testApp wires an app against an isolated schema with a poll interval short
// enough that tests never wait on the wall clock for long, and a stub clock so
// item scheduling is driven explicitly.
type testApp struct {
	*App
	store *postgres.Store
	clock *dqtest.StubClock
}

func newTestApp(t *testing.T, mutate ...func(*Config)) *testApp {
	t.Helper()
	clock := dqtest.NewStubClockNow()
	store := dqtest.NewStore(t, clock)

	cfg := Config{
		Store:             store,
		WorkerID:          "test-worker",
		PollInterval:      2 * time.Millisecond,
		PollJitter:        0,
		LeaseDuration:     30 * time.Second,
		HeartbeatInterval: 5 * time.Millisecond,
		HandlerTimeout:    5 * time.Second,
		DrainTimeout:      2 * time.Second,
		ReclaimInterval:   5 * time.Millisecond,
		Clock:             clock,
		Logger:            testLogger(t),
	}
	for _, m := range mutate {
		m(&cfg)
	}
	app, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return &testApp{App: app, store: store, clock: clock}
}

// armed registers a queue whose pool signals are initialised before it runs.
func (a *testApp) armed(t *testing.T, name string, handler any, opts ...QueueOption) (*Queue, *runtime.Pool) {
	t.Helper()
	q := a.Queue(name, opts...)
	if err := q.Work(handler); err != nil {
		t.Fatalf("Work: %v", err)
	}
	ready := make(chan *runtime.Pool, 1)
	q.mu.Lock()
	q.poolReady = func(p *runtime.Pool, h *leasing.Heartbeater) {
		p.TestSignals.Init()
		h.TestSignals.Init()
		ready <- p
	}
	q.mu.Unlock()

	if err := a.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.Stop(ctx)
	})
	select {
	case p := <-ready:
		return q, p
	case <-time.After(5 * time.Second):
		t.Fatalf("pool never started")
		return nil, nil
	}
}

func TestWorkerProcessesItems(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	type payload struct {
		URL string `json:"url"`
	}
	var seen sync.Map
	q, pool := app.armed(t, "indexing", func(ctx context.Context, in payload) error {
		seen.Store(in.URL, true)
		return nil
	})

	if _, err := q.Enqueue(t.Context(), payload{URL: "https://example.test/a"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	pool.TestSignals.Acked.WaitOrTimeout(t)

	if _, ok := seen.Load("https://example.test/a"); !ok {
		t.Fatalf("handler never saw the payload")
	}
	st, err := q.Stats(t.Context())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.Done != 1 {
		t.Fatalf("done: got %d, want 1", st.Done)
	}
}

// TestConcurrencyCapHolds proves the worker never runs more handlers than it
// was configured for, whatever the queue depth.
func TestConcurrencyCapHolds(t *testing.T) {
	t.Parallel()
	const cap = 4
	app := newTestApp(t)

	var (
		live atomic.Int64
		high atomic.Int64
	)
	release := make(chan struct{})
	q, pool := app.armed(t, "capped", func(ctx context.Context, _ struct{}) error {
		n := live.Add(1)
		for {
			h := high.Load()
			if n <= h || high.CompareAndSwap(h, n) {
				break
			}
		}
		<-release
		live.Add(-1)
		return nil
	}, WithConcurrency(cap))

	for i := 0; i < 40; i++ {
		if _, err := q.Enqueue(t.Context(), struct{}{}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// Wait until the pool is saturated, observed rather than slept for.
	for {
		if n := pool.TestSignals.Concurrency.WaitOrTimeout(t); n >= cap {
			break
		}
	}
	if got := high.Load(); got > cap {
		t.Fatalf("ran %d handlers at once with a cap of %d", got, cap)
	}
	close(release)

	for i := 0; i < 40; i++ {
		pool.TestSignals.Acked.WaitOrTimeout(t)
	}
	if got := high.Load(); got > cap {
		t.Fatalf("high-water mark %d exceeded the cap of %d", got, cap)
	}
}

// TestGracefulStopDoesNotConsumeAnAttempt is the invariant that makes rolling
// deploys safe: work handed back because the process is stopping must not
// count against the item's budget.
func TestGracefulStopDoesNotConsumeAnAttempt(t *testing.T) {
	t.Parallel()
	app := newTestApp(t, func(c *Config) { c.DrainTimeout = 20 * time.Millisecond })

	entered := make(chan struct{}, 1)
	q, pool := app.armed(t, "draining", func(ctx context.Context, _ struct{}) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done() // cooperative handler: returns when asked to stop
		return ctx.Err()
	})

	item, err := q.Enqueue(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	<-entered

	claimed, err := app.Store().GetItem(t.Context(), item.ID)
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if claimed.Attempt != 1 {
		t.Fatalf("attempt while running: got %d, want 1", claimed.Attempt)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	pool.TestSignals.Released.WaitOrTimeout(t)

	got, err := app.Store().GetItem(context.Background(), item.ID)
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got.State != storage.StateReady {
		t.Fatalf("state after graceful stop: got %s, want ready", got.State)
	}
	if got.Attempt != 0 {
		t.Fatalf("graceful stop consumed an attempt: attempt is %d, want 0", got.Attempt)
	}
}

// TestPanicIsANormalFailurePath proves a panicking handler retries and finally
// dead-letters, rather than taking the process down.
func TestPanicIsANormalFailurePath(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	var attempts atomic.Int64
	q, pool := app.armed(t, "panics", func(ctx context.Context, _ struct{}) error {
		attempts.Add(1)
		panic("handler exploded")
	}, WithPolicy(Policy{MaxAttempts: 3, Schedule: []time.Duration{0}, Jitter: 0}))

	item, err := q.Enqueue(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	pool.TestSignals.Retried.WaitOrTimeout(t)
	pool.TestSignals.Retried.WaitOrTimeout(t)
	pool.TestSignals.DeadLettered.WaitOrTimeout(t)

	got, err := app.Store().GetItem(t.Context(), item.ID)
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got.State != storage.StateDLQ {
		t.Fatalf("state: got %s, want dlq", got.State)
	}
	if got.Queue != q.DLQ() {
		t.Fatalf("queue: got %s, want %s", got.Queue, q.DLQ())
	}
	if got.Attempt != 3 {
		t.Fatalf("attempts: got %d, want 3", got.Attempt)
	}
	if n := attempts.Load(); n != 3 {
		t.Fatalf("handler ran %d times, want 3", n)
	}
}

// TestStuckHandlerFreesItsSlot proves one wedged item cannot starve the pool.
func TestStuckHandlerFreesItsSlot(t *testing.T) {
	t.Parallel()
	app := newTestApp(t, func(c *Config) { c.HandlerTimeout = 30 * time.Millisecond })

	wedge := make(chan struct{})
	var progressed atomic.Int64
	q, pool := app.armed(t, "stuck", func(ctx context.Context, in map[string]any) error {
		if in["wedge"] == true {
			<-wedge // ignores cancellation on purpose
			return nil
		}
		progressed.Add(1)
		return nil
	}, WithConcurrency(1), WithPolicy(Policy{MaxAttempts: 5, Schedule: []time.Duration{0}, Jitter: 0}))
	// Released before the app stops, so shutdown is not waiting on a handler
	// that is deliberately ignoring cancellation.
	t.Cleanup(func() { close(wedge) })

	if _, err := q.Enqueue(t.Context(), map[string]any{"wedge": true}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	pool.TestSignals.Stuck.WaitOrTimeout(t)

	// The slot is free again, so ordinary work still flows.
	if _, err := q.Enqueue(t.Context(), map[string]any{"wedge": false}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	for progressed.Load() == 0 {
		pool.TestSignals.Acked.WaitOrTimeout(t)
	}
}

// TestUndecodablePayloadDeadLettersEventually proves a payload this worker will
// never understand fails its attempts and stops, instead of looping for ever.
func TestUndecodablePayloadDeadLettersEventually(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	type expected struct {
		Count int `json:"count"`
	}
	var ran atomic.Int64
	q, pool := app.armed(t, "skewed", func(ctx context.Context, in expected) error {
		ran.Add(1)
		return nil
	}, WithPolicy(Policy{MaxAttempts: 2, Schedule: []time.Duration{0}, Jitter: 0}))

	// A payload written by a newer producer that this worker cannot decode.
	item, err := q.Enqueue(t.Context(), map[string]any{"count": "not-a-number"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	pool.TestSignals.Retried.WaitOrTimeout(t)
	pool.TestSignals.DeadLettered.WaitOrTimeout(t)

	got, err := app.Store().GetItem(t.Context(), item.ID)
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got.State != storage.StateDLQ {
		t.Fatalf("state: got %s, want dlq", got.State)
	}
	if got.LastError == "" {
		t.Fatalf("a decode failure must record why")
	}
	if n := ran.Load(); n != 0 {
		t.Fatalf("handler body ran %d times for an undecodable payload, want 0", n)
	}
}

// TestDiscardSkipsRemainingAttempts proves a handler can say "retrying will
// not help" and have the item dead-lettered at once.
func TestDiscardSkipsRemainingAttempts(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	var ran atomic.Int64
	q, pool := app.armed(t, "discards", func(ctx context.Context, _ struct{}) error {
		ran.Add(1)
		return Discard(errors.New("this payload will never be valid"))
	}, WithPolicy(Policy{MaxAttempts: 10, Schedule: []time.Duration{0}, Jitter: 0}))

	item, err := q.Enqueue(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	pool.TestSignals.DeadLettered.WaitOrTimeout(t)

	got, err := app.Store().GetItem(t.Context(), item.ID)
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got.State != storage.StateDLQ {
		t.Fatalf("state: got %s, want dlq", got.State)
	}
	if n := ran.Load(); n != 1 {
		t.Fatalf("discarded item ran %d times, want 1", n)
	}
}

// TestRetryBecomingReadyIsPickedUp proves a short backoff is not lost between
// poll ticks: the item becomes available on the clock and the very next poll
// takes it.
func TestRetryBecomingReadyIsPickedUp(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	var attempts atomic.Int64
	q, pool := app.armed(t, "shortretry", func(ctx context.Context, _ struct{}) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient")
		}
		return nil
	}, WithPolicy(Policy{MaxAttempts: 5, Schedule: []time.Duration{time.Minute}, Jitter: 0}))

	if _, err := q.Enqueue(t.Context(), struct{}{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	pool.TestSignals.Retried.WaitOrTimeout(t)

	// Nothing happens while the backoff has not elapsed...
	if n := attempts.Load(); n != 1 {
		t.Fatalf("handler ran %d times before the backoff elapsed", n)
	}
	// ...and the item returns purely because the clock moved.
	app.clock.Advance(2 * time.Minute)
	pool.TestSignals.Acked.WaitOrTimeout(t)

	if n := attempts.Load(); n != 2 {
		t.Fatalf("handler ran %d times, want 2", n)
	}
}

// TestPauseStopsClaiming proves the operational lever works while the worker
// is running, and that resuming restarts the flow.
func TestPauseStopsClaiming(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	var ran atomic.Int64
	q, pool := app.armed(t, "pausable", func(ctx context.Context, _ struct{}) error {
		ran.Add(1)
		return nil
	})

	if err := q.Pause(t.Context()); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if _, err := q.Enqueue(t.Context(), struct{}{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Several polls must come back empty while the queue is paused.
	for i := 0; i < 5; i++ {
		if n := pool.TestSignals.Polled.WaitOrTimeout(t); n != 0 {
			t.Fatalf("claimed %d items from a paused queue", n)
		}
	}
	if n := ran.Load(); n != 0 {
		t.Fatalf("handler ran %d times while paused", n)
	}

	if err := q.Resume(t.Context()); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	pool.TestSignals.Acked.WaitOrTimeout(t)
	if n := ran.Load(); n != 1 {
		t.Fatalf("handler ran %d times after resume, want 1", n)
	}
}

// TestHeartbeatKeepsALongHandlersLease proves a handler outliving one lease
// period stays the owner: the heartbeat pushes the deadline forward as time
// passes, which is what stops the reclaimer taking live work.
func TestHeartbeatKeepsALongHandlersLease(t *testing.T) {
	t.Parallel()
	app := newTestApp(t, func(c *Config) {
		c.LeaseDuration = 30 * time.Second
	})

	release := make(chan struct{})
	q, _ := app.armed(t, "longhandler", func(ctx context.Context, _ struct{}) error {
		<-release
		return nil
	})
	q.mu.Lock()
	hb := q.heartbeat
	q.mu.Unlock()
	t.Cleanup(func() { close(release) })

	item, err := q.Enqueue(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	q.workerPool().TestSignals.Started.WaitOrTimeout(t)

	first, err := app.Store().GetItem(t.Context(), item.ID)
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}

	// Time passes while the handler is still working. The heartbeat must
	// move the deadline with it.
	app.clock.Advance(10 * time.Second)
	// Discard beats buffered before the clock moved, then take two: the first
	// may already have been in flight with the old deadline, the second
	// cannot be.
	hb.TestSignals.Beat.Drain()
	for seen := 0; seen < 2; {
		if n := hb.TestSignals.Beat.WaitOrTimeout(t); n > 0 {
			seen++
		}
	}

	later, err := app.Store().GetItem(t.Context(), item.ID)
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if !later.LeaseUntil.After(first.LeaseUntil) {
		t.Fatalf("lease was not renewed while the handler ran: %s then %s",
			first.LeaseUntil, later.LeaseUntil)
	}
	if got, want := later.LeaseUntil.Sub(first.LeaseUntil), 10*time.Second; got != want {
		t.Fatalf("lease moved by %s, want %s (the time that actually passed)", got, want)
	}

	// And the reclaimer leaves it alone, because the lease is still live.
	res := app.lazyReclaimer().Pass(t.Context())
	if res.Reclaimed != 0 {
		t.Fatalf("reclaimer took %d items whose lease was heartbeated", res.Reclaimed)
	}
}

// TestQueueWithoutAHandlerIsNotClaimed proves a producer-only queue is left
// alone rather than having its items fail.
func TestQueueWithoutAHandlerIsNotClaimed(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	served, pool := app.armed(t, "served", func(ctx context.Context, _ struct{}) error { return nil })
	idle := app.Queue("unserved")

	if _, err := idle.Enqueue(t.Context(), struct{}{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := served.Enqueue(t.Context(), struct{}{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	pool.TestSignals.Acked.WaitOrTimeout(t)

	st, err := idle.Stats(t.Context())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.Ready != 1 {
		t.Fatalf("unserved queue: ready %d, want the item left waiting", st.Ready)
	}
	if st.DLQ != 0 {
		t.Fatalf("unserved queue: %d items dead-lettered; an unhandled queue must not fail its work", st.DLQ)
	}
}

// TestHandlerSignatureIsCheckedAtRegistration proves a wiring mistake is
// reported when it is made, not on the first item in production.
func TestHandlerSignatureIsCheckedAtRegistration(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	bad := []any{
		func() error { return nil },
		func(ctx context.Context) error { return nil },
		func(ctx context.Context, a, b int) error { return nil },
		func(ctx context.Context, a int) {},
		func(a int, ctx context.Context) error { return nil },
		"not a function",
		nil,
	}
	for i, h := range bad {
		q := app.Queue(fmt.Sprintf("bad-%d", i))
		if err := q.Work(h); err == nil {
			t.Fatalf("handler %d (%T) was accepted", i, h)
		}
	}

	good := app.Queue("good")
	if err := good.Work(func(ctx context.Context, in struct{}) error { return nil }); err != nil {
		t.Fatalf("valid handler rejected: %v", err)
	}
	if err := good.Work(func(ctx context.Context, in struct{}) error { return nil }); err == nil {
		t.Fatalf("a second handler on the same queue was accepted")
	}
}

// TestRawAndItemHandlers proves the escape hatches decode as documented.
func TestRawAndItemHandlers(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	got := make(chan string, 2)
	q, pool := app.armed(t, "raw", func(ctx context.Context, raw []byte) error {
		got <- string(raw)
		return nil
	})
	if _, err := q.Enqueue(t.Context(), map[string]int{"n": 7}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	pool.TestSignals.Acked.WaitOrTimeout(t)
	if v := <-got; v != `{"n": 7}` && v != `{"n":7}` {
		t.Fatalf("raw payload: got %q", v)
	}
}

// TestStartStopStress catches shutdown races and leaked goroutines across many
// start/stop cycles.
func TestStartStopStress(t *testing.T) {
	// Not parallel: it counts every goroutine in the process, so tests running
	// alongside it would move the count and fail it spuriously.

	baseline := 0
	for cycle := 0; cycle < 8; cycle++ {
		func() {
			app := newTestApp(t)
			q := app.Queue("stress")
			if err := q.Work(func(ctx context.Context, _ struct{}) error { return nil }); err != nil {
				t.Fatalf("Work: %v", err)
			}
			if err := app.Start(t.Context()); err != nil {
				t.Fatalf("Start: %v", err)
			}
			for i := 0; i < 5; i++ {
				if _, err := q.Enqueue(t.Context(), struct{}{}); err != nil {
					t.Fatalf("Enqueue: %v", err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := app.Stop(ctx); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			// Stopping twice must be safe.
			if err := app.Stop(ctx); err != nil {
				t.Fatalf("second Stop: %v", err)
			}
		}()
		if cycle == 1 {
			baseline = goruntime.NumGoroutine()
		}
	}

	// After Stop returns, every goroutine the app owns has exited, so growth
	// across cycles would be a leak rather than scheduling lag.
	if got := goruntime.NumGoroutine(); got > baseline+8 {
		t.Fatalf("goroutines grew from %d to %d across start/stop cycles", baseline, got)
	}
}

// TestPollJitterSpreadsPolls proves N workers do not line up on the same tick.
func TestPollJitterSpreadsPolls(t *testing.T) {
	t.Parallel()
	base := 100 * time.Millisecond

	if got := runtime.JitteredInterval(base, 0, 0.5); got != base {
		t.Fatalf("zero jitter must be exact: got %s", got)
	}

	seen := map[time.Duration]bool{}
	for i := 0; i <= 20; i++ {
		r := float64(i) / 20
		d := runtime.JitteredInterval(base, 0.2, r)
		seen[d] = true
		if d < 80*time.Millisecond || d > 120*time.Millisecond {
			t.Fatalf("jittered interval %s outside +/-20%% of %s", d, base)
		}
	}
	if len(seen) < 10 {
		t.Fatalf("jitter produced only %d distinct intervals; polls would still stampede", len(seen))
	}

	// A misconfigured jitter must not turn polling into a busy loop.
	if got := runtime.JitteredInterval(base, 5, 0); got < base/10 {
		t.Fatalf("jitter floor breached: got %s", got)
	}
}

// TestShutdownIsNotMistakenForAStuckHandler proves the stuck detector stays
// quiet during a deliberate stop. Reporting shutdown as "stuck" would turn
// every rolling deploy into a page.
func TestShutdownIsNotMistakenForAStuckHandler(t *testing.T) {
	t.Parallel()
	app := newTestApp(t, func(c *Config) {
		// Long enough that Stop marks the pool stopping first, short enough
		// that the timeout path is genuinely reached during the drain.
		c.HandlerTimeout = 150 * time.Millisecond
		c.DrainTimeout = 10 * time.Millisecond
	})

	wedge := make(chan struct{})
	q, pool := app.armed(t, "shutdown", func(ctx context.Context, _ struct{}) error {
		<-wedge // deliberately ignores cancellation
		return nil
	})
	t.Cleanup(func() { close(wedge) })

	item, err := q.Enqueue(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	pool.TestSignals.Started.WaitOrTimeout(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	pool.TestSignals.Released.WaitOrTimeout(t)

	if id, ok := pool.TestSignals.Stuck.TryReceive(); ok {
		t.Fatalf("item %d was reported stuck during a deliberate shutdown", id)
	}
	got, err := app.Store().GetItem(context.Background(), item.ID)
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got.State != storage.StateReady {
		t.Fatalf("state after shutdown: got %s, want ready", got.State)
	}
	if got.Attempt != 0 {
		t.Fatalf("shutdown consumed an attempt: attempt %d, want 0", got.Attempt)
	}
}

// newRealClockApp builds an app on the wall clock. Almost every test drives a
// stub clock instead, which is what makes them deterministic; the load and soak
// tests cannot, because a frozen clock means a retry scheduled ten milliseconds
// out never becomes available and the run simply stalls.
func newRealClockApp(t *testing.T, mutate ...func(*Config)) *testApp {
	t.Helper()
	store := dqtest.NewStore(t, storage.RealClock())

	cfg := Config{
		Store:           store,
		WorkerID:        "test-worker",
		PollInterval:    5 * time.Millisecond,
		PollJitter:      0.2,
		LeaseDuration:   30 * time.Second,
		HandlerTimeout:  30 * time.Second,
		DrainTimeout:    5 * time.Second,
		ReclaimInterval: time.Second,
		Clock:           storage.RealClock(),
		Logger:          testLogger(t),
	}
	for _, m := range mutate {
		m(&cfg)
	}
	app, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return &testApp{App: app, store: store, clock: dqtest.NewStubClockNow()}
}
