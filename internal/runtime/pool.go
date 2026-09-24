// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

// Package runtime holds the worker loop: claiming work, dispatching it to
// handlers, and turning each handler outcome into exactly one durable state
// transition.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/unilinq/durableq/internal/dqsignal"
	"github.com/unilinq/durableq/internal/telemetry"
	"github.com/unilinq/durableq/storage"
)

// HandlerResult is what a handler produced. A handler that produced nothing
// and set Filtered completed successfully; that is different from a handler
// that produced output, and both are different from an error.
type HandlerResult struct {
	Produced []storage.NewItem
	Filtered bool
	Dropped  int
	Capped   bool
}

// Handler processes one item.
type Handler func(ctx context.Context, item storage.Item) (HandlerResult, error)

// ErrDiscard, wrapped by a handler's error, sends the item straight to the
// dead-letter queue without spending its remaining attempts.
var ErrDiscard = errors.New("durableq: discard this item")

// Config parameterises a Pool.
type Config struct {
	Store       storage.Store
	Queue       string
	Worker      string
	Handler     Handler
	Concurrency int

	// PollInterval is how often the pool asks for work. It is wall-clock
	// time: polling is about latency, not about item scheduling.
	PollInterval time.Duration
	// PollJitter spreads polls so N workers do not stampede the database.
	PollJitter float64

	LeaseDuration time.Duration
	// HandlerTimeout bounds one attempt. A handler that overruns it frees its
	// worker slot so one wedged item cannot starve the pool.
	HandlerTimeout time.Duration

	Clock  storage.Clock
	Logger *slog.Logger
	// Metrics receives runtime events. Nil means discard them.
	Metrics telemetry.Sink
}

// Pool claims work from one queue and runs it.
type Pool struct {
	cfg  Config
	rand *rand.Rand
	rmu  sync.Mutex

	mu       sync.Mutex
	inflight map[int64]*inflightItem
	stopping bool

	wg   sync.WaitGroup
	done chan struct{}

	// TestSignals is armed by tests so they can wait on real events instead
	// of sleeping. In production every emit is a no-op.
	TestSignals Signals
}

// Signals are the observable moments inside the pool.
type Signals struct {
	Polled       dqsignal.Signal[int]   // items claimed on this poll
	Started      dqsignal.Signal[int64] // handler entered
	Acked        dqsignal.Signal[int64]
	Retried      dqsignal.Signal[int64]
	DeadLettered dqsignal.Signal[int64]
	Released     dqsignal.Signal[int64]
	Stuck        dqsignal.Signal[int64]
	Concurrency  dqsignal.Signal[int] // in-flight count after each dispatch
}

// Init arms every signal.
func (s *Signals) Init() {
	s.Polled.Init()
	s.Started.Init()
	s.Acked.Init()
	s.Retried.Init()
	s.DeadLettered.Init()
	s.Released.Init()
	s.Stuck.Init()
	s.Concurrency.Init()
}

type inflightItem struct {
	item     storage.Item
	cancel   context.CancelFunc
	finish   sync.Once
	softStop bool
}

// New builds a Pool.
func New(cfg Config) (*Pool, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("runtime: Store is required")
	}
	if cfg.Handler == nil {
		return nil, fmt.Errorf("runtime: Handler is required")
	}
	if cfg.Queue == "" {
		return nil, fmt.Errorf("runtime: Queue is required")
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 200 * time.Millisecond
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.HandlerTimeout <= 0 {
		cfg.HandlerTimeout = 5 * time.Minute
	}
	if cfg.Clock == nil {
		cfg.Clock = storage.RealClock()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = telemetry.Nop{}
	}
	return &Pool{
		cfg:      cfg,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		inflight: map[int64]*inflightItem{},
		done:     make(chan struct{}),
	}, nil
}

// Queue returns the queue this pool serves.
func (p *Pool) Queue() string { return p.cfg.Queue }

// Inflight reports how many handlers are running right now.
func (p *Pool) Inflight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.inflight)
}

// InflightIDs returns the ids currently leased by this pool, for the
// heartbeater to renew.
func (p *Pool) InflightIDs() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]int64, 0, len(p.inflight))
	for id := range p.inflight {
		out = append(out, id)
	}
	return out
}

// JitteredInterval spreads a poll interval by frac, never below a tenth of the
// base so a misconfigured jitter cannot turn polling into a busy loop.
func JitteredInterval(base time.Duration, frac float64, r float64) time.Duration {
	if frac <= 0 {
		return base
	}
	delta := (r*2 - 1) * frac * float64(base)
	out := time.Duration(float64(base) + delta)
	if min := base / 10; out < min {
		return min
	}
	return out
}

func (p *Pool) nextInterval() time.Duration {
	p.rmu.Lock()
	r := p.rand.Float64()
	p.rmu.Unlock()
	return JitteredInterval(p.cfg.PollInterval, p.cfg.PollJitter, r)
}

// Run claims and processes work until ctx is cancelled. On cancellation it
// stops claiming, gives running handlers until drainFor to finish, and returns
// whatever is still running to the queue without consuming an attempt.
func (p *Pool) Run(ctx context.Context, drainFor time.Duration) error {
	defer close(p.done)

	timer := time.NewTimer(p.nextInterval())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			p.drain(drainFor)
			return nil
		case <-timer.C:
			p.pollOnce(ctx)
			timer.Reset(p.nextInterval())
		}
	}
}

// pollOnce claims as much work as there is free capacity for and dispatches it.
func (p *Pool) pollOnce(ctx context.Context) {
	p.mu.Lock()
	if p.stopping {
		p.mu.Unlock()
		return
	}
	free := p.cfg.Concurrency - len(p.inflight)
	p.mu.Unlock()
	if free <= 0 {
		p.TestSignals.Polled.Emit(0)
		return
	}

	started := time.Now()
	items, err := p.cfg.Store.Claim(ctx, storage.ClaimOpts{
		Queue:         p.cfg.Queue,
		Worker:        p.cfg.Worker,
		Limit:         free,
		LeaseDuration: p.cfg.LeaseDuration,
	})
	took := time.Since(started)
	if err != nil {
		if ctx.Err() == nil {
			p.cfg.Logger.Error("durableq: claim failed", "queue", p.cfg.Queue, "error", err)
		}
		p.TestSignals.Polled.Emit(0)
		return
	}
	if len(items) > 0 {
		// An empty poll emits nothing: an idle worker should not produce
		// metric noise that looks like activity.
		p.cfg.Metrics.ItemsClaimed(p.cfg.Queue, len(items), took)
	}
	for _, item := range items {
		p.dispatch(ctx, item)
	}
	p.TestSignals.Polled.Emit(len(items))
}

// dispatch runs one item in its own goroutine, holding a worker slot.
func (p *Pool) dispatch(parent context.Context, item storage.Item) {
	handlerCtx, cancel := context.WithCancel(context.WithoutCancel(parent))
	live := &inflightItem{item: item, cancel: cancel}

	p.mu.Lock()
	if p.stopping {
		p.mu.Unlock()
		cancel()
		// Claimed just as the pool began stopping: hand it straight back.
		p.release(item, "pool stopping before dispatch")
		return
	}
	p.inflight[item.ID] = live
	n := len(p.inflight)
	p.mu.Unlock()
	p.TestSignals.Concurrency.Emit(n)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer cancel()
		p.TestSignals.Started.Emit(item.ID)
		p.cfg.Metrics.ItemStarted(p.cfg.Queue, item.StepID)
		handlerStart := time.Now()

		// A handler that overruns its timeout must not hold its slot for
		// ever: the slot is freed and the item retried, while the goroutine
		// is left to finish on its own.
		timeout := time.NewTimer(p.cfg.HandlerTimeout)
		defer timeout.Stop()

		type outcome struct {
			res HandlerResult
			err error
		}
		out := make(chan outcome, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					out <- outcome{err: fmt.Errorf("durableq: handler panicked: %v", r)}
				}
			}()
			res, err := p.cfg.Handler(handlerCtx, item)
			out <- outcome{res: res, err: err}
		}()

		select {
		case o := <-out:
			p.finish(live, o.res, o.err, time.Since(handlerStart))
		case <-timeout.C:
			p.mu.Lock()
			stopping := p.stopping
			p.mu.Unlock()
			if stopping {
				// Shutdown is not a stuck handler.
				p.finishRelease(live, "handler did not finish before shutdown")
				return
			}
			cancel()
			p.TestSignals.Stuck.Emit(item.ID)
			p.cfg.Metrics.ItemStuck(p.cfg.Queue, item.StepID, p.cfg.HandlerTimeout)
			p.finish(live, HandlerResult{},
				fmt.Errorf("durableq: handler exceeded %s", p.cfg.HandlerTimeout),
				time.Since(handlerStart))
		}
	}()
}

// finish turns one handler outcome into exactly one durable transition.
func (p *Pool) finish(live *inflightItem, res HandlerResult, err error, took time.Duration) {
	live.finish.Do(func() {
		defer p.forget(live.item.ID)

		p.mu.Lock()
		soft := live.softStop
		p.mu.Unlock()
		if soft && err != nil {
			// The handler did not finish, and it did not finish because the
			// process is stopping. Hand the work back without spending the
			// attempt. A handler that *did* finish is acked below: throwing
			// away completed work would only make it run twice.
			p.releaseLive(live, "shutting down")
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		switch {
		case err == nil:
			p.ack(ctx, live.item, res)
			outcome := telemetry.OutcomeSuccess
			if res.Filtered {
				outcome = telemetry.OutcomeFiltered
			}
			p.cfg.Metrics.ItemFinished(p.cfg.Queue, live.item.StepID, outcome, took)
		case errors.Is(err, ErrDiscard), live.item.Policy.Exhausted(live.item.Attempt):
			p.deadLetter(ctx, live.item, err.Error())
			p.cfg.Metrics.ItemFinished(p.cfg.Queue, live.item.StepID, telemetry.OutcomeDeadLettered, took)
		default:
			p.retry(ctx, live.item, err.Error())
			p.cfg.Metrics.ItemFinished(p.cfg.Queue, live.item.StepID, telemetry.OutcomeRetried, took)
		}
	})
}

func (p *Pool) finishRelease(live *inflightItem, reason string) {
	live.finish.Do(func() {
		defer p.forget(live.item.ID)
		p.releaseLive(live, reason)
	})
}

func (p *Pool) releaseLive(live *inflightItem, reason string) {
	p.release(live.item, reason)
}

func (p *Pool) release(item storage.Item, reason string) {
	p.cfg.Metrics.ItemFinished(p.cfg.Queue, item.StepID, telemetry.OutcomeReleased, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := p.cfg.Store.Release(ctx, storage.ReleaseRequest{
		ID: item.ID, Worker: p.cfg.Worker, Reason: reason,
	})
	p.logResult("release", item.ID, res, err)
	p.TestSignals.Released.Emit(item.ID)
}

func (p *Pool) ack(ctx context.Context, item storage.Item, res HandlerResult) {
	out, err := p.cfg.Store.Complete(ctx, storage.CompleteRequest{
		ID: item.ID, Worker: p.cfg.Worker,
		Produced: res.Produced, Filtered: res.Filtered,
		Dropped: res.Dropped, Capped: res.Capped,
	})
	p.logResult("complete", item.ID, out, err)
	p.TestSignals.Acked.Emit(item.ID)
}

func (p *Pool) retry(ctx context.Context, item storage.Item, msg string) {
	out, err := p.cfg.Store.Retry(ctx, storage.RetryRequest{
		ID: item.ID, Worker: p.cfg.Worker, Error: msg,
	})
	p.logResult("retry", item.ID, out, err)
	p.TestSignals.Retried.Emit(item.ID)
}

func (p *Pool) deadLetter(ctx context.Context, item storage.Item, msg string) {
	out, err := p.cfg.Store.DeadLetter(ctx, storage.DeadLetterRequest{
		ID: item.ID, Worker: p.cfg.Worker, Error: msg,
	})
	p.logResult("dead-letter", item.ID, out, err)
	p.TestSignals.DeadLettered.Emit(item.ID)
}

// logResult reports a transition that did not apply. Losing a race with the
// reclaimer is normal and expected; anything else is worth seeing.
func (p *Pool) logResult(op string, id int64, res []storage.Result, err error) {
	if err != nil {
		p.cfg.Logger.Error("durableq: "+op+" failed", "queue", p.cfg.Queue, "item", id, "error", err)
		return
	}
	for _, r := range res {
		if r.Err != nil {
			p.cfg.Logger.Warn("durableq: "+op+" did not apply",
				"queue", p.cfg.Queue, "item", r.ID, "reason", r.Err)
		}
	}
}

func (p *Pool) forget(id int64) {
	p.mu.Lock()
	delete(p.inflight, id)
	n := len(p.inflight)
	p.mu.Unlock()
	p.TestSignals.Concurrency.Emit(n)
}

// drain stops claiming, waits up to drainFor for running handlers, then hands
// back whatever is still running without consuming its attempt.
func (p *Pool) drain(drainFor time.Duration) {
	p.mu.Lock()
	p.stopping = true
	p.mu.Unlock()

	finished := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(finished)
	}()

	timer := time.NewTimer(drainFor)
	defer timer.Stop()
	select {
	case <-finished:
		return
	case <-timer.C:
	}

	// Still running: mark them for release and cancel their contexts. The
	// handler goroutine finishes through finish(), which sees softStop.
	p.mu.Lock()
	live := make([]*inflightItem, 0, len(p.inflight))
	for _, l := range p.inflight {
		l.softStop = true
		live = append(live, l)
	}
	p.mu.Unlock()
	for _, l := range live {
		l.cancel()
	}

	select {
	case <-finished:
	case <-time.After(drainFor):
		// Handlers that ignore cancellation cannot be waited on for ever.
		// Their items are released here so the queue is not left holding
		// leases that only expire on their own.
		p.mu.Lock()
		stuck := make([]*inflightItem, 0, len(p.inflight))
		for _, l := range p.inflight {
			stuck = append(stuck, l)
		}
		p.mu.Unlock()
		for _, l := range stuck {
			p.finishRelease(l, "handler ignored shutdown")
		}
	}
}

// Wait blocks until Run has returned.
func (p *Pool) Wait() { <-p.done }
