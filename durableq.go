// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

// Package durableq is a durable queue with multi-step job semantics on top of
// an ordinary database.
//
// It provides item-level reliability — at-least-once delivery, independent
// retries, worker leases, per-queue dead-letter queues — together with
// run-level observability, so an operator can ask what happened to a whole
// ingestion run and not only to one item.
//
// A queue is usable on its own:
//
//	app, _ := durableq.New(durableq.Config{Store: store})
//	q := app.Queue("indexing")
//	q.Enqueue(ctx, IndexInput{URL: u})
//	q.Work(func(ctx context.Context, in IndexInput) error { return index(in) })
//	app.Start(ctx)
//
// Processing is at-least-once. A handler may see the same item twice, so its
// side effects must tolerate that. DurableQ does not claim exactly-once.
package durableq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/unilinq/durableq/internal/leasing"
	"github.com/unilinq/durableq/internal/runtime"
	"github.com/unilinq/durableq/internal/scheduler"
	"github.com/unilinq/durableq/internal/telemetry"
	"github.com/unilinq/durableq/storage"
)

// Config configures an App.
type Config struct {
	// Store is the durable backend. Required.
	Store storage.Store

	// WorkerID identifies this process in leases and dead-letter metadata.
	// Empty derives one from the hostname and pid.
	WorkerID string

	// PollInterval is how often a worker asks for work. Wall-clock time:
	// polling is a latency choice, not part of item scheduling.
	PollInterval time.Duration
	// PollJitter spreads polls across workers. Default 0.2.
	PollJitter float64
	// LeaseDuration is how long a claim is owned before it can be reclaimed.
	LeaseDuration time.Duration
	// HeartbeatInterval is how often a worker renews the leases it holds.
	// Zero uses a third of LeaseDuration, which tolerates two missed beats.
	HeartbeatInterval time.Duration
	// HandlerTimeout bounds one attempt. A handler that overruns frees its
	// worker slot and the item is retried.
	HandlerTimeout time.Duration
	// DrainTimeout is how long Stop waits for running handlers before handing
	// their work back.
	DrainTimeout time.Duration
	// ReclaimInterval is how often lapsed leases are recovered.
	ReclaimInterval time.Duration
	// ReclaimBatch bounds one reclaim pass.
	ReclaimBatch int

	// Retention is how long a completed item is kept before it is swept.
	// storage.RetentionNever keeps completed items for ever. Dead-lettered
	// items are never swept whatever this says.
	Retention time.Duration
	// SweepInterval is how often retention is applied.
	SweepInterval time.Duration
	// SweepBatch bounds one sweep pass.
	SweepBatch int

	// DefaultPolicy applies to queues that do not set their own.
	DefaultPolicy storage.Policy

	Clock  storage.Clock
	Logger *slog.Logger

	// Observer receives runtime events so the application can turn them into
	// metrics, logs or traces. Nil discards them.
	Observer Observer
	// DepthInterval is how often queue depth is sampled. Depth is a gauge and
	// cannot be derived from events, so it is the one thing that is polled.
	// Zero disables sampling, and then no extra query is ever made.
	DepthInterval time.Duration
}

func (c *Config) withDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = 200 * time.Millisecond
	}
	if c.PollJitter == 0 {
		c.PollJitter = 0.2
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = c.LeaseDuration / 3
	}
	if c.HandlerTimeout <= 0 {
		c.HandlerTimeout = 5 * time.Minute
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 30 * time.Second
	}
	if c.ReclaimInterval <= 0 {
		c.ReclaimInterval = 5 * time.Second
	}
	if c.ReclaimBatch <= 0 {
		c.ReclaimBatch = 100
	}
	if c.Retention == 0 {
		c.Retention = 7 * 24 * time.Hour
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = time.Minute
	}
	if c.SweepBatch <= 0 {
		c.SweepBatch = 1000
	}
	if c.DefaultPolicy.MaxAttempts == 0 && len(c.DefaultPolicy.Schedule) == 0 {
		c.DefaultPolicy = storage.DefaultPolicy()
	}
	if c.Clock == nil {
		c.Clock = storage.RealClock()
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Observer == nil {
		c.Observer = NopObserver{}
	}
	if c.WorkerID == "" {
		host, err := os.Hostname()
		if err != nil {
			host = "unknown"
		}
		c.WorkerID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
}

// App owns the queues, the workers that serve them, and the background
// maintenance that keeps leases honest.
type App struct {
	cfg Config

	mu      sync.Mutex
	queues  map[string]*Queue
	jobs    map[string]*Job
	started bool
	stopped bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	reclaimer *scheduler.Reclaimer
	sweeper   *scheduler.Sweeper
	sampler   *telemetry.Sampler
	metrics   telemetry.Sink
}

// New builds an App.
func New(cfg Config) (*App, error) {
	if cfg.Store == nil {
		return nil, errors.New("durableq: Config.Store is required")
	}
	cfg.withDefaults()
	return &App{
		cfg:     cfg,
		queues:  map[string]*Queue{},
		jobs:    map[string]*Job{},
		metrics: observerSink{cfg.Observer},
	}, nil
}

// Store returns the durable store, for callers that need the contract directly.
func (a *App) Store() storage.Store { return a.cfg.Store }

// WorkerID returns this process's identity in leases and DLQ metadata.
func (a *App) WorkerID() string { return a.cfg.WorkerID }

// Logger returns the app's logger.
func (a *App) Logger() *slog.Logger { return a.cfg.Logger }

// Start launches every registered worker plus background maintenance. It
// returns as soon as they are running.
func (a *App) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		return errors.New("durableq: already started")
	}
	a.started = true
	jobs := make([]*Job, 0, len(a.jobs))
	for _, j := range a.jobs {
		jobs = append(jobs, j)
	}
	a.mu.Unlock()

	// Jobs register their step queues here, so a job defined before Start has
	// its workers running after it.
	for _, j := range jobs {
		if err := j.wire(); err != nil {
			a.mu.Lock()
			a.started = false
			a.mu.Unlock()
			return err
		}
	}

	a.mu.Lock()
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	a.cancel = cancel
	queues := make([]*Queue, 0, len(a.queues))
	for _, q := range a.queues {
		queues = append(queues, q)
	}
	a.mu.Unlock()

	for _, q := range queues {
		if err := q.start(runCtx, a); err != nil {
			cancel()
			return err
		}
	}

	a.reclaimer = scheduler.NewReclaimer(scheduler.ReclaimerConfig{
		Store:     a.cfg.Store,
		Interval:  a.cfg.ReclaimInterval,
		BatchSize: a.cfg.ReclaimBatch,
		Clock:     a.cfg.Clock,
		Logger:    a.cfg.Logger,
	})
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		_ = a.reclaimer.Run(runCtx)
	}()

	a.sweeper = a.Sweeper()
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		_ = a.sweeper.Run(runCtx)
	}()

	a.sampler = telemetry.NewSampler(telemetry.SamplerConfig{
		Store:    a.cfg.Store,
		Sink:     a.metrics,
		Interval: a.cfg.DepthInterval,
		Logger:   a.cfg.Logger,
	})
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		_ = a.sampler.Run(runCtx)
	}()

	// Stop when the caller's context ends, so an App started with a context
	// that is later cancelled does not keep running. The watcher also exits
	// when Stop is called directly, otherwise Stop would wait on a context
	// that may outlive the app.
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		select {
		case <-ctx.Done():
			cancel()
		case <-runCtx.Done():
		}
	}()
	return nil
}

// Stop drains gracefully: workers stop claiming, running handlers are given
// DrainTimeout to finish, and anything still running is handed back to its
// queue without consuming an attempt.
func (a *App) Stop(ctx context.Context) error {
	a.mu.Lock()
	if !a.started || a.stopped {
		a.mu.Unlock()
		return nil
	}
	a.stopped = true
	cancel := a.cancel
	queues := make([]*Queue, 0, len(a.queues))
	for _, q := range a.queues {
		queues = append(queues, q)
	}
	a.mu.Unlock()

	cancel()
	for _, q := range queues {
		q.wait()
	}
	a.wg.Wait()
	return nil
}

// Reclaimer exposes the background lease recovery, so an operator tool can run
// one pass without starting the app.
func (a *App) Reclaimer() *scheduler.Reclaimer {
	if a.reclaimer == nil {
		a.reclaimer = scheduler.NewReclaimer(scheduler.ReclaimerConfig{
			Store:     a.cfg.Store,
			Interval:  a.cfg.ReclaimInterval,
			BatchSize: a.cfg.ReclaimBatch,
			Clock:     a.cfg.Clock,
			Logger:    a.cfg.Logger,
			Metrics:   a.metrics,
		})
	}
	return a.reclaimer
}

// Sweeper exposes retention sweeping, so an operator tool can run one pass
// without starting the app.
func (a *App) Sweeper() *scheduler.Sweeper {
	if a.sweeper == nil {
		a.sweeper = scheduler.NewSweeper(scheduler.SweeperConfig{
			Store:     a.cfg.Store,
			Interval:  a.cfg.SweepInterval,
			Retention: a.cfg.Retention,
			BatchSize: a.cfg.SweepBatch,
			Clock:     a.cfg.Clock,
			Logger:    a.cfg.Logger,
			Metrics:   a.metrics,
		})
	}
	return a.sweeper
}

// poolConfig builds the runtime configuration shared by every worker.
func (a *App) poolConfig(queue string, handler runtime.Handler, concurrency int) runtime.Config {
	return runtime.Config{
		Store:          a.cfg.Store,
		Queue:          queue,
		Worker:         a.cfg.WorkerID,
		Handler:        handler,
		Concurrency:    concurrency,
		PollInterval:   a.cfg.PollInterval,
		PollJitter:     a.cfg.PollJitter,
		LeaseDuration:  a.cfg.LeaseDuration,
		HandlerTimeout: a.cfg.HandlerTimeout,
		Clock:          a.cfg.Clock,
		Logger:         a.cfg.Logger,
		Metrics:        a.metrics,
	}
}

func (a *App) heartbeaterFor(pool *runtime.Pool) *leasing.Heartbeater {
	return leasing.New(leasing.Config{
		Store:         a.cfg.Store,
		Worker:        a.cfg.WorkerID,
		Items:         pool.InflightIDs,
		LeaseDuration: a.cfg.LeaseDuration,
		Interval:      a.cfg.HeartbeatInterval,
		Clock:         a.cfg.Clock,
		Logger:        a.cfg.Logger,
		Metrics:       a.metrics,
	})
}
