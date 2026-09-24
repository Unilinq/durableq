// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/unilinq/durableq/internal/dqsignal"
	"github.com/unilinq/durableq/internal/telemetry"
	"github.com/unilinq/durableq/storage"
)

// ReclaimerConfig parameterises a Reclaimer.
type ReclaimerConfig struct {
	Store storage.Store
	// Interval is how often to look for lapsed leases.
	Interval time.Duration
	// BatchSize bounds one reclaim pass.
	BatchSize int
	Clock     storage.Clock
	Logger    *slog.Logger
	// Metrics receives runtime events. Nil means discard them.
	Metrics telemetry.Sink
}

// Reclaimer returns items whose lease lapsed to their queue, or to the DLQ if
// the lapsed attempt was their last.
type Reclaimer struct {
	cfg     ReclaimerConfig
	breaker *Breaker
	done    chan struct{}

	// TestSignals lets tests observe passes instead of waiting on the clock.
	TestSignals ReclaimerSignals
}

// ReclaimerSignals are the observable moments inside the reclaimer.
type ReclaimerSignals struct {
	Pass dqsignal.Signal[storage.ReclaimResult]
}

// Init arms every signal.
func (s *ReclaimerSignals) Init() { s.Pass.Init() }

// NewReclaimer builds a Reclaimer.
func NewReclaimer(cfg ReclaimerConfig) *Reclaimer {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
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
	return &Reclaimer{
		cfg:     cfg,
		breaker: NewBreaker(cfg.BatchSize, 1),
		done:    make(chan struct{}),
	}
}

// Run reclaims until ctx is cancelled.
func (r *Reclaimer) Run(ctx context.Context) error {
	defer close(r.done)
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.Pass(ctx)
		}
	}
}

// Pass runs one reclaim pass. Exported so an operator tool, or a test, can run
// exactly one without starting the loop.
func (r *Reclaimer) Pass(ctx context.Context) storage.ReclaimResult {
	res, err := r.cfg.Store.ReclaimExpired(ctx, storage.ReclaimOpts{Limit: r.breaker.Batch()})
	if err != nil {
		if ctx.Err() == nil {
			r.cfg.Logger.Error("durableq: reclaim failed",
				"batch", r.breaker.Batch(), "error", err)
			r.breaker.Trip()
		}
		r.TestSignals.Pass.Emit(storage.ReclaimResult{})
		return storage.ReclaimResult{}
	}
	r.breaker.Reset()
	if res.Reclaimed > 0 || res.DeadLettered > 0 {
		r.cfg.Logger.Info("durableq: reclaimed lapsed leases",
			"reclaimed", res.Reclaimed, "dead_lettered", res.DeadLettered)
		r.cfg.Metrics.LeasesExpired(res.Reclaimed, res.DeadLettered)
	}
	r.TestSignals.Pass.Emit(res)
	return res
}

// Wait blocks until Run has returned.
func (r *Reclaimer) Wait() { <-r.done }
