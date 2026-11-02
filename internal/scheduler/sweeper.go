// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/unilinq/durableq/internal/dqsignal"
	"github.com/unilinq/durableq/internal/telemetry"
	"github.com/unilinq/durableq/storage"
)

// SweeperConfig parameterises a Sweeper.
type SweeperConfig struct {
	Store storage.Store
	// Interval is how often a pass starts.
	Interval time.Duration
	// Retention is how long a completed item is kept.
	// storage.RetentionNever disables sweeping entirely; zero and
	// storage.RetentionZero make every completed item due.
	Retention time.Duration
	// BatchSize bounds one sweep batch.
	BatchSize int
	// Budget is how long one pass may keep deleting full batches. Zero runs
	// one batch per pass.
	Budget time.Duration
	// Fold makes every batch fold the items it deletes into saved counts and
	// item history (storage.SweepOpts.Fold). The store must implement
	// storage.Folder.
	Fold bool
	// HistoryRetention is how long folded items' history is kept.
	HistoryRetention time.Duration
	// FilterLabels returns the payload flags counted among each job's filtered
	// items when they are folded (storage.SweepOpts.FilterLabels). It is
	// called once per pass, so jobs defined after the sweeper starts count.
	FilterLabels func() map[string][]string
	// Queue limits the sweep to one queue. Empty sweeps every queue.
	Queue  string
	Clock  storage.Clock
	Logger *slog.Logger
	// Metrics receives runtime events. Nil means discard them.
	Metrics telemetry.Sink
}

// Sweeper deletes terminal successes past their retention. It never touches
// dead-lettered work: that is the record of what went wrong.
//
// A pass keeps deleting batches until one comes back short, the budget runs
// out or the context ends, so a store that finishes more items between passes
// than one batch holds still catches up.
type Sweeper struct {
	cfg     SweeperConfig
	breaker *Breaker
	done    chan struct{}

	// TestSignals lets tests observe passes instead of waiting on the clock.
	TestSignals SweeperSignals
}

// SweeperSignals are the observable moments inside the sweeper.
type SweeperSignals struct {
	Pass dqsignal.Signal[int] // items deleted on this pass
}

// Init arms every signal.
func (s *SweeperSignals) Init() { s.Pass.Init() }

// PassResult is what one pass did.
type PassResult struct {
	Deleted int
	Batches int
	Took    time.Duration
	// OutOfTime is set when the pass used up its budget on full batches, so
	// due items are probably left for the next pass.
	OutOfTime bool
	// Backlog is, when the pass ran out of time, how long before the cutoff
	// the newest item it deleted was finalized: roughly how far behind the
	// sweep is. Zero when the pass caught up.
	Backlog time.Duration
}

// NewSweeper builds a Sweeper.
func NewSweeper(cfg SweeperConfig) *Sweeper {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Minute
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
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
	return &Sweeper{
		cfg:     cfg,
		breaker: NewBreaker(cfg.BatchSize, 1),
		done:    make(chan struct{}),
	}
}

// Run starts a pass every Interval until ctx is cancelled. Passes never
// overlap.
func (s *Sweeper) Run(ctx context.Context) error {
	defer close(s.done)
	if s.cfg.Retention == storage.RetentionNever {
		<-ctx.Done()
		return nil
	}
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			_, _ = s.Pass(ctx)
		}
	}
}

// errNoFolder explains a sweeper asked to fold over a store that cannot.
var errNoFolder = errors.New("durableq: Fold is set but the store does not implement storage.Folder")

// Pass deletes due items until a batch comes back short, the budget runs out
// or ctx is cancelled, and returns what it did. Exported so tests can run
// exactly one without starting the loop.
//
// The cutoff is fixed for the whole pass: on a busy store items fall due every
// second, and a cutoff that moved with each batch could keep every batch full.
// Each batch starts at the newest finalized_at the previous one deleted.
// Without that, every batch walks the index past the items already deleted,
// which PostgreSQL can skip only once no open transaction can still see them,
// so a long transaction elsewhere makes the pass quadratic.
//
// A failed batch halves the next one, so an oversized batch cannot fail for
// ever; a batch that lands restores the full size. A batch cut off by ctx
// rolls back whole.
func (s *Sweeper) Pass(ctx context.Context) (PassResult, error) {
	var res PassResult
	if s.cfg.Retention == storage.RetentionNever {
		s.TestSignals.Pass.Emit(0)
		return res, nil
	}
	retention := s.cfg.Retention
	if retention == storage.RetentionZero {
		retention = 0
	}
	start := s.cfg.Clock.Now()

	var labels map[string][]string
	if s.cfg.Fold {
		folder, ok := s.cfg.Store.(storage.Folder)
		if !ok {
			return s.failed(ctx, res, errNoFolder, false)
		}
		if err := folder.PrepareFold(ctx, storage.FoldOpts{
			Now: start, HistoryRetention: s.cfg.HistoryRetention,
		}); err != nil {
			return s.failed(ctx, res, err, false)
		}
		if s.cfg.FilterLabels != nil {
			labels = s.cfg.FilterLabels()
		}
	}

	var after time.Time // the zero time is earlier than any finalized_at
	for {
		limit := s.breaker.Batch()
		batch, err := s.cfg.Store.Sweep(ctx, storage.SweepOpts{
			Queue:            s.cfg.Queue,
			Retention:        retention,
			Limit:            limit,
			Now:              start,
			After:            after,
			Fold:             s.cfg.Fold,
			HistoryRetention: s.cfg.HistoryRetention,
			FilterLabels:     labels,
		})
		res.Took = s.cfg.Clock.Now().Sub(start)
		if err != nil {
			return s.failed(ctx, res, err, true)
		}
		s.breaker.Reset()
		res.Batches++
		res.Deleted += batch.Deleted
		if batch.Deleted < limit {
			break
		}
		after = batch.Last
		if ctx.Err() != nil {
			break
		}
		if res.Took >= s.cfg.Budget {
			res.OutOfTime = true
			res.Backlog = start.Add(-retention).Sub(batch.Last)
			break
		}
	}
	s.report(res)
	s.TestSignals.Pass.Emit(res.Deleted)
	return res, ctx.Err()
}

// failed ends a pass on err. A failed batch trips the breaker; a pass ended by
// shutdown is not logged as a failure.
func (s *Sweeper) failed(ctx context.Context, res PassResult, err error, batch bool) (PassResult, error) {
	if res.Deleted > 0 {
		s.cfg.Metrics.ItemsSwept(res.Deleted)
	}
	if ctx.Err() == nil {
		if batch {
			s.breaker.Trip()
		}
		s.cfg.Logger.Error("durableq: sweep failed",
			"deleted", res.Deleted, "batches", res.Batches, "next_batch", s.breaker.Batch(), "error", err)
	}
	s.TestSignals.Pass.Emit(res.Deleted)
	return res, err
}

// report logs a pass and records its metrics. A pass that deleted nothing logs
// at debug, so a store that keeps up does not log every minute.
func (s *Sweeper) report(res PassResult) {
	if res.Deleted > 0 {
		s.cfg.Metrics.ItemsSwept(res.Deleted)
	}
	s.cfg.Metrics.SweepBacklog(res.Backlog)
	level := slog.LevelDebug
	if res.Deleted > 0 || res.OutOfTime {
		level = slog.LevelInfo
	}
	s.cfg.Logger.Log(context.Background(), level, "durableq: swept completed items",
		"deleted", res.Deleted, "batches", res.Batches, "took", res.Took,
		"out_of_time", res.OutOfTime, "backlog", res.Backlog, "fold", s.cfg.Fold)
}

// Wait blocks until Run has returned.
func (s *Sweeper) Wait() { <-s.done }
