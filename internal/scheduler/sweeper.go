package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/unilinq/durableq/internal/dqsignal"
	"github.com/unilinq/durableq/internal/telemetry"
	"github.com/unilinq/durableq/storage"
)

// SweeperConfig parameterises a Sweeper.
type SweeperConfig struct {
	Store storage.Store
	// Interval is how often to sweep.
	Interval time.Duration
	// Retention is how long a completed item is kept.
	// storage.RetentionNever disables sweeping entirely.
	Retention time.Duration
	// BatchSize bounds one sweep pass.
	BatchSize int
	// Queue limits the sweep to one queue. Empty sweeps every queue.
	Queue  string
	Clock  storage.Clock
	Logger *slog.Logger
	// Metrics receives runtime events. Nil means discard them.
	Metrics telemetry.Sink
}

// Sweeper deletes terminal successes past their retention. It never touches
// dead-lettered work: that is the record of what went wrong.
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

// Run sweeps until ctx is cancelled.
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
			s.Pass(ctx)
		}
	}
}

// Pass runs one sweep. Exported so the CLI's gc verb and tests can run exactly
// one without starting the loop.
func (s *Sweeper) Pass(ctx context.Context) int {
	if s.cfg.Retention == storage.RetentionNever {
		s.TestSignals.Pass.Emit(0)
		return 0
	}
	n, err := s.cfg.Store.Sweep(ctx, storage.SweepOpts{
		Queue:     s.cfg.Queue,
		Retention: s.cfg.Retention,
		Limit:     s.breaker.Batch(),
	})
	if err != nil {
		if ctx.Err() == nil {
			s.cfg.Logger.Error("durableq: sweep failed", "batch", s.breaker.Batch(), "error", err)
			s.breaker.Trip()
		}
		s.TestSignals.Pass.Emit(0)
		return 0
	}
	s.breaker.Reset()
	if n > 0 {
		s.cfg.Logger.Info("durableq: swept completed items", "deleted", n)
		s.cfg.Metrics.ItemsSwept(n)
	}
	s.TestSignals.Pass.Emit(n)
	return n
}

// Wait blocks until Run has returned.
func (s *Sweeper) Wait() { <-s.done }
