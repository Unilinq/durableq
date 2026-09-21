// Package leasing keeps a worker's claim on the items it is still processing.
//
// A lease is the long-lived ownership mechanism: the database row says who
// holds the item and until when, and no database lock is involved. A handler
// that runs longer than one lease period stays the owner only because this
// heartbeat renews it.
package leasing

import (
	"context"
	"log/slog"
	"time"

	"github.com/unilinq/durableq/internal/dqsignal"
	"github.com/unilinq/durableq/internal/telemetry"
	"github.com/unilinq/durableq/storage"
)

// Config parameterises a Heartbeater.
type Config struct {
	Store  storage.Store
	Worker string
	// Items reports the ids this worker currently holds.
	Items func() []int64
	// LeaseDuration is the lease length renewed on each beat.
	LeaseDuration time.Duration
	// Interval is how often to renew. Zero uses a third of LeaseDuration,
	// which tolerates two missed beats before an item is reclaimable.
	Interval time.Duration
	Clock    storage.Clock
	Logger   *slog.Logger
	// Metrics receives runtime events. Nil means discard them.
	Metrics telemetry.Sink
}

// Heartbeater renews leases for in-flight work.
type Heartbeater struct {
	cfg  Config
	done chan struct{}

	// TestSignals lets tests observe beats instead of waiting on the clock.
	TestSignals Signals
}

// Signals are the observable moments inside the heartbeater.
type Signals struct {
	Beat dqsignal.Signal[int] // items renewed on this beat
}

// Init arms every signal.
func (s *Signals) Init() { s.Beat.Init() }

// New builds a Heartbeater.
func New(cfg Config) *Heartbeater {
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.Interval <= 0 {
		cfg.Interval = cfg.LeaseDuration / 3
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
	return &Heartbeater{cfg: cfg, done: make(chan struct{})}
}

// Run renews leases until ctx is cancelled.
func (h *Heartbeater) Run(ctx context.Context) error {
	defer close(h.done)
	ticker := time.NewTicker(h.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			h.beat(ctx)
		}
	}
}

func (h *Heartbeater) beat(ctx context.Context) {
	ids := h.cfg.Items()
	if len(ids) == 0 {
		h.TestSignals.Beat.Emit(0)
		return
	}
	until := h.cfg.Clock.Now().Add(h.cfg.LeaseDuration)
	res, err := h.cfg.Store.ExtendLease(ctx, h.cfg.Worker, until, ids...)
	if err != nil {
		if ctx.Err() == nil {
			h.cfg.Logger.Error("durableq: lease renewal failed", "worker", h.cfg.Worker, "error", err)
		}
		h.TestSignals.Beat.Emit(0)
		return
	}
	renewed := 0
	for _, r := range res {
		if r.Err == nil {
			renewed++
			continue
		}
		// Losing a lease means something already reclaimed the item. The
		// handler will find out when its own transition is rejected.
		h.cfg.Logger.Warn("durableq: lease lost", "worker", h.cfg.Worker, "item", r.ID, "reason", r.Err)
	}
	if renewed > 0 {
		h.cfg.Metrics.LeasesRenewed(h.cfg.Worker, renewed)
	}
	h.TestSignals.Beat.Emit(renewed)
}

// Wait blocks until Run has returned.
func (h *Heartbeater) Wait() { <-h.done }
