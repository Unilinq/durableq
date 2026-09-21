// Package telemetry carries durableq's runtime events to whatever the
// application wants to do with them, and samples the gauges that cannot be
// derived from events alone.
package telemetry

import (
	"context"
	"log/slog"
	"time"

	"github.com/unilinq/durableq/storage"
)

// Outcome mirrors the public durableq.Outcome. It is repeated here so the
// runtime packages do not import the root package, which imports them.
type Outcome string

// Attempt outcomes.
const (
	OutcomeSuccess      Outcome = "success"
	OutcomeFiltered     Outcome = "filtered"
	OutcomeRetried      Outcome = "retried"
	OutcomeDeadLettered Outcome = "dead_lettered"
	OutcomeReleased     Outcome = "released"
)

// Sink receives runtime events. The root package adapts the public Observer to
// this interface.
type Sink interface {
	ItemsClaimed(queue string, n int, took time.Duration)
	ItemStarted(queue, step string)
	ItemFinished(queue, step string, outcome Outcome, took time.Duration)
	ItemStuck(queue, step string, after time.Duration)
	LeasesRenewed(worker string, n int)
	LeasesExpired(reclaimed, deadLettered int)
	QueueDepth(queue string, ready, running, dlq int)
	ItemsSwept(deleted int)
}

// Nop discards every event.
type Nop struct{}

var _ Sink = Nop{}

func (Nop) ItemsClaimed(string, int, time.Duration)             {}
func (Nop) ItemStarted(string, string)                          {}
func (Nop) ItemFinished(string, string, Outcome, time.Duration) {}
func (Nop) ItemStuck(string, string, time.Duration)             {}
func (Nop) LeasesRenewed(string, int)                           {}
func (Nop) LeasesExpired(int, int)                              {}
func (Nop) QueueDepth(string, int, int, int)                    {}
func (Nop) ItemsSwept(int)                                      {}

// SamplerConfig parameterises a queue-depth sampler.
type SamplerConfig struct {
	Store storage.Store
	Sink  Sink
	// Interval between samples. Zero disables sampling entirely.
	Interval time.Duration
	// Queues limits sampling. Empty samples every known queue.
	Queues []string
	Logger *slog.Logger
}

// Sampler reports queue depth on an interval. Depth is a gauge and cannot be
// derived from events, so it is the one thing that has to be polled.
type Sampler struct {
	cfg  SamplerConfig
	done chan struct{}
}

// NewSampler builds a Sampler.
func NewSampler(cfg SamplerConfig) *Sampler {
	if cfg.Sink == nil {
		cfg.Sink = Nop{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Sampler{cfg: cfg, done: make(chan struct{})}
}

// Run samples until ctx is cancelled. With no interval it does nothing at all,
// so an application that wants no metrics pays for no queries.
func (s *Sampler) Run(ctx context.Context) error {
	defer close(s.done)
	if s.cfg.Interval <= 0 {
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
			s.Sample(ctx)
		}
	}
}

// Sample takes one reading.
func (s *Sampler) Sample(ctx context.Context) {
	stats, err := s.cfg.Store.Stats(ctx, s.cfg.Queues...)
	if err != nil {
		if ctx.Err() == nil {
			s.cfg.Logger.Error("durableq: sampling queue depth", "error", err)
		}
		return
	}
	for _, st := range stats {
		s.cfg.Sink.QueueDepth(st.Queue, st.Ready, st.Running, st.DLQ)
	}
}

// Wait blocks until Run has returned.
func (s *Sampler) Wait() { <-s.done }
