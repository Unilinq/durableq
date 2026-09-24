// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package durableq

import (
	"time"

	"github.com/unilinq/durableq/internal/telemetry"
)

// Outcome is how one attempt at an item ended. It is the dimension that makes
// the difference between a queue that is working and one that is thrashing.
type Outcome string

const (
	// OutcomeSuccess is a handler that returned without error.
	OutcomeSuccess Outcome = "success"
	// OutcomeFiltered is a step that deliberately produced nothing.
	OutcomeFiltered Outcome = "filtered"
	// OutcomeRetried is a failure with attempts left.
	OutcomeRetried Outcome = "retried"
	// OutcomeDeadLettered is a failure with no attempts left.
	OutcomeDeadLettered Outcome = "dead_lettered"
	// OutcomeReleased is work handed back at shutdown, which consumed no
	// attempt and is not a failure.
	OutcomeReleased Outcome = "released"
)

// Observer receives durableq's runtime events so an application can turn them
// into metrics, logs or traces.
//
// DurableQ deliberately does not depend on a metrics library. "Application and
// a database, nothing else" is one of its goals, and the application already
// has a metrics stack; an adapter over this interface is a few lines.
//
// Note what these methods do *not* carry: no execution id, no item id, no
// payload. Those are high-cardinality and belong in logs and traces, never in
// metric labels. The interface makes that structural rather than a convention
// somebody has to remember.
//
// Implementations must be safe for concurrent use and must not block: they are
// called from the worker's hot path.
type Observer interface {
	// ItemsClaimed reports a poll that returned work, with how long the claim
	// took. It is not called when a poll comes back empty, so an idle worker
	// produces no metric noise.
	ItemsClaimed(queue string, n int, took time.Duration)

	// ItemStarted reports a handler being entered.
	ItemStarted(queue, step string)

	// ItemFinished reports how an attempt ended and how long the handler ran.
	ItemFinished(queue, step string, outcome Outcome, took time.Duration)

	// ItemStuck reports a handler that overran its timeout and had its worker
	// slot taken back.
	ItemStuck(queue, step string, after time.Duration)

	// LeasesRenewed reports a heartbeat.
	LeasesRenewed(worker string, n int)

	// LeasesExpired reports a reclaim pass: work returned to its queue, and
	// work dead-lettered because the lapsed attempt was its last.
	LeasesExpired(reclaimed, deadLettered int)

	// QueueDepth reports a sampled queue gauge.
	QueueDepth(queue string, ready, running, dlq int)

	// ItemsSwept reports retention deletion.
	ItemsSwept(deleted int)
}

// NopObserver discards everything. It is the default, so an application that
// does not want metrics pays nothing.
type NopObserver struct{}

var _ Observer = NopObserver{}

func (NopObserver) ItemsClaimed(string, int, time.Duration)             {}
func (NopObserver) ItemStarted(string, string)                          {}
func (NopObserver) ItemFinished(string, string, Outcome, time.Duration) {}
func (NopObserver) ItemStuck(string, string, time.Duration)             {}
func (NopObserver) LeasesRenewed(string, int)                           {}
func (NopObserver) LeasesExpired(int, int)                              {}
func (NopObserver) QueueDepth(string, int, int, int)                    {}
func (NopObserver) ItemsSwept(int)                                      {}

// observerSink adapts the public Observer to the internal sink the runtime
// packages use. The indirection keeps the runtime from importing the root
// package, which imports it.
type observerSink struct{ o Observer }

var _ telemetry.Sink = observerSink{}

func (s observerSink) ItemsClaimed(queue string, n int, took time.Duration) {
	s.o.ItemsClaimed(queue, n, took)
}

func (s observerSink) ItemStarted(queue, step string) { s.o.ItemStarted(queue, step) }

func (s observerSink) ItemFinished(queue, step string, outcome telemetry.Outcome, took time.Duration) {
	s.o.ItemFinished(queue, step, Outcome(outcome), took)
}

func (s observerSink) ItemStuck(queue, step string, after time.Duration) {
	s.o.ItemStuck(queue, step, after)
}

func (s observerSink) LeasesRenewed(worker string, n int) { s.o.LeasesRenewed(worker, n) }

func (s observerSink) LeasesExpired(reclaimed, deadLettered int) {
	s.o.LeasesExpired(reclaimed, deadLettered)
}

func (s observerSink) QueueDepth(queue string, ready, running, dlq int) {
	s.o.QueueDepth(queue, ready, running, dlq)
}

func (s observerSink) ItemsSwept(deleted int) { s.o.ItemsSwept(deleted) }
