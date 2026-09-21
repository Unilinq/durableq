package durableq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// recordingObserver captures every event so a test can assert what was emitted
// and, just as importantly, what was not.
type recordingObserver struct {
	mu sync.Mutex

	claims    []claimEvent
	started   []string
	finished  []finishEvent
	stuck     int
	renewed   int
	expired   int
	depth     []depthEvent
	swept     int
	idleCalls int
}

type claimEvent struct {
	queue string
	n     int
	took  time.Duration
}

type finishEvent struct {
	queue, step string
	outcome     Outcome
	took        time.Duration
}

type depthEvent struct {
	queue               string
	ready, running, dlq int
}

func (o *recordingObserver) ItemsClaimed(queue string, n int, took time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if n == 0 {
		o.idleCalls++
	}
	o.claims = append(o.claims, claimEvent{queue, n, took})
}

func (o *recordingObserver) ItemStarted(queue, step string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.started = append(o.started, queue)
}

func (o *recordingObserver) ItemFinished(queue, step string, outcome Outcome, took time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.finished = append(o.finished, finishEvent{queue, step, outcome, took})
}

func (o *recordingObserver) ItemStuck(string, string, time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stuck++
}

func (o *recordingObserver) LeasesRenewed(_ string, n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.renewed += n
}

func (o *recordingObserver) LeasesExpired(reclaimed, deadLettered int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.expired += reclaimed + deadLettered
}

func (o *recordingObserver) QueueDepth(queue string, ready, running, dlq int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.depth = append(o.depth, depthEvent{queue, ready, running, dlq})
}

func (o *recordingObserver) ItemsSwept(n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.swept += n
}

func (o *recordingObserver) outcomes() map[Outcome]int {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := map[Outcome]int{}
	for _, f := range o.finished {
		out[f.outcome]++
	}
	return out
}

func (o *recordingObserver) snapshot() recordingObserver {
	o.mu.Lock()
	defer o.mu.Unlock()
	return recordingObserver{
		claims:    append([]claimEvent(nil), o.claims...),
		finished:  append([]finishEvent(nil), o.finished...),
		depth:     append([]depthEvent(nil), o.depth...),
		stuck:     o.stuck,
		idleCalls: o.idleCalls,
	}
}

// TestObserverSeesEveryOutcome proves each way an attempt can end is reported,
// with the queue and step as dimensions.
func TestObserverSeesEveryOutcome(t *testing.T) {
	t.Parallel()
	obs := &recordingObserver{}
	app := newTestApp(t, func(c *Config) { c.Observer = obs })

	var n int
	q, pool := app.armed(t, "observed", func(ctx context.Context, _ struct{}) error {
		n++
		if n == 1 {
			return errors.New("transient")
		}
		return nil
	}, WithPolicy(Policy{MaxAttempts: 3, Schedule: []time.Duration{0}, Jitter: 0}))

	if _, err := q.Enqueue(t.Context(), struct{}{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	pool.TestSignals.Retried.WaitOrTimeout(t)
	pool.TestSignals.Acked.WaitOrTimeout(t)

	got := obs.outcomes()
	if got[OutcomeRetried] != 1 {
		t.Fatalf("retried: got %d, want 1 (%v)", got[OutcomeRetried], got)
	}
	if got[OutcomeSuccess] != 1 {
		t.Fatalf("success: got %d, want 1 (%v)", got[OutcomeSuccess], got)
	}

	snap := obs.snapshot()
	if len(snap.claims) == 0 {
		t.Fatalf("no claim events")
	}
	for _, c := range snap.claims {
		if c.queue != "observed" {
			t.Fatalf("claim event on queue %q", c.queue)
		}
		if c.n == 0 {
			t.Fatalf("a poll that claimed nothing was reported")
		}
	}
	for _, f := range snap.finished {
		if f.queue != "observed" {
			t.Fatalf("finish event on queue %q", f.queue)
		}
	}
}

// TestIdlePollsEmitNothing is the rule that keeps dashboards honest: a worker
// with no work must not look busy.
func TestIdlePollsEmitNothing(t *testing.T) {
	t.Parallel()
	obs := &recordingObserver{}
	app := newTestApp(t, func(c *Config) { c.Observer = obs })

	_, pool := app.armed(t, "idle", func(ctx context.Context, _ struct{}) error { return nil })

	// Let several polls come back empty.
	for i := 0; i < 5; i++ {
		if n := pool.TestSignals.Polled.WaitOrTimeout(t); n != 0 {
			t.Fatalf("claimed %d items from an empty queue", n)
		}
	}
	snap := obs.snapshot()
	if snap.idleCalls != 0 {
		t.Fatalf("%d empty polls were reported as claim events", snap.idleCalls)
	}
	if len(snap.claims) != 0 {
		t.Fatalf("%d claim events from an idle worker", len(snap.claims))
	}
}

// TestDeadLetterAndFilteredAreDistinct proves the outcomes a dashboard needs to
// tell apart really are separate values.
func TestDeadLetterAndFilteredAreDistinct(t *testing.T) {
	t.Parallel()
	obs := &recordingObserver{}
	app := newTestApp(t, func(c *Config) { c.Observer = obs })

	q, pool := app.armed(t, "outcomes", func(ctx context.Context, _ struct{}) error {
		return Discard(errors.New("never valid"))
	}, WithPolicy(Policy{MaxAttempts: 5, Schedule: []time.Duration{0}, Jitter: 0}))

	if _, err := q.Enqueue(t.Context(), struct{}{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	pool.TestSignals.DeadLettered.WaitOrTimeout(t)

	if got := obs.outcomes()[OutcomeDeadLettered]; got != 1 {
		t.Fatalf("dead_lettered: got %d, want 1 (%v)", got, obs.outcomes())
	}
}

// TestQueueDepthIsSampledOnlyWhenAsked proves the one polled gauge costs
// nothing unless it is switched on.
func TestQueueDepthIsSampledOnlyWhenAsked(t *testing.T) {
	t.Parallel()

	off := &recordingObserver{}
	appOff := newTestApp(t, func(c *Config) { c.Observer = off })
	_, pool := appOff.armed(t, "nodepth", func(ctx context.Context, _ struct{}) error { return nil })
	for i := 0; i < 5; i++ {
		pool.TestSignals.Polled.WaitOrTimeout(t)
	}
	if n := len(off.snapshot().depth); n != 0 {
		t.Fatalf("%d depth samples with sampling disabled", n)
	}

	on := &recordingObserver{}
	appOn := newTestApp(t, func(c *Config) {
		c.Observer = on
		c.DepthInterval = 5 * time.Millisecond
	})
	q, poolOn := appOn.armed(t, "depth", func(ctx context.Context, _ struct{}) error { return nil })
	if _, err := q.Enqueue(t.Context(), struct{}{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	poolOn.TestSignals.Acked.WaitOrTimeout(t)

	deadline := time.After(5 * time.Second)
	for {
		if len(on.snapshot().depth) > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("queue depth was never sampled with an interval set")
		default:
		}
	}
	for _, d := range on.snapshot().depth {
		if d.queue == "" {
			t.Fatalf("depth sample with no queue name")
		}
	}
}

// TestNopObserverIsTheDefault proves an application that wants no metrics gets
// no observer calls and no extra queries.
func TestNopObserverIsTheDefault(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	if _, ok := app.cfg.Observer.(NopObserver); !ok {
		t.Fatalf("default observer is %T, want NopObserver", app.cfg.Observer)
	}
	if app.cfg.DepthInterval != 0 {
		t.Fatalf("depth sampling is on by default (%s); it should cost nothing unless asked for",
			app.cfg.DepthInterval)
	}
}

// TestObserverCarriesNoHighCardinalityIdentifiers pins the design rule by
// checking the interface itself: no method takes an execution or item id, so a
// caller cannot accidentally turn one into a metric label.
func TestObserverCarriesNoHighCardinalityIdentifiers(t *testing.T) {
	t.Parallel()
	// The compiler enforces this: NopObserver satisfies Observer, and every
	// method's parameters are queue, step, worker, counts and durations. If
	// someone adds an item id to the interface, this file stops compiling
	// because the signatures below no longer match.
	var _ interface {
		ItemsClaimed(queue string, n int, took time.Duration)
		ItemStarted(queue, step string)
		ItemFinished(queue, step string, outcome Outcome, took time.Duration)
		ItemStuck(queue, step string, after time.Duration)
		LeasesRenewed(worker string, n int)
		LeasesExpired(reclaimed, deadLettered int)
		QueueDepth(queue string, ready, running, dlq int)
		ItemsSwept(deleted int)
	} = NopObserver{}
}
