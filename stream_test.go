// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package durableq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unilinq/durableq/storage"
)

// gate lets a streaming handler stop at a checkpoint so the test can inspect
// durable state while the handler is still running. Nothing here sleeps: the
// handler and the test hand control to each other over channels.
type gate struct {
	reached chan struct{}
	resume  chan struct{}
}

func newGate() *gate {
	return &gate{reached: make(chan struct{}), resume: make(chan struct{})}
}

// hit is called by the handler. It returns when the test opens the gate, or
// when the handler's context ends.
func (g *gate) hit(ctx context.Context) {
	select {
	case g.reached <- struct{}{}:
	case <-ctx.Done():
		return
	}
	select {
	case <-g.resume:
	case <-ctx.Done():
	}
}

// waitReached blocks the test until the handler is at the gate.
func (g *gate) waitReached(t *testing.T) {
	t.Helper()
	select {
	case <-g.reached:
	case <-time.After(10 * time.Second):
		t.Fatalf("the streaming handler never reached its checkpoint")
	}
}

// open lets the handler continue.
func (g *gate) open(t *testing.T) {
	t.Helper()
	select {
	case g.resume <- struct{}{}:
	case <-time.After(10 * time.Second):
		t.Fatalf("the streaming handler did not resume")
	}
}

// recorder is a successor handler that remembers what it received.
type recorder struct {
	mu   sync.Mutex
	urls []string
}

func (r *recorder) handle(ctx context.Context, in crawlIn) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.urls = append(r.urls, in.URL)
	return nil
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := slices.Clone(r.urls)
	slices.Sort(out)
	return out
}

func noop(ctx context.Context, in crawlIn) error { return nil }

func urls(prefix string, n int) []crawlIn {
	out := make([]crawlIn, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, crawlIn{URL: fmt.Sprintf("%s%d", prefix, i)})
	}
	return out
}

func urlStrings(in []crawlIn) []string {
	out := make([]string, 0, len(in))
	for _, c := range in {
		out = append(out, c.URL)
	}
	return out
}

func sorted(in ...string) []string {
	out := slices.Clone(in)
	slices.Sort(out)
	return out
}

// itemsOf lists the items of one step of an execution.
func (a *testApp) itemsOf(t *testing.T, execID, stepID string) []storage.Item {
	t.Helper()
	all, err := a.store.ListItems(t.Context(), storage.ItemFilter{ExecutionID: execID, Limit: 100000})
	if err != nil {
		t.Fatalf("ListItems: %v", err)
	}
	var out []storage.Item
	for _, it := range all {
		if it.StepID == stepID {
			out = append(out, it)
		}
	}
	return out
}

// parentOf returns the single "discover" item of an execution.
func (a *testApp) parentOf(t *testing.T, execID string) storage.Item {
	t.Helper()
	items := a.itemsOf(t, execID, "discover")
	if len(items) != 1 {
		t.Fatalf("want exactly one discover item, got %d", len(items))
	}
	return items[0]
}

func payloadURL(t *testing.T, it storage.Item) string {
	t.Helper()
	var c crawlIn
	if err := json.Unmarshal(it.Payload, &c); err != nil {
		t.Fatalf("payload %s: %v", it.Payload, err)
	}
	return c.URL
}

func payloadURLs(t *testing.T, items []storage.Item) []string {
	t.Helper()
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, payloadURL(t, it))
	}
	slices.Sort(out)
	return out
}

func requireURLs(t *testing.T, got, want []string, what string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

// R1. Items committed by a flush are claimable by successor workers while the
// producing handler is still running.
func TestStreamItemsAreProcessedWhileTheProducerStillRuns(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	g := newGate()
	rec := &recorder{}
	job := app.Job("stream-live").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			if err := out.Add(urls("early", 3)...); err != nil {
				return err
			}
			if err := out.Flush(); err != nil {
				return err
			}
			g.hit(ctx) // the producer is still running here
			return nil
		})).
		Step("crawl", rec.handle, After("discover"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	g.waitReached(t)

	// The downstream handler must finish all three while the producer is blocked.
	deadline := time.After(10 * time.Second)
	for len(rec.snapshot()) < 3 {
		select {
		case <-deadline:
			t.Fatalf("downstream received %v while the producer was running, want 3 items", rec.snapshot())
		case <-time.After(2 * time.Millisecond):
		}
	}
	requireURLs(t, rec.snapshot(), sorted("early0", "early1", "early2"), "items processed downstream")
	if got := app.parentOf(t, exec.ID); got.State != storage.StateRunning {
		t.Fatalf("producer state: got %s, want running", got.State)
	}

	g.open(t)
	app.waitForExecution(t, exec.ID, 1+3)
}

// flushProbe runs a streaming step that adds the given batch sizes and stops at
// a gate after each one. At each gate it checks how many successor rows are
// durable. It returns the execution once the run has settled.
func flushProbe(t *testing.T, app *testApp, opts []StepOption, batches, wantAtGate []int) storage.Execution {
	t.Helper()
	g := newGate()
	next := 0
	job := app.Job("probe").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			for _, n := range batches {
				if err := out.Add(urls(fmt.Sprintf("b%d-", next), n)...); err != nil {
					return err
				}
				next++
				g.hit(ctx)
			}
			return nil
		}), opts...).
		Step("crawl", noop, After("discover"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	total := 0
	for i, want := range wantAtGate {
		g.waitReached(t)
		if got := len(app.itemsOf(t, exec.ID, "crawl")); got != want {
			t.Fatalf("after batch %d (%d items): %d successor items are durable, want %d",
				i, batches[i], got, want)
		}
		total += batches[i]
		g.open(t)
	}
	app.waitForExecution(t, exec.ID, 1+total)
	return exec
}

// R2. Add flushes once the buffer holds FlushSize items.
func TestStreamAddFlushesAtFlushSize(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	exec := flushProbe(t, app,
		[]StepOption{StepFlush(3, time.Hour)},
		[]int{1, 1, 1, 7, 1},
		[]int{0, 0, 3, 10, 10})

	// R4 at the end: the one item still buffered is committed with the ack.
	if got := len(app.itemsOf(t, exec.ID, "crawl")); got != 11 {
		t.Fatalf("after the handler returned: %d successor items, want 11", got)
	}
}

// R2. The default FlushSize is 500.
func TestStreamDefaultFlushSizeIs500(t *testing.T) {
	t.Parallel()
	app := newTestApp(t, func(c *Config) { c.FlushInterval = time.Hour })
	flushProbe(t, app, nil, []int{499, 1}, []int{0, 500})
}

// R2. Config.FlushSize applies to steps without their own setting.
func TestStreamConfigFlushSizeApplies(t *testing.T) {
	t.Parallel()
	app := newTestApp(t, func(c *Config) {
		c.FlushSize = 2
		c.FlushInterval = time.Hour
	})
	flushProbe(t, app, nil, []int{1, 1}, []int{0, 2})
}

// R2. StepFlush overrides the Config for one step.
func TestStreamStepFlushOverridesConfig(t *testing.T) {
	t.Parallel()
	app := newTestApp(t, func(c *Config) {
		c.FlushSize = 2
		c.FlushInterval = time.Hour
	})
	flushProbe(t, app, []StepOption{StepFlush(4, time.Hour)}, []int{3, 1}, []int{0, 4})
}

// R3. Add flushes when FlushInterval has passed since the handler started or
// since the last flush. No background goroutine does it: with no further call
// to Add, nothing is committed however long the handler waits.
func TestStreamAddFlushesAfterFlushInterval(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	const interval = 200 * time.Millisecond
	g := newGate()
	job := app.Job("interval").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			// First flush: measured from the start of the handler. The test
			// clock and the wall clock both move past the interval.
			time.Sleep(2 * interval)
			app.clock.Advance(time.Second)
			if err := out.Add(crawlIn{URL: "first"}); err != nil {
				return err
			}
			g.hit(ctx)

			// Buffered right after a flush, then left alone. Nothing may
			// flush it in the background.
			if err := out.Add(crawlIn{URL: "second"}); err != nil {
				return err
			}
			time.Sleep(2 * interval)
			app.clock.Advance(time.Second)
			g.hit(ctx)

			// The next Add sees the interval has passed and flushes both.
			if err := out.Add(crawlIn{URL: "third"}); err != nil {
				return err
			}
			g.hit(ctx)
			return nil
		}, StepFlush(1000, interval))).
		Step("crawl", noop, After("discover"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	g.waitReached(t)
	if got := len(app.itemsOf(t, exec.ID, "crawl")); got != 1 {
		t.Fatalf("interval passed since the handler started: %d items durable, want 1", got)
	}
	g.open(t)

	g.waitReached(t)
	if got := len(app.itemsOf(t, exec.ID, "crawl")); got != 1 {
		t.Fatalf("no Add ran after the interval: %d items durable, want still 1 (no background flush)", got)
	}
	g.open(t)

	g.waitReached(t)
	if got := len(app.itemsOf(t, exec.ID, "crawl")); got != 3 {
		t.Fatalf("Add after the interval: %d items durable, want 3", got)
	}
	g.open(t)
	app.waitForExecution(t, exec.ID, 1+3)
}

// R4. Items still buffered when the handler returns nil are committed with the
// ack.
func TestStreamEndOfHandlerCommitsTheRemainder(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	exec := flushProbe(t, app, nil, []int{5}, []int{0})

	if got := payloadURLs(t, app.itemsOf(t, exec.ID, "crawl")); len(got) != 5 {
		t.Fatalf("successor items: %v, want 5", got)
	}
	parent := app.parentOf(t, exec.ID)
	if parent.State != storage.StateDone || parent.Outcome != storage.OutcomeSuccess || parent.Produced != 5 {
		t.Fatalf("parent: state %s outcome %s produced %d, want done/success/5",
			parent.State, parent.Outcome, parent.Produced)
	}
}

// R5. An error from the handler drops what is not flushed. Flushed items stay.
// The item dead-letters when its attempts are used up.
func TestStreamErrorDiscardsTheBufferAndDeadLetters(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("stream-error").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			if err := out.Add(crawlIn{URL: "flushed-a"}, crawlIn{URL: "flushed-b"}); err != nil {
				return err
			}
			if err := out.Flush(); err != nil {
				return err
			}
			if err := out.Add(crawlIn{URL: "lost-a"}, crawlIn{URL: "lost-b"}); err != nil {
				return err
			}
			return errors.New("boom")
		}, StepFlush(1000, time.Hour),
			StepPolicy(Policy{MaxAttempts: 1, Schedule: []time.Duration{0}, Jitter: 0}))).
		Step("crawl", noop, After("discover"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1+2)

	requireURLs(t, payloadURLs(t, app.itemsOf(t, exec.ID, "crawl")),
		sorted("flushed-a", "flushed-b"), "successor items")
	parent := app.parentOf(t, exec.ID)
	if parent.State != storage.StateDLQ {
		t.Fatalf("parent state: got %s, want dlq", parent.State)
	}
	if !strings.Contains(parent.LastError, "boom") {
		t.Fatalf("parent last error: %q, want it to carry the handler error", parent.LastError)
	}
}

// R5. A retried streaming step runs again. What the first attempt flushed
// stays, what it left in the buffer is gone, and the retry sends its own items.
func TestStreamErrorRetriesAndDropsOnlyTheBuffer(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	var attempts atomic.Int64
	job := app.Job("stream-retry").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			if attempts.Add(1) == 1 {
				if err := out.Add(crawlIn{URL: "first-flushed"}); err != nil {
					return err
				}
				if err := out.Flush(); err != nil {
					return err
				}
				if err := out.Add(crawlIn{URL: "first-lost"}); err != nil {
					return err
				}
				return errors.New("transient")
			}
			return out.Add(crawlIn{URL: "second"})
		}, StepFlush(1000, time.Hour),
			StepPolicy(Policy{MaxAttempts: 3, Schedule: []time.Duration{0}, Jitter: 0}))).
		Step("crawl", noop, After("discover"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1+2)

	if n := attempts.Load(); n != 2 {
		t.Fatalf("handler ran %d times, want 2", n)
	}
	requireURLs(t, payloadURLs(t, app.itemsOf(t, exec.ID, "crawl")),
		sorted("first-flushed", "second"), "successor items")
	parent := app.parentOf(t, exec.ID)
	if parent.State != storage.StateDone || parent.Attempt != 2 {
		t.Fatalf("parent: state %s attempt %d, want done on attempt 2", parent.State, parent.Attempt)
	}
}

// overranHandler drives R6 for a handler that outlives HandlerTimeout. The pool
// gives its item back for retry; the abandoned goroutine then tries to commit.
func overranHandler(t *testing.T, flushExplicitly bool) {
	t.Helper()
	app := newTestApp(t, func(c *Config) { c.HandlerTimeout = 30 * time.Millisecond })

	wedge := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(wedge) }) }
	t.Cleanup(release) // runs before the app stops, so shutdown never waits on the wedge

	result := make(chan error, 1)
	var attempts atomic.Int64
	job := app.Job("overran").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			if attempts.Add(1) > 1 {
				return nil
			}
			if err := out.Add(crawlIn{URL: "before-timeout"}); err != nil {
				return err
			}
			if err := out.Flush(); err != nil {
				return err
			}
			<-wedge // ignores cancellation on purpose
			var err error
			if flushExplicitly {
				if err = out.Add(crawlIn{URL: "after-timeout"}); err == nil {
					err = out.Flush()
				}
			} else {
				err = out.Add(crawlIn{URL: "after-1"}, crawlIn{URL: "after-2"}) // reaches FlushSize
			}
			result <- err
			return err
		}, StepFlush(2, time.Hour),
			// The retry waits an hour on the frozen test clock, so no second
			// attempt claims the item and hides the fencing.
			StepPolicy(Policy{MaxAttempts: 5, Schedule: []time.Duration{time.Hour}, Jitter: 0}))).
		Step("crawl", noop, After("discover"))

	pools := app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	pools["discover"].TestSignals.Stuck.WaitOrTimeout(t)
	pools["discover"].TestSignals.Retried.WaitOrTimeout(t)

	release()
	var flushErr error
	select {
	case flushErr = <-result:
	case <-time.After(10 * time.Second):
		t.Fatalf("the abandoned handler never finished its commit")
	}
	if !errors.Is(flushErr, storage.ErrLeaseLost) {
		t.Fatalf("commit by a handler that overran its timeout: got %v, want an error wrapping storage.ErrLeaseLost", flushErr)
	}
	requireURLs(t, payloadURLs(t, app.itemsOf(t, exec.ID, "crawl")),
		[]string{"before-timeout"}, "successor items (the late commit must insert nothing)")
}

// R6. A handler that overran HandlerTimeout and keeps running cannot commit
// through an Add that reaches FlushSize.
func TestStreamOverranHandlerIsFencedOnAdd(t *testing.T) {
	t.Parallel()
	overranHandler(t, false)
}

// R6. The same, through an explicit Flush.
func TestStreamOverranHandlerIsFencedOnFlush(t *testing.T) {
	t.Parallel()
	overranHandler(t, true)
}

// R7. Every flushed item carries the parent's lineage and a fresh item id.
func TestStreamFlushedItemsCarryLineage(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("lineage").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			if err := out.Add(urls("flushed", 2)...); err != nil {
				return err
			}
			if err := out.Flush(); err != nil {
				return err
			}
			return out.Add(urls("final", 2)...)
		})).
		Step("crawl", noop, After("discover"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1+4)

	parent := app.parentOf(t, exec.ID)
	if parent.ItemID == "" {
		t.Fatalf("parent has no item id")
	}
	children := app.itemsOf(t, exec.ID, "crawl")
	if len(children) != 4 {
		t.Fatalf("successor items: %d, want 4", len(children))
	}
	seen := map[string]bool{}
	for _, c := range children {
		if c.ExecutionID != exec.ID {
			t.Fatalf("%s: execution id %q, want %q", payloadURL(t, c), c.ExecutionID, exec.ID)
		}
		if c.StepID != "crawl" {
			t.Fatalf("%s: step id %q, want crawl", payloadURL(t, c), c.StepID)
		}
		if c.ParentItemID != parent.ItemID {
			t.Fatalf("%s: parent item id %q, want %q", payloadURL(t, c), c.ParentItemID, parent.ItemID)
		}
		if c.ItemID == "" || c.ItemID == parent.ItemID || seen[c.ItemID] {
			t.Fatalf("%s: item id %q is empty, the parent's, or reused", payloadURL(t, c), c.ItemID)
		}
		seen[c.ItemID] = true
	}
}

// R8. A streaming step with two successors delivers every item to both. One
// payload sent to two successors shares one item id.
func TestStreamFanOutDeliversToEverySuccessor(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	docs, metas := &recorder{}, &recorder{}
	job := app.Job("stream-fan-out").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			if err := out.Add(urls("flushed", 2)...); err != nil {
				return err
			}
			if err := out.Flush(); err != nil {
				return err
			}
			return out.Add(urls("final", 2)...)
		})).
		Step("document", docs.handle, After("discover")).
		Step("metadata", metas.handle, After("discover"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1+4*2)

	want := sorted("flushed0", "flushed1", "final0", "final1")
	requireURLs(t, docs.snapshot(), want, "document handler")
	requireURLs(t, metas.snapshot(), want, "metadata handler")

	idByURL := func(step string) map[string]string {
		m := map[string]string{}
		for _, it := range app.itemsOf(t, exec.ID, step) {
			m[payloadURL(t, it)] = it.ItemID
		}
		return m
	}
	docIDs, metaIDs := idByURL("document"), idByURL("metadata")
	distinct := map[string]bool{}
	for _, url := range want {
		if docIDs[url] == "" || docIDs[url] != metaIDs[url] {
			t.Fatalf("%s: item id %q on document, %q on metadata, want one shared id", url, docIDs[url], metaIDs[url])
		}
		distinct[docIDs[url]] = true
	}
	if len(distinct) != len(want) {
		t.Fatalf("payloads share item ids: %d distinct ids for %d payloads", len(distinct), len(want))
	}
}

// R9. produced counts every NewItem created across all flushes and the final
// commit, and a projection taken while the step runs, or after it ended,
// reports no leak.
func TestStreamProducedCountsEveryFlushAndTheFinalCommit(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	g := newGate()
	job := app.Job("stream-counts").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			if err := out.Add(urls("flushed", 3)...); err != nil {
				return err
			}
			if err := out.Flush(); err != nil {
				return err
			}
			g.hit(ctx)
			return out.Add(urls("final", 2)...)
		}, StepFlush(1000, time.Hour))).
		Step("document", noop, After("discover")).
		Step("metadata", noop, After("discover"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	g.waitReached(t)
	mid, err := app.Projection(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("Projection: %v", err)
	}
	if mid.HasLeaks() {
		t.Fatalf("a streaming step mid-run reported leaks: %v", mid.Leaks)
	}
	if got := app.parentOf(t, exec.ID).Produced; got != 3*2 {
		t.Fatalf("produced while running after one flush: got %d, want 6 (3 payloads * 2 successors)", got)
	}
	g.open(t)

	// 5 payloads * 2 successors.
	counts := countsByStep(app.waitForExecution(t, exec.ID, 1+5*2))
	if got := counts["discover"]; got.Produced != 10 || got.Succeeded != 1 {
		t.Fatalf("discover: %+v, want 1 succeeded producing 10", got)
	}
	parent := app.parentOf(t, exec.ID)
	if parent.Produced != 10 || parent.Outcome != storage.OutcomeSuccess {
		t.Fatalf("parent: produced %d outcome %s, want 10 / success", parent.Produced, parent.Outcome)
	}
	for _, step := range []string{"document", "metadata"} {
		if got := len(app.itemsOf(t, exec.ID, step)); got != 5 {
			t.Fatalf("%s items: %d, want 5", step, got)
		}
	}
	p, err := app.Projection(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("Projection: %v", err)
	}
	if p.HasLeaks() {
		t.Fatalf("a finished streaming run reported leaks: %v", p.Leaks)
	}
	if p.Status != StatusComplete {
		t.Fatalf("status: got %s, want COMPLETE", p.Status)
	}
}

// R9. A streaming step that produced only through flushes is a success, not a
// filter, and its count is the flushed total.
func TestStreamOnlyFlushedItemsIsSuccess(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("stream-flush-only").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			if err := out.Add(urls("x", 2)...); err != nil {
				return err
			}
			return out.Flush() // nothing is left in the buffer at the end
		})).
		Step("crawl", noop, After("discover"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1+2)

	parent := app.parentOf(t, exec.ID)
	if parent.Outcome != storage.OutcomeSuccess || parent.Produced != 2 {
		t.Fatalf("parent: outcome %s produced %d, want success / 2", parent.Outcome, parent.Produced)
	}
	p, err := app.Projection(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("Projection: %v", err)
	}
	if p.HasLeaks() || p.Status != StatusComplete {
		t.Fatalf("projection: status %s leaks %v", p.Status, p.Leaks)
	}
}

// R9. A streaming step that produced nothing at all is filtered.
func TestStreamProducingNothingIsFiltered(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("stream-filtered").
		Step("discover", Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error {
			return nil
		})).
		Step("crawl", noop, After("discover"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1)

	parent := app.parentOf(t, exec.ID)
	if parent.Outcome != storage.OutcomeFiltered || parent.Produced != 0 {
		t.Fatalf("parent: outcome %s produced %d, want filtered / 0", parent.Outcome, parent.Produced)
	}
	p, err := app.Projection(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("Projection: %v", err)
	}
	if p.HasLeaks() || p.Status != StatusComplete {
		t.Fatalf("projection: status %s leaks %v", p.Status, p.Leaks)
	}
}

// R10. A streaming step needs somewhere to send its output.
func TestStreamWithoutASuccessorIsABuildError(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)
	handler := Stream(func(ctx context.Context, in discoverIn, out Out[crawlIn]) error { return nil })

	only := app.Job("stream-alone").Step("only", handler)
	if err := only.wire(); err == nil {
		t.Fatalf("a streaming step with no successor was accepted")
	} else if !strings.Contains(err.Error(), "nowhere to send it") {
		t.Fatalf("unhelpful error for a dangling streaming step: %v", err)
	}

	last := app.Job("stream-last").
		Step("first", func(ctx context.Context, in discoverIn) (discoverIn, error) { return in, nil }).
		Step("last", handler)
	if err := last.wire(); err == nil {
		t.Fatalf("a final streaming step was accepted")
	} else if !strings.Contains(err.Error(), "nowhere to send it") {
		t.Fatalf("unhelpful error for a final streaming step: %v", err)
	}

	// Having a successor is enough.
	ok := app.Job("stream-ok").Step("first", handler).Step("second", noop)
	if err := ok.wire(); err != nil {
		t.Fatalf("a streaming step with a successor was rejected: %v", err)
	}
}

// R11. Streaming steps sit next to the existing handler forms, and those forms
// behave as before, including their produced counts after a retry.
func TestStreamLeavesTheOldHandlerFormsAlone(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	var sliceAttempts atomic.Int64
	job := app.Job("mixed-forms").
		Step("discover", func(ctx context.Context, in discoverIn) ([]crawlIn, error) {
			if sliceAttempts.Add(1) == 1 {
				return nil, errors.New("transient")
			}
			return urls("page", 4), nil
		}, StepPolicy(Policy{MaxAttempts: 3, Schedule: []time.Duration{0}, Jitter: 0})).
		Step("expand", Stream(func(ctx context.Context, in crawlIn, out Out[processIn]) error {
			return out.Add(processIn{URL: in.URL, Body: "a"}, processIn{URL: in.URL, Body: "b"})
		})).
		Step("single", func(ctx context.Context, in processIn) (indexIn, error) {
			return indexIn{URL: in.URL, Text: in.Body}, nil
		}).
		Step("sink", func(ctx context.Context, in indexIn) error { return nil })

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	counts := countsByStep(app.waitForExecution(t, exec.ID, 1+4+8+8))

	if got := counts["discover"]; got.Produced != 4 || got.Succeeded != 1 {
		t.Fatalf("slice-form step after a retry: %+v, want 1 succeeded producing 4", got)
	}
	if got := counts["expand"]; got.Received != 4 || got.Succeeded != 4 || got.Produced != 8 {
		t.Fatalf("streaming step: %+v, want 4 received and succeeded producing 8", got)
	}
	if got := counts["single"]; got.Received != 8 || got.Produced != 8 {
		t.Fatalf("single-form step: %+v, want 8 received producing 8", got)
	}
	if got := counts["sink"]; got.Received != 8 || got.Succeeded != 8 {
		t.Fatalf("error-form step: %+v, want 8 received and succeeded", got)
	}
	p, err := app.Projection(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("Projection: %v", err)
	}
	if p.HasLeaks() || p.Status != StatusComplete {
		t.Fatalf("projection: status %s leaks %v", p.Status, p.Leaks)
	}
}
