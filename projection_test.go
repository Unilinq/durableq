package durableq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/unilinq/durableq/internal/execution"
	"github.com/unilinq/durableq/storage"
)

// TestProjectionFromDurableState proves the run table is derived from the
// database, not from counters the worker happened to keep.
func TestProjectionFromDurableState(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	const pages = 9
	job := app.Job("projected").
		Step("discover", func(ctx context.Context, in discoverIn) ([]crawlIn, error) {
			out := make([]crawlIn, 0, pages)
			for i := 0; i < pages; i++ {
				out = append(out, crawlIn{URL: fmt.Sprint(i)})
			}
			return out, nil
		}).
		Step("crawl", func(ctx context.Context, in crawlIn) ([]indexIn, error) {
			switch in.URL {
			case "1", "2":
				return nil, errors.New("permanent failure")
			case "3":
				return nil, nil // filtered
			}
			return []indexIn{{URL: in.URL}}, nil
		}, StepConcurrency(3), StepPolicy(Policy{MaxAttempts: 1, Schedule: []time.Duration{0}, Jitter: 0})).
		Step("index", func(ctx context.Context, in indexIn) error { return nil }, StepConcurrency(3))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "s"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1+pages+(pages-3))

	p, err := app.Projection(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("Projection: %v", err)
	}
	if p.Status != StatusComplete {
		t.Fatalf("status: got %s, want COMPLETE\n%+v", p.Status, p.Steps)
	}

	byStep := map[string]StepProjection{}
	for _, s := range p.Steps {
		byStep[s.StepID] = s
	}
	if got := byStep["crawl"]; got.Received != pages || got.DLQ != 2 || got.Filtered != 1 || got.Succeeded != pages-3 {
		t.Fatalf("crawl: %+v, want %d received, 2 dlq, 1 filtered, %d succeeded", got, pages, pages-3)
	}
	if got := byStep["index"]; got.Received != pages-3 || got.Succeeded != pages-3 {
		t.Fatalf("index: %+v, want %d received and succeeded", got, pages-3)
	}
	if p.TerminalSuccess != pages-3 {
		t.Fatalf("terminal success: got %d, want %d", p.TerminalSuccess, pages-3)
	}
	if p.TerminalDLQ != 2 {
		t.Fatalf("terminal DLQ: got %d, want 2", p.TerminalDLQ)
	}
	if p.Active != 0 {
		t.Fatalf("active: got %d, want 0", p.Active)
	}
	if p.HasLeaks() {
		t.Fatalf("a healthy run reported leaks: %v", p.Leaks)
	}

	// Every edge balances.
	for _, s := range p.Steps {
		if s.Received != s.Succeeded+s.Filtered+s.DLQ+s.Active {
			t.Fatalf("edge invariant broken at %s: %+v", s.StepID, s)
		}
	}
}

// TestProjectionIsCorrectMidFlight proves the answer is meaningful while the
// pipeline is still working, not only once it settles.
func TestProjectionIsCorrectMidFlight(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	const pages = 6
	hold := make(chan struct{})
	entered := make(chan struct{}, pages)

	job := app.Job("midflight").
		Step("discover", func(ctx context.Context, in struct{}) ([]crawlIn, error) {
			out := make([]crawlIn, 0, pages)
			for i := 0; i < pages; i++ {
				out = append(out, crawlIn{URL: fmt.Sprint(i)})
			}
			return out, nil
		}).
		Step("crawl", func(ctx context.Context, in crawlIn) (indexIn, error) {
			entered <- struct{}{}
			<-hold
			return indexIn{URL: in.URL}, nil
		}, StepConcurrency(2)).
		Step("index", func(ctx context.Context, in indexIn) error { return nil })

	pools := app.startJob(t, job)
	t.Cleanup(func() { close(hold) })

	exec, err := job.Run(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Wait until discover is done and two crawl handlers are in flight.
	pools["discover"].TestSignals.Acked.WaitOrTimeout(t)
	<-entered
	<-entered

	p, err := app.Projection(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("Projection: %v", err)
	}
	if p.Status != StatusRunning {
		t.Fatalf("status mid-flight: got %s, want RUNNING", p.Status)
	}
	byStep := map[string]StepProjection{}
	for _, s := range p.Steps {
		byStep[s.StepID] = s
	}
	if got := byStep["discover"]; got.Succeeded != 1 || got.Produced != pages {
		t.Fatalf("discover mid-flight: %+v, want 1 succeeded producing %d", got, pages)
	}
	if got := byStep["crawl"]; got.Received != pages || got.Active != pages {
		t.Fatalf("crawl mid-flight: %+v, want %d received and all still active", got, pages)
	}
	// Nothing has reached index yet, and that is not a leak: crawl is working.
	if got := byStep["index"]; got.Received != 0 {
		t.Fatalf("index mid-flight: %+v, want nothing received yet", got)
	}
	if p.HasLeaks() {
		t.Fatalf("a healthy in-flight run reported leaks: %v", p.Leaks)
	}
}

// TestProjectionReportsDeletedWorkAsALeak is the section 13 case: an item that
// silently disappeared must be reported, not absorbed.
func TestProjectionReportsDeletedWorkAsALeak(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	const pages = 8
	job := app.Job("leaky").
		Step("discover", func(ctx context.Context, in struct{}) ([]crawlIn, error) {
			out := make([]crawlIn, 0, pages)
			for i := 0; i < pages; i++ {
				out = append(out, crawlIn{URL: fmt.Sprint(i)})
			}
			return out, nil
		}).
		Step("crawl", func(ctx context.Context, in crawlIn) error { return nil }, StepConcurrency(3))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1+pages)

	before, err := app.Projection(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("Projection: %v", err)
	}
	if before.HasLeaks() {
		t.Fatalf("run leaked before anything was deleted: %v", before.Leaks)
	}

	// Something outside durableq removes two rows: a stray DELETE, a partial
	// restore, a retention job pointed at the wrong table.
	deleted := deleteItems(t, app, exec.ID, "crawl", 2)
	if deleted != 2 {
		t.Fatalf("deleted %d rows, want 2", deleted)
	}

	after, err := app.Projection(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("Projection: %v", err)
	}
	if !after.HasLeaks() {
		t.Fatalf("two items vanished and the projection reported nothing:\n%+v", after.Steps)
	}
	var leak *Leak
	for i := range after.Leaks {
		if after.Leaks[i].Kind == execution.LeakContinuity || after.Leaks[i].Kind == execution.LeakEdgeImbalance {
			leak = &after.Leaks[i]
		}
	}
	if leak == nil {
		t.Fatalf("expected a continuity or edge-imbalance leak, got %v", after.Leaks)
	}
	if leak.Missing != 2 {
		t.Fatalf("leak reports %d missing, want 2 (%s)", leak.Missing, leak.Detail)
	}
}

// TestProjectionSurfacesCappedDiscovery is the section 21 limitation made
// visible: a discovery step that emitted 200 of 5000 must say so.
func TestProjectionSurfacesCappedDiscovery(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	// The capped counters are set through the store contract, which is what a
	// bounded discovery step uses when it reports what it discarded.
	store := app.Store()
	ctx := t.Context()

	execID := NewExecutionID()
	es, err := app.executionStore()
	if err != nil {
		t.Fatalf("execution store: %v", err)
	}
	if err := es.CreateExecution(ctx, storage.Execution{ID: execID, Job: "capped"},
		[]storage.StepDef{
			{StepID: "discover", Idx: 0, Queue: "capped.discover", Next: []string{"crawl"}},
			{StepID: "crawl", Idx: 1, Queue: "capped.crawl"},
		}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}

	items, err := store.Enqueue(ctx, storage.NewItem{
		Queue: "capped.discover", ExecutionID: execID, StepID: "discover", ItemID: "root",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := store.Claim(ctx, storage.ClaimOpts{
		Queue: "capped.discover", Worker: "w", Limit: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim: %v (%d items)", err, len(claimed))
	}

	// Found 5000, configured to emit 200.
	var produced []storage.NewItem
	for i := 0; i < 200; i++ {
		produced = append(produced, storage.NewItem{
			Queue: "capped.crawl", ExecutionID: execID, StepID: "crawl",
			ItemID: fmt.Sprintf("item_%d", i), ParentItemID: "root",
		})
	}
	if _, err := store.Complete(ctx, storage.CompleteRequest{
		ID: items[0].ID, Worker: "w", Produced: produced, Dropped: 4800, Capped: true,
	}); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	p, err := app.Projection(ctx, execID)
	if err != nil {
		t.Fatalf("Projection: %v", err)
	}
	// The counts balance perfectly, which is exactly why this needs saying.
	for _, s := range p.Steps {
		if s.Received != s.Succeeded+s.Filtered+s.DLQ+s.Active {
			t.Fatalf("edge invariant broken at %s: %+v", s.StepID, s)
		}
	}
	var capped *Leak
	for i := range p.Leaks {
		if p.Leaks[i].Kind == execution.LeakCappedOutput {
			capped = &p.Leaks[i]
		}
	}
	if capped == nil {
		t.Fatalf("capped discovery was not surfaced: %v", p.Leaks)
	}
	if capped.Missing != 4800 {
		t.Fatalf("capped leak reports %d dropped, want 4800", capped.Missing)
	}
}

// TestProjectionOnAFanOut is criterion 6: per-leaf terminal counts, per-group
// continuity, the per-step invariant, and a deliberately deleted row reported
// as a leak naming the right edge.
func TestProjectionOnAFanOut(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	const per = 5
	job := app.Job("projected-fanout").
		Step("split", func(ctx context.Context, in struct{}) ([]crawlIn, error) {
			out := make([]crawlIn, 0, per)
			for i := 0; i < per; i++ {
				out = append(out, crawlIn{URL: fmt.Sprint(i)})
			}
			return out, nil
		}).
		Step("b1", func(ctx context.Context, in crawlIn) error { return nil }, After("split"), StepConcurrency(2)).
		Step("b2", func(ctx context.Context, in crawlIn) error { return nil }, After("split"), StepConcurrency(2)).
		Step("b3", func(ctx context.Context, in crawlIn) error { return nil }, After("split"), StepConcurrency(2))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1+per*3)

	before, err := app.Projection(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("Projection: %v", err)
	}
	if before.HasLeaks() {
		t.Fatalf("a healthy fan-out reported leaks: %v", before.Leaks)
	}
	// Terminal steps are the leaves: b1, b2 and b3, not "the last step by idx".
	if before.TerminalSuccess != per*3 {
		t.Fatalf("terminal success: got %d, want %d (every leaf's successes)", before.TerminalSuccess, per*3)
	}
	byStep := map[string]StepProjection{}
	for _, s := range before.Steps {
		byStep[s.StepID] = s
	}
	for _, b := range []string{"b1", "b2", "b3"} {
		if got := byStep[b]; got.Received != per || got.Succeeded != per {
			t.Fatalf("%s: %+v, want %d received and succeeded", b, got, per)
		}
	}
	for _, s := range before.Steps {
		if s.Received != s.Succeeded+s.Filtered+s.DLQ+s.Active {
			t.Fatalf("edge invariant broken at %s: %+v", s.StepID, s)
		}
	}

	// Something outside durableq removes one row from one branch only.
	deleted := deleteItems(t, app, exec.ID, "b2", 1)
	if deleted != 1 {
		t.Fatalf("deleted %d rows, want 1", deleted)
	}

	after, err := app.Projection(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("Projection: %v", err)
	}
	if !after.HasLeaks() {
		t.Fatalf("a deleted row on one branch was not reported: %+v", after.Steps)
	}
	var divergence *Leak
	for i := range after.Leaks {
		if after.Leaks[i].Kind == execution.LeakBranchDivergence {
			divergence = &after.Leaks[i]
		}
	}
	if divergence == nil {
		t.Fatalf("expected a branch-divergence leak naming the short branch, got %v", after.Leaks)
	}
	if divergence.StepID != "split" || divergence.Edge != "split->b2" {
		t.Fatalf("leak names step %q edge %q, want split->b2", divergence.StepID, divergence.Edge)
	}
	if divergence.Missing != 1 {
		t.Fatalf("leak reports %d missing, want 1 (%s)", divergence.Missing, divergence.Detail)
	}
	// A sibling that is merely faster than b2 must never itself be named.
	for _, l := range after.Leaks {
		if l.Kind == execution.LeakBranchDivergence && l.Edge != "split->b2" {
			t.Fatalf("an unaffected branch was reported as leaking: %+v", l)
		}
	}
}

// TestSlowBranchIsNotALeak is criterion 7: a branch that is only slower than
// its siblings, with work still in flight, must never be reported as a leak.
func TestSlowBranchIsNotALeak(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	hold := make(chan struct{})
	entered := make(chan struct{}, 1)

	job := app.Job("slow-branch").
		Step("split", func(ctx context.Context, in struct{}) (crawlIn, error) {
			return crawlIn{URL: "shared"}, nil
		}).
		Step("fast", func(ctx context.Context, in crawlIn) error { return nil }, After("split")).
		Step("slow", func(ctx context.Context, in crawlIn) error {
			entered <- struct{}{}
			<-hold
			return nil
		}, After("split"))

	pools := app.startJob(t, job)
	t.Cleanup(func() { close(hold) })

	exec, err := job.Run(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	pools["split"].TestSignals.Acked.WaitOrTimeout(t)
	pools["fast"].TestSignals.Acked.WaitOrTimeout(t)
	<-entered

	p, err := app.Projection(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("Projection: %v", err)
	}
	if p.Status != StatusRunning {
		t.Fatalf("status: got %s, want RUNNING while the slow branch is in flight", p.Status)
	}
	byStep := map[string]StepProjection{}
	for _, s := range p.Steps {
		byStep[s.StepID] = s
	}
	if got := byStep["fast"]; got.Succeeded != 1 {
		t.Fatalf("fast: %+v, want 1 succeeded", got)
	}
	if got := byStep["slow"]; got.Received != 1 || got.Active != 1 || got.Succeeded != 0 {
		t.Fatalf("slow: %+v, want 1 received, still active, not yet succeeded", got)
	}
	if p.HasLeaks() {
		t.Fatalf("a branch that is merely slower than its sibling was reported as a leak: %v", p.Leaks)
	}
}

// deleteItems removes rows from under a running execution, simulating work
// disappearing for reasons outside durableq's control.
func deleteItems(t *testing.T, app *testApp, execID, stepID string, n int) int {
	t.Helper()
	items, err := app.store.ListItems(t.Context(), storage.ItemFilter{
		ExecutionID: execID, Limit: 1000,
	})
	if err != nil {
		t.Fatalf("ListItems: %v", err)
	}
	deleted := 0
	for _, it := range items {
		if it.StepID != stepID || deleted >= n {
			continue
		}
		if err := app.store.DeleteItemForTest(t.Context(), it.ID); err != nil {
			t.Fatalf("delete: %v", err)
		}
		deleted++
	}
	return deleted
}
