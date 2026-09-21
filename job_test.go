package durableq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unilinq/durableq/internal/leasing"
	"github.com/unilinq/durableq/internal/runtime"
	"github.com/unilinq/durableq/storage"
)

// Payloads for the example pipeline from the design document:
// discover -> crawl -> process -> index.
type (
	discoverIn struct {
		Site string `json:"site"`
	}
	crawlIn struct {
		URL string `json:"url"`
	}
	processIn struct {
		URL  string `json:"url"`
		Body string `json:"body"`
	}
	indexIn struct {
		URL  string `json:"url"`
		Text string `json:"text"`
	}
)

// startJob wires a job, arms every step's pool signals, and starts the app.
func (a *testApp) startJob(t *testing.T, j *Job) map[string]*runtime.Pool {
	t.Helper()
	if err := j.wire(); err != nil {
		t.Fatalf("wire: %v", err)
	}
	pools := map[string]*runtime.Pool{}
	var mu sync.Mutex
	ready := make(chan string, len(j.Steps()))

	for _, id := range j.Steps() {
		id := id
		q := a.Queue(StepQueue(j.Name(), id))
		q.mu.Lock()
		q.poolReady = func(p *runtime.Pool, h *leasing.Heartbeater) {
			p.TestSignals.Init()
			h.TestSignals.Init()
			mu.Lock()
			pools[id] = p
			mu.Unlock()
			ready <- id
		}
		q.mu.Unlock()
	}

	if err := a.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.Stop(ctx)
	})
	for range j.Steps() {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatalf("not every step's pool started")
		}
	}
	return pools
}

// waitForExecution polls durable state until the pipeline settles, which is
// what an operator would do. It never sleeps on a fixed guess.
func (a *testApp) waitForExecution(t *testing.T, execID string, wantTerminal int) []storage.StepCount {
	t.Helper()
	deadline := time.After(20 * time.Second)
	es, err := a.executionStore()
	if err != nil {
		t.Fatalf("execution store: %v", err)
	}
	for {
		counts, err := es.StepCounts(context.Background(), execID)
		if err != nil {
			t.Fatalf("StepCounts: %v", err)
		}
		terminal, active := 0, 0
		for _, c := range counts {
			terminal += c.Succeeded + c.Filtered + c.DLQ
			active += c.Active
		}
		if active == 0 && terminal >= wantTerminal {
			return counts
		}
		select {
		case <-deadline:
			t.Fatalf("pipeline did not settle: terminal %d (want %d), active %d\ncounts: %+v",
				terminal, wantTerminal, active, counts)
		default:
		}
	}
}

func countsByStep(counts []storage.StepCount) map[string]storage.StepCount {
	out := map[string]storage.StepCount{}
	for _, c := range counts {
		out[c.StepID] = c
	}
	return out
}

// TestJobRunsAFourStepPipeline is the design document's example end to end.
func TestJobRunsAFourStepPipeline(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	const pages = 12
	job := app.Job("document-ingestion").
		Step("discover", func(ctx context.Context, in discoverIn) ([]crawlIn, error) {
			out := make([]crawlIn, 0, pages)
			for i := 0; i < pages; i++ {
				out = append(out, crawlIn{URL: fmt.Sprintf("%s/page/%d", in.Site, i)})
			}
			return out, nil
		}).
		Step("crawl", func(ctx context.Context, in crawlIn) (processIn, error) {
			return processIn{URL: in.URL, Body: "<html>" + in.URL + "</html>"}, nil
		}, StepConcurrency(4)).
		Step("process", func(ctx context.Context, in processIn) (indexIn, error) {
			return indexIn{URL: in.URL, Text: strings.TrimSuffix(strings.TrimPrefix(in.Body, "<html>"), "</html>")}, nil
		}, StepConcurrency(4)).
		Step("index", func(ctx context.Context, in indexIn) error {
			return nil
		}, StepConcurrency(4))

	app.startJob(t, job)

	exec, err := job.Run(t.Context(), discoverIn{Site: "https://example.test"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exec.ID == "" || exec.Job != "document-ingestion" {
		t.Fatalf("execution: %+v", exec)
	}

	counts := countsByStep(app.waitForExecution(t, exec.ID, 1+pages*3))

	for _, step := range []string{"discover", "crawl", "process", "index"} {
		if _, ok := counts[step]; !ok {
			t.Fatalf("no counts for step %q", step)
		}
	}
	if got := counts["discover"]; got.Received != 1 || got.Succeeded != 1 || got.Produced != pages {
		t.Fatalf("discover: %+v, want 1 received, 1 succeeded, %d produced", got, pages)
	}
	for _, step := range []string{"crawl", "process", "index"} {
		c := counts[step]
		if c.Received != pages || c.Succeeded != pages || c.DLQ != 0 || c.Active != 0 {
			t.Fatalf("%s: %+v, want %d received and succeeded", step, c, pages)
		}
	}
	// Edge continuity: what one step produced is what the next received.
	if counts["crawl"].Produced != counts["process"].Received {
		t.Fatalf("crawl produced %d but process received %d",
			counts["crawl"].Produced, counts["process"].Received)
	}
	if counts["process"].Produced != counts["index"].Received {
		t.Fatalf("process produced %d but index received %d",
			counts["process"].Produced, counts["index"].Received)
	}
}

// TestJobCreatesOneQueuePerStep records the deviation from the design doc's
// "three internal queues": the first step gets a queue too, so the trigger is
// durable.
func TestJobCreatesOneQueuePerStep(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("ingest").
		Step("a", func(ctx context.Context, in struct{}) (struct{}, error) { return struct{}{}, nil }).
		Step("b", func(ctx context.Context, in struct{}) (struct{}, error) { return struct{}{}, nil }).
		Step("c", func(ctx context.Context, in struct{}) error { return nil })
	app.startJob(t, job)

	exec, err := job.Run(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	steps, err := app.ExecutionSteps(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("ExecutionSteps: %v", err)
	}
	if len(steps) != 3 {
		t.Fatalf("recorded %d steps, want 3", len(steps))
	}
	want := []storage.StepDef{
		{StepID: "a", Idx: 0, Queue: "ingest.a", NextQueue: "ingest.b"},
		{StepID: "b", Idx: 1, Queue: "ingest.b", NextQueue: "ingest.c"},
		{StepID: "c", Idx: 2, Queue: "ingest.c", NextQueue: ""},
	}
	for i, w := range want {
		if steps[i] != w {
			t.Fatalf("step %d: got %+v, want %+v", i, steps[i], w)
		}
	}
}

// TestLineageFollowsAnItemAcrossSteps is the design document's section 10
// query: where is item ABC?
func TestLineageFollowsAnItemAcrossSteps(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("lineage").
		Step("discover", func(ctx context.Context, in discoverIn) ([]crawlIn, error) {
			return []crawlIn{{URL: in.Site + "/ok"}, {URL: in.Site + "/doomed"}}, nil
		}).
		Step("crawl", func(ctx context.Context, in crawlIn) (processIn, error) {
			return processIn{URL: in.URL}, nil
		}, StepConcurrency(2)).
		Step("process", func(ctx context.Context, in processIn) (indexIn, error) {
			if strings.HasSuffix(in.URL, "/doomed") {
				return indexIn{}, errors.New("unsupported_pdf_encoding")
			}
			return indexIn{URL: in.URL}, nil
		}, StepConcurrency(2), StepPolicy(Policy{MaxAttempts: 2, Schedule: []time.Duration{0}, Jitter: 0})).
		Step("index", func(ctx context.Context, in indexIn) error { return nil }, StepConcurrency(2))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "https://example.test"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 1 discover + 2 crawl + 2 process + 1 index that gets there.
	app.waitForExecution(t, exec.ID, 6)

	store, err := app.executionStore()
	if err != nil {
		t.Fatalf("execution store: %v", err)
	}
	items, err := app.Store().(interface {
		ListItems(context.Context, storage.ItemFilter) ([]storage.Item, error)
	}).ListItems(t.Context(), storage.ItemFilter{ExecutionID: exec.ID, Limit: 100})
	if err != nil {
		t.Fatalf("ListItems: %v", err)
	}

	// Find the item that was dead-lettered at process, and trace it.
	var doomedItemID string
	for _, it := range items {
		if it.StepID == "process" && it.State == storage.StateDLQ {
			doomedItemID = it.ItemID
		}
	}
	if doomedItemID == "" {
		t.Fatalf("no item was dead-lettered at process")
	}

	trace, err := store.Lineage(t.Context(), exec.ID, doomedItemID)
	if err != nil {
		t.Fatalf("Lineage: %v", err)
	}
	if len(trace) != 4 {
		t.Fatalf("lineage has %d entries, want one per step", len(trace))
	}
	byStep := map[string]storage.LineageEntry{}
	for _, e := range trace {
		byStep[e.StepID] = e
	}
	if e := byStep["crawl"]; !e.Entered || e.State != storage.StateDone {
		t.Fatalf("crawl: %+v, want entered and done", e)
	}
	if e := byStep["process"]; !e.Entered || e.State != storage.StateDLQ || e.Attempt != 2 {
		t.Fatalf("process: %+v, want dead-lettered after 2 attempts", e)
	}
	if e := byStep["index"]; e.Entered {
		t.Fatalf("index: %+v, want never entered - a dead-lettered item produces nothing downstream", e)
	}
}

// TestDeadLetteredItemProducesNothingDownstream is the independence property:
// one item failing does not stop the others.
func TestDeadLetteredItemProducesNothingDownstream(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	const total = 10
	job := app.Job("independent").
		Step("fan", func(ctx context.Context, in struct{}) ([]crawlIn, error) {
			out := make([]crawlIn, 0, total)
			for i := 0; i < total; i++ {
				out = append(out, crawlIn{URL: fmt.Sprint(i)})
			}
			return out, nil
		}).
		Step("work", func(ctx context.Context, in crawlIn) (indexIn, error) {
			if in.URL == "3" || in.URL == "7" {
				return indexIn{}, errors.New("permanent")
			}
			return indexIn{URL: in.URL}, nil
		}, StepConcurrency(4), StepPolicy(Policy{MaxAttempts: 1, Schedule: []time.Duration{0}, Jitter: 0})).
		Step("sink", func(ctx context.Context, in indexIn) error { return nil }, StepConcurrency(4))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	counts := countsByStep(app.waitForExecution(t, exec.ID, 1+total+(total-2)))

	if got := counts["work"]; got.Received != total || got.DLQ != 2 || got.Succeeded != total-2 {
		t.Fatalf("work: %+v, want %d received, 2 dead-lettered, %d succeeded", got, total, total-2)
	}
	if got := counts["sink"]; got.Received != total-2 {
		t.Fatalf("sink received %d, want %d: the two failures must produce nothing downstream",
			got.Received, total-2)
	}
}

// TestFilteredStepIsTerminalSuccess proves returning nothing is a deliberate
// filter, not a failure and not lost work.
func TestFilteredStepIsTerminalSuccess(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("filtering").
		Step("fan", func(ctx context.Context, in struct{}) ([]crawlIn, error) {
			return []crawlIn{{URL: "keep"}, {URL: "drop"}, {URL: "keep"}}, nil
		}).
		Step("filter", func(ctx context.Context, in crawlIn) ([]indexIn, error) {
			if in.URL == "drop" {
				return nil, nil // filtered: nothing downstream, and that is fine
			}
			return []indexIn{{URL: in.URL}}, nil
		}, StepConcurrency(2)).
		Step("sink", func(ctx context.Context, in indexIn) error { return nil }, StepConcurrency(2))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	counts := countsByStep(app.waitForExecution(t, exec.ID, 1+3+2))

	f := counts["filter"]
	if f.Received != 3 || f.Succeeded != 2 || f.Filtered != 1 || f.DLQ != 0 {
		t.Fatalf("filter: %+v, want 3 received, 2 succeeded, 1 filtered, 0 dlq", f)
	}
	if got := counts["sink"].Received; got != 2 {
		t.Fatalf("sink received %d, want 2", got)
	}
	// The edge invariant still balances with a filter in it.
	if f.Received != f.Succeeded+f.Filtered+f.DLQ+f.Active {
		t.Fatalf("edge invariant broken at filter: %+v", f)
	}
}

// TestConcurrentExecutionsDoNotMix proves two runs of the same job keep their
// lineage and counts separate.
func TestConcurrentExecutionsDoNotMix(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	var seen sync.Map
	job := app.Job("parallel").
		Step("fan", func(ctx context.Context, in discoverIn) ([]crawlIn, error) {
			return []crawlIn{{URL: in.Site + "/1"}, {URL: in.Site + "/2"}, {URL: in.Site + "/3"}}, nil
		}).
		Step("sink", func(ctx context.Context, in crawlIn) error {
			seen.Store(in.URL, true)
			return nil
		}, StepConcurrency(4))

	app.startJob(t, job)

	a, err := job.Run(t.Context(), discoverIn{Site: "a"})
	if err != nil {
		t.Fatalf("Run a: %v", err)
	}
	b, err := job.Run(t.Context(), discoverIn{Site: "b"})
	if err != nil {
		t.Fatalf("Run b: %v", err)
	}
	if a.ID == b.ID {
		t.Fatalf("two runs share an execution id: %s", a.ID)
	}

	countsA := countsByStep(app.waitForExecution(t, a.ID, 4))
	countsB := countsByStep(app.waitForExecution(t, b.ID, 4))

	for name, counts := range map[string]map[string]storage.StepCount{"a": countsA, "b": countsB} {
		if got := counts["fan"].Received; got != 1 {
			t.Fatalf("execution %s fan received %d, want 1", name, got)
		}
		if got := counts["sink"].Received; got != 3 {
			t.Fatalf("execution %s sink received %d, want 3 - executions are leaking into each other",
				name, got)
		}
	}
}

// TestStepHandlerShapesAreChecked proves a wiring mistake is caught when the
// job is built, not on the first item.
func TestStepHandlerShapesAreChecked(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	// A step that produces output must have somewhere to send it.
	bad := app.Job("bad-last-step").
		Step("only", func(ctx context.Context, in struct{}) (struct{}, error) { return struct{}{}, nil })
	if err := bad.wire(); err == nil {
		t.Fatalf("a final step producing output was accepted")
	} else if !strings.Contains(err.Error(), "nowhere to send") {
		t.Fatalf("unhelpful error for a dangling final step: %v", err)
	}

	empty := app.Job("empty")
	if err := empty.wire(); err == nil {
		t.Fatalf("a job with no steps was accepted")
	}

	dup := app.Job("dup").
		Step("a", func(ctx context.Context, in struct{}) error { return nil }).
		Step("a", func(ctx context.Context, in struct{}) error { return nil })
	if err := dup.wire(); err == nil {
		t.Fatalf("duplicate step ids were accepted")
	}

	wrong := app.Job("wrong").
		Step("a", func(ctx context.Context, in struct{}) (struct{}, struct{}) { return struct{}{}, struct{}{} })
	if err := wrong.wire(); err == nil {
		t.Fatalf("a handler that does not return an error was accepted")
	}

	good := app.Job("good").
		Step("a", func(ctx context.Context, in struct{}) ([]struct{}, error) { return nil, nil }).
		Step("b", func(ctx context.Context, in struct{}) error { return nil })
	if err := good.wire(); err != nil {
		t.Fatalf("a valid job was rejected: %v", err)
	}
}

// TestRunRecordsTheExecutionBeforeEnqueuing proves the execution row exists for
// anything that later reads the work, and that the input is preserved.
func TestRunRecordsTheExecutionBeforeEnqueuing(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("recorded").
		Step("only", func(ctx context.Context, in discoverIn) error { return nil })
	if err := job.wire(); err != nil {
		t.Fatalf("wire: %v", err)
	}
	// Deliberately not started: Run must be durable on its own.
	exec, err := job.Run(t.Context(), discoverIn{Site: "https://example.test"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, err := app.Execution(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("Execution: %v", err)
	}
	if got.State != storage.ExecutionRunning || got.Job != "recorded" {
		t.Fatalf("execution: %+v", got)
	}
	if !strings.Contains(string(got.Input), "example.test") {
		t.Fatalf("execution input not preserved: %s", got.Input)
	}

	list, err := app.Executions(t.Context(), 10)
	if err != nil {
		t.Fatalf("Executions: %v", err)
	}
	if len(list) != 1 || list[0].ID != exec.ID {
		t.Fatalf("Executions returned %d rows", len(list))
	}

	st, err := app.Queue(StepQueue("recorded", "only")).Stats(t.Context())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.Ready != 1 {
		t.Fatalf("the trigger was not made durable: ready %d", st.Ready)
	}
}

// TestFanOutOfOneKeepsItemIdentity documents the lineage rule: one output is
// the same logical item moving on, several outputs are new items.
func TestFanOutOfOneKeepsItemIdentity(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	var fanned atomic.Int64
	job := app.Job("identity").
		Step("one", func(ctx context.Context, in struct{}) (crawlIn, error) {
			return crawlIn{URL: "single"}, nil
		}).
		Step("many", func(ctx context.Context, in crawlIn) ([]indexIn, error) {
			fanned.Add(1)
			return []indexIn{{URL: "a"}, {URL: "b"}}, nil
		}).
		Step("sink", func(ctx context.Context, in indexIn) error { return nil }, StepConcurrency(2))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1+1+2)

	lister := app.Store().(interface {
		ListItems(context.Context, storage.ItemFilter) ([]storage.Item, error)
	})
	items, err := lister.ListItems(t.Context(), storage.ItemFilter{ExecutionID: exec.ID, Limit: 100})
	if err != nil {
		t.Fatalf("ListItems: %v", err)
	}
	byStep := map[string][]storage.Item{}
	for _, it := range items {
		byStep[it.StepID] = append(byStep[it.StepID], it)
	}
	if len(byStep["one"]) != 1 || len(byStep["many"]) != 1 || len(byStep["sink"]) != 2 {
		t.Fatalf("item counts per step: one %d, many %d, sink %d",
			len(byStep["one"]), len(byStep["many"]), len(byStep["sink"]))
	}
	// One output: identity carried over, so a lineage query follows it.
	if byStep["one"][0].ItemID != byStep["many"][0].ItemID {
		t.Fatalf("a single output changed the item's identity: %s then %s",
			byStep["one"][0].ItemID, byStep["many"][0].ItemID)
	}
	// Several outputs: new identities pointing back at the parent.
	parent := byStep["many"][0].ItemID
	for _, child := range byStep["sink"] {
		if child.ItemID == parent {
			t.Fatalf("a fan-out child reused its parent's identity %s", parent)
		}
		if child.ParentItemID != parent {
			t.Fatalf("child %s points at parent %q, want %q", child.ItemID, child.ParentItemID, parent)
		}
	}
}
