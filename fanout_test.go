package durableq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/unilinq/durableq/storage"
)

// TestJobFanOutDeliversToAllBranches is the design example from the stage 10
// brief: one step with three successors, each getting its own copy of every
// payload. k payloads at a step with n successors produce k*n downstream
// items.
func TestJobFanOutDeliversToAllBranches(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	const pages = 4
	job := app.Job("sharepoint-ingest").
		Step("discover", func(ctx context.Context, in discoverIn) ([]crawlIn, error) {
			out := make([]crawlIn, 0, pages)
			for i := 0; i < pages; i++ {
				out = append(out, crawlIn{URL: fmt.Sprintf("%s/%d", in.Site, i)})
			}
			return out, nil
		}).
		Step("document", func(ctx context.Context, in crawlIn) error { return nil }, After("discover")).
		Step("metadata", func(ctx context.Context, in crawlIn) error { return nil }, After("discover")).
		Step("acl", func(ctx context.Context, in crawlIn) error { return nil }, After("discover"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), discoverIn{Site: "https://example.test"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 1 discover + pages on each of the three branches.
	counts := countsByStep(app.waitForExecution(t, exec.ID, 1+pages*3))

	if got := counts["discover"]; got.Succeeded != 1 || got.Produced != pages*3 {
		t.Fatalf("discover: %+v, want 1 succeeded producing %d (%d pages * 3 branches)",
			got, pages*3, pages)
	}
	for _, branch := range []string{"document", "metadata", "acl"} {
		c := counts[branch]
		if c.Received != pages || c.Succeeded != pages || c.DLQ != 0 || c.Active != 0 {
			t.Fatalf("%s: %+v, want %d received and succeeded, matching every other branch",
				branch, c, pages)
		}
	}

	steps, err := app.ExecutionSteps(t.Context(), exec.ID)
	if err != nil {
		t.Fatalf("ExecutionSteps: %v", err)
	}
	var discoverDef storage.StepDef
	for _, s := range steps {
		if s.StepID == "discover" {
			discoverDef = s
		}
	}
	wantNext := map[string]bool{"document": true, "metadata": true, "acl": true}
	if len(discoverDef.Next) != 3 {
		t.Fatalf("discover.Next: %v, want 3 successors", discoverDef.Next)
	}
	for _, n := range discoverDef.Next {
		if !wantNext[n] {
			t.Fatalf("discover.Next: unexpected successor %q", n)
		}
	}
}

// TestFanOutBranchesAreDeadLetterIndependent proves one branch dead-lettering
// does not stop, delay or otherwise affect its siblings.
func TestFanOutBranchesAreDeadLetterIndependent(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("independent-branches").
		Step("split", func(ctx context.Context, in struct{}) (crawlIn, error) {
			return crawlIn{URL: "shared"}, nil
		}).
		Step("good-a", func(ctx context.Context, in crawlIn) error { return nil }, After("split")).
		Step("good-b", func(ctx context.Context, in crawlIn) error { return nil }, After("split")).
		Step("doomed", func(ctx context.Context, in crawlIn) error { return errors.New("permanent") },
			After("split"), StepPolicy(Policy{MaxAttempts: 1, Schedule: []time.Duration{0}, Jitter: 0}))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 1 split + 1 on each of good-a, good-b, doomed (which dead-letters).
	counts := countsByStep(app.waitForExecution(t, exec.ID, 1+1+1+1))

	if got := counts["good-a"]; got.Received != 1 || got.Succeeded != 1 {
		t.Fatalf("good-a: %+v, want 1 received and succeeded", got)
	}
	if got := counts["good-b"]; got.Received != 1 || got.Succeeded != 1 {
		t.Fatalf("good-b: %+v, want 1 received and succeeded", got)
	}
	if got := counts["doomed"]; got.Received != 1 || got.DLQ != 1 || got.Succeeded != 0 {
		t.Fatalf("doomed: %+v, want 1 received, 1 dead-lettered", got)
	}
}

// TestFanOutBranchesArePauseIndependent proves pausing one branch's queue
// does not stop the others from being claimed and processed.
func TestFanOutBranchesArePauseIndependent(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("pause-branches").
		Step("split", func(ctx context.Context, in struct{}) (crawlIn, error) {
			return crawlIn{URL: "shared"}, nil
		}).
		Step("runs", func(ctx context.Context, in crawlIn) error { return nil }, After("split")).
		Step("paused", func(ctx context.Context, in crawlIn) error { return nil }, After("split"))

	app.startJob(t, job)

	if err := app.Queue(StepQueue("pause-branches", "paused")).Pause(t.Context()); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	exec, err := job.Run(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The unpaused branch settles even though its sibling is paused.
	deadline := time.After(10 * time.Second)
	for {
		counts, err := app.Projection(t.Context(), exec.ID)
		if err != nil {
			t.Fatalf("Projection: %v", err)
		}
		byStep := map[string]StepProjection{}
		for _, s := range counts.Steps {
			byStep[s.StepID] = s
		}
		if byStep["runs"].Succeeded == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("the unpaused branch never settled while its sibling was paused")
		default:
		}
	}

	// The paused branch's item is still sitting ready, untouched.
	stats, err := app.Queue(StepQueue("pause-branches", "paused")).Stats(t.Context())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Ready != 1 || stats.Done != 0 {
		t.Fatalf("paused branch stats: %+v, want 1 ready and 0 done", stats)
	}

	if err := app.Queue(StepQueue("pause-branches", "paused")).Resume(t.Context()); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1+1+1)
}

// TestFanOutBranchesHaveIndependentPolicy proves a per-step policy applies to
// that branch alone: siblings keep their own (or the job default).
func TestFanOutBranchesHaveIndependentPolicy(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("branch-policy").
		Step("split", func(ctx context.Context, in struct{}) (crawlIn, error) {
			return crawlIn{URL: "shared"}, nil
		}).
		Step("patient", func(ctx context.Context, in crawlIn) error { return errors.New("fails every time") },
			After("split"),
			StepPolicy(Policy{MaxAttempts: 3, Schedule: []time.Duration{0}, Jitter: 0})).
		Step("impatient", func(ctx context.Context, in crawlIn) error { return errors.New("fails every time") },
			After("split"),
			StepPolicy(Policy{MaxAttempts: 1, Schedule: []time.Duration{0}, Jitter: 0}))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	app.waitForExecution(t, exec.ID, 1+1+1)

	lister := app.Store().(interface {
		ListItems(context.Context, storage.ItemFilter) ([]storage.Item, error)
	})
	items, err := lister.ListItems(t.Context(), storage.ItemFilter{ExecutionID: exec.ID, Limit: 100})
	if err != nil {
		t.Fatalf("ListItems: %v", err)
	}
	attempts := map[string]int{}
	for _, it := range items {
		attempts[it.StepID] = it.Attempt
	}
	if attempts["patient"] != 3 {
		t.Fatalf("patient branch attempt count: got %d, want 3 (its own policy)", attempts["patient"])
	}
	if attempts["impatient"] != 1 {
		t.Fatalf("impatient branch attempt count: got %d, want 1 (its own policy)", attempts["impatient"])
	}
}

// TestLineageAcrossFanOut proves an item's trace shows the branch steps it
// entered - every branch, since output is broadcast - and the ones a later
// failure kept it from ever reaching.
func TestLineageAcrossFanOut(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("lineage-fanout").
		Step("discover", func(ctx context.Context, in struct{}) (crawlIn, error) {
			return crawlIn{URL: "one"}, nil
		}).
		Step("document", func(ctx context.Context, in crawlIn) (indexIn, error) {
			return indexIn{URL: in.URL}, nil
		}, After("discover")).
		Step("metadata", func(ctx context.Context, in crawlIn) (indexIn, error) {
			return indexIn{URL: in.URL}, nil
		}, After("discover")).
		Step("acl", func(ctx context.Context, in crawlIn) (indexIn, error) {
			return indexIn{}, errors.New("acl_lookup_failed")
		}, After("discover"), StepPolicy(Policy{MaxAttempts: 1, Schedule: []time.Duration{0}, Jitter: 0})).
		Step("doc-sink", func(ctx context.Context, in indexIn) error { return nil }, After("document")).
		Step("meta-sink", func(ctx context.Context, in indexIn) error { return nil }, After("metadata")).
		Step("acl-sink", func(ctx context.Context, in indexIn) error { return nil }, After("acl"))

	app.startJob(t, job)
	exec, err := job.Run(t.Context(), struct{}{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 1 discover + document + metadata + acl (dead-lettered) + doc-sink + meta-sink.
	app.waitForExecution(t, exec.ID, 1+1+1+1+1+1)

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
	var itemID string
	for _, it := range items {
		if it.StepID == "discover" {
			itemID = it.ItemID
		}
	}
	if itemID == "" {
		t.Fatalf("no discover item found")
	}

	trace, err := store.Lineage(t.Context(), exec.ID, itemID)
	if err != nil {
		t.Fatalf("Lineage: %v", err)
	}
	byStep := map[string]storage.LineageEntry{}
	for _, e := range trace {
		byStep[e.StepID] = e
	}
	for _, entered := range []string{"discover", "document", "metadata", "acl", "doc-sink", "meta-sink"} {
		if e := byStep[entered]; !e.Entered {
			t.Fatalf("%s: %+v, want entered - the broadcast reaches every branch", entered, e)
		}
	}
	if e := byStep["acl"]; e.State != storage.StateDLQ {
		t.Fatalf("acl: %+v, want dead-lettered", e)
	}
	if e := byStep["acl-sink"]; e.Entered {
		t.Fatalf("acl-sink: %+v, want never entered - acl dead-lettered and produced nothing downstream", e)
	}
}

// TestAfterUnknownPredecessorIsABuildError proves naming a predecessor that
// does not exist is caught at build time with a clear error.
func TestAfterUnknownPredecessorIsABuildError(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("bad-after-unknown").
		Step("a", func(ctx context.Context, in struct{}) error { return nil }).
		Step("b", func(ctx context.Context, in struct{}) error { return nil }, After("does-not-exist"))

	err := job.wire()
	if err == nil {
		t.Fatalf("an unknown After target was accepted")
	}
	if !strings.Contains(err.Error(), "unknown predecessor") {
		t.Fatalf("unhelpful error for an unknown After target: %v", err)
	}
}

// TestAfterJoinIsRejected proves naming more than one predecessor is rejected
// as a join, a different feature reserved for later - not silently accepted.
func TestAfterJoinIsRejected(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	job := app.Job("bad-after-join").
		Step("a", func(ctx context.Context, in struct{}) error { return nil }).
		Step("b", func(ctx context.Context, in struct{}) error { return nil }).
		Step("c", func(ctx context.Context, in struct{}) error { return nil }, After("a", "b"))

	err := job.wire()
	if err == nil {
		t.Fatalf("a join (After with more than one argument) was accepted")
	}
	if !strings.Contains(err.Error(), "durableq: join is not supported") {
		t.Fatalf("unhelpful error for a join: %v", err)
	}
}

// TestAfterCycleIsRejected proves a step graph with a cycle is caught at
// build time with a distinct, clear error.
func TestAfterCycleIsRejected(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	// x and y each name the other as predecessor: neither is reachable from
	// the job's first step by any acyclic path.
	job := app.Job("bad-after-cycle").
		Step("x", func(ctx context.Context, in struct{}) error { return nil }, After("y")).
		Step("y", func(ctx context.Context, in struct{}) error { return nil }, After("x"))

	err := job.wire()
	if err == nil {
		t.Fatalf("a cycle was accepted")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("unhelpful error for a cycle: %v", err)
	}
}

// TestAfterUnreachableStepIsRejected proves a step no path reaches from the
// job's first step is caught at build time, distinctly from a cycle.
func TestAfterUnreachableStepIsRejected(t *testing.T) {
	t.Parallel()
	app := newTestApp(t)

	// "b" explicitly opts out of the declaration-order default (After with no
	// arguments), so it has no predecessor and nothing names it as a
	// successor either: it is an orphan branch the first step never reaches.
	job := app.Job("bad-after-unreachable").
		Step("a", func(ctx context.Context, in struct{}) error { return nil }).
		Step("b", func(ctx context.Context, in struct{}) error { return nil }, After())

	err := job.wire()
	if err == nil {
		t.Fatalf("an unreachable step was accepted")
	}
	if !strings.Contains(err.Error(), "not reached from the first step") {
		t.Fatalf("unhelpful error for an unreachable step: %v", err)
	}
}
