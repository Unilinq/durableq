// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package execution_test

import (
	"testing"

	"github.com/unilinq/durableq/internal/execution"
	"github.com/unilinq/durableq/storage"
)

func counts(cs ...storage.StepCount) []storage.StepCount {
	for i := range cs {
		cs[i].Idx = i
	}
	return cs
}

// linearEdges builds the successor map for a straight chain, in the order the
// counts were given - the shape every test before fan-out assumed implicitly
// via adjacent index. It exists so those tests can keep asserting the exact
// same thing through the now-explicit edges parameter: |S| == 1 everywhere,
// which is exactly the case Project must reduce to the old behaviour for.
func linearEdges(cs []storage.StepCount) map[string][]string {
	edges := make(map[string][]string, len(cs))
	for i := 0; i+1 < len(cs); i++ {
		edges[cs[i].StepID] = []string{cs[i+1].StepID}
	}
	return edges
}

// TestProjectAllSuccess is the clean case: everything flowed through.
func TestProjectAllSuccess(t *testing.T) {
	t.Parallel()
	cs1 := counts(
		storage.StepCount{StepID: "discover", Received: 1, Succeeded: 1, Produced: 100},
		storage.StepCount{StepID: "crawl", Received: 100, Succeeded: 100, Produced: 100},
		storage.StepCount{StepID: "index", Received: 100, Succeeded: 100},
	)
	p := execution.Project(storage.Execution{ID: "exec_1"}, cs1, linearEdges(cs1))

	if p.Status != execution.StatusComplete {
		t.Fatalf("status: got %s, want COMPLETE", p.Status)
	}
	if p.TerminalSuccess != 100 {
		t.Fatalf("terminal success: got %d, want 100 (only the last step's successes leave the pipeline)",
			p.TerminalSuccess)
	}
	if p.TerminalDLQ != 0 || p.Active != 0 {
		t.Fatalf("dlq %d active %d, want 0 and 0", p.TerminalDLQ, p.Active)
	}
	if p.HasLeaks() {
		t.Fatalf("clean run reported leaks: %v", p.Leaks)
	}
}

// TestProjectMatchesTheDesignDocumentExample reproduces section 12 exactly.
func TestProjectMatchesTheDesignDocumentExample(t *testing.T) {
	t.Parallel()
	cs123 := counts(
		storage.StepCount{StepID: "crawl", Received: 10000, Succeeded: 9990, DLQ: 10, Produced: 9990},
		storage.StepCount{StepID: "process", Received: 9990, Succeeded: 9985, DLQ: 5, Produced: 9985},
		storage.StepCount{StepID: "index", Received: 9985, Succeeded: 9983, DLQ: 2},
	)
	p := execution.Project(storage.Execution{ID: "exec_123"}, cs123, linearEdges(cs123))

	if p.Status != execution.StatusComplete {
		t.Fatalf("status: got %s, want COMPLETE", p.Status)
	}
	if p.TerminalSuccess != 9983 {
		t.Fatalf("terminal successful: got %d, want 9983", p.TerminalSuccess)
	}
	if p.TerminalDLQ != 17 {
		t.Fatalf("terminal DLQ: got %d, want 17 (10 + 5 + 2)", p.TerminalDLQ)
	}
	if p.Active != 0 {
		t.Fatalf("active: got %d, want 0", p.Active)
	}
	if p.HasLeaks() {
		t.Fatalf("the document's example is internally consistent but reported leaks: %v", p.Leaks)
	}
}

// TestProjectWhileRunning proves a projection is meaningful mid-flight, not
// only at rest, and that a shortfall behind a working step is not called a leak.
func TestProjectWhileRunning(t *testing.T) {
	t.Parallel()
	cs2 := counts(
		storage.StepCount{StepID: "discover", Received: 1, Succeeded: 1, Produced: 50},
		storage.StepCount{StepID: "crawl", Received: 50, Succeeded: 20, Active: 30, Produced: 20},
		storage.StepCount{StepID: "index", Received: 18, Succeeded: 15, Active: 3},
	)
	p := execution.Project(storage.Execution{ID: "exec_2"}, cs2, linearEdges(cs2))

	if p.Status != execution.StatusRunning {
		t.Fatalf("status: got %s, want RUNNING", p.Status)
	}
	if p.Active != 33 {
		t.Fatalf("active: got %d, want 33", p.Active)
	}
	// crawl produced 20 but index has only received 18: the other two are in
	// flight, not lost, because crawl is still working.
	if p.HasLeaks() {
		t.Fatalf("a mid-flight shortfall was reported as a leak: %v", p.Leaks)
	}
}

// TestProjectDetectsDeletedRows is the leak case the design document asks for:
// work that silently disappeared between stages.
func TestProjectDetectsDeletedRows(t *testing.T) {
	t.Parallel()
	// 100 entered crawl, 90 accounted for. Ten rows vanished.
	cs3 := counts(
		storage.StepCount{StepID: "discover", Received: 1, Succeeded: 1, Produced: 100},
		storage.StepCount{StepID: "crawl", Received: 100, Succeeded: 88, DLQ: 2, Produced: 88},
		storage.StepCount{StepID: "index", Received: 88, Succeeded: 88},
	)
	p := execution.Project(storage.Execution{ID: "exec_3"}, cs3, linearEdges(cs3))

	if !p.HasLeaks() {
		t.Fatalf("ten missing items were not reported")
	}
	var found *execution.Leak
	for i := range p.Leaks {
		if p.Leaks[i].Kind == execution.LeakEdgeImbalance {
			found = &p.Leaks[i]
		}
	}
	if found == nil {
		t.Fatalf("expected an edge-imbalance leak, got %v", p.Leaks)
	}
	if found.StepID != "crawl" || found.Missing != 10 {
		t.Fatalf("leak: got step %q missing %d, want crawl missing 10", found.StepID, found.Missing)
	}
}

// TestProjectDetectsAHandOffGap covers work lost between two settled steps.
func TestProjectDetectsAHandOffGap(t *testing.T) {
	t.Parallel()
	cs4 := counts(
		storage.StepCount{StepID: "crawl", Received: 10, Succeeded: 10, Produced: 10},
		storage.StepCount{StepID: "index", Received: 7, Succeeded: 7},
	)
	p := execution.Project(storage.Execution{ID: "exec_4"}, cs4, linearEdges(cs4))

	var found *execution.Leak
	for i := range p.Leaks {
		if p.Leaks[i].Kind == execution.LeakContinuity {
			found = &p.Leaks[i]
		}
	}
	if found == nil {
		t.Fatalf("a settled hand-off gap was not reported: %v", p.Leaks)
	}
	if found.Missing != 3 {
		t.Fatalf("missing: got %d, want 3", found.Missing)
	}
}

// TestProjectDetectsDuplication covers the opposite failure: more arrived than
// was produced.
func TestProjectDetectsDuplication(t *testing.T) {
	t.Parallel()
	cs5 := counts(
		storage.StepCount{StepID: "crawl", Received: 10, Succeeded: 10, Produced: 10},
		storage.StepCount{StepID: "index", Received: 13, Succeeded: 13},
	)
	p := execution.Project(storage.Execution{ID: "exec_5"}, cs5, linearEdges(cs5))

	var found bool
	for _, l := range p.Leaks {
		if l.Kind == execution.LeakDuplication {
			found = true
		}
	}
	if !found {
		t.Fatalf("duplication was not reported: %v", p.Leaks)
	}
}

// TestProjectSurfacesCappedOutput is the design document's section 21: a step
// that found 5000 and was configured to emit 200 must not look like clean
// arithmetic.
func TestProjectSurfacesCappedOutput(t *testing.T) {
	t.Parallel()
	cs6 := counts(
		storage.StepCount{
			StepID: "discover", Received: 1, Succeeded: 1,
			Produced: 200, Dropped: 4800, Capped: 1,
		},
		storage.StepCount{StepID: "crawl", Received: 200, Succeeded: 200},
	)
	p := execution.Project(storage.Execution{ID: "exec_6"}, cs6, linearEdges(cs6))

	// The counts themselves balance perfectly, which is exactly the danger.
	var found *execution.Leak
	for i := range p.Leaks {
		if p.Leaks[i].Kind == execution.LeakCappedOutput {
			found = &p.Leaks[i]
		}
	}
	if found == nil {
		t.Fatalf("capped output was not surfaced: %v", p.Leaks)
	}
	if found.Missing != 4800 {
		t.Fatalf("dropped: got %d, want 4800", found.Missing)
	}
}

// TestProjectCountsFilteredAsAccountedFor proves a deliberate filter balances
// the edge rather than looking like loss.
func TestProjectCountsFilteredAsAccountedFor(t *testing.T) {
	t.Parallel()
	cs7 := counts(
		storage.StepCount{StepID: "filter", Received: 10, Succeeded: 6, Filtered: 4, Produced: 6},
		storage.StepCount{StepID: "sink", Received: 6, Succeeded: 6},
	)
	p := execution.Project(storage.Execution{ID: "exec_7"}, cs7, linearEdges(cs7))
	if p.HasLeaks() {
		t.Fatalf("filtered items were treated as lost: %v", p.Leaks)
	}
	if p.TerminalSuccess != 6 {
		t.Fatalf("terminal success: got %d, want 6", p.TerminalSuccess)
	}
}

// TestProjectWithRetriesInFlight proves retrying items count as active, not as
// lost or finished.
func TestProjectWithRetriesInFlight(t *testing.T) {
	t.Parallel()
	cs8 := counts(
		storage.StepCount{StepID: "flaky", Received: 20, Succeeded: 15, Active: 5, Produced: 15},
		storage.StepCount{StepID: "sink", Received: 15, Succeeded: 15},
	)
	p := execution.Project(storage.Execution{ID: "exec_8"}, cs8, linearEdges(cs8))
	if p.Status != execution.StatusRunning {
		t.Fatalf("status: got %s, want RUNNING while retries are pending", p.Status)
	}
	if p.HasLeaks() {
		t.Fatalf("retrying items were reported as a leak: %v", p.Leaks)
	}
}
