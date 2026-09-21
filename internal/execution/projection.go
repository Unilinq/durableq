// Package execution derives run-level state from the durable facts that
// workers and queues leave behind.
//
// The derivation lives here, not in a dashboard, because durableq owns the
// lineage and queue state the answer needs. Everything in this package is a
// pure function over counts read from the store, so the arithmetic is testable
// without a database and identical whichever process asks.
package execution

import (
	"fmt"

	"github.com/unilinq/durableq/storage"
)

// Status is how far along an execution is.
//
// There are deliberately only two values. Whether a run with dead-lettered
// items counts as a success is a business question durableq does not answer;
// it reports the counts and lets the caller decide.
type Status string

const (
	// StatusRunning means work is still ready or running somewhere.
	StatusRunning Status = "RUNNING"
	// StatusComplete means nothing is active: every item reached a terminal
	// state, successfully or otherwise.
	StatusComplete Status = "COMPLETE"
)

// StepProjection is one row of the run table.
type StepProjection struct {
	StepID    string
	Idx       int
	Queue     string
	Received  int
	Succeeded int
	Filtered  int
	DLQ       int
	Active    int
	Produced  int
	Dropped   int
	Capped    int
}

// Terminal is how many of the step's items have finished, one way or another.
func (s StepProjection) Terminal() int { return s.Succeeded + s.Filtered + s.DLQ }

// LeakKind classifies a gap between what a pipeline should account for and
// what it does.
type LeakKind string

const (
	// LeakEdgeImbalance means a step's items do not add up: work entered and
	// is neither finished nor in flight. Rows were removed from under the run.
	LeakEdgeImbalance LeakKind = "edge-imbalance"
	// LeakContinuity means a settled step produced more items than the next
	// step ever received, so work disappeared in the hand-off.
	LeakContinuity LeakKind = "continuity"
	// LeakDuplication means a step received more than the previous step
	// produced.
	LeakDuplication LeakKind = "duplication"
	// LeakCappedOutput means a step deliberately discarded output. Not a bug,
	// but the one case where a shrinking count is expected, so it is surfaced
	// rather than left to look like clean arithmetic.
	LeakCappedOutput LeakKind = "capped-output"
)

// Leak is one discrepancy found while projecting a run.
type Leak struct {
	Kind    LeakKind
	StepID  string
	Missing int
	Detail  string
}

func (l Leak) String() string {
	return fmt.Sprintf("%s at step %q: %s", l.Kind, l.StepID, l.Detail)
}

// Projection is the run-level answer to "what happened to this run?".
type Projection struct {
	Execution storage.Execution
	Steps     []StepProjection
	Status    Status

	// TerminalSuccess counts items that finished successfully at the last
	// step, which is the only place an item leaves the pipeline for good.
	TerminalSuccess int
	// TerminalDLQ counts every dead-lettered item across all steps.
	TerminalDLQ int
	// Active counts everything still ready or running anywhere.
	Active int

	// Leaks are discrepancies the counts cannot explain.
	Leaks []Leak
}

// HasLeaks reports whether anything could not be accounted for.
func (p Projection) HasLeaks() bool { return len(p.Leaks) > 0 }

// Project turns per-step counts into a run-level projection and checks the
// invariants that reveal work disappearing between stages.
func Project(exec storage.Execution, counts []storage.StepCount) Projection {
	p := Projection{Execution: exec, Status: StatusComplete}

	for _, c := range counts {
		p.Steps = append(p.Steps, StepProjection{
			StepID:    c.StepID,
			Idx:       c.Idx,
			Queue:     c.Queue,
			Received:  c.Received,
			Succeeded: c.Succeeded,
			Filtered:  c.Filtered,
			DLQ:       c.DLQ,
			Active:    c.Active,
			Produced:  c.Produced,
			Dropped:   c.Dropped,
			Capped:    c.Capped,
		})
	}

	for i, s := range p.Steps {
		p.TerminalDLQ += s.DLQ
		p.Active += s.Active
		if s.Active > 0 {
			p.Status = StatusRunning
		}
		// Only the final step's successes leave the pipeline; every other
		// step's success becomes work further down.
		if i == len(p.Steps)-1 {
			p.TerminalSuccess += s.Succeeded
		}

		// Per-step invariant: everything that entered is finished or in flight.
		if got, want := s.Terminal()+s.Active, s.Received; got != want {
			p.Leaks = append(p.Leaks, Leak{
				Kind:    LeakEdgeImbalance,
				StepID:  s.StepID,
				Missing: want - got,
				Detail: fmt.Sprintf(
					"%d items entered but %d are accounted for (%d succeeded, %d filtered, %d dead-lettered, %d active)",
					want, got, s.Succeeded, s.Filtered, s.DLQ, s.Active),
			})
		}

		if s.Capped > 0 {
			p.Leaks = append(p.Leaks, Leak{
				Kind:    LeakCappedOutput,
				StepID:  s.StepID,
				Missing: s.Dropped,
				Detail: fmt.Sprintf(
					"%d items reported capped output, discarding %d items that were found but never emitted",
					s.Capped, s.Dropped),
			})
		}
	}

	// Continuity: what one step produced is what the next one received.
	for i := 0; i+1 < len(p.Steps); i++ {
		up, down := p.Steps[i], p.Steps[i+1]
		if down.Received > up.Produced {
			p.Leaks = append(p.Leaks, Leak{
				Kind:    LeakDuplication,
				StepID:  down.StepID,
				Missing: up.Produced - down.Received,
				Detail: fmt.Sprintf("received %d items but %q only produced %d",
					down.Received, up.StepID, up.Produced),
			})
			continue
		}
		// While the upstream step is still working, the shortfall is just work
		// that has not been produced yet.
		if up.Active == 0 && down.Received < up.Produced {
			p.Leaks = append(p.Leaks, Leak{
				Kind:    LeakContinuity,
				StepID:  down.StepID,
				Missing: up.Produced - down.Received,
				Detail: fmt.Sprintf("%q finished producing %d items but only %d reached %q",
					up.StepID, up.Produced, down.Received, down.StepID),
			})
		}
	}
	return p
}
