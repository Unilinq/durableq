// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

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
	"strings"

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
	// LeakBranchDivergence means two successors of the same fan-out step
	// received different counts. A step's output is broadcast identically and
	// atomically to every successor, so siblings under the same step always
	// receive the same count; any difference names the exact edge that lost
	// (or gained) work, which the group-level continuity check alone cannot
	// do once a step has more than one successor.
	LeakBranchDivergence LeakKind = "branch-divergence"
)

// Leak is one discrepancy found while projecting a run.
type Leak struct {
	Kind LeakKind
	// StepID is the from-step: the step whose output the leak was detected
	// on.
	StepID string
	// Edge names the specific edge (or edge group) the leak was detected on,
	// formatted "from->to" or "from->{to1,to2,...}" for a group-level check
	// spanning every successor of StepID.
	Edge    string
	Missing int
	Detail  string
}

func (l Leak) String() string {
	if l.Edge != "" {
		return fmt.Sprintf("%s on edge %s: %s", l.Kind, l.Edge, l.Detail)
	}
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
//
// edges is the step graph: for each step id, the successor ids its output is
// broadcast to. A step with no entry, or an empty one, is a leaf - a
// terminal step of the pipeline. Edges are authoritative for topology; Idx on
// each step is for stable display ordering only, and nothing here infers
// shape from it.
func Project(exec storage.Execution, counts []storage.StepCount, edges map[string][]string) Projection {
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

	byStep := make(map[string]StepProjection, len(p.Steps))
	for _, s := range p.Steps {
		byStep[s.StepID] = s
	}

	for _, s := range p.Steps {
		p.TerminalDLQ += s.DLQ
		p.Active += s.Active
		if s.Active > 0 {
			p.Status = StatusRunning
		}
		// Terminal steps are the leaves: steps with no successors. Every other
		// step's success becomes work further down.
		if len(edges[s.StepID]) == 0 {
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

	// Continuity is per fan-out group: for a step with successors S, what it
	// broadcasts goes to every successor identically and atomically (in the
	// same transaction as the ack that produced it), so
	// sum(received for s in S) == produced. That is exactly the old adjacent-
	// step check when |S| == 1.
	for _, up := range p.Steps {
		succIDs := edges[up.StepID]
		if len(succIDs) == 0 {
			continue
		}

		// Branch divergence: every successor of the same step must receive
		// exactly up.Produced / len(succIDs), whatever the state of the run,
		// because the broadcast to every successor happens together and
		// identically - it is not merely that siblings should agree with each
		// other (comparing against the largest sibling blames whichever
		// branches are healthy when one branch gains an extra row, since
		// every other branch then looks short by comparison). Comparing each
		// branch against the exact expected count instead names the actual
		// anomalous edge regardless of whether it lost or gained work.
		if len(succIDs) > 1 {
			expected := up.Produced / len(succIDs)
			for _, to := range succIDs {
				got := byStep[to].Received
				if got == expected {
					continue
				}
				diff := expected - got
				direction := "fewer than"
				if got > expected {
					direction = "more than"
				}
				p.Leaks = append(p.Leaks, Leak{
					Kind:    LeakBranchDivergence,
					StepID:  up.StepID,
					Edge:    up.StepID + "->" + to,
					Missing: diff,
					Detail: fmt.Sprintf(
						"%q received %d but should have received %d (%s its siblings under %q)",
						to, got, expected, direction, up.StepID),
				})
			}
		}

		received := 0
		for _, to := range succIDs {
			received += byStep[to].Received
		}
		edge := up.StepID + "->" + strings.Join(succIDs, ",")
		if len(succIDs) > 1 {
			edge = up.StepID + "->{" + strings.Join(succIDs, ",") + "}"
		}
		if received > up.Produced {
			p.Leaks = append(p.Leaks, Leak{
				Kind:    LeakDuplication,
				StepID:  up.StepID,
				Edge:    edge,
				Missing: up.Produced - received,
				Detail: fmt.Sprintf("successors of %q received %d items but it only produced %d",
					up.StepID, received, up.Produced),
			})
			continue
		}
		// While the upstream step is still working, the shortfall is just work
		// that has not been produced yet.
		if up.Active == 0 && received < up.Produced {
			p.Leaks = append(p.Leaks, Leak{
				Kind:    LeakContinuity,
				StepID:  up.StepID,
				Edge:    edge,
				Missing: up.Produced - received,
				Detail: fmt.Sprintf("%q finished producing %d items but only %d reached its successors",
					up.StepID, up.Produced, received),
			})
		}
	}
	return p
}
