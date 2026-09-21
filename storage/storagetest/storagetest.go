// Package storagetest is the conformance suite every durableq storage adapter
// must pass. An adapter is finished when Run is green against it.
//
// The suite is exported rather than internal on purpose: it is what makes a
// second backend a small change instead of a re-derivation of the semantics.
package storagetest

import (
	"testing"
	"time"

	"github.com/unilinq/durableq/storage"
)

// Clock is a clock the suite drives. Adapters under test must read time from
// it so that time-dependent behaviour is asserted, never slept through.
type Clock interface {
	storage.Clock
	// Set moves the clock to an absolute time.
	Set(time.Time)
	// Advance moves the clock forward.
	Advance(time.Duration)
}

// Harness builds the subject under test.
type Harness struct {
	// New returns a store isolated to this test together with the clock that
	// drives it. The store must be empty and must not share rows with any
	// other test.
	New func(t *testing.T) (storage.Store, Clock)
}

// Run executes the whole conformance suite.
func Run(t *testing.T, h Harness) {
	t.Helper()
	t.Run("Enqueue", func(t *testing.T) { runEnqueue(t, h) })
	t.Run("GetItem", func(t *testing.T) { runGetItem(t, h) })
	t.Run("Stats", func(t *testing.T) { runStats(t, h) })
	t.Run("PauseResume", func(t *testing.T) { runPauseResume(t, h) })
	t.Run("Isolation", func(t *testing.T) { runIsolation(t, h) })
	t.Run("Claim", func(t *testing.T) { runClaim(t, h) })
	t.Run("Transitions", func(t *testing.T) { runTransitions(t, h) })
	t.Run("NotFound", func(t *testing.T) { runNotFound(t, h) })
	t.Run("WrongState", func(t *testing.T) { runWrongState(t, h) })
	t.Run("Batch", func(t *testing.T) { runBatch(t, h) })
	t.Run("Lease", func(t *testing.T) { runLease(t, h) })
	t.Run("Replay", func(t *testing.T) { runReplay(t, h) })
	t.Run("Sweep", func(t *testing.T) { runSweep(t, h) })
	t.Run("Contention", func(t *testing.T) { runContention(t, h) })
	t.Run("DLQContention", func(t *testing.T) { runDLQContention(t, h) })
	t.Run("RaceGate", func(t *testing.T) { runRaceGate(t, h) })
}
