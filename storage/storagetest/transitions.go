package storagetest

import (
	"fmt"
	"testing"
	"time"

	"github.com/unilinq/durableq/storage"
)

// missingID is an id no store will ever have issued.
const missingID int64 = 9_000_000_123

func runTransitions(t *testing.T, h Harness) {
	t.Run("CompleteMarksDone", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "w1", 1)[0]

		res, err := store.Complete(t.Context(), storage.CompleteRequest{ID: it.ID, Worker: "w1"})
		requireNoErr(t, err, "Complete")
		requireLen(t, res, 1, "one result")
		requireNoErr(t, res[0].Err, "Complete result")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.State, storage.StateDone, "state")
		requireEqual(t, got.Outcome, storage.OutcomeSuccess, "outcome")
		requireEqual(t, got.LeasedBy, "", "lease released")
		requireTimeEqual(t, got.FinalizedAt, clock.Now(), "finalized_at")
		requireEqual(t, got.LastWorker, "w1", "last_worker survives completion")
	})

	t.Run("CompleteFilteredIsDistinctFromSuccess", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "w1", 1)[0]

		res, err := store.Complete(t.Context(), storage.CompleteRequest{ID: it.ID, Worker: "w1", Filtered: true})
		requireNoErr(t, err, "Complete")
		requireNoErr(t, res[0].Err, "Complete result")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.State, storage.StateDone, "filtered item is still terminal-success")
		requireEqual(t, got.Outcome, storage.OutcomeFiltered, "outcome records the filter")
	})

	t.Run("CompleteEnqueuesDownstreamAtomically", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "crawl", ExecutionID: "e1", ItemID: "i1"})
		it := claim(t, store, "crawl", "w1", 1)[0]

		res, err := store.Complete(t.Context(), storage.CompleteRequest{
			ID: it.ID, Worker: "w1",
			Produced: []storage.NewItem{
				{Queue: "process", ExecutionID: "e1", ItemID: "i1", ParentItemID: "i1"},
				{Queue: "process", ExecutionID: "e1", ItemID: "i2", ParentItemID: "i1"},
			},
		})
		requireNoErr(t, err, "Complete")
		requireNoErr(t, res[0].Err, "Complete result")

		requireEqual(t, statFor(t, store, "process").Ready, 2, "downstream items exist")
		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.Produced, 2, "produced count recorded on the acked item")
	})

	t.Run("CompleteRecordsCappedOutput", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "discover"})
		it := claim(t, store, "discover", "w1", 1)[0]

		// A discovery step that found 5000 but was configured to emit 200 must
		// say so, otherwise 4800 items vanish with no signal anywhere.
		_, err := store.Complete(t.Context(), storage.CompleteRequest{
			ID: it.ID, Worker: "w1",
			Produced: []storage.NewItem{{Queue: "crawl"}},
			Dropped:  4800,
			Capped:   true,
		})
		requireNoErr(t, err, "Complete")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.Dropped, 4800, "dropped count")
		requireEqual(t, got.ProducedCapped, true, "capped flag")
	})

	t.Run("CompleteDoesNotTouchAReadyItem", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		it := mustEnqueue(t, store, storage.NewItem{Queue: "q"})[0]

		res, err := store.Complete(t.Context(), storage.CompleteRequest{ID: it.ID, Worker: "w1"})
		requireNoErr(t, err, "Complete")
		requireErrIs(t, res[0].Err, storage.ErrWrongState, "completing a ready item")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.State, storage.StateReady, "item left alone")
	})

	t.Run("CompleteByTheWrongWorkerIsRejected", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "owner", 1)[0]

		res, err := store.Complete(t.Context(), storage.CompleteRequest{ID: it.ID, Worker: "impostor"})
		requireNoErr(t, err, "Complete")
		requireErrIs(t, res[0].Err, storage.ErrWrongState, "completing work owned by another worker")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.State, storage.StateRunning, "item still owned by the real worker")
	})

	t.Run("RetryReschedulesByPolicy", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		policy := storage.Policy{
			MaxAttempts: 5,
			Schedule:    []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute},
			Jitter:      0, // exact assertions
		}
		mustEnqueue(t, store, storage.NewItem{Queue: "q", Policy: &policy})

		want := []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}
		for i, w := range want {
			it := claim(t, store, "q", "w1", 1)
			requireLen(t, it, 1, "claim for attempt "+itoa(i+1))
			requireEqual(t, it[0].Attempt, i+1, "attempt number")

			res, err := store.Retry(t.Context(), storage.RetryRequest{ID: it[0].ID, Worker: "w1", Error: "boom"})
			requireNoErr(t, err, "Retry")
			requireNoErr(t, res[0].Err, "Retry result")

			got, err := store.GetItem(t.Context(), it[0].ID)
			requireNoErr(t, err, "GetItem")
			requireEqual(t, got.State, storage.StateReady, "state after retry")
			requireEqual(t, got.LastError, "boom", "last_error recorded")
			requireTimeEqual(t, got.AvailableAt, clock.Now().Add(w), "backoff after attempt "+itoa(i+1))

			// Move to the next attempt without waiting on the wall clock.
			clock.Advance(w)
		}
	})

	t.Run("RetryKeepsTheAttemptItConsumed", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		p := storage.Policy{MaxAttempts: 5, Schedule: []time.Duration{time.Second}, Jitter: 0}
		mustEnqueue(t, store, storage.NewItem{Queue: "q", Policy: &p})

		it := claim(t, store, "q", "w1", 1)[0]
		requireEqual(t, it.Attempt, 1, "attempt after claim")
		_, err := store.Retry(t.Context(), storage.RetryRequest{ID: it.ID, Worker: "w1", Error: "x"})
		requireNoErr(t, err, "Retry")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.Attempt, 1, "retry must not re-increment the attempt claim already counted")

		clock.Advance(time.Second)
		again := claim(t, store, "q", "w1", 1)[0]
		requireEqual(t, again.Attempt, 2, "second claim consumes the second attempt")
	})

	t.Run("RetryJitterStaysInBounds", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		p := storage.Policy{MaxAttempts: 10, Schedule: []time.Duration{100 * time.Second}, Jitter: 0.1}
		low, high := p.JitterBounds(1)

		for i := 0; i < 20; i++ {
			items := mustEnqueue(t, store, storage.NewItem{Queue: "q", Policy: &p})
			it := claim(t, store, "q", "w1", 1)
			requireLen(t, it, 1, "claim")
			_, err := store.Retry(t.Context(), storage.RetryRequest{ID: it[0].ID, Worker: "w1"})
			requireNoErr(t, err, "Retry")

			got, err := store.GetItem(t.Context(), items[0].ID)
			requireNoErr(t, err, "GetItem")
			delay := got.AvailableAt.Sub(clock.Now())
			if delay < low || delay > high {
				t.Fatalf("jittered backoff %s outside bounds %s..%s", delay, low, high)
			}
			// Leave the item out of the way of the next iteration.
			clock.Advance(high)
			next := claim(t, store, "q", "w1", 10)
			for _, n := range next {
				_, _ = store.Complete(t.Context(), storage.CompleteRequest{ID: n.ID, Worker: "w1"})
			}
			clock.Set(clock.Now())
		}
	})

	t.Run("RetryAtOverridesThePolicy", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "w1", 1)[0]

		at := clock.Now().Add(7 * time.Hour)
		_, err := store.Retry(t.Context(), storage.RetryRequest{ID: it.ID, Worker: "w1", At: at})
		requireNoErr(t, err, "Retry")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireTimeEqual(t, got.AvailableAt, at, "explicit retry time")
	})

	t.Run("DeadLetterPreservesMetadata", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "indexing", ExecutionID: "e1", ItemID: "i1"})
		it := claim(t, store, "indexing", "worker-v1.8", 1)[0]

		res, err := store.DeadLetter(t.Context(), storage.DeadLetterRequest{
			ID: it.ID, Worker: "worker-v1.8", Error: "unsupported_pdf_encoding",
		})
		requireNoErr(t, err, "DeadLetter")
		requireNoErr(t, res[0].Err, "DeadLetter result")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.State, storage.StateDLQ, "state")
		requireEqual(t, got.Queue, "indexing"+storage.DLQSuffix, "moved to the queue's DLQ")
		requireEqual(t, got.LastError, "unsupported_pdf_encoding", "error kept as metadata")
		requireEqual(t, got.Attempt, 1, "attempt count kept")
		requireEqual(t, got.LastWorker, "worker-v1.8", "worker identity kept")
		requireEqual(t, got.ExecutionID, "e1", "lineage kept")
		requireEqual(t, got.ItemID, "i1", "lineage kept")

		// The DLQ is a queue in its own right and must be visible as one.
		requireEqual(t, statFor(t, store, "indexing"+storage.DLQSuffix).DLQ, 1, "DLQ visible in stats")
	})

	t.Run("ReleaseReturnsWorkWithoutConsumingAnAttempt", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "w1", 1)[0]
		requireEqual(t, it.Attempt, 1, "attempt after claim")

		res, err := store.Release(t.Context(), storage.ReleaseRequest{ID: it.ID, Worker: "w1", Reason: "shutting down"})
		requireNoErr(t, err, "Release")
		requireNoErr(t, res[0].Err, "Release result")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.State, storage.StateReady, "state after release")
		requireEqual(t, got.Attempt, 0, "release gives the attempt back")
		requireTimeEqual(t, got.AvailableAt, clock.Now(), "released work is immediately claimable")
	})

	t.Run("ExtendLeaseRenewsOwnership", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "w1", 1)[0]

		until := clock.Now().Add(5 * time.Minute)
		res, err := store.ExtendLease(t.Context(), "w1", until, it.ID)
		requireNoErr(t, err, "ExtendLease")
		requireNoErr(t, res[0].Err, "ExtendLease result")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireTimeEqual(t, got.LeaseUntil, until, "lease extended")
	})

	t.Run("ExtendLeaseByTheWrongWorkerIsRejected", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "owner", 1)[0]

		res, err := store.ExtendLease(t.Context(), "impostor", clock.Now().Add(time.Hour), it.ID)
		requireNoErr(t, err, "ExtendLease")
		requireErrIs(t, res[0].Err, storage.ErrWrongState, "extending another worker's lease")
	})
}

// runNotFound proves every mutator reports a missing item as ErrNotFound rather
// than silently succeeding. A silent success here would hide real bugs in
// worker code for as long as the system runs.
func runNotFound(t *testing.T, h Harness) {
	mutators := map[string]func(store storage.Store) ([]storage.Result, error){
		"Complete": func(s storage.Store) ([]storage.Result, error) {
			return s.Complete(t.Context(), storage.CompleteRequest{ID: missingID, Worker: "w"})
		},
		"Retry": func(s storage.Store) ([]storage.Result, error) {
			return s.Retry(t.Context(), storage.RetryRequest{ID: missingID, Worker: "w"})
		},
		"DeadLetter": func(s storage.Store) ([]storage.Result, error) {
			return s.DeadLetter(t.Context(), storage.DeadLetterRequest{ID: missingID, Worker: "w"})
		},
		"Release": func(s storage.Store) ([]storage.Result, error) {
			return s.Release(t.Context(), storage.ReleaseRequest{ID: missingID, Worker: "w"})
		},
		"ExtendLease": func(s storage.Store) ([]storage.Result, error) {
			return s.ExtendLease(t.Context(), "w", time.Now().Add(time.Minute), missingID)
		},
	}
	for name, fn := range mutators {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store, _ := h.New(t)
			res, err := fn(store)
			requireNoErr(t, err, name+" call")
			requireLen(t, res, 1, name+" results")
			requireErrIs(t, res[0].Err, storage.ErrNotFound, name+" on a missing item")
			requireEqual(t, res[0].ID, missingID, name+" result id")
		})
	}
}

// runWrongState proves every mutator is a no-op against an item that is not
// running, and says so.
func runWrongState(t *testing.T, h Harness) {
	mutators := map[string]func(store storage.Store, id int64) ([]storage.Result, error){
		"Complete": func(s storage.Store, id int64) ([]storage.Result, error) {
			return s.Complete(t.Context(), storage.CompleteRequest{ID: id, Worker: "w1"})
		},
		"Retry": func(s storage.Store, id int64) ([]storage.Result, error) {
			return s.Retry(t.Context(), storage.RetryRequest{ID: id, Worker: "w1"})
		},
		"DeadLetter": func(s storage.Store, id int64) ([]storage.Result, error) {
			return s.DeadLetter(t.Context(), storage.DeadLetterRequest{ID: id, Worker: "w1"})
		},
		"Release": func(s storage.Store, id int64) ([]storage.Result, error) {
			return s.Release(t.Context(), storage.ReleaseRequest{ID: id, Worker: "w1"})
		},
		"ExtendLease": func(s storage.Store, id int64) ([]storage.Result, error) {
			return s.ExtendLease(t.Context(), "w1", time.Now().Add(time.Minute), id)
		},
	}
	for name, fn := range mutators {
		t.Run(name+"OnDoneItem", func(t *testing.T) {
			t.Parallel()
			store, _ := h.New(t)
			mustEnqueue(t, store, storage.NewItem{Queue: "q"})
			it := claim(t, store, "q", "w1", 1)[0]
			_, err := store.Complete(t.Context(), storage.CompleteRequest{ID: it.ID, Worker: "w1"})
			requireNoErr(t, err, "Complete to reach a terminal state")

			res, err := fn(store, it.ID)
			requireNoErr(t, err, name+" call")
			requireErrIs(t, res[0].Err, storage.ErrWrongState, name+" against a done item")

			got, err := store.GetItem(t.Context(), it.ID)
			requireNoErr(t, err, "GetItem")
			requireEqual(t, got.State, storage.StateDone, name+" left the terminal item alone")
		})
	}
}

// runBatch proves a batch transition reports each item individually instead of
// failing wholesale because one member was missing or in the wrong state.
func runBatch(t *testing.T, h Harness) {
	t.Run("MixedOutcomesReportedPerItem", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"}, storage.NewItem{Queue: "q"})
		running := claim(t, store, "q", "w1", 2)
		requireLen(t, running, 2, "claimed two")
		ready := mustEnqueue(t, store, storage.NewItem{Queue: "q"})[0]

		res, err := store.Complete(t.Context(),
			storage.CompleteRequest{ID: running[0].ID, Worker: "w1"},
			storage.CompleteRequest{ID: missingID, Worker: "w1"},
			storage.CompleteRequest{ID: ready.ID, Worker: "w1"},
			storage.CompleteRequest{ID: running[1].ID, Worker: "w1"},
		)
		requireNoErr(t, err, "Complete batch")
		requireLen(t, res, 4, "one result per request")
		requireNoErr(t, res[0].Err, "first running item")
		requireErrIs(t, res[1].Err, storage.ErrNotFound, "missing item")
		requireErrIs(t, res[2].Err, storage.ErrWrongState, "ready item")
		requireNoErr(t, res[3].Err, "second running item")

		// The two good items really completed; the bad ones really did not.
		st := statFor(t, store, "q")
		requireEqual(t, st.Done, 2, "two items completed")
		requireEqual(t, st.Ready, 1, "the ready item is untouched")
	})

	t.Run("DownstreamOfAFailedAckIsNotEnqueued", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		ready := mustEnqueue(t, store, storage.NewItem{Queue: "q"})[0]

		// Acking an item that is not running must not create its downstream
		// work; otherwise a retry would duplicate it.
		res, err := store.Complete(t.Context(), storage.CompleteRequest{
			ID: ready.ID, Worker: "w1",
			Produced: []storage.NewItem{{Queue: "next"}},
		})
		requireNoErr(t, err, "Complete")
		requireErrIs(t, res[0].Err, storage.ErrWrongState, "ack of a ready item")

		stats, err := store.Stats(t.Context(), "next")
		requireNoErr(t, err, "Stats")
		for _, s := range stats {
			requireEqual(t, s.Ready, 0, "no downstream work from a rejected ack")
		}
	})
}

func itoa(n int) string {
	return fmt.Sprint(n)
}
