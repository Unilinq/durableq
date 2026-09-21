package storagetest

import (
	"testing"
	"time"

	"github.com/unilinq/durableq/storage"
)

func runLease(t *testing.T, h Harness) {
	t.Run("ExpiredLeaseIsReclaimedWithAttemptIntact", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "crashed-worker", 1)[0]
		requireEqual(t, it.Attempt, 1, "attempt after claim")

		// Nothing acks. The lease simply lapses.
		clock.Advance(testLease + time.Second)
		res, err := store.ReclaimExpired(t.Context(), storage.ReclaimOpts{Limit: 10})
		requireNoErr(t, err, "ReclaimExpired")
		requireEqual(t, res.Reclaimed, 1, "one item reclaimed")
		requireEqual(t, res.DeadLettered, 0, "nothing dead-lettered yet")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.State, storage.StateReady, "reclaimed item is claimable again")
		requireEqual(t, got.Attempt, 1, "the spent attempt is neither lost nor double-counted")
		requireEqual(t, got.LeasedBy, "", "ownership released")

		// The next claim takes the second attempt, not the first again.
		next := claim(t, store, "q", "fresh-worker", 1)
		requireLen(t, next, 1, "reclaimed item is claimable")
		requireEqual(t, next[0].Attempt, 2, "second attempt")
	})

	t.Run("LiveLeaseIsNotReclaimed", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "w1", 1)[0]

		clock.Advance(testLease / 2)
		res, err := store.ReclaimExpired(t.Context(), storage.ReclaimOpts{Limit: 10})
		requireNoErr(t, err, "ReclaimExpired")
		requireEqual(t, res.Reclaimed, 0, "a live lease must be left alone")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.State, storage.StateRunning, "item still running")
	})

	t.Run("ExtendedLeaseSurvivesReclaim", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "w1", 1)[0]

		// A long-running handler heartbeats; the reclaimer must respect it.
		clock.Advance(testLease - time.Second)
		_, err := store.ExtendLease(t.Context(), "w1", clock.Now().Add(testLease), it.ID)
		requireNoErr(t, err, "ExtendLease")

		clock.Advance(testLease - time.Second)
		res, err := store.ReclaimExpired(t.Context(), storage.ReclaimOpts{Limit: 10})
		requireNoErr(t, err, "ReclaimExpired")
		requireEqual(t, res.Reclaimed, 0, "heartbeated work must not be stolen")
	})

	t.Run("ExhaustedItemIsDeadLetteredOnReclaim", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		p := storage.Policy{MaxAttempts: 1, Schedule: []time.Duration{time.Second}, Jitter: 0}
		mustEnqueue(t, store, storage.NewItem{Queue: "q", Policy: &p})

		it := claim(t, store, "q", "doomed", 1)[0]
		requireEqual(t, it.Attempt, 1, "the only attempt")

		clock.Advance(testLease + time.Second)
		res, err := store.ReclaimExpired(t.Context(), storage.ReclaimOpts{Limit: 10})
		requireNoErr(t, err, "ReclaimExpired")
		requireEqual(t, res.Reclaimed, 0, "nothing returned to the queue")
		requireEqual(t, res.DeadLettered, 1, "the exhausted item is dead-lettered")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.State, storage.StateDLQ, "state")
		requireEqual(t, got.Queue, "q"+storage.DLQSuffix, "moved to the DLQ")
		if got.LastError == "" {
			t.Fatalf("a lease-expiry dead-letter must record why")
		}
	})

	t.Run("AckAfterLeaseExpiredLosesToTheReclaimer", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "slow-worker", 1)[0]

		// The reclaimer gets there first.
		clock.Advance(testLease + time.Second)
		_, err := store.ReclaimExpired(t.Context(), storage.ReclaimOpts{Limit: 10})
		requireNoErr(t, err, "ReclaimExpired")

		// The original worker finally finishes and tries to ack. It must lose:
		// the item is already back in the queue and may be running elsewhere.
		res, err := store.Complete(t.Context(), storage.CompleteRequest{ID: it.ID, Worker: "slow-worker"})
		requireNoErr(t, err, "Complete")
		requireErrIs(t, res[0].Err, storage.ErrWrongState, "ack after the lease lapsed")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.State, storage.StateReady, "item is neither done nor lost; exactly one outcome")
	})

	t.Run("ReclaimHonoursItsLimit", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		for i := 0; i < 10; i++ {
			mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		}
		requireLen(t, claim(t, store, "q", "w1", 10), 10, "claimed all")

		clock.Advance(testLease + time.Second)
		res, err := store.ReclaimExpired(t.Context(), storage.ReclaimOpts{Limit: 4})
		requireNoErr(t, err, "ReclaimExpired")
		requireEqual(t, res.Reclaimed, 4, "limit respected")

		res, err = store.ReclaimExpired(t.Context(), storage.ReclaimOpts{Limit: 100})
		requireNoErr(t, err, "ReclaimExpired")
		requireEqual(t, res.Reclaimed, 6, "the rest on a second pass")
	})

	t.Run("ReclaimOnAnEmptyQueueIsANoOp", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		res, err := store.ReclaimExpired(t.Context(), storage.ReclaimOpts{Limit: 10})
		requireNoErr(t, err, "ReclaimExpired")
		requireEqual(t, res.Reclaimed, 0, "nothing to reclaim")
		requireEqual(t, res.DeadLettered, 0, "nothing to dead-letter")
	})
}
