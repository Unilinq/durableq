// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package storagetest

import (
	"fmt"
	"testing"
	"time"

	"github.com/unilinq/durableq/storage"
)

// emitItems builds n downstream items for the "next" queue.
func emitItems(n int, parentItemID string) []storage.NewItem {
	out := make([]storage.NewItem, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, storage.NewItem{
			Queue:        "next",
			ExecutionID:  "e1",
			StepID:       "next",
			ItemID:       fmt.Sprintf("child-%d", i),
			ParentItemID: parentItemID,
		})
	}
	return out
}

func producedOf(t *testing.T, store storage.Store, id int64) int {
	t.Helper()
	got, err := store.GetItem(t.Context(), id)
	requireNoErr(t, err, "GetItem")
	return got.Produced
}

// runEmit covers Store.Emit: a running item hands part of its output to the
// next queues before it finishes, fenced by state and lease owner.
func runEmit(t *testing.T, h Harness) {
	t.Run("RunningItemOfTheRightWorkerInsertsAndCounts", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q", ExecutionID: "e1", ItemID: "p1"})
		it := claim(t, store, "q", "w1", 1)[0]

		requireNoErr(t, store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "w1", Produced: emitItems(2, "p1"),
		}), "Emit")

		requireEqual(t, statFor(t, store, "next").Ready, 2, "emitted items are ready")
		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.Produced, 2, "produced count recorded on the emitting item")
		requireEqual(t, got.State, storage.StateRunning, "emit does not finish the item")
		requireEqual(t, got.LeasedBy, "w1", "emit keeps the lease")
	})

	t.Run("EmittedItemsKeepTheirLineage", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q", ExecutionID: "e1", ItemID: "p1"})
		it := claim(t, store, "q", "w1", 1)[0]

		requireNoErr(t, store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "w1", Produced: emitItems(1, "p1"),
		}), "Emit")

		children := claim(t, store, "next", "w2", 10)
		requireLen(t, children, 1, "emitted item is claimable at once")
		requireEqual(t, children[0].ExecutionID, "e1", "execution_id")
		requireEqual(t, children[0].StepID, "next", "step_id")
		requireEqual(t, children[0].ItemID, "child-0", "item_id")
		requireEqual(t, children[0].ParentItemID, "p1", "parent_item_id")
	})

	t.Run("EmitsAccumulateProduced", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "w1", 1)[0]

		requireNoErr(t, store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "w1", Produced: emitItems(2, ""),
		}), "first Emit")
		requireNoErr(t, store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "w1", Produced: emitItems(3, ""),
		}), "second Emit")

		requireEqual(t, producedOf(t, store, it.ID), 5, "produced is the sum of both emits")
		requireEqual(t, statFor(t, store, "next").Ready, 5, "both emits inserted")
	})

	t.Run("CompleteAddsToWhatEmitCounted", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "w1", 1)[0]

		requireNoErr(t, store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "w1", Produced: emitItems(2, ""),
		}), "Emit")

		res, err := store.Complete(t.Context(), storage.CompleteRequest{
			ID: it.ID, Worker: "w1", Produced: emitItems(3, ""),
		})
		requireNoErr(t, err, "Complete")
		requireNoErr(t, res[0].Err, "Complete result")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.Produced, 5, "Complete adds to the count Emit left, it does not overwrite it")
		requireEqual(t, got.State, storage.StateDone, "state")
		requireEqual(t, statFor(t, store, "next").Ready, 5, "emit and complete both inserted")
	})

	t.Run("CompleteWithNothingKeepsWhatEmitCounted", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "w1", 1)[0]

		requireNoErr(t, store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "w1", Produced: emitItems(4, ""),
		}), "Emit")
		res, err := store.Complete(t.Context(), storage.CompleteRequest{ID: it.ID, Worker: "w1"})
		requireNoErr(t, err, "Complete")
		requireNoErr(t, res[0].Err, "Complete result")

		requireEqual(t, producedOf(t, store, it.ID), 4, "an empty Complete must not reset produced")
	})

	t.Run("WrongWorkerIsRejectedAndInsertsNothing", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "owner", 1)[0]

		err := store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "impostor", Produced: emitItems(2, ""),
		})
		requireErrIs(t, err, storage.ErrLeaseLost, "Emit by a worker that does not hold the lease")

		_, nextExists := tryStat(t, store, "next")
		requireEqual(t, nextExists, false, "nothing inserted")
		requireEqual(t, producedOf(t, store, it.ID), 0, "produced untouched")
		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.State, storage.StateRunning, "item still owned by the real worker")
		requireEqual(t, got.LeasedBy, "owner", "lease untouched")
	})

	t.Run("ReadyItemIsRejected", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		it := mustEnqueue(t, store, storage.NewItem{Queue: "q"})[0]

		err := store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "w1", Produced: emitItems(1, ""),
		})
		requireErrIs(t, err, storage.ErrLeaseLost, "Emit on a ready item")
		_, nextExists := tryStat(t, store, "next")
		requireEqual(t, nextExists, false, "nothing inserted")
	})

	t.Run("DoneItemIsRejected", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "w1", 1)[0]
		res, err := store.Complete(t.Context(), storage.CompleteRequest{ID: it.ID, Worker: "w1"})
		requireNoErr(t, err, "Complete")
		requireNoErr(t, res[0].Err, "Complete result")

		err = store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "w1", Produced: emitItems(1, ""),
		})
		requireErrIs(t, err, storage.ErrLeaseLost, "Emit after the item was acked")
		_, nextExists := tryStat(t, store, "next")
		requireEqual(t, nextExists, false, "nothing inserted")
		requireEqual(t, producedOf(t, store, it.ID), 0, "produced untouched")
	})

	t.Run("RetriedItemIsRejected", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		policy := storage.Policy{MaxAttempts: 5, Schedule: []time.Duration{time.Hour}}
		mustEnqueue(t, store, storage.NewItem{Queue: "q", Policy: &policy})
		it := claim(t, store, "q", "w1", 1)[0]
		res, err := store.Retry(t.Context(), storage.RetryRequest{ID: it.ID, Worker: "w1", Error: "boom"})
		requireNoErr(t, err, "Retry")
		requireNoErr(t, res[0].Err, "Retry result")

		err = store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "w1", Produced: emitItems(1, ""),
		})
		requireErrIs(t, err, storage.ErrLeaseLost, "Emit after the item went back to ready")
		_, nextExists := tryStat(t, store, "next")
		requireEqual(t, nextExists, false, "nothing inserted")
	})

	t.Run("UnknownIDIsRejected", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		err := store.Emit(t.Context(), storage.EmitRequest{
			ID: missingID, Worker: "w1", Produced: emitItems(1, ""),
		})
		requireErrIs(t, err, storage.ErrLeaseLost, "Emit on an id that does not exist")
		_, nextExists := tryStat(t, store, "next")
		requireEqual(t, nextExists, false, "nothing inserted")
	})

	t.Run("LeaseReclaimedByAnotherWorkerIsRejected", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "slow", 1)[0]

		// The lease lapses and the reclaimer returns the item to the queue.
		clock.Advance(testLease + time.Second)
		_, err := store.ReclaimExpired(t.Context(), storage.ReclaimOpts{Limit: 10})
		requireNoErr(t, err, "ReclaimExpired")

		err = store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "slow", Produced: emitItems(2, ""),
		})
		requireErrIs(t, err, storage.ErrLeaseLost, "Emit after the lease was reclaimed")

		// Another worker takes it. The old owner still loses; the new one wins.
		again := claim(t, store, "q", "fast", 1)
		requireLen(t, again, 1, "reclaimed item is claimable")
		err = store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "slow", Produced: emitItems(2, ""),
		})
		requireErrIs(t, err, storage.ErrLeaseLost, "Emit by the old owner while another worker runs the item")
		_, nextExists := tryStat(t, store, "next")
		requireEqual(t, nextExists, false, "the old owner inserted nothing")

		requireNoErr(t, store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "fast", Produced: emitItems(1, ""),
		}), "Emit by the new owner")
		requireEqual(t, statFor(t, store, "next").Ready, 1, "only the new owner's emit exists")
	})

	t.Run("EmptyEmitByTheOwnerSucceeds", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "w1", 1)[0]

		requireNoErr(t, store.Emit(t.Context(), storage.EmitRequest{ID: it.ID, Worker: "w1"}), "empty Emit")
		requireEqual(t, producedOf(t, store, it.ID), 0, "nothing counted")
	})

	t.Run("LargeEmitIsInsertedWhole", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		it := claim(t, store, "q", "w1", 1)[0]

		// Large enough to exceed a single statement's bind-parameter limit if
		// the insert is not chunked.
		const n = 20000
		requireNoErr(t, store.Emit(t.Context(), storage.EmitRequest{
			ID: it.ID, Worker: "w1", Produced: emitItems(n, ""),
		}), "Emit")
		requireEqual(t, statFor(t, store, "next").Ready, n, "every emitted item inserted")
		requireEqual(t, producedOf(t, store, it.ID), n, "produced counts every emitted item")
	})
}

// tryStat reports a queue's stat, and whether the store knows the queue at all.
func tryStat(t *testing.T, store storage.Store, queue string) (storage.QueueStat, bool) {
	t.Helper()
	stats, err := store.Stats(t.Context(), queue)
	requireNoErr(t, err, "Stats")
	for _, s := range stats {
		if s.Queue == queue && s.Ready+s.Running+s.Done+s.DLQ > 0 {
			return s, true
		}
	}
	return storage.QueueStat{}, false
}
