package storagetest

import (
	"testing"
	"time"

	"github.com/unilinq/durableq/storage"
)

// deadLetter drives an item all the way to its DLQ through the normal path.
func deadLetter(t *testing.T, store storage.Store, queue, errText string) storage.Item {
	t.Helper()
	items := mustEnqueue(t, store, storage.NewItem{Queue: queue})
	it := claim(t, store, queue, "w1", 1)
	requireLen(t, it, 1, "claim before dead-lettering")
	res, err := store.DeadLetter(t.Context(), storage.DeadLetterRequest{
		ID: it[0].ID, Worker: "w1", Error: errText,
	})
	requireNoErr(t, err, "DeadLetter")
	requireNoErr(t, res[0].Err, "DeadLetter result")
	got, err := store.GetItem(t.Context(), items[0].ID)
	requireNoErr(t, err, "GetItem")
	return got
}

func runReplay(t *testing.T, h Harness) {
	t.Run("ReturnsItemsToTheWorkingQueue", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		it := deadLetter(t, store, "indexing", "unsupported_pdf_encoding")
		requireEqual(t, it.State, storage.StateDLQ, "starts in the DLQ")

		n, err := store.Replay(t.Context(), storage.ReplayOpts{Queue: "indexing" + storage.DLQSuffix})
		requireNoErr(t, err, "Replay")
		requireEqual(t, n, 1, "one item replayed")

		got, err := store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.State, storage.StateReady, "back to ready")
		requireEqual(t, got.Queue, "indexing", "back on the working queue")
		requireEqual(t, got.Attempt, 0, "attempt budget reset")
		requireTimeEqual(t, got.AvailableAt, clock.Now(), "immediately claimable")
		if !got.FinalizedAt.IsZero() {
			t.Fatalf("replayed item is still marked finalized: %s", got.FinalizedAt)
		}

		// And it really can be worked again.
		again := claim(t, store, "indexing", "w2", 1)
		requireLen(t, again, 1, "replayed item is claimable")
		requireEqual(t, again[0].Attempt, 1, "first attempt of the new budget")
	})

	t.Run("HonoursItsLimit", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		for i := 0; i < 5; i++ {
			deadLetter(t, store, "q", "boom")
		}
		n, err := store.Replay(t.Context(), storage.ReplayOpts{Queue: "q" + storage.DLQSuffix, Limit: 2})
		requireNoErr(t, err, "Replay")
		requireEqual(t, n, 2, "limit respected")

		st := statFor(t, store, "q"+storage.DLQSuffix)
		requireEqual(t, st.DLQ, 3, "the rest stay dead-lettered")
	})

	t.Run("FiltersByError", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		deadLetter(t, store, "q", "unsupported_pdf_encoding")
		deadLetter(t, store, "q", "unsupported_pdf_encoding")
		deadLetter(t, store, "q", "connection_refused")

		// Only the class of failure the new worker version fixes.
		n, err := store.Replay(t.Context(), storage.ReplayOpts{
			Queue: "q" + storage.DLQSuffix, ErrorContains: "pdf",
		})
		requireNoErr(t, err, "Replay")
		requireEqual(t, n, 2, "only matching items replayed")
		requireEqual(t, statFor(t, store, "q"+storage.DLQSuffix).DLQ, 1, "the other failure is left alone")
	})

	t.Run("EmptyDLQIsANoOp", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		n, err := store.Replay(t.Context(), storage.ReplayOpts{Queue: "nothing" + storage.DLQSuffix})
		requireNoErr(t, err, "Replay")
		requireEqual(t, n, 0, "nothing to replay")
	})

	t.Run("DoesNotTouchRunningOrReadyItems", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		// Claim first, then add the item that stays ready: claiming takes the
		// oldest available item, so the order here is what decides which is
		// which.
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		running := claim(t, store, "q", "w1", 1)[0]
		ready := mustEnqueue(t, store, storage.NewItem{Queue: "q"})[0]

		n, err := store.Replay(t.Context(), storage.ReplayOpts{Queue: "q", Limit: 100})
		requireNoErr(t, err, "Replay against a working queue name")
		requireEqual(t, n, 0, "replay only moves dead-lettered items")

		r, err := store.GetItem(t.Context(), ready.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, r.State, storage.StateReady, "ready item untouched")
		w, err := store.GetItem(t.Context(), running.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, w.State, storage.StateRunning, "running item untouched")
	})
}

func runSweep(t *testing.T, h Harness) {
	// completeOne drives one item to done and returns it.
	completeOne := func(t *testing.T, store storage.Store, queue string) storage.Item {
		t.Helper()
		items := mustEnqueue(t, store, storage.NewItem{Queue: queue})
		it := claim(t, store, queue, "w1", 1)
		requireLen(t, it, 1, "claim")
		res, err := store.Complete(t.Context(), storage.CompleteRequest{ID: it[0].ID, Worker: "w1"})
		requireNoErr(t, err, "Complete")
		requireNoErr(t, res[0].Err, "Complete result")
		got, err := store.GetItem(t.Context(), items[0].ID)
		requireNoErr(t, err, "GetItem")
		return got
	}

	t.Run("DeletesDoneItemsPastRetention", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		old := completeOne(t, store, "q")

		clock.Advance(48 * time.Hour)
		fresh := completeOne(t, store, "q")

		n, err := store.Sweep(t.Context(), storage.SweepOpts{Retention: 24 * time.Hour})
		requireNoErr(t, err, "Sweep")
		requireEqual(t, n, 1, "only the item past retention is deleted")

		_, err = store.GetItem(t.Context(), old.ID)
		requireErrIs(t, err, storage.ErrNotFound, "old item swept")
		_, err = store.GetItem(t.Context(), fresh.ID)
		requireNoErr(t, err, "recent item kept")
	})

	t.Run("RetentionNeverDeletesNothing", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		it := completeOne(t, store, "q")
		clock.Advance(365 * 24 * time.Hour)

		n, err := store.Sweep(t.Context(), storage.SweepOpts{Retention: storage.RetentionNever})
		requireNoErr(t, err, "Sweep")
		requireEqual(t, n, 0, "retention off means off")

		_, err = store.GetItem(t.Context(), it.ID)
		requireNoErr(t, err, "item kept for ever")
	})

	t.Run("NeverSweepsTheDLQ", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		dead := deadLetter(t, store, "q", "boom")

		// A year past any plausible retention.
		clock.Advance(365 * 24 * time.Hour)
		n, err := store.Sweep(t.Context(), storage.SweepOpts{Retention: time.Hour})
		requireNoErr(t, err, "Sweep")
		requireEqual(t, n, 0, "the DLQ is the record of what went wrong; it is never swept on a timer")

		got, err := store.GetItem(t.Context(), dead.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, got.State, storage.StateDLQ, "still dead-lettered")
	})

	t.Run("NeverSweepsLiveWork", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		running := claim(t, store, "q", "w1", 1)[0]
		ready := mustEnqueue(t, store, storage.NewItem{Queue: "q"})[0]

		clock.Advance(365 * 24 * time.Hour)
		n, err := store.Sweep(t.Context(), storage.SweepOpts{Retention: time.Second})
		requireNoErr(t, err, "Sweep")
		requireEqual(t, n, 0, "a sweep must never race a live lease")

		_, err = store.GetItem(t.Context(), ready.ID)
		requireNoErr(t, err, "ready item kept")
		_, err = store.GetItem(t.Context(), running.ID)
		requireNoErr(t, err, "running item kept")
	})

	t.Run("HonoursBatchLimit", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		for i := 0; i < 5; i++ {
			completeOne(t, store, "q")
		}
		clock.Advance(48 * time.Hour)

		n, err := store.Sweep(t.Context(), storage.SweepOpts{Retention: time.Hour, Limit: 2})
		requireNoErr(t, err, "Sweep")
		requireEqual(t, n, 2, "batch limit respected")

		n, err = store.Sweep(t.Context(), storage.SweepOpts{Retention: time.Hour, Limit: 100})
		requireNoErr(t, err, "Sweep")
		requireEqual(t, n, 3, "the rest on a second pass")
	})

	t.Run("ScopedToOneQueue", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		a := completeOne(t, store, "a")
		b := completeOne(t, store, "b")
		clock.Advance(48 * time.Hour)

		n, err := store.Sweep(t.Context(), storage.SweepOpts{Queue: "a", Retention: time.Hour})
		requireNoErr(t, err, "Sweep")
		requireEqual(t, n, 1, "only queue a swept")

		_, err = store.GetItem(t.Context(), a.ID)
		requireErrIs(t, err, storage.ErrNotFound, "queue a item swept")
		_, err = store.GetItem(t.Context(), b.ID)
		requireNoErr(t, err, "queue b item kept")
	})
}
