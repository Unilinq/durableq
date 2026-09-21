package storagetest

import (
	"testing"
	"time"

	"github.com/unilinq/durableq/storage"
)

func runEnqueue(t *testing.T, h Harness) {
	t.Run("AssignsIDsAndDefaults", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		now := clock.Now()

		items := mustEnqueue(t, store, storage.NewItem{Queue: "indexing", Payload: []byte(`{"url":"a"}`)})
		it := items[0]

		if it.ID == 0 {
			t.Fatalf("Enqueue: item has no id")
		}
		requireEqual(t, it.Queue, "indexing", "queue")
		requireEqual(t, it.State, storage.StateReady, "state")
		requireEqual(t, it.Attempt, 0, "attempt")
		requireEqual(t, string(it.Payload), `{"url": "a"}`, "payload round trip")
		if it.AvailableAt.Before(now.Add(-time.Second)) || it.AvailableAt.After(now.Add(time.Second)) {
			t.Fatalf("available_at: got %s, want about %s", it.AvailableAt, now)
		}
		if !it.FinalizedAt.IsZero() {
			t.Fatalf("finalized_at: a ready item must not be finalized, got %s", it.FinalizedAt)
		}
		if it.LeasedBy != "" {
			t.Fatalf("leased_by: a ready item must have no owner, got %q", it.LeasedBy)
		}
	})

	t.Run("EmptyPayloadDefaultsToEmptyObject", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		items := mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		requireEqual(t, string(items[0].Payload), `{}`, "default payload")
	})

	t.Run("DelayedItemKeepsItsAvailableAt", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		want := clock.Now().Add(90 * time.Second)
		items := mustEnqueue(t, store, storage.NewItem{Queue: "q", AvailableAt: want})
		requireTimeEqual(t, items[0].AvailableAt, want, "delayed available_at")
	})

	t.Run("PolicyIsSnapshottedOntoTheItem", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		p := storage.Policy{
			MaxAttempts: 3,
			Schedule:    []time.Duration{time.Second, 2 * time.Second},
			Jitter:      0,
		}
		items := mustEnqueue(t, store, storage.NewItem{Queue: "q", Policy: &p})
		got := items[0]
		requireEqual(t, got.MaxAttempts, 3, "max_attempts from policy")
		requireEqual(t, got.Policy.MaxAttempts, 3, "snapshotted policy max attempts")
		requireLen(t, got.Policy.Schedule, 2, "snapshotted schedule")
		requireEqual(t, got.Policy.Schedule[1], 2*time.Second, "snapshotted schedule value")

		// Re-reading must return the same snapshot, not a live default.
		reread, err := store.GetItem(t.Context(), got.ID)
		requireNoErr(t, err, "GetItem")
		requireEqual(t, reread.Policy.MaxAttempts, 3, "policy survives a round trip")
	})

	t.Run("LineageRoundTrips", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		items := mustEnqueue(t, store, storage.NewItem{
			Queue:        "crawl",
			ExecutionID:  "exec_123",
			StepID:       "crawl",
			ItemID:       "item_abc",
			ParentItemID: "item_root",
		})
		got := items[0]
		requireEqual(t, got.ExecutionID, "exec_123", "execution_id")
		requireEqual(t, got.StepID, "crawl", "step_id")
		requireEqual(t, got.ItemID, "item_abc", "item_id")
		requireEqual(t, got.ParentItemID, "item_root", "parent_item_id")
	})

	t.Run("BatchInsertReturnsEveryItem", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		var in []storage.NewItem
		for i := 0; i < 50; i++ {
			in = append(in, storage.NewItem{Queue: "bulk"})
		}
		out := mustEnqueue(t, store, in...)
		seen := map[int64]bool{}
		for _, it := range out {
			if seen[it.ID] {
				t.Fatalf("Enqueue: duplicate id %d in a batch", it.ID)
			}
			seen[it.ID] = true
		}
		requireEqual(t, statFor(t, store, "bulk").Ready, 50, "ready count after batch insert")
	})

	t.Run("NoItemsIsNotAnError", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		out, err := store.Enqueue(t.Context())
		requireNoErr(t, err, "Enqueue with no items")
		requireLen(t, out, 0, "empty enqueue result")
	})
}

func runGetItem(t *testing.T, h Harness) {
	t.Run("ReturnsErrNotFoundForMissingItem", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		_, err := store.GetItem(t.Context(), 987654321)
		requireErrIs(t, err, storage.ErrNotFound, "GetItem on a missing id")
	})
}

func runStats(t *testing.T, h Harness) {
	t.Run("CountsByState", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store,
			storage.NewItem{Queue: "a"},
			storage.NewItem{Queue: "a"},
			storage.NewItem{Queue: "b"},
		)
		requireEqual(t, statFor(t, store, "a").Ready, 2, "queue a ready")
		requireEqual(t, statFor(t, store, "b").Ready, 1, "queue b ready")
	})

	t.Run("IncludesKnownQueuesWithNoItems", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		requireNoErr(t, store.PauseQueue(t.Context(), "empty"), "PauseQueue")
		st := statFor(t, store, "empty")
		requireEqual(t, st.Ready, 0, "empty queue ready count")
		requireEqual(t, st.Paused, true, "empty queue paused")
	})

	t.Run("NoFilterReturnsEveryQueue", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "x"}, storage.NewItem{Queue: "y"})
		stats, err := store.Stats(t.Context())
		requireNoErr(t, err, "Stats")
		requireLen(t, stats, 2, "unfiltered stats")
	})
}

func runPauseResume(t *testing.T, h Harness) {
	t.Run("PauseThenResume", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "p"})
		requireEqual(t, statFor(t, store, "p").Paused, false, "queue starts unpaused")

		requireNoErr(t, store.PauseQueue(t.Context(), "p"), "PauseQueue")
		requireEqual(t, statFor(t, store, "p").Paused, true, "queue paused")

		requireNoErr(t, store.ResumeQueue(t.Context(), "p"), "ResumeQueue")
		requireEqual(t, statFor(t, store, "p").Paused, false, "queue resumed")
	})

	t.Run("PausingAnUnknownQueueCreatesIt", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		requireNoErr(t, store.PauseQueue(t.Context(), "never-seen"), "PauseQueue on unknown queue")
		requireEqual(t, statFor(t, store, "never-seen").Paused, true, "unknown queue paused")
	})

	t.Run("PauseIsIdempotent", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		requireNoErr(t, store.PauseQueue(t.Context(), "p"), "PauseQueue")
		requireNoErr(t, store.PauseQueue(t.Context(), "p"), "PauseQueue again")
		requireEqual(t, statFor(t, store, "p").Paused, true, "still paused")
	})
}

func runIsolation(t *testing.T, h Harness) {
	// Two stores built by the harness must not see each other's rows. This is
	// the property the whole parallel suite rests on.
	t.Run("StoresDoNotShareRows", func(t *testing.T) {
		t.Parallel()
		a, _ := h.New(t)
		b, _ := h.New(t)

		mustEnqueue(t, a, storage.NewItem{Queue: "shared-name"})

		statsB, err := b.Stats(t.Context())
		requireNoErr(t, err, "Stats on the second store")
		for _, s := range statsB {
			if s.Ready != 0 {
				t.Fatalf("isolation broken: second store sees %d ready items in %q", s.Ready, s.Queue)
			}
		}
		requireEqual(t, statFor(t, a, "shared-name").Ready, 1, "first store still sees its own row")
	})
}
