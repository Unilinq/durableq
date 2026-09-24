// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package storagetest

import (
	"testing"
	"time"

	"github.com/unilinq/durableq/storage"
)

const testLease = 30 * time.Second

func claim(t *testing.T, store storage.Store, queue, worker string, limit int) []storage.Item {
	t.Helper()
	items, err := store.Claim(t.Context(), storage.ClaimOpts{
		Queue: queue, Worker: worker, Limit: limit, LeaseDuration: testLease,
	})
	requireNoErr(t, err, "Claim")
	return items
}

func runClaim(t *testing.T, h Harness) {
	t.Run("HonoursLimit", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		for i := 0; i < 10; i++ {
			mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		}
		requireLen(t, claim(t, store, "q", "w1", 3), 3, "claim limited to 3")
		requireLen(t, claim(t, store, "q", "w1", 100), 7, "claim of the remainder")
		requireLen(t, claim(t, store, "q", "w1", 100), 0, "claim on a drained queue")
	})

	t.Run("ConstrainedToQueue", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "a"}, storage.NewItem{Queue: "b"})
		got := claim(t, store, "a", "w1", 10)
		requireLen(t, got, 1, "claim from queue a")
		requireEqual(t, got[0].Queue, "a", "claimed item queue")
	})

	t.Run("SkipsItemsNotYetAvailable", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		now := clock.Now()
		mustEnqueue(t, store,
			storage.NewItem{Queue: "q", AvailableAt: now.Add(-time.Second)},
			storage.NewItem{Queue: "q", AvailableAt: now.Add(time.Hour)},
		)
		requireLen(t, claim(t, store, "q", "w1", 10), 1, "only the available item is claimed")

		// The future item becomes claimable purely by moving the clock. No
		// sleeping, and no second enqueue.
		clock.Advance(2 * time.Hour)
		requireLen(t, claim(t, store, "q", "w1", 10), 1, "delayed item after the clock advances")
	})

	t.Run("HonoursAnInjectedNow", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		now := clock.Now()
		mustEnqueue(t, store, storage.NewItem{Queue: "q", AvailableAt: now.Add(time.Hour)})

		requireLen(t, claim(t, store, "q", "w1", 10), 0, "not yet available")

		got, err := store.Claim(t.Context(), storage.ClaimOpts{
			Queue: "q", Worker: "w1", Limit: 10, LeaseDuration: testLease,
			Now: now.Add(2 * time.Hour),
		})
		requireNoErr(t, err, "Claim with an explicit now")
		requireLen(t, got, 1, "claim with a now override")
	})

	t.Run("OrderIsDeterministic", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		now := clock.Now()
		// Enqueued newest-first so insertion order cannot be what makes the
		// assertion pass.
		third := mustEnqueue(t, store, storage.NewItem{Queue: "q", AvailableAt: now.Add(-1 * time.Second)})[0]
		first := mustEnqueue(t, store, storage.NewItem{Queue: "q", AvailableAt: now.Add(-3 * time.Second)})[0]
		second := mustEnqueue(t, store, storage.NewItem{Queue: "q", AvailableAt: now.Add(-2 * time.Second)})[0]

		got := claim(t, store, "q", "w1", 3)
		requireLen(t, got, 3, "claimed all three")
		requireEqual(t, got[0].ID, first.ID, "oldest available_at first")
		requireEqual(t, got[1].ID, second.ID, "second by available_at")
		requireEqual(t, got[2].ID, third.ID, "third by available_at")
	})

	t.Run("TiesBreakByID", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		at := clock.Now().Add(-time.Second)
		a := mustEnqueue(t, store, storage.NewItem{Queue: "q", AvailableAt: at})[0]
		b := mustEnqueue(t, store, storage.NewItem{Queue: "q", AvailableAt: at})[0]

		got := claim(t, store, "q", "w1", 2)
		requireLen(t, got, 2, "claimed both")
		requireEqual(t, got[0].ID, a.ID, "lower id first on an available_at tie")
		requireEqual(t, got[1].ID, b.ID, "higher id second")
	})

	t.Run("IncrementsAttemptAndSetsLease", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		got := claim(t, store, "q", "worker-17", 1)[0]

		requireEqual(t, got.State, storage.StateRunning, "state after claim")
		requireEqual(t, got.Attempt, 1, "attempt after first claim")
		requireEqual(t, got.LeasedBy, "worker-17", "leased_by")
		requireTimeEqual(t, got.LeaseUntil, clock.Now().Add(testLease), "lease_until")
	})

	t.Run("OverLongWorkerIdentityIsTruncatedNotRejected", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})

		long := ""
		for i := 0; i < 400; i++ {
			long += "x"
		}
		got := claim(t, store, "q", long, 1)
		requireLen(t, got, 1, "claim with an over-long worker identity")
		if len(got[0].LeasedBy) >= len(long) {
			t.Fatalf("leased_by was not truncated: stored %d characters", len(got[0].LeasedBy))
		}
		if len(got[0].LeasedBy) == 0 {
			t.Fatalf("leased_by was truncated to nothing")
		}
	})

	t.Run("SkipsPausedQueues", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "p"})
		requireNoErr(t, store.PauseQueue(t.Context(), "p"), "PauseQueue")
		requireLen(t, claim(t, store, "p", "w1", 10), 0, "claim against a paused queue")

		requireNoErr(t, store.ResumeQueue(t.Context(), "p"), "ResumeQueue")
		requireLen(t, claim(t, store, "p", "w1", 10), 1, "claim after resume")
	})

	t.Run("ZeroLimitClaimsNothing", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "q"})
		requireLen(t, claim(t, store, "q", "w1", 0), 0, "claim with limit 0")
		requireEqual(t, statFor(t, store, "q").Ready, 1, "item untouched")
	})
}
