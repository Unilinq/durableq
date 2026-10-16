// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/unilinq/durableq/internal/dqtest"
	"github.com/unilinq/durableq/storage"
)

func nextItems(n int) []storage.NewItem {
	out := make([]storage.NewItem, n)
	for i := range out {
		out[i] = storage.NewItem{Queue: "next", ExecutionID: "e1", StepID: "next", ItemID: fmt.Sprintf("c%d", i)}
	}
	return out
}

func claimOne(t *testing.T, store storage.Store, worker string) storage.Item {
	t.Helper()
	got, err := store.Claim(t.Context(), storage.ClaimOpts{Queue: "q", Worker: worker, Limit: 1, LeaseDuration: time.Minute})
	if err != nil || len(got) != 1 {
		t.Fatalf("Claim: %v (%d items)", err, len(got))
	}
	return got[0]
}

// An emit under an earlier attempt is refused even when the same worker holds
// the item again.
func TestEmitAttemptMismatchIsRejectedAndInsertsNothing(t *testing.T) {
	t.Parallel()
	clock := dqtest.NewStubClockNow()
	store := dqtest.NewStore(t, clock)
	if _, err := store.Enqueue(t.Context(), storage.NewItem{Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	first := claimOne(t, store, "w1")
	res, err := store.Retry(t.Context(), storage.RetryRequest{ID: first.ID, Worker: "w1", Error: "x"})
	if err != nil || res[0].Err != nil {
		t.Fatalf("Retry: %v %v", err, res)
	}
	clock.Advance(time.Hour) // past the retry backoff
	second := claimOne(t, store, "w1")
	if second.Attempt != first.Attempt+1 {
		t.Fatalf("attempt %d, want %d", second.Attempt, first.Attempt+1)
	}

	err = store.Emit(t.Context(), storage.EmitRequest{ID: first.ID, Worker: "w1", Attempt: first.Attempt, Produced: nextItems(2)})
	if !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("Emit under attempt 1: got %v, want ErrLeaseLost", err)
	}
	got, err := store.GetItem(t.Context(), first.ID)
	if err != nil || got.Produced != 0 {
		t.Fatalf("produced %d (%v), want 0", got.Produced, err)
	}
	stats, err := store.Stats(t.Context(), "next")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	for _, s := range stats {
		if s.Queue == "next" && s.Ready > 0 {
			t.Fatalf("emit under the old attempt inserted %d items", s.Ready)
		}
	}
	if err := store.Emit(t.Context(), storage.EmitRequest{ID: first.ID, Worker: "w1", Attempt: second.Attempt, Produced: nextItems(2)}); err != nil {
		t.Fatalf("Emit under the current attempt: %v", err)
	}
}

// Complete commits more items than one statement can carry.
func TestCompleteLargeProducedIsInsertedWhole(t *testing.T) {
	t.Parallel()
	store := dqtest.NewStore(t, dqtest.NewStubClockNow())
	if _, err := store.Enqueue(t.Context(), storage.NewItem{Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	it := claimOne(t, store, "w1")
	const n = 20000
	res, err := store.Complete(t.Context(), storage.CompleteRequest{ID: it.ID, Worker: "w1", Produced: nextItems(n)})
	if err != nil || res[0].Err != nil {
		t.Fatalf("Complete: %v %v", err, res)
	}
	stats, err := store.Stats(t.Context(), "next")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	ready := 0
	for _, s := range stats {
		if s.Queue == "next" {
			ready = s.Ready
		}
	}
	if ready != n {
		t.Fatalf("%d items inserted, want %d", ready, n)
	}
	got, _ := store.GetItem(t.Context(), it.ID)
	if got.Produced != n {
		t.Fatalf("produced %d, want %d", got.Produced, n)
	}
}
