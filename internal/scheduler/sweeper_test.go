// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package scheduler_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/unilinq/durableq/internal/scheduler"
	"github.com/unilinq/durableq/storage"
)

// fakeStore implements only what the sweeper touches. Embedding the interface
// means any other call panics loudly rather than silently succeeding.
type fakeStore struct {
	storage.Store
	mu    sync.Mutex
	calls []storage.SweepOpts
	fn    func(storage.SweepOpts) (int, error)
}

func (f *fakeStore) Sweep(_ context.Context, opts storage.SweepOpts) (int, error) {
	f.mu.Lock()
	f.calls = append(f.calls, opts)
	f.mu.Unlock()
	return f.fn(opts)
}

func (f *fakeStore) seen() []storage.SweepOpts {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]storage.SweepOpts(nil), f.calls...)
}

func TestSweeperPassesRetentionThrough(t *testing.T) {
	t.Parallel()
	store := &fakeStore{fn: func(storage.SweepOpts) (int, error) { return 7, nil }}
	s := scheduler.NewSweeper(scheduler.SweeperConfig{
		Store: store, Retention: 24 * time.Hour, BatchSize: 500, Queue: "indexing",
	})

	if got := s.Pass(context.Background()); got != 7 {
		t.Fatalf("Pass: got %d, want 7", got)
	}
	calls := store.seen()
	if len(calls) != 1 {
		t.Fatalf("expected one sweep, got %d", len(calls))
	}
	if calls[0].Retention != 24*time.Hour || calls[0].Limit != 500 || calls[0].Queue != "indexing" {
		t.Fatalf("sweep options not passed through: %+v", calls[0])
	}
}

// TestSweeperDoesNothingWhenRetentionIsNever proves "off" really is off: the
// store is never even asked.
func TestSweeperDoesNothingWhenRetentionIsNever(t *testing.T) {
	t.Parallel()
	store := &fakeStore{fn: func(storage.SweepOpts) (int, error) {
		t.Error("sweeper called the store with retention disabled")
		return 0, nil
	}}
	s := scheduler.NewSweeper(scheduler.SweeperConfig{
		Store: store, Retention: storage.RetentionNever,
	})
	if got := s.Pass(context.Background()); got != 0 {
		t.Fatalf("Pass: got %d, want 0", got)
	}
	if n := len(store.seen()); n != 0 {
		t.Fatalf("store was called %d times with retention disabled", n)
	}
}

// TestSweeperBreakerShrinksAndRecovers proves a sweeper that hits a database
// error backs off to a smaller batch and returns to full size once one lands,
// instead of retrying the same oversized statement for ever.
func TestSweeperBreakerShrinksAndRecovers(t *testing.T) {
	t.Parallel()
	var fail bool
	store := &fakeStore{fn: func(storage.SweepOpts) (int, error) {
		if fail {
			return 0, errors.New("statement too large")
		}
		return 1, nil
	}}
	s := scheduler.NewSweeper(scheduler.SweeperConfig{
		Store: store, Retention: time.Hour, BatchSize: 1000,
	})

	fail = true
	for i := 0; i < 3; i++ {
		s.Pass(context.Background())
	}
	calls := store.seen()
	if got := calls[len(calls)-1].Limit; got >= 1000 {
		t.Fatalf("batch did not shrink after failures: still %d", got)
	}

	fail = false
	s.Pass(context.Background())
	s.Pass(context.Background())
	calls = store.seen()
	if got := calls[len(calls)-1].Limit; got != 1000 {
		t.Fatalf("batch did not recover after a success: %d", got)
	}
}
