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
	"github.com/unilinq/durableq/internal/telemetry"
	"github.com/unilinq/durableq/storage"
)

// fakeStore implements only what the sweeper touches. Embedding the interface
// means any other call panics loudly rather than silently succeeding.
type fakeStore struct {
	storage.Store
	mu    sync.Mutex
	calls []storage.SweepOpts
	fn    func(storage.SweepOpts) (storage.SweepResult, error)
}

func (f *fakeStore) Sweep(_ context.Context, opts storage.SweepOpts) (storage.SweepResult, error) {
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

// foldingStore is a fakeStore that can fold.
type foldingStore struct {
	*fakeStore
	prepared []storage.FoldOpts
	err      error
}

func (f *foldingStore) PrepareFold(_ context.Context, opts storage.FoldOpts) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepared = append(f.prepared, opts)
	if f.err != nil {
		return f.err
	}
	// Preparing comes before any batch of the pass.
	if len(f.calls) > 0 {
		return errors.New("PrepareFold after a batch")
	}
	return nil
}

// stepClock advances by step every time it is read.
type stepClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.now
	c.now = c.now.Add(c.step)
	return t
}

// recordingSink records the sweep metrics.
type recordingSink struct {
	telemetry.Nop
	mu      sync.Mutex
	swept   []int
	backlog []time.Duration
}

func (r *recordingSink) ItemsSwept(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.swept = append(r.swept, n)
}

func (r *recordingSink) SweepBacklog(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backlog = append(r.backlog, d)
}

var epoch = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

// itemsFrom is when the first item fullBatches deletes finished.
var itemsFrom = epoch.Add(-2 * time.Hour)

// fullBatches answers full batches until total items are gone. Each batch's
// newest item finished one second after the previous batch's, starting at
// itemsFrom.
func fullBatches(total int) func(storage.SweepOpts) (storage.SweepResult, error) {
	left := total
	last := itemsFrom
	return func(o storage.SweepOpts) (storage.SweepResult, error) {
		n := min(left, o.Limit)
		left -= n
		if n == 0 {
			return storage.SweepResult{}, nil
		}
		last = last.Add(time.Second)
		return storage.SweepResult{Deleted: n, Last: last}, nil
	}
}

func TestSweeperPassesRetentionThrough(t *testing.T) {
	t.Parallel()
	store := &fakeStore{fn: func(storage.SweepOpts) (storage.SweepResult, error) {
		return storage.SweepResult{Deleted: 7}, nil
	}}
	s := scheduler.NewSweeper(scheduler.SweeperConfig{
		Store: store, Retention: 24 * time.Hour, BatchSize: 500, Queue: "indexing",
	})

	res, err := s.Pass(context.Background())
	if err != nil || res.Deleted != 7 || res.Batches != 1 {
		t.Fatalf("Pass: got %+v, %v; want 7 deleted in 1 batch", res, err)
	}
	calls := store.seen()
	if len(calls) != 1 {
		t.Fatalf("expected one sweep, got %d", len(calls))
	}
	if calls[0].Retention != 24*time.Hour || calls[0].Limit != 500 || calls[0].Queue != "indexing" ||
		calls[0].Fold || !calls[0].After.IsZero() {
		t.Fatalf("sweep options not passed through: %+v", calls[0])
	}
}

// TestSweeperDoesNothingWhenRetentionIsNever proves "off" really is off: the
// store is never even asked.
func TestSweeperDoesNothingWhenRetentionIsNever(t *testing.T) {
	t.Parallel()
	store := &fakeStore{fn: func(storage.SweepOpts) (storage.SweepResult, error) {
		t.Error("sweeper called the store with retention disabled")
		return storage.SweepResult{}, nil
	}}
	s := scheduler.NewSweeper(scheduler.SweeperConfig{
		Store: store, Retention: storage.RetentionNever,
	})
	if res, _ := s.Pass(context.Background()); res.Deleted != 0 {
		t.Fatalf("Pass: got %d, want 0", res.Deleted)
	}
	if n := len(store.seen()); n != 0 {
		t.Fatalf("store was called %d times with retention disabled", n)
	}
}

// TestSweeperRetentionZero proves the sentinel reaches the store as zero.
func TestSweeperRetentionZero(t *testing.T) {
	t.Parallel()
	store := &fakeStore{fn: fullBatches(0)}
	s := scheduler.NewSweeper(scheduler.SweeperConfig{Store: store, Retention: storage.RetentionZero})
	if _, err := s.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.seen()[0].Retention; got != 0 {
		t.Fatalf("Retention: got %v, want 0", got)
	}
}

// TestSweeperOneBatchWithoutBudget proves a sweeper with no budget deletes one
// batch per pass, as it did before the budget existed.
func TestSweeperOneBatchWithoutBudget(t *testing.T) {
	t.Parallel()
	store := &fakeStore{fn: fullBatches(5000)}
	s := scheduler.NewSweeper(scheduler.SweeperConfig{Store: store, Retention: time.Hour, BatchSize: 1000})

	res, err := s.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 1000 || res.Batches != 1 || !res.OutOfTime {
		t.Fatalf("got %+v, want one full batch and out of time", res)
	}
}

// TestSweeperCatchesUp proves a pass keeps deleting until a batch comes back
// short, with one cutoff for the whole pass and each batch starting where the
// previous one ended.
func TestSweeperCatchesUp(t *testing.T) {
	t.Parallel()
	store := &fakeStore{fn: fullBatches(3500)}
	clock := &stepClock{now: epoch}
	s := scheduler.NewSweeper(scheduler.SweeperConfig{
		Store: store, Retention: time.Hour, BatchSize: 1000, Budget: time.Minute, Clock: clock,
	})

	res, err := s.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 3500 || res.Batches != 4 || res.OutOfTime || res.Backlog != 0 {
		t.Fatalf("got %+v, want 3500 in 4 batches, caught up", res)
	}
	calls := store.seen()
	for i, c := range calls {
		if !c.Now.Equal(epoch) {
			t.Fatalf("batch %d: Now %v, want the pass start %v (the cutoff must not move)", i, c.Now, epoch)
		}
		want := time.Time{}
		if i > 0 {
			want = itemsFrom.Add(time.Duration(i) * time.Second)
		}
		if !c.After.Equal(want) {
			t.Fatalf("batch %d: After %v, want %v", i, c.After, want)
		}
	}
}

// TestSweeperStopsAtBudget proves a pass gives up once its budget is spent on
// full batches, and reports how far behind it is.
func TestSweeperStopsAtBudget(t *testing.T) {
	t.Parallel()
	store := &fakeStore{fn: fullBatches(1_000_000)}
	// Every read of the clock is 10s later: the start, then one per batch.
	clock := &stepClock{now: epoch, step: 10 * time.Second}
	sink := &recordingSink{}
	s := scheduler.NewSweeper(scheduler.SweeperConfig{
		Store: store, Retention: time.Hour, BatchSize: 100, Budget: 30 * time.Second,
		Clock: clock, Metrics: sink,
	})

	res, err := s.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Batches != 3 || res.Deleted != 300 || !res.OutOfTime {
		t.Fatalf("got %+v, want 3 batches then out of time", res)
	}
	// The cutoff is an hour before epoch; the third batch's newest item
	// finished 3s after two hours before it.
	if want := time.Hour - 3*time.Second; res.Backlog != want {
		t.Fatalf("Backlog: got %v, want %v", res.Backlog, want)
	}
	if len(sink.swept) != 1 || sink.swept[0] != 300 {
		t.Fatalf("ItemsSwept: got %v, want [300]", sink.swept)
	}
	if len(sink.backlog) != 1 || sink.backlog[0] != res.Backlog {
		t.Fatalf("SweepBacklog: got %v, want [%v]", sink.backlog, res.Backlog)
	}
}

// TestSweeperStopsOnShutdown proves a pass ends after the batch in flight
// when its context is cancelled.
func TestSweeperStopsOnShutdown(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	inner := fullBatches(1_000_000)
	batches := 0
	store := &fakeStore{fn: func(o storage.SweepOpts) (storage.SweepResult, error) {
		batches++
		if batches == 2 {
			cancel()
		}
		return inner(o)
	}}
	s := scheduler.NewSweeper(scheduler.SweeperConfig{
		Store: store, Retention: time.Hour, BatchSize: 10, Budget: time.Hour,
	})
	res, err := s.Pass(ctx)
	if !errors.Is(err, context.Canceled) || res.Batches != 2 {
		t.Fatalf("got %+v, %v; want 2 batches and context.Canceled", res, err)
	}
}

// TestSweeperBreakerShrinksAndRecovers proves a sweeper that hits a database
// error backs off to a smaller batch and returns to full size once one lands,
// instead of retrying the same oversized statement for ever.
func TestSweeperBreakerShrinksAndRecovers(t *testing.T) {
	t.Parallel()
	var fail bool
	store := &fakeStore{fn: func(storage.SweepOpts) (storage.SweepResult, error) {
		if fail {
			return storage.SweepResult{}, errors.New("statement too large")
		}
		return storage.SweepResult{Deleted: 1}, nil
	}}
	s := scheduler.NewSweeper(scheduler.SweeperConfig{
		Store: store, Retention: time.Hour, BatchSize: 1000,
	})

	fail = true
	for i := 0; i < 3; i++ {
		if _, err := s.Pass(context.Background()); err == nil {
			t.Fatal("Pass: want the store's error")
		}
	}
	calls := store.seen()
	if got := calls[len(calls)-1].Limit; got != 250 {
		t.Fatalf("batch did not halve after each failure: %d, want 250", got)
	}

	fail = false
	_, _ = s.Pass(context.Background())
	_, _ = s.Pass(context.Background())
	calls = store.seen()
	if got := calls[len(calls)-1].Limit; got != 1000 {
		t.Fatalf("batch did not recover after a success: %d", got)
	}
}

// TestSweeperFolds proves a folding sweeper prepares the store once per pass,
// before any batch, and asks every batch to fold with the job labels.
func TestSweeperFolds(t *testing.T) {
	t.Parallel()
	store := &foldingStore{fakeStore: &fakeStore{fn: fullBatches(150)}}
	labels := map[string][]string{"ingest": {"gone", "excluded"}}
	s := scheduler.NewSweeper(scheduler.SweeperConfig{
		Store: store, Retention: storage.RetentionZero, BatchSize: 100, Budget: time.Hour,
		Fold: true, HistoryRetention: 48 * time.Hour,
		FilterLabels: func() map[string][]string { return labels },
		Clock:        &stepClock{now: epoch},
	})

	res, err := s.Pass(context.Background())
	if err != nil || res.Deleted != 150 {
		t.Fatalf("got %+v, %v", res, err)
	}
	if len(store.prepared) != 1 || !store.prepared[0].Now.Equal(epoch) ||
		store.prepared[0].HistoryRetention != 48*time.Hour {
		t.Fatalf("PrepareFold: got %+v, want one call at the pass start", store.prepared)
	}
	for i, c := range store.seen() {
		if !c.Fold || c.HistoryRetention != 48*time.Hour || len(c.FilterLabels["ingest"]) != 2 {
			t.Fatalf("batch %d not folding with the labels: %+v", i, c)
		}
	}
}

// TestSweeperDoesNotDeleteWhenItCannotFold proves a sweeper asked to fold
// never deletes without folding: a store that cannot fold, or a failed
// preparation, sweeps nothing.
func TestSweeperDoesNotDeleteWhenItCannotFold(t *testing.T) {
	t.Parallel()
	never := func(storage.SweepOpts) (storage.SweepResult, error) {
		t.Error("swept without folding")
		return storage.SweepResult{}, nil
	}

	plain := scheduler.NewSweeper(scheduler.SweeperConfig{
		Store: &fakeStore{fn: never}, Retention: time.Hour, Fold: true,
	})
	if _, err := plain.Pass(context.Background()); err == nil {
		t.Fatal("Pass over a store that cannot fold: want an error")
	}

	broken := scheduler.NewSweeper(scheduler.SweeperConfig{
		Store:     &foldingStore{fakeStore: &fakeStore{fn: never}, err: errors.New("no partition")},
		Retention: time.Hour, Fold: true,
	})
	if _, err := broken.Pass(context.Background()); err == nil {
		t.Fatal("Pass after a failed PrepareFold: want an error")
	}
}
