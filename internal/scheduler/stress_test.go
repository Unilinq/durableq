package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/unilinq/durableq/internal/scheduler"
	"github.com/unilinq/durableq/storage"
)

type stressStore struct {
	storage.Store
	mu    sync.Mutex
	calls int
}

func (s *stressStore) ReclaimExpired(context.Context, storage.ReclaimOpts) (storage.ReclaimResult, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return storage.ReclaimResult{}, nil
}

func (s *stressStore) Sweep(context.Context, storage.SweepOpts) (int, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return 0, nil
}

// TestReclaimerStartStopStress catches shutdown races: a maintenance loop
// started and stopped repeatedly from many goroutines must never race and must
// always return.
func TestReclaimerStartStopStress(t *testing.T) {
	t.Parallel()
	store := &stressStore{}

	for i := 0; i < 50; i++ {
		r := scheduler.NewReclaimer(scheduler.ReclaimerConfig{
			Store: store, Interval: time.Millisecond, BatchSize: 10,
		})
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.Run(ctx)
		}()
		// Operators running a pass by hand while the loop runs.
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r.Pass(ctx)
			}()
		}
		// Cancellation racing all of it.
		wg.Add(1)
		go func() {
			defer wg.Done()
			cancel()
		}()
		wg.Wait()
		r.Wait()
		cancel()
	}
}

// TestSweeperStartStopStress is the same for retention sweeping.
func TestSweeperStartStopStress(t *testing.T) {
	t.Parallel()
	store := &stressStore{}

	for i := 0; i < 50; i++ {
		s := scheduler.NewSweeper(scheduler.SweeperConfig{
			Store: store, Interval: time.Millisecond, Retention: time.Hour, BatchSize: 10,
		})
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Run(ctx)
		}()
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.Pass(ctx)
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			cancel()
		}()
		wg.Wait()
		s.Wait()
		cancel()
	}
}

// TestSweeperWithRetentionNeverStillStops proves the disabled path is not a
// goroutine that ignores cancellation.
func TestSweeperWithRetentionNeverStillStops(t *testing.T) {
	t.Parallel()
	s := scheduler.NewSweeper(scheduler.SweeperConfig{
		Store: &stressStore{}, Retention: storage.RetentionNever,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = s.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sweeper with retention disabled ignored cancellation")
	}
}
