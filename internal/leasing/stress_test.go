package leasing_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/unilinq/durableq/internal/leasing"
	"github.com/unilinq/durableq/storage"
)

type stubStore struct {
	storage.Store
	mu   sync.Mutex
	seen [][]int64
}

func (s *stubStore) ExtendLease(_ context.Context, _ string, _ time.Time, ids ...int64) ([]storage.Result, error) {
	s.mu.Lock()
	s.seen = append(s.seen, ids)
	s.mu.Unlock()
	out := make([]storage.Result, len(ids))
	for i, id := range ids {
		out[i].ID = id
	}
	return out, nil
}

// TestHeartbeaterStartStopStress catches shutdown races in the lease renewal
// loop, which runs for the whole life of every worker process.
func TestHeartbeaterStartStopStress(t *testing.T) {
	t.Parallel()
	store := &stubStore{}
	ids := []int64{1, 2, 3}

	for i := 0; i < 50; i++ {
		h := leasing.New(leasing.Config{
			Store:         store,
			Worker:        "w",
			Items:         func() []int64 { return ids },
			LeaseDuration: 30 * time.Millisecond,
			Interval:      time.Millisecond,
		})
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = h.Run(ctx)
		}()
		go func() {
			defer wg.Done()
			cancel()
		}()
		wg.Wait()
		h.Wait()
		cancel()
	}
}

// TestHeartbeaterSkipsEmptyBeats proves an idle worker does not hammer the
// database with empty renewals.
func TestHeartbeaterSkipsEmptyBeats(t *testing.T) {
	t.Parallel()
	store := &stubStore{}
	h := leasing.New(leasing.Config{
		Store:         store,
		Worker:        "w",
		Items:         func() []int64 { return nil },
		LeaseDuration: 30 * time.Millisecond,
		Interval:      time.Millisecond,
	})
	h.TestSignals.Init()

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = h.Run(ctx) }()
	for i := 0; i < 5; i++ {
		if n := h.TestSignals.Beat.WaitOrTimeout(t); n != 0 {
			t.Fatalf("idle heartbeat renewed %d items", n)
		}
	}
	cancel()
	h.Wait()

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.seen) != 0 {
		t.Fatalf("idle heartbeater called the store %d times", len(store.seen))
	}
}
