package storagetest

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/unilinq/durableq/storage"
)

// claimLedger records who claimed what, so "exactly one worker at a time" is an
// assertion about observed history rather than a count at the end.
type claimLedger struct {
	mu       sync.Mutex
	owner    map[int64]string // currently held
	everHeld map[int64]int    // total times claimed
	dupes    []string
}

func newClaimLedger() *claimLedger {
	return &claimLedger{owner: map[int64]string{}, everHeld: map[int64]int{}}
}

func (l *claimLedger) take(id int64, worker string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if prev, held := l.owner[id]; held {
		l.dupes = append(l.dupes, "item "+itoa(int(id))+" claimed by "+worker+" while held by "+prev)
	}
	l.owner[id] = worker
	l.everHeld[id]++
}

func (l *claimLedger) release(id int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.owner, id)
}

func (l *claimLedger) problems() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.dupes...)
}

func (l *claimLedger) distinct() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.everHeld)
}

func runContention(t *testing.T, h Harness) {
	// The property the whole system rests on: concurrent claimers never hand
	// the same item to two workers at once, and nothing is lost.
	t.Run("NoDoubleDelivery", func(t *testing.T) {
		t.Parallel()
		const (
			items   = 1000
			workers = 8
			batch   = 10
		)
		store, _ := h.New(t)

		var in []storage.NewItem
		for i := 0; i < items; i++ {
			in = append(in, storage.NewItem{Queue: "contended"})
		}
		// Insert in chunks so one statement does not carry 1000 tuples.
		for i := 0; i < len(in); i += 100 {
			end := min(i+100, len(in))
			mustEnqueue(t, store, in[i:end]...)
		}

		ledger := newClaimLedger()
		var completed int64
		var mu sync.Mutex
		var firstErr error

		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				worker := "worker-" + itoa(w)
				for {
					mu.Lock()
					done := completed >= items || firstErr != nil
					mu.Unlock()
					if done {
						return
					}
					got, err := store.Claim(context.Background(), storage.ClaimOpts{
						Queue: "contended", Worker: worker, Limit: batch, LeaseDuration: time.Minute,
					})
					if err != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						mu.Unlock()
						return
					}
					if len(got) == 0 {
						continue
					}
					reqs := make([]storage.CompleteRequest, 0, len(got))
					for _, it := range got {
						ledger.take(it.ID, worker)
						reqs = append(reqs, storage.CompleteRequest{ID: it.ID, Worker: worker})
					}
					res, err := store.Complete(context.Background(), reqs...)
					if err != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						mu.Unlock()
						return
					}
					n := 0
					for i, r := range res {
						ledger.release(got[i].ID)
						if r.Err == nil {
							n++
						}
					}
					mu.Lock()
					completed += int64(n)
					mu.Unlock()
				}
			}(w)
		}
		wg.Wait()

		if firstErr != nil {
			t.Fatalf("worker error during contention: %v", firstErr)
		}
		if problems := ledger.problems(); len(problems) > 0 {
			t.Fatalf("an item was delivered to two workers at once:\n%s", strings.Join(problems, "\n"))
		}
		requireEqual(t, ledger.distinct(), items, "every item claimed exactly once")
		requireEqual(t, int(completed), items, "every item completed exactly once")

		st := statFor(t, store, "contended")
		requireEqual(t, st.Done, items, "all items done")
		requireEqual(t, st.Ready, 0, "nothing left ready")
		requireEqual(t, st.Running, 0, "nothing left running")
	})

	// Claim, ack, retry, enqueue and reclaim all run at once against the same
	// rows. Any lock-ordering mistake shows up here as a deadlock, which is a
	// defect and not something to retry around.
	t.Run("NoDeadlocksUnderMixedLoad", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		for i := 0; i < 200; i++ {
			mustEnqueue(t, store, storage.NewItem{Queue: "mixed"})
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var (
			mu     sync.Mutex
			errs   []string
			rounds int
		)
		record := func(op string, err error) bool {
			if err == nil {
				return false
			}
			mu.Lock()
			defer mu.Unlock()
			errs = append(errs, op+": "+err.Error())
			return true
		}

		var wg sync.WaitGroup
		// Claimers that ack half and retry half.
		for w := 0; w < 6; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				worker := "mix-" + itoa(w)
				for i := 0; i < 40; i++ {
					if ctx.Err() != nil {
						return
					}
					got, err := store.Claim(ctx, storage.ClaimOpts{
						Queue: "mixed", Worker: worker, Limit: 5, LeaseDuration: time.Minute,
					})
					if record("claim", err) {
						return
					}
					for j, it := range got {
						if j%2 == 0 {
							_, err = store.Complete(ctx, storage.CompleteRequest{ID: it.ID, Worker: worker})
							if record("complete", err) {
								return
							}
						} else {
							_, err = store.Retry(ctx, storage.RetryRequest{ID: it.ID, Worker: worker, Error: "transient"})
							if record("retry", err) {
								return
							}
						}
					}
					mu.Lock()
					rounds++
					mu.Unlock()
				}
			}(w)
		}
		// A producer adding work under the claimers' feet.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				if ctx.Err() != nil {
					return
				}
				_, err := store.Enqueue(ctx, storage.NewItem{Queue: "mixed"})
				if record("enqueue", err) {
					return
				}
			}
		}()
		// A reclaimer sweeping for lapsed leases throughout.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 60; i++ {
				if ctx.Err() != nil {
					return
				}
				_, err := store.ReclaimExpired(ctx, storage.ReclaimOpts{Limit: 20})
				if record("reclaim", err) {
					return
				}
			}
		}()
		wg.Wait()

		mu.Lock()
		defer mu.Unlock()
		for _, e := range errs {
			// 40P01 is PostgreSQL's deadlock_detected.
			if strings.Contains(e, "40P01") || strings.Contains(strings.ToLower(e), "deadlock") {
				t.Fatalf("deadlock under mixed load: %s", e)
			}
		}
		if len(errs) > 0 {
			t.Fatalf("errors under mixed load (%d rounds completed):\n%s", rounds, strings.Join(errs, "\n"))
		}
	})

	// Two claimers racing for a single item: exactly one wins, and the loser is
	// told nothing rather than handed a duplicate.
	t.Run("SingleItemRace", func(t *testing.T) {
		t.Parallel()
		store, _ := h.New(t)
		mustEnqueue(t, store, storage.NewItem{Queue: "solo"})

		const racers = 16
		start := make(chan struct{})
		results := make(chan int, racers)
		var wg sync.WaitGroup
		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				got, err := store.Claim(context.Background(), storage.ClaimOpts{
					Queue: "solo", Worker: "racer-" + itoa(i), Limit: 1, LeaseDuration: time.Minute,
				})
				if err != nil {
					results <- -1
					return
				}
				results <- len(got)
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)

		wins := 0
		for n := range results {
			if n < 0 {
				t.Fatalf("a racer failed to claim")
			}
			wins += n
		}
		requireEqual(t, wins, 1, "exactly one racer may win a single item")
	})
}

// runDLQContention covers the three-way race on a dead-lettered item: an
// operator replaying it, the sweeper running, and a worker claiming. Exactly
// one coherent outcome must survive, and the item must never be lost.
func runDLQContention(t *testing.T, h Harness) {
	t.Run("ReplaySweepAndClaimOnOneItem", func(t *testing.T) {
		t.Parallel()
		store, clock := h.New(t)

		const items = 40
		ids := make([]int64, 0, items)
		for i := 0; i < items; i++ {
			ids = append(ids, deadLetter(t, store, "recovered", "transient").ID)
		}
		// Well past any retention, so a sweeper that wrongly touched the DLQ
		// would delete every one of these.
		clock.Advance(365 * 24 * time.Hour)

		var (
			mu      sync.Mutex
			errs    []string
			claimed int
		)
		note := func(op string, err error) {
			if err == nil {
				return
			}
			mu.Lock()
			errs = append(errs, op+": "+err.Error())
			mu.Unlock()
		}

		var wg sync.WaitGroup
		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					_, err := store.Replay(context.Background(), storage.ReplayOpts{
						Queue: "recovered" + storage.DLQSuffix, Limit: 5,
					})
					note("replay", err)
				}
			}()
		}
		for s := 0; s < 2; s++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 20; i++ {
					_, err := store.Sweep(context.Background(), storage.SweepOpts{
						Retention: time.Hour, Limit: 100,
					})
					note("sweep", err)
				}
			}()
		}
		for c := 0; c < 4; c++ {
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				for i := 0; i < 20; i++ {
					got, err := store.Claim(context.Background(), storage.ClaimOpts{
						Queue: "recovered", Worker: "rc-" + itoa(c), Limit: 5,
						LeaseDuration: time.Minute,
					})
					note("claim", err)
					if len(got) == 0 {
						continue
					}
					reqs := make([]storage.CompleteRequest, 0, len(got))
					for _, it := range got {
						reqs = append(reqs, storage.CompleteRequest{ID: it.ID, Worker: "rc-" + itoa(c)})
					}
					res, err := store.Complete(context.Background(), reqs...)
					note("complete", err)
					mu.Lock()
					for _, r := range res {
						if r.Err == nil {
							claimed++
						}
					}
					mu.Unlock()
				}
			}(c)
		}
		wg.Wait()

		mu.Lock()
		defer mu.Unlock()
		for _, e := range errs {
			if strings.Contains(e, "40P01") {
				t.Fatalf("deadlock during DLQ contention: %s", e)
			}
		}
		if len(errs) > 0 {
			t.Fatalf("errors during DLQ contention:\n%s", strings.Join(errs, "\n"))
		}

		// Every item must still be accounted for: nothing deleted by the
		// sweeper, nothing duplicated by a racing replay.
		seen := map[storage.State]int{}
		for _, id := range ids {
			it, err := store.GetItem(context.Background(), id)
			if err != nil {
				t.Fatalf("item %d disappeared during replay/sweep/claim contention: %v", id, err)
			}
			seen[it.State]++
		}
		total := seen[storage.StateReady] + seen[storage.StateRunning] +
			seen[storage.StateDone] + seen[storage.StateDLQ]
		requireEqual(t, total, items, "every item still accounted for")
		if seen[storage.StateDone] != claimed {
			t.Fatalf("completed %d items but %d are marked done", claimed, seen[storage.StateDone])
		}
	})
}
