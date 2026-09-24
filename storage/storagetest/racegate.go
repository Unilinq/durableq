// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package storagetest

import (
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unilinq/durableq/storage"
)

// soakDuration is short by default so the gate can be run repeatedly, and long
// when DURABLEQ_RACE_SOAK is set for the full-length evidence run.
func soakDuration() time.Duration {
	if os.Getenv("DURABLEQ_RACE_SOAK") != "" {
		return 60 * time.Second
	}
	return 3 * time.Second
}

// runRaceGate is the concurrency gate: every case asserts an invariant about
// observed history, not a count reached after waiting.
func runRaceGate(t *testing.T, h Harness) {
	t.Run("NoDoubleDeliveryAtScale", func(t *testing.T) { raceNoDoubleDelivery(t, h) })
	t.Run("AckRacesReclaim", func(t *testing.T) { raceAckVsReclaim(t, h) })
	t.Run("TransitionsRaceEachOther", func(t *testing.T) { raceTransitions(t, h) })
	t.Run("RetryBoundary", func(t *testing.T) { raceRetryBoundary(t, h) })
	t.Run("FanOutAtomicity", func(t *testing.T) { raceFanOutAtomicity(t, h) })
	t.Run("SustainedMixedLoad", func(t *testing.T) { raceSustained(t, h) })
	t.Run("ClockSkew", func(t *testing.T) { raceClockSkew(t, h) })
}

// raceNoDoubleDelivery is criterion one: whatever the worker count, an item is
// handed to exactly one worker at a time and nothing is lost.
func raceNoDoubleDelivery(t *testing.T, h Harness) {
	for _, workers := range []int{8, 16, 32} {
		t.Run(itoa(workers)+"Workers", func(t *testing.T) {
			t.Parallel()
			const items = 2000
			store, _ := h.New(t)

			batch := make([]storage.NewItem, 0, 200)
			for i := 0; i < items; i++ {
				batch = append(batch, storage.NewItem{Queue: "scale"})
				if len(batch) == 200 {
					mustEnqueue(t, store, batch...)
					batch = batch[:0]
				}
			}
			if len(batch) > 0 {
				mustEnqueue(t, store, batch...)
			}

			ledger := newClaimLedger()
			var completed atomic.Int64
			var firstErr atomic.Value

			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					worker := "w" + itoa(w)
					for completed.Load() < items {
						if firstErr.Load() != nil {
							return
						}
						got, err := store.Claim(context.Background(), storage.ClaimOpts{
							Queue: "scale", Worker: worker, Limit: 7, LeaseDuration: time.Minute,
						})
						if err != nil {
							firstErr.Store(err)
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
							firstErr.Store(err)
							return
						}
						for i, r := range res {
							ledger.release(got[i].ID)
							if r.Err == nil {
								completed.Add(1)
							}
						}
					}
				}(w)
			}
			wg.Wait()

			if err := firstErr.Load(); err != nil {
				t.Fatalf("%d workers: %v", workers, err)
			}
			if problems := ledger.problems(); len(problems) > 0 {
				t.Fatalf("%d workers: an item was held by two at once:\n%s",
					workers, strings.Join(problems, "\n"))
			}
			requireEqual(t, ledger.distinct(), items, "every item claimed exactly once")
			requireEqual(t, int(completed.Load()), items, "every item completed exactly once")
			requireEqual(t, statFor(t, store, "scale").Done, items, "all done in the database")
		})
	}
}

// raceAckVsReclaim is the invariant that keeps at-least-once from becoming
// at-least-twice-and-also-lost: a worker acking after its lease lapsed, while
// the reclaimer acts on the same item, must produce exactly one outcome.
func raceAckVsReclaim(t *testing.T, h Harness) {
	t.Parallel()
	const items = 200
	store, clock := h.New(t)

	ids := make([]int64, 0, items)
	for i := 0; i < items; i++ {
		got := mustEnqueue(t, store, storage.NewItem{Queue: "expiring"})
		ids = append(ids, got[0].ID)
	}
	claimed := claim(t, store, "expiring", "slow-worker", items)
	requireLen(t, claimed, items, "claimed everything")

	// Every lease lapses at once.
	clock.Advance(testLease + time.Second)

	start := make(chan struct{})
	var wg sync.WaitGroup
	var ackOK, ackRejected atomic.Int64

	// The worker finally finishing.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := w; i < len(claimed); i += 4 {
				res, err := store.Complete(context.Background(), storage.CompleteRequest{
					ID: claimed[i].ID, Worker: "slow-worker",
				})
				if err != nil {
					continue
				}
				if res[0].Err == nil {
					ackOK.Add(1)
				} else {
					ackRejected.Add(1)
				}
			}
		}(w)
	}
	// The reclaimer, at the same moment.
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 20; i++ {
				_, _ = store.ReclaimExpired(context.Background(), storage.ReclaimOpts{Limit: 50})
			}
		}()
	}
	close(start)
	wg.Wait()

	// The invariant: each item is in exactly one terminal-or-ready state, and
	// an acked item is never also back in the queue.
	var done, ready, running, dead int
	for _, id := range ids {
		it, err := store.GetItem(context.Background(), id)
		requireNoErr(t, err, "GetItem after the race")
		switch it.State {
		case storage.StateDone:
			done++
			if it.LeasedBy != "" {
				t.Fatalf("item %d is done but still leased by %q", id, it.LeasedBy)
			}
		case storage.StateReady:
			ready++
		case storage.StateRunning:
			running++
		case storage.StateDLQ:
			dead++
		}
	}
	requireEqual(t, done+ready+running+dead, items, "every item in exactly one state")
	requireEqual(t, int(ackOK.Load()), done, "acks that succeeded match the items marked done")
	if ackOK.Load()+ackRejected.Load() != items {
		t.Fatalf("acks accounted for %d of %d items", ackOK.Load()+ackRejected.Load(), items)
	}
}

// raceTransitions fires every competing transition at one item at once. All
// orderings are exercised by racing them, and exactly one must win.
func raceTransitions(t *testing.T, h Harness) {
	t.Parallel()
	store, _ := h.New(t)

	const rounds = 60
	for i := 0; i < rounds; i++ {
		mustEnqueue(t, store, storage.NewItem{Queue: "contended"})
	}
	items := claim(t, store, "contended", "w1", rounds)
	requireLen(t, items, rounds, "claimed every item")

	for _, it := range items {
		it := it
		start := make(chan struct{})
		var wg sync.WaitGroup
		var wins atomic.Int64

		record := func(res []storage.Result, err error) {
			if err != nil || len(res) == 0 {
				return
			}
			if res[0].Err == nil {
				wins.Add(1)
			}
		}

		wg.Add(4)
		go func() {
			defer wg.Done()
			<-start
			record(store.Complete(context.Background(), storage.CompleteRequest{ID: it.ID, Worker: "w1"}))
		}()
		go func() {
			defer wg.Done()
			<-start
			record(store.Retry(context.Background(), storage.RetryRequest{ID: it.ID, Worker: "w1", Error: "x"}))
		}()
		go func() {
			defer wg.Done()
			<-start
			record(store.DeadLetter(context.Background(), storage.DeadLetterRequest{ID: it.ID, Worker: "w1", Error: "x"}))
		}()
		go func() {
			defer wg.Done()
			<-start
			record(store.Release(context.Background(), storage.ReleaseRequest{ID: it.ID, Worker: "w1"}))
		}()
		close(start)
		wg.Wait()

		if got := wins.Load(); got != 1 {
			cur, _ := store.GetItem(context.Background(), it.ID)
			t.Fatalf("item %d: %d transitions succeeded, want exactly 1 (item is now %s on %s)",
				it.ID, got, cur.State, cur.Queue)
		}
	}
}

// raceRetryBoundary covers the moment an item becomes available: a claimer
// scanning exactly then must take it once, and never skip it for ever.
func raceRetryBoundary(t *testing.T, h Harness) {
	t.Parallel()
	store, clock := h.New(t)

	const items = 100
	p := storage.Policy{MaxAttempts: 10, Schedule: []time.Duration{time.Second}, Jitter: 0}
	ids := make([]int64, 0, items)
	for i := 0; i < items; i++ {
		got := mustEnqueue(t, store, storage.NewItem{Queue: "boundary", Policy: &p})
		ids = append(ids, got[0].ID)
	}
	first := claim(t, store, "boundary", "w1", items)
	requireLen(t, first, items, "claimed everything")
	reqs := make([]storage.RetryRequest, 0, items)
	for _, it := range first {
		reqs = append(reqs, storage.RetryRequest{ID: it.ID, Worker: "w1", Error: "transient"})
	}
	_, err := store.Retry(context.Background(), reqs...)
	requireNoErr(t, err, "Retry")

	// Claimers scan continuously across the instant the backoff elapses.
	ledger := newClaimLedger()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			worker := "b" + itoa(w)
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, err := store.Claim(context.Background(), storage.ClaimOpts{
					Queue: "boundary", Worker: worker, Limit: 5, LeaseDuration: time.Minute,
				})
				if err != nil || len(got) == 0 {
					continue
				}
				reqs := make([]storage.CompleteRequest, 0, len(got))
				for _, it := range got {
					ledger.take(it.ID, worker)
					reqs = append(reqs, storage.CompleteRequest{ID: it.ID, Worker: worker})
				}
				res, _ := store.Complete(context.Background(), reqs...)
				for i := range res {
					ledger.release(got[i].ID)
				}
			}
		}(w)
	}

	// The boundary itself.
	clock.Advance(time.Second)

	deadline := time.After(20 * time.Second)
	for {
		if statFor(t, store, "boundary").Done == items {
			break
		}
		select {
		case <-deadline:
			close(stop)
			wg.Wait()
			t.Fatalf("items became available but were never claimed: %d of %d done",
				statFor(t, store, "boundary").Done, items)
		default:
		}
	}
	close(stop)
	wg.Wait()

	if problems := ledger.problems(); len(problems) > 0 {
		t.Fatalf("double delivery at the availability boundary:\n%s", strings.Join(problems, "\n"))
	}
	requireEqual(t, ledger.distinct(), items, "each item claimed exactly once after becoming ready")
}

// raceFanOutAtomicity proves that acking a step and enqueuing the work it
// produced are one atomic write under contention: every acked upstream item
// has exactly one downstream item, with no orphan and no duplicate.
//
// The harsher version of this — killing database connections mid-transaction —
// needs a database of its own so it cannot disturb sibling tests, so it lives
// in the adapter's own suite as TestFanOutSurvivesConnectionLoss.
func raceFanOutAtomicity(t *testing.T, h Harness) {
	t.Parallel()
	store, _ := h.New(t)

	const items = 400
	for i := 0; i < items; i += 100 {
		batch := make([]storage.NewItem, 0, 100)
		for j := 0; j < 100; j++ {
			batch = append(batch, storage.NewItem{Queue: "upstream"})
		}
		mustEnqueue(t, store, batch...)
	}

	var acked atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			worker := "fan-" + itoa(w)
			for {
				got, err := store.Claim(context.Background(), storage.ClaimOpts{
					Queue: "upstream", Worker: worker, Limit: 5, LeaseDuration: time.Minute,
				})
				if err != nil {
					return
				}
				if len(got) == 0 {
					if acked.Load() >= items {
						return
					}
					continue
				}
				for _, it := range got {
					res, err := store.Complete(context.Background(), storage.CompleteRequest{
						ID: it.ID, Worker: worker,
						Produced: []storage.NewItem{
							{Queue: "downstream", ParentItemID: itoa(int(it.ID))},
							{Queue: "downstream", ParentItemID: itoa(int(it.ID))},
						},
					})
					if err != nil || len(res) == 0 {
						continue
					}
					if res[0].Err == nil {
						acked.Add(1)
					}
				}
			}
		}(w)
	}
	wg.Wait()

	up := statFor(t, store, "upstream")
	down := statFor(t, store, "downstream")
	requireEqual(t, up.Done, items, "every upstream item acked")
	requireEqual(t, int(acked.Load()), items, "every ack reported success exactly once")
	// Two downstream items per ack, no more and no fewer.
	requireEqual(t, down.Ready, items*2, "exactly two downstream items per acked upstream item")
}

// raceSustained runs every operation against the same rows continuously. A
// lock-ordering mistake surfaces here as a deadlock, which is a defect.
func raceSustained(t *testing.T, h Harness) {
	t.Parallel()
	store, _ := h.New(t)

	for i := 0; i < 500; i += 100 {
		batch := make([]storage.NewItem, 0, 100)
		for j := 0; j < 100; j++ {
			batch = append(batch, storage.NewItem{Queue: "soak"})
		}
		mustEnqueue(t, store, batch...)
	}

	ctx, cancel := context.WithTimeout(context.Background(), soakDuration())
	defer cancel()

	var (
		mu       sync.Mutex
		failures []string
	)
	note := func(op string, err error) {
		if err == nil || ctx.Err() != nil {
			return
		}
		mu.Lock()
		failures = append(failures, op+": "+err.Error())
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			worker := "soak-" + itoa(w)
			for ctx.Err() == nil {
				got, err := store.Claim(ctx, storage.ClaimOpts{
					Queue: "soak", Worker: worker, Limit: 5, LeaseDuration: 2 * time.Second,
				})
				note("claim", err)
				for i, it := range got {
					switch i % 3 {
					case 0:
						_, err = store.Complete(ctx, storage.CompleteRequest{ID: it.ID, Worker: worker})
						note("complete", err)
					case 1:
						_, err = store.Retry(ctx, storage.RetryRequest{ID: it.ID, Worker: worker, Error: "soak"})
						note("retry", err)
					default:
						_, err = store.Release(ctx, storage.ReleaseRequest{ID: it.ID, Worker: worker})
						note("release", err)
					}
				}
			}
		}(w)
	}
	// Producers, reclaimers, sweepers and replayers all at once.
	wg.Add(4)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			_, err := store.Enqueue(ctx, storage.NewItem{Queue: "soak"})
			note("enqueue", err)
		}
	}()
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			_, err := store.ReclaimExpired(ctx, storage.ReclaimOpts{Limit: 50})
			note("reclaim", err)
		}
	}()
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			_, err := store.Sweep(ctx, storage.SweepOpts{Retention: time.Millisecond, Limit: 50})
			note("sweep", err)
		}
	}()
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			_, err := store.Replay(ctx, storage.ReplayOpts{Queue: "soak" + storage.DLQSuffix, Limit: 25})
			note("replay", err)
		}
	}()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	for _, f := range failures {
		if strings.Contains(f, "40P01") || strings.Contains(strings.ToLower(f), "deadlock detected") {
			t.Fatalf("deadlock under sustained load: %s", f)
		}
	}
	if len(failures) > 0 {
		shown := failures
		if len(shown) > 10 {
			shown = shown[:10]
		}
		t.Fatalf("%d errors under sustained load, first few:\n%s",
			len(failures), strings.Join(shown, "\n"))
	}
}

// raceClockSkew proves availability is decided by the time the caller supplies,
// not by whatever the database or a drifting host believes.
func raceClockSkew(t *testing.T, h Harness) {
	t.Parallel()
	store, clock := h.New(t)

	base := clock.Now()
	mustEnqueue(t, store, storage.NewItem{Queue: "skewed", AvailableAt: base.Add(time.Hour)})

	// A claimer whose clock is an hour behind must not see it.
	got, err := store.Claim(context.Background(), storage.ClaimOpts{
		Queue: "skewed", Worker: "behind", Limit: 10,
		LeaseDuration: time.Minute, Now: base.Add(-time.Hour),
	})
	requireNoErr(t, err, "Claim with a lagging clock")
	requireLen(t, got, 0, "an item scheduled for later must not be claimed early")

	// A claimer whose clock is ahead sees it exactly once.
	got, err = store.Claim(context.Background(), storage.ClaimOpts{
		Queue: "skewed", Worker: "ahead", Limit: 10,
		LeaseDuration: time.Minute, Now: base.Add(2 * time.Hour),
	})
	requireNoErr(t, err, "Claim with a leading clock")
	requireLen(t, got, 1, "the item is claimable once its time has come")

	again, err := store.Claim(context.Background(), storage.ClaimOpts{
		Queue: "skewed", Worker: "ahead2", Limit: 10,
		LeaseDuration: time.Minute, Now: base.Add(2 * time.Hour),
	})
	requireNoErr(t, err, "second Claim")
	requireLen(t, again, 0, "already claimed")
}
