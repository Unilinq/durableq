// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/unilinq/durableq/internal/dqtest"
	"github.com/unilinq/durableq/storage"
	"github.com/unilinq/durableq/storage/postgres"
)

// foldLabels are the payload flags the fold tests count among filtered items.
var foldLabels = map[string][]string{"crawl": {"gone", "excluded"}}

// foldRig is one store with an execution of the job "crawl" whose steps are
// discover -> fetch -> {index, acl}.
type foldRig struct {
	t     *testing.T
	store *postgres.Store
	clock *dqtest.StubClock
	exec  string
	// items and keys are every item id and item key the rig has seen, so a
	// view still asks about them once they are folded.
	items map[int64]bool
	keys  map[string]bool
}

func newFoldRig(t *testing.T) *foldRig {
	t.Helper()
	clock := dqtest.NewStubClock(time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC))
	r := &foldRig{t: t, store: dqtest.NewStore(t, clock), clock: clock, exec: "exec_fold", items: map[int64]bool{}, keys: map[string]bool{}}
	if err := r.store.CreateExecution(t.Context(), storage.Execution{ID: r.exec, Job: "crawl"}, []storage.StepDef{
		{StepID: "discover", Idx: 0, Queue: "crawl.discover", Next: []string{"fetch"}},
		{StepID: "fetch", Idx: 1, Queue: "crawl.fetch", Next: []string{"index", "acl"}},
		{StepID: "index", Idx: 2, Queue: "crawl.index"},
		{StepID: "acl", Idx: 3, Queue: "crawl.acl"},
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	return r
}

func (r *foldRig) newItem(step, key string, payload map[string]any) storage.NewItem {
	raw, _ := json.Marshal(payload)
	r.keys[key] = true
	return storage.NewItem{Queue: "crawl." + step, Payload: raw, ExecutionID: r.exec, StepID: step, ItemID: key}
}

func (r *foldRig) enqueue(items ...storage.NewItem) []storage.Item {
	r.t.Helper()
	out, err := r.store.Enqueue(r.t.Context(), items...)
	if err != nil {
		r.t.Fatalf("Enqueue: %v", err)
	}
	r.clock.Advance(time.Second)
	return out
}

// track records every item in the table, so views keep asking about it.
func (r *foldRig) track() {
	r.t.Helper()
	items, err := r.store.ListItems(r.t.Context(), storage.ItemFilter{Limit: 100000})
	if err != nil {
		r.t.Fatalf("ListItems: %v", err)
	}
	for _, it := range items {
		r.items[it.ID] = true
	}
}

func (r *foldRig) claim(step, worker string, n int) []storage.Item {
	r.t.Helper()
	got, err := r.store.Claim(r.t.Context(), storage.ClaimOpts{Queue: "crawl." + step, Worker: worker, Limit: n, LeaseDuration: time.Minute})
	if err != nil {
		r.t.Fatalf("Claim: %v", err)
	}
	if len(got) != n {
		r.t.Fatalf("Claim %s: got %d items, want %d", step, len(got), n)
	}
	r.clock.Advance(time.Second)
	return got
}

func (r *foldRig) complete(reqs ...storage.CompleteRequest) {
	r.t.Helper()
	res, err := r.store.Complete(r.t.Context(), reqs...)
	if err != nil {
		r.t.Fatalf("Complete: %v", err)
	}
	for _, x := range res {
		if x.Err != nil {
			r.t.Fatalf("Complete %d: %v", x.ID, x.Err)
		}
	}
	for _, q := range reqs {
		for _, p := range q.Produced {
			r.keys[p.ItemID] = true
		}
	}
	r.clock.Advance(time.Second)
}

// seed runs a small execution to the end: discover fans out to three fetches,
// one fetched page goes on to index and acl, one is gone and one excluded.
func (r *foldRig) seed() {
	r.t.Helper()
	r.enqueue(r.newItem("discover", "root", nil))
	d := r.claim("discover", "w1", 1)[0]
	r.complete(storage.CompleteRequest{ID: d.ID, Worker: "w1", Produced: []storage.NewItem{
		r.newItem("fetch", "p1", map[string]any{"url": "a"}),
		r.newItem("fetch", "p2", map[string]any{"url": "b", "gone": true}),
		r.newItem("fetch", "p3", map[string]any{"url": "c", "excluded": true}),
	}, Dropped: 2, Capped: true})
	for _, it := range r.claim("fetch", "w1", 3) {
		req := storage.CompleteRequest{ID: it.ID, Worker: "w1", Filtered: true}
		if it.ItemID == "p1" {
			req = storage.CompleteRequest{ID: it.ID, Worker: "w1", Produced: []storage.NewItem{
				r.newItem("index", "p1", nil), r.newItem("acl", "p1", nil),
			}}
		}
		r.complete(req)
	}
	r.complete(storage.CompleteRequest{ID: r.claim("index", "w2", 1)[0].ID, Worker: "w2"})
	r.complete(storage.CompleteRequest{ID: r.claim("acl", "w2", 1)[0].ID, Worker: "w2"})
	r.track()
}

// fold sweeps everything finished, folding it.
func (r *foldRig) fold() storage.SweepResult {
	r.t.Helper()
	ctx := r.t.Context()
	if err := r.store.PrepareFold(ctx, storage.FoldOpts{Now: r.clock.Now()}); err != nil {
		r.t.Fatalf("PrepareFold: %v", err)
	}
	res, err := r.store.Sweep(ctx, storage.SweepOpts{
		Retention: storage.RetentionZero, Limit: 1000, Fold: true, FilterLabels: foldLabels,
	})
	if err != nil {
		r.t.Fatalf("Sweep: %v", err)
	}
	return res
}

// view is everything a reader can see of the rig's execution, with item
// fields history does not keep blanked.
type view struct {
	Counts   []storage.StepCount
	Labels   map[string]map[string]int
	Stats    []storage.QueueStat
	Items    map[int64]string
	Attempts map[int64][]storage.Attempt
	Lineage  map[string][]storage.LineageEntry
}

func (r *foldRig) view() view {
	r.t.Helper()
	ctx := r.t.Context()
	v := view{Items: map[int64]string{}, Attempts: map[int64][]storage.Attempt{}, Lineage: map[string][]storage.LineageEntry{}}
	var err error
	if v.Counts, err = r.store.StepCounts(ctx, r.exec); err != nil {
		r.t.Fatalf("StepCounts: %v", err)
	}
	if v.Labels, err = r.store.FilteredByLabel(ctx, r.exec, "gone", "excluded"); err != nil {
		r.t.Fatalf("FilteredByLabel: %v", err)
	}
	if v.Stats, err = r.store.Stats(ctx); err != nil {
		r.t.Fatalf("Stats: %v", err)
	}
	for id := range r.items {
		v.Items[id] = itemString(r.store.GetItem(ctx, id))
		if v.Attempts[id], err = r.store.Attempts(ctx, id); err != nil {
			r.t.Fatalf("Attempts: %v", err)
		}
	}
	for key := range r.keys {
		if v.Lineage[key], err = r.store.Lineage(ctx, r.exec, key); err != nil {
			r.t.Fatalf("Lineage: %v", err)
		}
	}
	return v
}

// itemString renders what GetItem returned, leaving out what history does not
// keep (payload, policy, available_at).
func itemString(it storage.Item, err error) string {
	if errors.Is(err, storage.ErrNotFound) {
		return "not found"
	}
	if err != nil {
		return "error: " + err.Error()
	}
	it.Payload, it.Policy, it.AvailableAt = nil, storage.Policy{}, time.Time{}
	return fmt.Sprintf("%+v", it)
}

func requireSameView(t *testing.T, got, want view, what string) {
	t.Helper()
	if reflect.DeepEqual(got, want) {
		return
	}
	g, _ := json.MarshalIndent(got, "", "  ")
	w, _ := json.MarshalIndent(want, "", "  ")
	t.Fatalf("%s: readers changed\n--- got ---\n%s\n--- want ---\n%s", what, g, w)
}

// TestFoldKeepsEveryReaderTheSame is the basic promise: folding a finished
// execution changes nothing any reader returns.
func TestFoldKeepsEveryReaderTheSame(t *testing.T) {
	t.Parallel()
	r := newFoldRig(t)
	r.seed()
	before := r.view()

	if res := r.fold(); res.Deleted != len(r.items) {
		t.Fatalf("fold deleted %d, want every one of the %d items", res.Deleted, len(r.items))
	}
	requireSameView(t, r.view(), before, "after folding")

	var live int
	if err := dqtest.Pool(t).QueryRow(t.Context(),
		`SELECT count(*) FROM `+postgres.QuoteIdent(r.store.Schema())+`.durableq_items`).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 0 {
		t.Fatalf("%d items left in durableq_items", live)
	}
	if got := before.Labels["fetch"]; got["gone"] != 1 || got["excluded"] != 1 {
		t.Fatalf("labels before folding: %v", got)
	}
}

// seedMany puts n fetch items through the rig's execution: a third filtered
// as gone, a sixth filtered as excluded, a sixth filtered plain, the rest
// producing one index and one acl item each, which then succeed.
func (r *foldRig) seedMany(n int) {
	r.t.Helper()
	var news []storage.NewItem
	for i := 0; i < n; i++ {
		p := map[string]any{"n": i}
		switch i % 6 {
		case 0, 1:
			p["gone"] = true
		case 2:
			p["excluded"] = true
		case 3:
			p["excluded"] = false
		}
		news = append(news, r.newItem("fetch", fmt.Sprintf("m%d", i), p))
	}
	r.enqueue(news...)
	var reqs []storage.CompleteRequest
	for _, it := range r.claim("fetch", "w1", n) {
		var p map[string]any
		_ = json.Unmarshal(it.Payload, &p)
		if p["gone"] == true || p["excluded"] != nil {
			reqs = append(reqs, storage.CompleteRequest{ID: it.ID, Worker: "w1", Filtered: true})
			continue
		}
		reqs = append(reqs, storage.CompleteRequest{ID: it.ID, Worker: "w1", Produced: []storage.NewItem{
			r.newItem("index", it.ItemID, nil), r.newItem("acl", it.ItemID, nil),
		}})
	}
	r.complete(reqs...)
	for _, step := range []string{"index", "acl"} {
		var stats []storage.QueueStat
		stats, _ = r.store.Stats(r.t.Context(), "crawl."+step)
		var done []storage.CompleteRequest
		for _, it := range r.claim(step, "w2", stats[0].Ready) {
			done = append(done, storage.CompleteRequest{ID: it.ID, Worker: "w2"})
		}
		r.complete(done...)
	}
	r.track()
}

func (r *foldRig) countRows(table string) int {
	r.t.Helper()
	var n int
	if err := dqtest.Pool(r.t).QueryRow(r.t.Context(),
		`SELECT count(*) FROM `+postgres.QuoteIdent(r.store.Schema())+`.`+table).Scan(&n); err != nil {
		r.t.Fatalf("counting %s: %v", table, err)
	}
	return n
}

// partitions lists the history partitions, oldest first.
func (r *foldRig) partitions() []string {
	r.t.Helper()
	rows, err := dqtest.Pool(r.t).Query(r.t.Context(), `
		SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = $1::regclass ORDER BY c.relname`,
		postgres.QuoteIdent(r.store.Schema())+".durableq_item_history")
	if err != nil {
		r.t.Fatalf("listing partitions: %v", err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		r.t.Fatalf("listing partitions: %v", err)
	}
	return names
}

func dayNames(from time.Time, days int) []string {
	var out []string
	for i := 0; i < days; i++ {
		out = append(out, "durableq_item_history_"+from.AddDate(0, 0, i).Format("20060102"))
	}
	return out
}

// TestPrepareFoldManagesPartitions proves history partitions are created for
// the retained days and a couple ahead, dropped once past retention, and that
// preparing is safe to repeat and to run from several sweepers at once.
func TestPrepareFoldManagesPartitions(t *testing.T) {
	t.Parallel()
	r := newFoldRig(t)
	ctx := t.Context()
	now := time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- r.store.PrepareFold(ctx, storage.FoldOpts{Now: now, HistoryRetention: 72 * time.Hour})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent PrepareFold: %v", err)
		}
	}
	// 26 Oct (three days back) to 31 Oct (two days ahead).
	if got, want := r.partitions(), dayNames(time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC), 6); !reflect.DeepEqual(got, want) {
		t.Fatalf("partitions: got %v, want %v", got, want)
	}

	// Four days on: 30 Oct is the oldest kept day, and 29 Oct stays one more
	// day for a sweeper whose pass began before midnight.
	if err := r.store.PrepareFold(ctx, storage.FoldOpts{Now: now.AddDate(0, 0, 4), HistoryRetention: 72 * time.Hour}); err != nil {
		t.Fatalf("PrepareFold: %v", err)
	}
	if got, want := r.partitions(), dayNames(time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC), 7); !reflect.DeepEqual(got, want) {
		t.Fatalf("partitions after four days: got %v, want %v", got, want)
	}
}

// TestFoldHistoryRetention proves an item that finished longer ago than the
// history retention is still counted when it is folded, but gets no history,
// as it would have been swept for good without folding; and that history is
// dropped with its partition.
func TestFoldHistoryRetention(t *testing.T) {
	t.Parallel()
	r := newFoldRig(t)
	ctx := t.Context()
	r.seed()
	before := r.view()

	// Fold ten days later with three days of history.
	r.clock.Advance(10 * 24 * time.Hour)
	now := r.clock.Now()
	if err := r.store.PrepareFold(ctx, storage.FoldOpts{Now: now, HistoryRetention: 72 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.store.Sweep(ctx, storage.SweepOpts{
		Retention: storage.RetentionZero, Fold: true, HistoryRetention: 72 * time.Hour, FilterLabels: foldLabels,
	}); err != nil {
		t.Fatal(err)
	}
	after := r.view()
	if !reflect.DeepEqual(after.Counts, before.Counts) || !reflect.DeepEqual(after.Labels, before.Labels) {
		t.Fatalf("counts changed:\n got %+v %v\nwant %+v %v", after.Counts, after.Labels, before.Counts, before.Labels)
	}
	if n := r.countRows("durableq_item_history"); n != 0 {
		t.Fatalf("%d history rows for items past the history retention", n)
	}
	for id, it := range after.Items {
		if it != "not found" {
			t.Fatalf("item %d readable past the history retention: %s", id, it)
		}
	}

	// A fresh item folded now has history, until its partition is dropped.
	fresh := r.enqueue(storage.NewItem{Queue: "plain"})[0]
	got, err := r.store.Claim(ctx, storage.ClaimOpts{Queue: "plain", Worker: "w1", Limit: 1})
	if err != nil || len(got) != 1 {
		t.Fatalf("Claim: %v", err)
	}
	r.complete(storage.CompleteRequest{ID: fresh.ID, Worker: "w1"})
	if _, err := r.store.Sweep(ctx, storage.SweepOpts{
		Retention: storage.RetentionZero, Fold: true, HistoryRetention: 72 * time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if it, err := r.store.GetItem(ctx, fresh.ID); err != nil || it.State != storage.StateDone || it.Queue != "plain" {
		t.Fatalf("fresh folded item: %+v, %v", it, err)
	}
	r.clock.Advance(5 * 24 * time.Hour)
	if err := r.store.PrepareFold(ctx, storage.FoldOpts{Now: r.clock.Now(), HistoryRetention: 72 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.store.GetItem(ctx, fresh.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("history kept past its retention: %v", err)
	}
}

// TestStatsDoneWindow proves Stats counts folded items for the done window
// after they finished, as the default seven-day sweep without folding would,
// and that PrepareFold prunes tallies past it.
func TestStatsDoneWindow(t *testing.T) {
	t.Parallel()
	r := newFoldRig(t)
	r.seed()
	before := r.view()
	r.fold()

	r.clock.Advance(6 * 24 * time.Hour)
	if got := r.view().Stats; !reflect.DeepEqual(got, before.Stats) {
		t.Fatalf("Stats six days on: got %+v, want %+v", got, before.Stats)
	}

	r.clock.Advance(26 * time.Hour)
	for _, st := range r.view().Stats {
		if st.Done != 0 {
			t.Fatalf("Stats past the window still counts folded items: %+v", st)
		}
	}
	if err := r.store.PrepareFold(t.Context(), storage.FoldOpts{Now: r.clock.Now()}); err != nil {
		t.Fatal(err)
	}
	if n := r.countRows("durableq_done_hourly"); n != 0 {
		t.Fatalf("%d tallies left past the window", n)
	}

	// A store with a one-hour window stops counting after an hour.
	short, err := postgres.New(postgres.Config{
		Pool: dqtest.Pool(t), Schema: r.store.Schema(), Clock: r.clock, DoneWindow: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	q := r.enqueue(storage.NewItem{Queue: "plain"})[0]
	if _, err := r.store.Claim(t.Context(), storage.ClaimOpts{Queue: "plain", Worker: "w1", Limit: 1}); err != nil {
		t.Fatal(err)
	}
	r.complete(storage.CompleteRequest{ID: q.ID, Worker: "w1"})
	r.fold()
	if st, _ := short.Stats(t.Context(), "plain"); st[0].Done != 1 {
		t.Fatalf("Stats within the hour: %+v", st)
	}
	r.clock.Advance(2 * time.Hour)
	if st, _ := short.Stats(t.Context(), "plain"); st[0].Done != 0 {
		t.Fatalf("Stats past a one-hour window: %+v", st)
	}
}

// TestFoldLeavesDeadLettersAndLiveWork proves folding touches only done items:
// dead-lettered, ready and running items keep their rows and their counts.
func TestFoldLeavesDeadLettersAndLiveWork(t *testing.T) {
	t.Parallel()
	r := newFoldRig(t)
	ctx := t.Context()
	r.seed()
	r.enqueue(r.newItem("index", "live1", nil), r.newItem("index", "live2", nil), r.newItem("index", "dead", nil))
	got := r.claim("index", "w3", 2)
	if _, err := r.store.DeadLetter(ctx, storage.DeadLetterRequest{ID: got[1].ID, Worker: "w3", Error: "boom"}); err != nil {
		t.Fatal(err)
	}
	r.clock.Advance(time.Second)
	r.track()
	before := r.view()

	res := r.fold()
	if res.Deleted != 6 {
		t.Fatalf("fold deleted %d, want the 6 done items only", res.Deleted)
	}
	requireSameView(t, r.view(), before, "after folding around live and dead work")
	if n := r.countRows("durableq_items"); n != 3 {
		t.Fatalf("%d rows left, want the ready, running and dead-lettered ones", n)
	}
}

// TestFoldConcurrentSweepers proves several sweepers folding at once, in small
// batches, count every item exactly once and never deadlock on the upserts.
func TestFoldConcurrentSweepers(t *testing.T) {
	t.Parallel()
	r := newFoldRig(t)
	ctx := t.Context()
	r.seedMany(600)
	before := r.view()
	if err := r.store.PrepareFold(ctx, storage.FoldOpts{Now: r.clock.Now()}); err != nil {
		t.Fatal(err)
	}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		total int
	)
	errs := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				res, err := r.store.Sweep(ctx, storage.SweepOpts{
					Retention: storage.RetentionZero, Limit: 37, Fold: true, FilterLabels: foldLabels,
				})
				if err != nil {
					errs <- err
					return
				}
				mu.Lock()
				total += res.Deleted
				mu.Unlock()
				if res.Deleted == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent fold: %v", err)
	}
	if total != len(r.items) {
		t.Fatalf("deleted %d, want %d", total, len(r.items))
	}
	requireSameView(t, r.view(), before, "after concurrent folding")
}

// TestFoldBatchCutOffChangesNothing kills a folding sweep in the middle of its
// statement, after it has deleted rows and while it waits to write a tally,
// and proves the batch rolled back whole: every reader is unchanged, and
// sweeping again folds every item exactly once.
func TestFoldBatchCutOffChangesNothing(t *testing.T) {
	t.Parallel()
	r := newFoldRig(t)
	ctx := t.Context()
	pool := dqtest.Pool(t)
	r.seedMany(60)
	before := r.view()
	if err := r.store.PrepareFold(ctx, storage.FoldOpts{Now: r.clock.Now()}); err != nil {
		t.Fatal(err)
	}
	// A first small batch creates the tallies the blocked batch will need.
	first, err := r.store.Sweep(ctx, storage.SweepOpts{Retention: storage.RetentionZero, Limit: 5, Fold: true, FilterLabels: foldLabels})
	if err != nil || first.Deleted != 5 {
		t.Fatalf("first batch: %+v, %v", first, err)
	}

	locker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = locker.Rollback(context.Background()) }()
	if _, err := locker.Exec(ctx, `SELECT * FROM `+postgres.QuoteIdent(r.store.Schema())+`.durableq_done_hourly FOR UPDATE`); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := r.store.Sweep(ctx, storage.SweepOpts{Retention: storage.RetentionZero, Limit: 1000, Fold: true, FilterLabels: foldLabels})
		done <- err
	}()

	// Poll until the sweep is blocked inside its statement, then kill it.
	var pid int
	deadline := time.Now().Add(10 * time.Second)
	for pid == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the sweep never blocked on the tally lock")
		}
		// pg_stat_activity keeps only the start of a long statement, so the
		// sweep is recognised by its schema and its due-items CTE.
		err := pool.QueryRow(ctx, `
			SELECT pid FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query LIKE '%WITH due AS%' || $1 || '%'`,
			r.store.Schema()).Scan(&pid)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `SELECT pg_terminate_backend($1)`, pid); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("the killed sweep reported success")
	}
	if err := locker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	requireSameView(t, r.view(), before, "after a batch killed mid-statement")
	if n := r.countRows("durableq_items"); n != len(r.items)-5 {
		t.Fatalf("%d rows left, want all but the first batch's 5", n)
	}

	res, err := r.store.Sweep(ctx, storage.SweepOpts{Retention: storage.RetentionZero, Limit: 1000, Fold: true, FilterLabels: foldLabels})
	if err != nil || res.Deleted != len(r.items)-5 {
		t.Fatalf("sweep after the crash: %+v, %v", res, err)
	}
	requireSameView(t, r.view(), before, "after sweeping again")
}

// TestLateCompleteOfAFoldedItem proves a worker acking an item that was
// already done and folded gets the same answer as before the fold: wrong
// state, not missing.
func TestLateCompleteOfAFoldedItem(t *testing.T) {
	t.Parallel()
	r := newFoldRig(t)
	r.seed()
	var id int64
	for k := range r.items {
		id = k
		break
	}
	want, err := r.store.Complete(t.Context(), storage.CompleteRequest{ID: id, Worker: "w1"})
	if err != nil {
		t.Fatal(err)
	}
	r.fold()
	got, err := r.store.Complete(t.Context(), storage.CompleteRequest{ID: id, Worker: "w1"})
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(got[0].Err, storage.ErrWrongState) || got[0].Err.Error() != want[0].Err.Error() {
		t.Fatalf("late complete after fold: %v, want %v", got[0].Err, want[0].Err)
	}
	if !strings.Contains(got[0].Err.Error(), "is done") {
		t.Fatalf("unexpected error: %v", got[0].Err)
	}
}
