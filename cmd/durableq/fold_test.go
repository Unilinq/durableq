// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/unilinq/durableq/internal/dqtest"
	"github.com/unilinq/durableq/internal/execution"
	"github.com/unilinq/durableq/internal/scheduler"
	"github.com/unilinq/durableq/storage"
	"github.com/unilinq/durableq/storage/postgres"
)

// TestFoldChangesNothingAnyoneSees runs one workload twice, step for step:
// once with nothing swept, and once with a folding sweep at retention zero
// after every step. After every step, every reader and every CLI view must be
// identical between the two. The workload covers retries, expired leases with
// late completes, streamed output, filters with payload flags, dead letters
// and a replay, a deliberate leak, a plain queue, and a run left mid-flight;
// the folding side also has a sweep killed in the middle of a batch.
func TestFoldChangesNothingAnyoneSees(t *testing.T) {
	t.Parallel()
	// Both sides share one start time, close to the wall clock so the CLI's
	// seven-day done window (it reads with the wall clock) covers the run.
	base := time.Now().UTC().Truncate(time.Second)

	kept := newWorld(t, "kept", base, false)
	folded := newWorld(t, "folded", base, true)
	runWorkload(kept)
	runWorkload(folded)

	if len(kept.frames) != len(folded.frames) {
		t.Fatalf("%d checkpoints without folding, %d with", len(kept.frames), len(folded.frames))
	}
	for i := range kept.frames {
		k, f := kept.frames[i], folded.frames[i]
		if k.Name != f.Name {
			t.Fatalf("checkpoint %d: %q vs %q", i, k.Name, f.Name)
		}
		if k.Readers != f.Readers {
			t.Fatalf("checkpoint %q: readers differ\n%s", k.Name, diffLines(k.Readers, f.Readers))
		}
		if !reflect.DeepEqual(k.CLI, f.CLI) {
			for _, cmd := range sortedKeys(k.CLI) {
				if k.CLI[cmd] != f.CLI[cmd] {
					t.Fatalf("checkpoint %q: durableq %s differs\n--- without folding ---\n%s--- with folding ---\n%s",
						k.Name, cmd, k.CLI[cmd], f.CLI[cmd])
				}
			}
			t.Fatalf("checkpoint %q: different CLI commands ran", k.Name)
		}
	}
	if folded.swept == 0 {
		t.Fatal("the folding side never swept anything")
	}
	if folded.live() != kept.live()-folded.swept {
		t.Fatalf("rows: %d kept, %d folded, %d swept", kept.live(), folded.live(), folded.swept)
	}
	t.Logf("%d checkpoints, %d items folded, %d rows left on the folding side",
		len(kept.frames), folded.swept, folded.live())
}

// world is one side of the comparison.
type world struct {
	t       *testing.T
	name    string
	store   *postgres.Store
	clock   *dqtest.StubClock
	g       globals
	sweeper *scheduler.Sweeper // nil on the side that never sweeps
	swept   int

	execs  []string
	keys   map[string][]string // execution -> item keys
	dlqs   []string
	frames []frame
}

// frame is everything visible at one checkpoint.
type frame struct {
	Name    string
	Readers string
	CLI     map[string]string
}

func newWorld(t *testing.T, name string, base time.Time, fold bool) *world {
	clock := dqtest.NewStubClock(base)
	store := dqtest.NewStore(t, clock)
	w := &world{
		t: t, name: name, store: store, clock: clock,
		g:    globals{schema: store.Schema(), pool: dqtest.Pool(t)},
		keys: map[string][]string{},
	}
	if fold {
		w.sweeper = scheduler.NewSweeper(scheduler.SweeperConfig{
			Store:        store,
			Retention:    storage.RetentionZero,
			BatchSize:    3,
			Budget:       time.Hour,
			Fold:         true,
			FilterLabels: func() map[string][]string { return map[string][]string{"crawl": {"gone", "excluded"}} },
			Clock:        clock,
		})
	}
	return w
}

func (w *world) fatalf(format string, args ...any) {
	w.t.Helper()
	w.t.Fatalf("%s: "+format, append([]any{w.name}, args...)...)
}

func (w *world) tick() { w.clock.Advance(time.Second) }

func pl(fields map[string]any) []byte {
	if fields == nil {
		return nil
	}
	b, _ := json.Marshal(fields)
	return b
}

func (w *world) createExec(id, job string, steps ...storage.StepDef) {
	w.t.Helper()
	if err := w.store.CreateExecution(w.t.Context(), storage.Execution{ID: id, Job: job, Input: pl(map[string]any{"run": id})}, steps); err != nil {
		w.fatalf("CreateExecution: %v", err)
	}
	w.execs = append(w.execs, id)
	w.tick()
}

// item describes an item of an execution's step; job "" is a plain queue.
func (w *world) item(exec, job, step, key string, payload map[string]any) storage.NewItem {
	ni := storage.NewItem{Queue: step, Payload: pl(payload)}
	if job != "" {
		ni = storage.NewItem{Queue: job + "." + step, Payload: pl(payload), ExecutionID: exec, StepID: step, ItemID: key}
		if !contains(w.keys[exec], key) {
			w.keys[exec] = append(w.keys[exec], key)
		}
	}
	return ni
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func (w *world) enqueue(items ...storage.NewItem) {
	w.t.Helper()
	if _, err := w.store.Enqueue(w.t.Context(), items...); err != nil {
		w.fatalf("Enqueue: %v", err)
	}
	w.tick()
}

// claim takes n items and checks they are the expected ones, so the script
// cannot drift silently.
func (w *world) claim(queue, worker string, want ...string) []storage.Item {
	w.t.Helper()
	got, err := w.store.Claim(w.t.Context(), storage.ClaimOpts{
		Queue: queue, Worker: worker, Limit: len(want), LeaseDuration: time.Minute,
	})
	if err != nil {
		w.fatalf("Claim %s: %v", queue, err)
	}
	var keys []string
	for _, it := range got {
		keys = append(keys, it.ItemID+string(it.Payload))
	}
	if len(got) != len(want) {
		w.fatalf("Claim %s: got %v, want %v", queue, keys, want)
	}
	for i := range want {
		if !strings.HasPrefix(keys[i], want[i]) {
			w.fatalf("Claim %s: got %v, want %v", queue, keys, want)
		}
	}
	w.tick()
	return got
}

// results renders transition results, errors included: both sides must reject
// the same late transitions with the same error.
func (w *world) results(op string, res []storage.Result, err error) string {
	w.t.Helper()
	if err != nil {
		w.fatalf("%s: %v", op, err)
	}
	var sb strings.Builder
	for _, r := range res {
		fmt.Fprintf(&sb, "%s %d: %v\n", op, r.ID, r.Err)
	}
	w.tick()
	return sb.String()
}

func (w *world) complete(reqs ...storage.CompleteRequest) string {
	res, err := w.store.Complete(w.t.Context(), reqs...)
	return w.results("complete", res, err)
}

func (w *world) emit(it storage.Item, worker string, produced ...storage.NewItem) {
	w.t.Helper()
	if err := w.store.Emit(w.t.Context(), storage.EmitRequest{ID: it.ID, Worker: worker, Attempt: it.Attempt, Produced: produced}); err != nil {
		w.fatalf("Emit: %v", err)
	}
	w.tick()
}

// retry returns the item to its queue after wait.
func (w *world) retry(it storage.Item, worker, msg string, wait time.Duration) string {
	res, err := w.store.Retry(w.t.Context(), storage.RetryRequest{ID: it.ID, Worker: worker, Error: msg, At: w.clock.Now().Add(wait)})
	return w.results("retry", res, err)
}

func (w *world) deadLetter(it storage.Item, worker, msg string) string {
	res, err := w.store.DeadLetter(w.t.Context(), storage.DeadLetterRequest{ID: it.ID, Worker: worker, Error: msg})
	return w.results("dead-letter", res, err)
}

func (w *world) release(it storage.Item, worker string) string {
	res, err := w.store.Release(w.t.Context(), storage.ReleaseRequest{ID: it.ID, Worker: worker, Reason: "shutdown"})
	return w.results("release", res, err)
}

func (w *world) reclaim() string {
	w.t.Helper()
	res, err := w.store.ReclaimExpired(w.t.Context(), storage.ReclaimOpts{})
	if err != nil {
		w.fatalf("ReclaimExpired: %v", err)
	}
	w.tick()
	return fmt.Sprintf("reclaim: %+v\n", res)
}

func (w *world) replay(dlq string) string {
	w.t.Helper()
	n, err := w.store.Replay(w.t.Context(), storage.ReplayOpts{Queue: dlq})
	if err != nil {
		w.fatalf("Replay: %v", err)
	}
	w.tick()
	return fmt.Sprintf("replay %s: %d\n", dlq, n)
}

// checkpoint records what everyone sees. On the folding side it first runs a
// sweep pass and proves the pass changed nothing.
func (w *world) checkpoint(name, notes string) {
	w.t.Helper()
	if w.sweeper != nil {
		before := w.view(true)
		res, err := w.sweeper.Pass(w.t.Context())
		if err != nil {
			w.fatalf("%s: sweep: %v", name, err)
		}
		w.swept += res.Deleted
		if after := w.view(true); after != before {
			w.fatalf("%s: the sweep changed what readers see\n%s", name, diffLines(before, after))
		}
	}
	w.frames = append(w.frames, frame{Name: name, Readers: notes + w.view(false), CLI: w.cli()})
}

// crashMidBatch, on the folding side, starts a sweep, kills its connection
// once the batch has deleted rows and waits on a lock to write their counts,
// and proves nothing changed. The next checkpoint's pass sweeps them again.
func (w *world) crashMidBatch() {
	w.t.Helper()
	if w.sweeper == nil {
		return
	}
	ctx := w.t.Context()
	pool := dqtest.Pool(w.t)
	before := w.view(true)

	locker, err := pool.Begin(ctx)
	if err != nil {
		w.fatalf("begin: %v", err)
	}
	defer func() { _ = locker.Rollback(context.Background()) }()
	schema := postgres.QuoteIdent(w.store.Schema())
	for _, table := range []string{"durableq_step_counts", "durableq_done_hourly"} {
		if _, err := locker.Exec(ctx, `SELECT * FROM `+schema+`.`+table+` FOR UPDATE`); err != nil {
			w.fatalf("locking %s: %v", table, err)
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := w.sweeper.Pass(ctx)
		done <- err
	}()
	// Poll, as an operator would: each query is the pause between looks.
	var pid int
	for deadline := time.Now().Add(10 * time.Second); pid == 0; {
		if time.Now().After(deadline) {
			w.fatalf("the sweep never blocked mid-batch")
		}
		err := pool.QueryRow(ctx, `
			SELECT pid FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query LIKE '%WITH due AS%' || $1 || '%'`,
			w.store.Schema()).Scan(&pid)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			w.fatalf("finding the sweep: %v", err)
		}
	}
	if _, err := pool.Exec(ctx, `SELECT pg_terminate_backend($1)`, pid); err != nil {
		w.fatalf("killing the sweep: %v", err)
	}
	if err := <-done; err == nil {
		w.fatalf("the killed sweep reported success")
	}
	if err := locker.Rollback(ctx); err != nil {
		w.fatalf("rollback: %v", err)
	}
	if after := w.view(true); after != before {
		w.fatalf("a batch killed mid-statement changed what readers see\n%s", diffLines(before, after))
	}
}

// maxID is the highest item id handed out so far, swept or not.
func (w *world) maxID() int64 {
	w.t.Helper()
	var n int64
	err := dqtest.Pool(w.t).QueryRow(w.t.Context(), `
		SELECT COALESCE(last_value, 0)
		FROM pg_sequences WHERE schemaname = $1 AND sequencename = 'durableq_items_id_seq'`,
		w.store.Schema()).Scan(&n)
	if err != nil {
		w.fatalf("reading the item sequence: %v", err)
	}
	return n
}

// live is how many rows durableq_items holds.
func (w *world) live() int {
	var n int
	if err := dqtest.Pool(w.t).QueryRow(w.t.Context(),
		`SELECT count(*) FROM `+postgres.QuoteIdent(w.store.Schema())+`.durableq_items`).Scan(&n); err != nil {
		w.fatalf("counting rows: %v", err)
	}
	return n
}

// view renders every store reader. Items leave out what history does not
// keep (payload, policy, available_at). Enqueue leaves created_at and
// updated_at to the database's wall clock, so they differ between the two
// sides and are kept only when comparing one side with itself.
func (w *world) view(withWallClock bool) string {
	w.t.Helper()
	ctx := w.t.Context()
	var sb strings.Builder
	put := func(label string, v any, err error) {
		if err != nil {
			fmt.Fprintf(&sb, "%s: error %v\n", label, err)
			return
		}
		b, _ := json.Marshal(v)
		fmt.Fprintf(&sb, "%s: %s\n", label, b)
	}

	stats, err := w.store.Stats(ctx)
	put("stats", stats, err)
	for _, id := range w.execs {
		exec, err := w.store.GetExecution(ctx, id)
		put("execution "+id, exec, err)
		counts, err := w.store.StepCounts(ctx, id)
		put("counts "+id, counts, err)
		steps, err := w.store.ExecutionSteps(ctx, id)
		put("steps "+id, steps, err)
		edges := map[string][]string{}
		for _, s := range steps {
			edges[s.StepID] = s.Next
		}
		put("projection "+id, execution.Project(exec, counts, edges), nil)
		if exec.Job == "crawl" {
			labels, err := w.store.FilteredByLabel(ctx, id, "gone", "excluded")
			put("labels "+id, labels, err)
		}
		for _, key := range w.keys[id] {
			lin, err := w.store.Lineage(ctx, id, key)
			put("lineage "+id+" "+key, lin, err)
		}
	}
	for id := int64(1); id <= w.maxID(); id++ {
		it, err := w.store.GetItem(ctx, id)
		if err == nil {
			it.Payload, it.Policy, it.AvailableAt = nil, storage.Policy{}, time.Time{}
			if !withWallClock {
				it.CreatedAt, it.UpdatedAt = time.Time{}, time.Time{}
			}
		}
		put(fmt.Sprintf("item %d", id), it, err)
		attempts, err := w.store.Attempts(ctx, id)
		put(fmt.Sprintf("attempts %d", id), attempts, err)
	}
	return sb.String()
}

// cli runs every read-only CLI view and keeps what it printed.
func (w *world) cli() map[string]string {
	w.t.Helper()
	ctx := w.t.Context()
	out := map[string]string{}
	run := func(name string, fn func(globals) error) {
		var buf bytes.Buffer
		g := w.g
		g.out = &buf
		if err := fn(g); err != nil {
			fmt.Fprintf(&buf, "error: %v\n", err)
		}
		out[name] = buf.String()
	}
	run("queues", func(g globals) error { return cmdQueues(ctx, g) })
	run("runs", func(g globals) error { return cmdRuns(ctx, g, nil) })
	for _, id := range w.execs {
		run("run "+id, func(g globals) error { return cmdRun(ctx, g, []string{id}) })
		for _, key := range w.keys[id] {
			run("trace "+id+" "+key, func(g globals) error { return cmdTrace(ctx, g, []string{id, key}) })
		}
	}
	for id := int64(1); id <= w.maxID(); id++ {
		arg := fmt.Sprint(id)
		run("item "+arg, func(g globals) error { return cmdItem(ctx, g, []string{arg}) })
	}
	for _, q := range w.dlqs {
		run("dlq ls "+q, func(g globals) error { return cmdDLQ(ctx, g, []string{"ls", q}) })
	}
	return out
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// diffLines shows the lines that differ between two renderings.
func diffLines(a, b string) string {
	al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
	var sb strings.Builder
	for i := 0; i < max(len(al), len(bl)); i++ {
		var x, y string
		if i < len(al) {
			x = al[i]
		}
		if i < len(bl) {
			y = bl[i]
		}
		if x != y {
			fmt.Fprintf(&sb, "- %s\n+ %s\n", x, y)
		}
	}
	return sb.String()
}

// runWorkload is the script both sides run. Every claim names the items it
// expects, so the two sides cannot drift apart unnoticed.
func runWorkload(w *world) {
	crawl := []storage.StepDef{
		{StepID: "discover", Idx: 0, Queue: "crawl.discover", Next: []string{"fetch"}},
		{StepID: "fetch", Idx: 1, Queue: "crawl.fetch", Next: []string{"acl", "index"}},
		{StepID: "index", Idx: 2, Queue: "crawl.index"},
		{StepID: "acl", Idx: 3, Queue: "crawl.acl"},
	}
	e1 := "exec_e1"
	fetch := func(key string, payload map[string]any) storage.NewItem {
		return w.item(e1, "crawl", "fetch", key, payload)
	}
	branches := func(exec, key string) []storage.NewItem {
		return []storage.NewItem{w.item(exec, "crawl", "acl", key, nil), w.item(exec, "crawl", "index", key, nil)}
	}
	w.dlqs = []string{"crawl.fetch.dlq", "crawl.index.dlq", "plain.b.dlq", "mail.dlq"}

	w.createExec(e1, "crawl", crawl...)
	w.enqueue(w.item(e1, "crawl", "discover", "root", map[string]any{"seed": "https://example.com"}))
	w.checkpoint("e1 started", "")

	// Discovery streams its output in two flushes, then finishes with a
	// capped remainder.
	d := w.claim("crawl.discover", "w1", "root")[0]
	w.emit(d, "w1", fetch("p1", map[string]any{"url": "1"}), fetch("p2", map[string]any{"url": "2"}))
	w.checkpoint("discover streamed two", "")
	w.emit(d, "w1", fetch("p3", map[string]any{"url": "3", "gone": true}), fetch("p4", map[string]any{"url": "4", "excluded": true}))
	w.checkpoint("discover streamed four", "")
	p6 := fetch("p6", map[string]any{"url": "6", "excluded": false})
	p6.Policy = &storage.Policy{MaxAttempts: 2, Schedule: []time.Duration{time.Second}}
	notes := w.complete(storage.CompleteRequest{ID: d.ID, Worker: "w1",
		Produced: []storage.NewItem{fetch("p5", map[string]any{"url": "5"}), p6}, Dropped: 3, Capped: true})
	w.checkpoint("discover finished", notes)

	// A retry ten minutes out, a page that goes on to both branches, and a
	// gone page.
	got := w.claim("crawl.fetch", "w2", "p1", "p2", "p3")
	notes = w.retry(got[0], "w2", "timeout", 10*time.Minute)
	notes += w.complete(
		storage.CompleteRequest{ID: got[1].ID, Worker: "w2", Produced: branches(e1, "p2")},
		storage.CompleteRequest{ID: got[2].ID, Worker: "w2", Filtered: true})
	w.checkpoint("fetch: retry, success, gone", notes)

	// An expired lease is reclaimed; the worker's late ack is refused.
	p4 := w.claim("crawl.fetch", "w3", "p4")[0]
	w.clock.Advance(2 * time.Minute)
	notes = w.reclaim()
	notes += w.complete(storage.CompleteRequest{ID: p4.ID, Worker: "w3", Filtered: true})
	w.checkpoint("fetch: lease expired, late ack refused", notes)

	// An expired lease nobody reclaimed yet: the late ack still lands.
	p5 := w.claim("crawl.fetch", "w5", "p5")[0]
	w.clock.Advance(2 * time.Minute)
	notes = w.complete(storage.CompleteRequest{ID: p5.ID, Worker: "w5", Produced: branches(e1, "p5")})
	w.checkpoint("fetch: expired lease, late ack lands", notes)

	// p6 has two attempts: one retried, then reclaimed into the DLQ.
	notes = w.retry(w.claim("crawl.fetch", "w6", "p6")[0], "w6", "http 503", 0)
	w.checkpoint("fetch: p6 retried", notes)

	// The excluded page, on its second attempt.
	got = w.claim("crawl.fetch", "w4", "p4")
	notes = w.complete(storage.CompleteRequest{ID: got[0].ID, Worker: "w4", Filtered: true})
	w.crashMidBatch()
	w.checkpoint("fetch: excluded on the second attempt", notes)

	p6b := w.claim("crawl.fetch", "w6", "p6")[0]
	w.clock.Advance(2 * time.Minute)
	notes = w.reclaim()
	notes += w.complete(storage.CompleteRequest{ID: p6b.ID, Worker: "w6"})
	w.checkpoint("fetch: p6 dead-lettered by the reclaimer", notes)

	notes = w.replay("crawl.fetch.dlq")
	notes += w.complete(storage.CompleteRequest{ID: w.claim("crawl.fetch", "w7", "p6")[0].ID, Worker: "w7", Filtered: true})
	w.checkpoint("fetch: p6 replayed and filtered", notes)

	w.clock.Advance(10 * time.Minute)
	got = w.claim("crawl.fetch", "w2", "p1")
	notes = w.complete(storage.CompleteRequest{ID: got[0].ID, Worker: "w2", Produced: branches(e1, "p1")})
	w.checkpoint("fetch: p1 on its last retry", notes)

	// The branches: a dead letter, a release, and successes.
	idx := w.claim("crawl.index", "w8", "p2", "p5", "p1")
	notes = w.complete(
		storage.CompleteRequest{ID: idx[0].ID, Worker: "w8"},
		storage.CompleteRequest{ID: idx[1].ID, Worker: "w8"})
	notes += w.deadLetter(idx[2], "w8", "bad document")
	acl := w.claim("crawl.acl", "w9", "p2", "p5", "p1")
	notes += w.release(acl[0], "w9")
	notes += w.complete(
		storage.CompleteRequest{ID: acl[1].ID, Worker: "w9"},
		storage.CompleteRequest{ID: acl[2].ID, Worker: "w9"})
	w.checkpoint("branches mostly done", notes)

	notes = w.complete(storage.CompleteRequest{ID: w.claim("crawl.acl", "w10", "p2")[0].ID, Worker: "w10"})
	w.checkpoint("branches done", notes)

	// Late transitions on items long done (and, on one side, folded).
	notes = w.complete(storage.CompleteRequest{ID: idx[0].ID, Worker: "w8"})
	ext, err := w.store.ExtendLease(w.t.Context(), "w8", w.clock.Now().Add(time.Minute), idx[1].ID)
	notes += w.results("extend", ext, err)
	notes += w.retry(acl[1], "w9", "late", 0)
	w.checkpoint("late transitions refused", notes)

	// A deliberate leak: an index item nobody produced.
	w.enqueue(w.item(e1, "crawl", "index", "stray", nil))
	notes = w.complete(storage.CompleteRequest{ID: w.claim("crawl.index", "w8", "stray")[0].ID, Worker: "w8"})
	w.checkpoint("a leak", notes)

	// A second job with no filter labels: its filtered items are only counted.
	e2 := "exec_e2"
	w.createExec(e2, "plain",
		storage.StepDef{StepID: "a", Idx: 0, Queue: "plain.a", Next: []string{"b"}},
		storage.StepDef{StepID: "b", Idx: 1, Queue: "plain.b"})
	w.enqueue(w.item(e2, "plain", "a", "x1", map[string]any{"gone": true}), w.item(e2, "plain", "a", "x2", nil))
	got = w.claim("plain.a", "w1", "x1", "x2")
	notes = w.complete(
		storage.CompleteRequest{ID: got[0].ID, Worker: "w1", Filtered: true},
		storage.CompleteRequest{ID: got[1].ID, Worker: "w1", Produced: []storage.NewItem{w.item(e2, "plain", "b", "x2", nil)}})
	notes += w.deadLetter(w.claim("plain.b", "w1", "x2")[0], "w1", "nope")
	w.checkpoint("second job", notes)

	// A plain queue: a success, a retry, a dead letter and its replay.
	w.enqueue(w.item("", "", "mail", "", map[string]any{"to": "a"}),
		w.item("", "", "mail", "", map[string]any{"to": "b"}),
		w.item("", "", "mail", "", map[string]any{"to": "c"}))
	mail := w.claim("mail", "w1", `{"to": "a"}`, `{"to": "b"}`, `{"to": "c"}`)
	notes = w.complete(storage.CompleteRequest{ID: mail[0].ID, Worker: "w1"})
	notes += w.retry(mail[1], "w1", "smtp busy", 0)
	notes += w.deadLetter(mail[2], "w1", "mailbox full")
	w.checkpoint("plain queue", notes)
	notes = w.complete(storage.CompleteRequest{ID: w.claim("mail", "w2", `{"to": "b"}`)[0].ID, Worker: "w2"})
	notes += w.replay("mail.dlq")
	notes += w.complete(storage.CompleteRequest{ID: w.claim("mail", "w2", `{"to": "c"}`)[0].ID, Worker: "w2"})
	w.checkpoint("plain queue replayed", notes)

	// A run left mid-flight: a gone page done, one running, one ready.
	e3 := "exec_e3"
	w.createExec(e3, "crawl", crawl...)
	w.enqueue(w.item(e3, "crawl", "discover", "root", nil))
	d = w.claim("crawl.discover", "w1", "root")[0]
	notes = w.complete(storage.CompleteRequest{ID: d.ID, Worker: "w1", Produced: []storage.NewItem{
		w.item(e3, "crawl", "fetch", "q1", map[string]any{"gone": true}),
		w.item(e3, "crawl", "fetch", "q2", nil),
		w.item(e3, "crawl", "fetch", "q3", nil),
	}})
	got = w.claim("crawl.fetch", "w2", "q1", "q2")
	notes += w.complete(storage.CompleteRequest{ID: got[0].ID, Worker: "w2", Filtered: true})
	w.checkpoint("e3 mid-run", notes)
}
