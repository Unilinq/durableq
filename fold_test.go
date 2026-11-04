// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package durableq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unilinq/durableq/internal/dqtest"
	"github.com/unilinq/durableq/storage"
	"github.com/unilinq/durableq/storage/postgres"
)

type (
	foldSite struct {
		Name  string `json:"name"`
		Pages int    `json:"pages"`
	}
	foldPage struct {
		Site string `json:"site"`
		N    int    `json:"n"`
		Skip bool   `json:"skip,omitempty"`
	}
)

// foldJob defines a job that streams pages, retries some, skips some with a
// payload flag and dead-letters one per site while broken is set.
func foldJob(app *testApp, broken *atomic.Bool, opts ...JobOption) *Job {
	var tries sync.Map
	return app.Job("site", opts...).
		Step("discover", Stream(func(ctx context.Context, in foldSite, out Out[foldPage]) error {
			for i := 0; i < in.Pages; i++ {
				if err := out.Add(foldPage{Site: in.Name, N: i, Skip: i%3 == 0}); err != nil {
					return err
				}
				if i%4 == 3 {
					if err := out.Flush(); err != nil {
						return err
					}
				}
			}
			return nil
		})).
		Step("fetch", func(ctx context.Context, p foldPage) ([]foldPage, error) {
			if p.Skip {
				return nil, nil
			}
			if p.N == 1 && broken.Load() {
				return nil, errors.New("broken page")
			}
			key := fmt.Sprintf("%s/%d", p.Site, p.N)
			if n, _ := tries.LoadOrStore(key, new(atomic.Int32)); n.(*atomic.Int32).Add(1) == 1 && p.N%4 == 2 {
				return nil, errors.New("first attempt fails")
			}
			return []foldPage{p}, nil
		}, StepConcurrency(3), StepPolicy(Policy{MaxAttempts: 2, Schedule: []time.Duration{0}})).
		Step("index", func(ctx context.Context, p foldPage) error { return nil }, StepConcurrency(3))
}

// runFoldSites runs three sites to completion and returns their executions.
func runFoldSites(t *testing.T, app *testApp, job *Job) []string {
	t.Helper()
	var ids []string
	for i, pages := range []int{7, 12, 5} {
		exec, err := job.Run(t.Context(), foldSite{Name: fmt.Sprint("site", i), Pages: pages})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		ids = append(ids, exec.ID)
	}
	for _, id := range ids {
		app.waitForExecution(t, id, 1)
	}
	return ids
}

// doneRows is how many done rows are left in the app's table.
func doneRows(t *testing.T, app *testApp) int {
	t.Helper()
	var n int
	if err := dqtest.Pool(t).QueryRow(t.Context(),
		`SELECT count(*) FROM `+postgres.QuoteIdent(app.store.Schema())+`.durableq_items WHERE state = 'done'`).Scan(&n); err != nil {
		t.Fatalf("counting done rows: %v", err)
	}
	return n
}

// waitSwept polls until the app's sweeper has folded every done item.
func waitSwept(t *testing.T, app *testApp) {
	t.Helper()
	for deadline := time.Now().Add(20 * time.Second); doneRows(t, app) > 0; {
		if time.Now().After(deadline) {
			t.Fatalf("%d done items never swept", doneRows(t, app))
		}
	}
}

// foldView is what an execution looks like from outside, minus its id.
type foldView struct {
	Projection Projection
	Counts     []storage.StepCount
	Labels     map[string]map[string]int
}

func viewsOf(t *testing.T, app *testApp, ids []string) []foldView {
	t.Helper()
	var out []foldView
	for _, id := range ids {
		p, err := app.Projection(t.Context(), id)
		if err != nil {
			t.Fatalf("Projection: %v", err)
		}
		p.Execution = storage.Execution{}
		counts, err := app.store.StepCounts(t.Context(), id)
		if err != nil {
			t.Fatalf("StepCounts: %v", err)
		}
		labels, err := app.store.FilteredByLabel(t.Context(), id, "skip")
		if err != nil {
			t.Fatalf("FilteredByLabel: %v", err)
		}
		out = append(out, foldView{Projection: p, Counts: counts, Labels: labels})
	}
	return out
}

func requireSameViews(t *testing.T, got, want []foldView, what string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s:\n got %+v\nwant %+v", what, got, want)
	}
}

// TestAppFoldsWhileJobsRun runs the same jobs on an app that never sweeps and
// on one that folds at retention zero while the jobs run, and proves both
// report the same projections, counts and label counts, before and after a
// DLQ replay, with every done row gone from the folding app.
func TestAppFoldsWhileJobsRun(t *testing.T) {
	t.Parallel()
	real := func(c *Config) { c.Clock = storage.RealClock() }

	var keptBroken, foldBroken atomic.Bool
	keptBroken.Store(true)
	foldBroken.Store(true)

	kept := newTestApp(t, real, func(c *Config) { c.Retention = storage.RetentionNever })
	keptJob := foldJob(kept, &keptBroken, FilterLabels("skip"))
	kept.startJob(t, keptJob)
	keptIDs := runFoldSites(t, kept, keptJob)

	folding := newTestApp(t, real, func(c *Config) {
		c.Retention = storage.RetentionZero
		c.Fold = true
		c.SweepInterval = 3 * time.Millisecond
		c.SweepBatch = 2
		c.SweepBudget = time.Hour
	})
	foldingJob := foldJob(folding, &foldBroken, FilterLabels("skip"))
	folding.startJob(t, foldingJob)
	foldIDs := runFoldSites(t, folding, foldingJob)
	waitSwept(t, folding)

	want := viewsOf(t, kept, keptIDs)
	requireSameViews(t, viewsOf(t, folding, foldIDs), want, "after folding")
	if got := want[1].Labels["fetch"]["skip"]; got != 4 {
		t.Fatalf("skip label at fetch for 12 pages: got %d, want 4", got)
	}
	if want[0].Projection.TerminalDLQ != 1 {
		t.Fatalf("expected one dead letter per site: %+v", want[0].Projection)
	}
	keptStats, _ := kept.store.Stats(t.Context())
	foldStats, _ := folding.store.Stats(t.Context())
	if !reflect.DeepEqual(foldStats, keptStats) {
		t.Fatalf("Stats:\n got %+v\nwant %+v", foldStats, keptStats)
	}

	// Fix the page and replay the dead letters on both.
	keptBroken.Store(false)
	foldBroken.Store(false)
	for _, app := range []*testApp{kept, folding} {
		if _, err := app.store.Replay(t.Context(), storage.ReplayOpts{Queue: storage.DLQName(StepQueue("site", "fetch"))}); err != nil {
			t.Fatalf("Replay: %v", err)
		}
	}
	for _, id := range keptIDs {
		kept.waitForExecution(t, id, 1)
	}
	for _, id := range foldIDs {
		folding.waitForExecution(t, id, 1)
	}
	waitSwept(t, folding)
	requireSameViews(t, viewsOf(t, folding, foldIDs), viewsOf(t, kept, keptIDs), "after a replay")
}

// TestFilterLabelsCountItemsFinishedBeforeTheOption proves labels are read
// from the payload when an item is folded, so items that finished before the
// job had the option are still counted.
func TestFilterLabelsCountItemsFinishedBeforeTheOption(t *testing.T) {
	t.Parallel()
	var broken atomic.Bool
	app := newTestApp(t, func(c *Config) {
		c.Clock = storage.RealClock()
		c.Retention = storage.RetentionZero
		c.Fold = true
		c.SweepInterval = time.Hour // passes run by hand below
	})
	job := foldJob(app, &broken)
	app.startJob(t, job)
	ids := runFoldSites(t, app, job)
	before := viewsOf(t, app, ids)

	app.Job("site", FilterLabels("skip", "skip", ""))
	if _, err := app.lazySweeper().Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if n := doneRows(t, app); n != 0 {
		t.Fatalf("%d done rows left", n)
	}
	requireSameViews(t, viewsOf(t, app, ids), before, "after folding")
}

// backlogObserver is an Observer that also hears the sweep backlog.
type backlogObserver struct {
	NopObserver
	mu      sync.Mutex
	backlog []time.Duration
}

func (o *backlogObserver) SweepBacklog(d time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.backlog = append(o.backlog, d)
}

// TestSweepObserverHearsTheBacklog proves an Observer that implements
// SweepObserver is told the backlog after every pass, and that one that does
// not still works.
func TestSweepObserverHearsTheBacklog(t *testing.T) {
	t.Parallel()
	obs := &backlogObserver{}
	app := newTestApp(t, func(c *Config) { c.Observer = obs; c.Retention = time.Hour })
	if _, err := app.lazySweeper().Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if len(obs.backlog) != 1 || obs.backlog[0] != 0 {
		t.Fatalf("backlog: got %v, want [0]", obs.backlog)
	}

	plain := newTestApp(t, func(c *Config) { c.Observer = NopObserver{}; c.Retention = time.Hour })
	if _, err := plain.lazySweeper().Pass(t.Context()); err != nil {
		t.Fatalf("Pass: %v", err)
	}
}
