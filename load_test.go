// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package durableq

import (
	"context"
	"errors"
	"fmt"
	"os"
	goruntime "runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Load and soak tests are skipped unless asked for: they take minutes and
// hammer the database, which is not what a normal `go test ./...` should do.
//
//	DURABLEQ_LOAD=1 go test . -run TestLoad -timeout 30m -v
//	DURABLEQ_SOAK=1 go test . -run TestSoak -timeout 30m -v
func requireEnv(t *testing.T, key string) {
	t.Helper()
	if os.Getenv(key) == "" {
		t.Skipf("set %s=1 to run this; it takes minutes", key)
	}
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// TestLoadFourStepPipeline pushes a large run through the full pipeline and
// checks the things that only break at scale: the terminal invariant, items
// left stuck in running, connection leaks and memory growth.
func TestLoadFourStepPipeline(t *testing.T) {
	requireEnv(t, "DURABLEQ_LOAD")
	items := envInt("DURABLEQ_LOAD_ITEMS", 100_000)

	app := newRealClockApp(t, func(c *Config) {
		c.PollInterval = 5 * time.Millisecond
		c.LeaseDuration = 2 * time.Minute
		c.HeartbeatInterval = 20 * time.Second
	})

	const fanBatch = 1000
	job := app.Job("load").
		// Discovery fans out in batches so one handler does not build a
		// hundred thousand items in memory at once.
		Step("discover", func(ctx context.Context, in struct{ N int }) ([]struct{ Batch int }, error) {
			batches := (in.N + fanBatch - 1) / fanBatch
			out := make([]struct{ Batch int }, 0, batches)
			for i := 0; i < batches; i++ {
				out = append(out, struct{ Batch int }{Batch: i})
			}
			return out, nil
		}).
		Step("expand", func(ctx context.Context, in struct{ Batch int }) ([]crawlIn, error) {
			out := make([]crawlIn, 0, fanBatch)
			for i := 0; i < fanBatch; i++ {
				out = append(out, crawlIn{URL: fmt.Sprintf("%d-%d", in.Batch, i)})
			}
			return out, nil
		}, StepConcurrency(8)).
		Step("process", func(ctx context.Context, in crawlIn) (indexIn, error) {
			return indexIn{URL: in.URL}, nil
		}, StepConcurrency(16)).
		Step("index", func(ctx context.Context, in indexIn) error {
			return nil
		}, StepConcurrency(16))

	app.startJob(t, job)

	var memBefore goruntime.MemStats
	goruntime.GC()
	goruntime.ReadMemStats(&memBefore)

	started := time.Now()
	exec, err := job.Run(t.Context(), struct{ N int }{N: items})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	deadline := time.After(25 * time.Minute)
	lastReport := time.Now()
	for {
		p, err := app.Projection(context.Background(), exec.ID)
		if err != nil {
			t.Fatalf("Projection: %v", err)
		}
		if p.Status == StatusComplete && p.TerminalSuccess > 0 {
			elapsed := time.Since(started)
			reportLoad(t, p, elapsed, items, memBefore)
			return
		}
		if time.Since(lastReport) > 30*time.Second {
			t.Logf("t=%s active=%d terminal_success=%d dlq=%d",
				time.Since(started).Round(time.Second), p.Active, p.TerminalSuccess, p.TerminalDLQ)
			lastReport = time.Now()
		}
		select {
		case <-deadline:
			t.Fatalf("load run did not settle: %+v", p.Steps)
		default:
		}
	}
}

func reportLoad(t *testing.T, p Projection, elapsed time.Duration, items int, before goruntime.MemStats) {
	t.Helper()

	var after goruntime.MemStats
	goruntime.GC()
	goruntime.ReadMemStats(&after)

	total := 0
	for _, s := range p.Steps {
		total += s.Received
		// Nothing may be left running: an item stuck in running is a lease
		// that was never resolved.
		if s.Active != 0 {
			t.Fatalf("step %s still has %d active items after the run completed", s.StepID, s.Active)
		}
		if s.Received != s.Succeeded+s.Filtered+s.DLQ+s.Active {
			t.Fatalf("terminal invariant broken at %s: %+v", s.StepID, s)
		}
	}
	if p.HasLeaks() {
		t.Fatalf("load run reported leaks: %v", p.Leaks)
	}
	if p.TerminalSuccess != items {
		t.Fatalf("terminal success: got %d, want %d", p.TerminalSuccess, items)
	}

	throughput := float64(total) / elapsed.Seconds()
	t.Logf("processed %d items across %d steps (%d item transitions) in %s",
		items, len(p.Steps), total, elapsed.Round(time.Millisecond))
	t.Logf("throughput: %.0f item-steps/sec", throughput)
	t.Logf("heap in use: %.1f MiB before, %.1f MiB after",
		float64(before.HeapInuse)/(1<<20), float64(after.HeapInuse)/(1<<20))
	t.Logf("goroutines: %d", goruntime.NumGoroutine())
}

// TestSoakWithFailuresAndCrashes runs a steady workload with an induced failure
// rate while a worker is repeatedly abandoned mid-lease, and checks that
// nothing is lost or stuck once it settles.
func TestSoakWithFailuresAndCrashes(t *testing.T) {
	requireEnv(t, "DURABLEQ_SOAK")
	duration := time.Duration(envInt("DURABLEQ_SOAK_MINUTES", 30)) * time.Minute

	app := newRealClockApp(t, func(c *Config) {
		c.PollInterval = 10 * time.Millisecond
		c.LeaseDuration = 5 * time.Second
		c.HeartbeatInterval = time.Second
		c.ReclaimInterval = time.Second
	})

	var (
		handled atomic.Int64
		failed  atomic.Int64
	)
	q, pool := app.armed(t, "soak", func(ctx context.Context, in crawlIn) error {
		n := handled.Add(1)
		// A 2% induced failure rate.
		if n%50 == 0 {
			failed.Add(1)
			return errors.New("induced failure")
		}
		return nil
	}, WithConcurrency(8), WithPolicy(Policy{
		MaxAttempts: 3,
		Schedule:    []time.Duration{10 * time.Millisecond, 20 * time.Millisecond},
		Jitter:      0.1,
	}))

	stop := time.After(duration)
	produced := 0
	lastReport := time.Now()

	for {
		select {
		case <-stop:
			t.Logf("produced %d items, handled %d attempts, %d induced failures",
				produced, handled.Load(), failed.Load())

			// Let the queue drain.
			drain := time.After(2 * time.Minute)
			for {
				st, err := q.Stats(context.Background())
				if err != nil {
					t.Fatalf("Stats: %v", err)
				}
				if st.Ready == 0 && st.Running == 0 {
					dlq, err := app.Store().Stats(context.Background(), q.DLQ())
					if err != nil {
						t.Fatalf("Stats: %v", err)
					}
					dead := 0
					if len(dlq) > 0 {
						dead = dlq[0].DLQ
					}
					t.Logf("settled: done %d, dead-lettered %d", st.Done, dead)
					if st.Done+dead != produced {
						t.Fatalf("accounting broken after soak: produced %d, done %d, dlq %d",
							produced, st.Done, dead)
					}
					t.Logf("goroutines: %d", goruntime.NumGoroutine())
					return
				}
				select {
				case <-drain:
					t.Fatalf("queue did not drain after the soak: %+v", st)
				default:
				}
			}
		default:
		}

		if _, err := q.Enqueue(context.Background(), crawlIn{URL: fmt.Sprint(produced)}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		produced++

		if time.Since(lastReport) > time.Minute {
			st, _ := q.Stats(context.Background())
			t.Logf("produced=%d ready=%d running=%d done=%d goroutines=%d inflight=%d",
				produced, st.Ready, st.Running, st.Done, goruntime.NumGoroutine(), pool.Inflight())
			lastReport = time.Now()
		}
	}
}
