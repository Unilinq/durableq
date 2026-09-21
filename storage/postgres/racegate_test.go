package postgres_test

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/unilinq/durableq/internal/dqtest"
	"github.com/unilinq/durableq/storage"
	"github.com/unilinq/durableq/storage/postgres"
)

// dedicatedDatabase creates a database of its own for a test, so destructive
// operations such as terminating backends cannot disturb sibling tests that
// share the main test database.
func dedicatedDatabase(t *testing.T, name string) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()
	dbName := fmt.Sprintf("durableq_%s_%d", name, os.Getpid())

	admin, err := pgx.Connect(ctx, dqtest.DatabaseURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()

	_, _ = admin.Exec(ctx, `DROP DATABASE IF EXISTS `+postgres.QuoteIdent(dbName)+` WITH (FORCE)`)
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+postgres.QuoteIdent(dbName)); err != nil {
		t.Fatalf("create database: %v", err)
	}

	url := replaceDatabase(dqtest.DatabaseURL(), dbName)
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect to %s: %v", dbName, err)
	}
	if err := postgres.MigrateUp(ctx, conn, "public"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_ = conn.Close(ctx)

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		a, err := pgx.Connect(context.Background(), dqtest.DatabaseURL())
		if err != nil {
			return
		}
		defer func() { _ = a.Close(context.Background()) }()
		_, _ = a.Exec(context.Background(),
			`DROP DATABASE IF EXISTS `+postgres.QuoteIdent(dbName)+` WITH (FORCE)`)
	})
	return pool, url
}

func replaceDatabase(url, dbName string) string {
	// postgres://user:pass@host:port/dbname?params
	q := ""
	if i := strings.Index(url, "?"); i >= 0 {
		q = url[i:]
		url = url[:i]
	}
	i := strings.LastIndex(url, "/")
	return url[:i+1] + dbName + q
}

// TestFanOutSurvivesConnectionLoss is the harshest form of the atomicity
// criterion: connections die underneath Complete, which acks a step and
// enqueues the work it produced in one transaction. Either both happened or
// neither did, so an acked item always has exactly one downstream item.
func TestFanOutSurvivesConnectionLoss(t *testing.T) {
	t.Parallel()
	pool, _ := dedicatedDatabase(t, "fanout")

	store, err := postgres.New(postgres.Config{Pool: pool, Schema: "public"})
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	ctx := context.Background()

	const items = 200
	for i := 0; i < items; i += 100 {
		batch := make([]storage.NewItem, 0, 100)
		for j := 0; j < 100; j++ {
			batch = append(batch, storage.NewItem{Queue: "upstream"})
		}
		if _, err := store.Enqueue(ctx, batch...); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var kills atomic.Int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		// Paced so workers get far enough into a transaction to be worth
		// interrupting. Killing in a tight loop mostly kills idle connections.
		ticker := time.NewTicker(3 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
			n, err := store.TerminateOtherBackends(ctx)
			if err == nil {
				kills.Add(int64(n))
			}
		}
	}()

	var acked atomic.Int64
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			worker := fmt.Sprintf("fan-%d", w)
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, err := store.Claim(ctx, storage.ClaimOpts{
					Queue: "upstream", Worker: worker, Limit: 5, LeaseDuration: time.Minute,
				})
				if err != nil || len(got) == 0 {
					continue
				}
				for _, it := range got {
					res, err := store.Complete(ctx, storage.CompleteRequest{
						ID: it.ID, Worker: worker,
						Produced: []storage.NewItem{{Queue: "downstream"}},
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

	deadline := time.After(30 * time.Second)
	for {
		done := false
		select {
		case <-deadline:
			done = true
		default:
		}
		up, err := statOf(ctx, store, "upstream")
		if err == nil && up.Done >= items {
			done = true
		}
		if done {
			break
		}
	}
	close(stop)
	wg.Wait()

	if kills.Load() == 0 {
		t.Fatalf("no connections were terminated; the test proved nothing")
	}

	up, err := statOf(ctx, store, "upstream")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	down, err := statOf(ctx, store, "downstream")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	downstreamTotal := down.Ready + down.Running + down.Done + down.DLQ
	if up.Done != downstreamTotal {
		t.Fatalf("fan-out was torn by connection loss: %d upstream acked, %d downstream items "+
			"(%d connections killed)", up.Done, downstreamTotal, kills.Load())
	}
	t.Logf("acked %d upstream items with %d downstream items across %d killed connections",
		up.Done, downstreamTotal, kills.Load())
}

// TestWorkIsRecoveredAfterSIGKILL is the crash case for real: a separate
// process claims work and is killed outright, leaving the lease held by
// nothing. The reclaimer must return that work exactly once.
func TestWorkIsRecoveredAfterSIGKILL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clock := dqtest.NewStubClockNow()
	schema := dqtest.Schema(t)
	store := dqtest.NewStoreInSchema(t, schema, clock)

	const items = 5
	var in []storage.NewItem
	for i := 0; i < items; i++ {
		in = append(in, storage.NewItem{Queue: "crashy"})
	}
	if _, err := store.Enqueue(ctx, in...); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	bin := filepath.Join(t.TempDir(), "crashworker")
	build := exec.Command("go", "build", "-o", bin, "github.com/unilinq/durableq/internal/dqtest/crashworker")
	build.Dir = repoRootFrom(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building crashworker: %v\n%s", err, out)
	}

	cmd := exec.Command(bin,
		"-database-url", dqtest.DatabaseURL(),
		"-schema", schema,
		"-queue", "crashy",
		"-worker", "doomed-process",
		"-count", fmt.Sprint(items),
		"-lease", "30s",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting crashworker: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	claimed := map[int64]bool{}
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "READY" {
			break
		}
		var id int64
		if _, err := fmt.Sscanf(line, "CLAIMED %d", &id); err == nil {
			claimed[id] = true
		}
	}
	if len(claimed) != items {
		t.Fatalf("crashworker claimed %d items, want %d", len(claimed), items)
	}

	// The process dies with the work still leased to it. No ack, no release,
	// no chance to clean up.
	if err := cmd.Process.Signal(os.Kill); err != nil {
		t.Fatalf("SIGKILL: %v", err)
	}
	_ = cmd.Wait()

	for id := range claimed {
		it, err := store.GetItem(ctx, id)
		if err != nil {
			t.Fatalf("GetItem: %v", err)
		}
		if it.State != storage.StateRunning {
			t.Fatalf("item %d is %s; a killed process should leave its work leased", id, it.State)
		}
		if it.Attempt != 1 {
			t.Fatalf("item %d attempt: got %d, want 1", id, it.Attempt)
		}
	}

	// The lease lapses and the reclaimer returns the work — once.
	clock.Advance(31 * time.Second)
	res, err := store.ReclaimExpired(ctx, storage.ReclaimOpts{Limit: 100})
	if err != nil {
		t.Fatalf("ReclaimExpired: %v", err)
	}
	if res.Reclaimed != items {
		t.Fatalf("reclaimed %d items, want %d", res.Reclaimed, items)
	}

	// A second pass must find nothing: recovery is not repeatable.
	again, err := store.ReclaimExpired(ctx, storage.ReclaimOpts{Limit: 100})
	if err != nil {
		t.Fatalf("ReclaimExpired (second): %v", err)
	}
	if again.Reclaimed != 0 {
		t.Fatalf("second reclaim pass took %d items; work was recovered twice", again.Reclaimed)
	}

	for id := range claimed {
		it, err := store.GetItem(ctx, id)
		if err != nil {
			t.Fatalf("GetItem: %v", err)
		}
		if it.State != storage.StateReady {
			t.Fatalf("item %d is %s after reclaim, want ready", id, it.State)
		}
		if it.Attempt != 1 {
			t.Fatalf("item %d attempt after reclaim: got %d, want the one attempt it spent", id, it.Attempt)
		}
	}
}

// TestPoolExhaustionDoesNotCorruptState proves that starving the store of
// connections produces honest errors and leaves every item claimable, rather
// than losing work or wedging.
func TestPoolExhaustionDoesNotCorruptState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cfg, err := pgxpool.ParseConfig(dqtest.DatabaseURL())
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.MaxConns = 2
	cfg.MaxConnLifetime = time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	schema := dqtest.Schema(t)
	store, err := postgres.New(postgres.Config{Pool: pool, Schema: schema})
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	const items = 200
	var in []storage.NewItem
	for i := 0; i < items; i++ {
		in = append(in, storage.NewItem{Queue: "starved"})
	}
	if _, err := store.Enqueue(ctx, in...); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	var completed atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 20; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			worker := fmt.Sprintf("starved-%d", w)
			for completed.Load() < items {
				got, err := store.Claim(ctx, storage.ClaimOpts{
					Queue: "starved", Worker: worker, Limit: 5, LeaseDuration: time.Minute,
				})
				if err != nil {
					continue
				}
				for _, it := range got {
					res, err := store.Complete(ctx, storage.CompleteRequest{ID: it.ID, Worker: worker})
					if err != nil || len(res) == 0 {
						continue
					}
					if res[0].Err == nil {
						completed.Add(1)
					}
				}
			}
		}(w)
	}
	wg.Wait()

	st, err := statOf(ctx, store, "starved")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.Done != items {
		t.Fatalf("with a 2-connection pool: %d of %d items completed (ready %d, running %d)",
			st.Done, items, st.Ready, st.Running)
	}
}

func statOf(ctx context.Context, store *postgres.Store, queue string) (storage.QueueStat, error) {
	stats, err := store.Stats(ctx, queue)
	if err != nil {
		return storage.QueueStat{}, err
	}
	for _, s := range stats {
		if s.Queue == queue {
			return s, nil
		}
	}
	return storage.QueueStat{Queue: queue}, nil
}

func repoRootFrom(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		if dir == filepath.Dir(dir) {
			t.Fatalf("no go.mod above %s", wd)
		}
	}
}
