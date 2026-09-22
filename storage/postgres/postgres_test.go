package postgres_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/unilinq/durableq/internal/dqtest"
	"github.com/unilinq/durableq/storage"
	"github.com/unilinq/durableq/storage/postgres"
	"github.com/unilinq/durableq/storage/storagetest"
)

// TestConformance runs the whole storage conformance suite against PostgreSQL.
func TestConformance(t *testing.T) {
	t.Parallel()
	storagetest.Run(t, storagetest.Harness{
		New: func(t *testing.T) (storage.Store, storagetest.Clock) {
			clock := dqtest.NewStubClockNow()
			return dqtest.NewStore(t, clock), clock
		},
	})
}

// TestMigrateUpDownUp proves migrations are reversible and repeatable: the
// schema after up/down/up is byte-identical to the schema after the first up.
func TestMigrateUpDownUp(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	schema := dqtest.Schema(t) // already migrated up once

	conn, err := pgx.Connect(ctx, dqtest.DatabaseURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	before := dumpSchema(t, conn, schema)
	if !strings.Contains(before, "durableq_items") {
		t.Fatalf("first migration did not create durableq_items:\n%s", before)
	}

	if err := postgres.MigrateDown(ctx, conn, schema, 0); err != nil {
		t.Fatalf("MigrateDown: %v", err)
	}
	afterDown := dumpSchema(t, conn, schema)
	if strings.Contains(afterDown, "durableq_items") {
		t.Fatalf("down migration left durableq_items behind:\n%s", afterDown)
	}

	if err := postgres.MigrateUp(ctx, conn, schema); err != nil {
		t.Fatalf("MigrateUp (second): %v", err)
	}
	after := dumpSchema(t, conn, schema)
	if before != after {
		t.Fatalf("schema differs after up/down/up\n--- before ---\n%s\n--- after ---\n%s", before, after)
	}
}

// TestStepEdgesBackfillMatchesALinearJob proves migration 00002's backfill
// produces exactly the edges a linear job's CreateExecution would write for
// the same pipeline shape, by seeding pre-00002 data (a next_queue-ordered
// chain) and letting the migration run.
func TestStepEdgesBackfillMatchesALinearJob(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	schema := dqtest.Schema(t) // already fully migrated

	conn, err := pgx.Connect(ctx, dqtest.DatabaseURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Roll back to just after 00001, where durableq_steps still has
	// next_queue and durableq_step_edges does not exist yet.
	if err := postgres.MigrateDown(ctx, conn, schema, 1); err != nil {
		t.Fatalf("MigrateDown to 1: %v", err)
	}

	if err := withSchema(ctx, conn, schema, func() error {
		if _, err := conn.Exec(ctx,
			`INSERT INTO durableq_executions (id, job) VALUES ('exec_backfill', 'chain')`); err != nil {
			return err
		}
		steps := []struct{ id, queue, next string }{
			{"a", "chain.a", "chain.b"},
			{"b", "chain.b", "chain.c"},
			{"c", "chain.c", ""},
		}
		for i, s := range steps {
			var next any
			if s.next != "" {
				next = s.next
			}
			if _, err := conn.Exec(ctx,
				`INSERT INTO durableq_steps (execution_id, step_id, idx, queue, next_queue) VALUES ($1,$2,$3,$4,$5)`,
				"exec_backfill", s.id, i, s.queue, next); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed pre-00002 data: %v", err)
	}

	// Reapplying 00002 backfills the edges from what we just seeded.
	if err := postgres.MigrateUp(ctx, conn, schema); err != nil {
		t.Fatalf("MigrateUp: %v", err)
	}

	got, err := queryEdges(ctx, conn, schema, "exec_backfill")
	if err != nil {
		t.Fatalf("queryEdges: %v", err)
	}
	want := []edge{{"a", "b"}, {"b", "c"}}
	if !edgesEqual(got, want) {
		t.Fatalf("backfilled edges: got %v, want %v", got, want)
	}

	// The same shape written directly through CreateExecution, post-00002,
	// produces the identical edge set.
	store, err := postgres.New(postgres.Config{Pool: dqtest.Pool(t), Schema: schema})
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := store.CreateExecution(ctx, storage.Execution{ID: "exec_direct", Job: "chain"},
		[]storage.StepDef{
			{StepID: "a", Idx: 0, Queue: "chain.a", Next: []string{"b"}},
			{StepID: "b", Idx: 1, Queue: "chain.b", Next: []string{"c"}},
			{StepID: "c", Idx: 2, Queue: "chain.c"},
		}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	direct, err := queryEdges(ctx, conn, schema, "exec_direct")
	if err != nil {
		t.Fatalf("queryEdges: %v", err)
	}
	if !edgesEqual(direct, want) {
		t.Fatalf("directly written edges: got %v, want %v", direct, want)
	}
	if !edgesEqual(got, direct) {
		t.Fatalf("backfilled edges %v do not match what a linear job writes directly %v", got, direct)
	}
}

type edge struct{ from, to string }

func queryEdges(ctx context.Context, conn *pgx.Conn, schema, execID string) ([]edge, error) {
	var out []edge
	err := withSchema(ctx, conn, schema, func() error {
		rows, err := conn.Query(ctx,
			`SELECT from_step_id, to_step_id FROM durableq_step_edges WHERE execution_id = $1 ORDER BY from_step_id, to_step_id`,
			execID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e edge
			if err := rows.Scan(&e.from, &e.to); err != nil {
				return err
			}
			out = append(out, e)
		}
		return rows.Err()
	})
	return out, err
}

func edgesEqual(a, b []edge) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// withSchema runs fn with search_path set to schema for the duration of one
// connection use, then restores it. Test-only convenience for issuing plain
// (unqualified) SQL against an isolated schema.
func withSchema(ctx context.Context, conn *pgx.Conn, schema string, fn func() error) error {
	if _, err := conn.Exec(ctx, "SET search_path TO "+postgres.QuoteIdent(schema)); err != nil {
		return err
	}
	defer func() { _, _ = conn.Exec(ctx, "SET search_path TO DEFAULT") }()
	return fn()
}

// TestMigrateUpIsIdempotent proves a second up is a no-op, which is what an
// operator re-running deploys relies on.
func TestMigrateUpIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	schema := dqtest.Schema(t)

	conn, err := pgx.Connect(ctx, dqtest.DatabaseURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	before := dumpSchema(t, conn, schema)
	if err := postgres.MigrateUp(ctx, conn, schema); err != nil {
		t.Fatalf("MigrateUp (repeat): %v", err)
	}
	if after := dumpSchema(t, conn, schema); before != after {
		t.Fatalf("repeat migration changed the schema\n--- before ---\n%s\n--- after ---\n%s", before, after)
	}

	version, err := postgres.AppliedVersion(ctx, conn, schema)
	if err != nil {
		t.Fatalf("AppliedVersion: %v", err)
	}
	if version < 1 {
		t.Fatalf("AppliedVersion: got %d, want at least 1", version)
	}
}

// TestAlternateSchema proves every statement is schema-qualified: a store in an
// oddly named schema works, and nothing leaks into public.
func TestAlternateSchema(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// A name needing quoting, to catch unquoted identifier interpolation.
	schema := dqtest.Schema(t) + "_Mixed-Case"
	conn, err := pgx.Connect(ctx, dqtest.DatabaseURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if err := postgres.MigrateUp(ctx, conn, schema); err != nil {
		t.Fatalf("MigrateUp into %q: %v", schema, err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), dqtest.DatabaseURL())
		if err != nil {
			return
		}
		defer func() { _ = c.Close(context.Background()) }()
		_ = postgres.DropSchema(context.Background(), c, schema)
	})

	store := dqtest.NewStoreInSchema(t, schema, nil)
	items, err := store.Enqueue(ctx, storage.NewItem{Queue: "alt"})
	if err != nil {
		t.Fatalf("Enqueue in alternate schema: %v", err)
	}
	got, err := store.GetItem(ctx, items[0].ID)
	if err != nil {
		t.Fatalf("GetItem in alternate schema: %v", err)
	}
	if got.Queue != "alt" {
		t.Fatalf("queue: got %q, want %q", got.Queue, "alt")
	}

	// public must be untouched: no durableq tables created there by accident.
	var count int
	err = conn.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_schema = 'public' AND table_name LIKE 'durableq%'`).Scan(&count)
	if err != nil {
		t.Fatalf("checking public schema: %v", err)
	}
	if count != 0 {
		t.Fatalf("public schema has %d durableq tables; statements are not schema-qualified", count)
	}
}

// TestClockIsInjectable proves time-dependent state is driven by the injected
// clock, so tests never wait on the wall clock.
func TestClockIsInjectable(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	base := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := dqtest.NewStubClock(base)
	store := dqtest.NewStore(t, clock)

	items, err := store.Enqueue(ctx, storage.NewItem{Queue: "clocked"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if got := items[0].AvailableAt; !got.Equal(base) {
		t.Fatalf("available_at: got %s, want the stubbed %s", got, base)
	}

	// Advancing the clock is observable immediately, with no sleeping.
	clock.Advance(72 * time.Hour)
	if got := clock.Now(); !got.Equal(base.Add(72 * time.Hour)) {
		t.Fatalf("clock did not advance: got %s", got)
	}
	items2, err := store.Enqueue(ctx, storage.NewItem{Queue: "clocked"})
	if err != nil {
		t.Fatalf("Enqueue after advance: %v", err)
	}
	if got := items2[0].AvailableAt; !got.Equal(base.Add(72 * time.Hour)) {
		t.Fatalf("available_at after advance: got %s, want %s", got, base.Add(72*time.Hour))
	}
}

// TestSchemaIsolation proves two schemas built by the harness cannot see each
// other's rows even when the queue names collide.
func TestSchemaIsolation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	a := dqtest.NewStore(t, nil)
	b := dqtest.NewStore(t, nil)
	if a.Schema() == b.Schema() {
		t.Fatalf("harness handed out the same schema twice: %s", a.Schema())
	}

	if _, err := a.Enqueue(ctx, storage.NewItem{Queue: "collide"}); err != nil {
		t.Fatalf("Enqueue into a: %v", err)
	}
	stats, err := b.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats on b: %v", err)
	}
	for _, s := range stats {
		if s.Ready != 0 {
			t.Fatalf("isolation broken: b sees %d ready items in %q", s.Ready, s.Queue)
		}
	}
}

// TestPolicyBackoff pins the documented retry schedule.
func TestPolicyBackoff(t *testing.T) {
	t.Parallel()
	p := storage.DefaultPolicy()
	want := []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}
	for i, w := range want {
		if got := p.Backoff(i + 1); got != w {
			t.Fatalf("Backoff(%d): got %s, want %s", i+1, got, w)
		}
	}
	// Past the end of the schedule the final wait repeats.
	if got := p.Backoff(99); got != 10*time.Minute {
		t.Fatalf("Backoff(99): got %s, want %s", got, 10*time.Minute)
	}
	if !p.Exhausted(5) || p.Exhausted(4) {
		t.Fatalf("Exhausted: got %v/%v for attempts 5/4, want true/false", p.Exhausted(5), p.Exhausted(4))
	}
	low, high := p.JitterBounds(1)
	if low != 4500*time.Millisecond || high != 5500*time.Millisecond {
		t.Fatalf("JitterBounds(1): got %s..%s, want 4.5s..5.5s", low, high)
	}
}

// dumpSchema renders the structure of a schema so two points in time can be
// compared exactly.
func dumpSchema(t *testing.T, conn *pgx.Conn, schema string) string {
	t.Helper()
	ctx := t.Context()
	var sb strings.Builder

	rows, err := conn.Query(ctx, `
		SELECT table_name, column_name, data_type, is_nullable, COALESCE(column_default, '')
		FROM information_schema.columns
		WHERE table_schema = $1
		ORDER BY table_name, column_name`, schema)
	if err != nil {
		t.Fatalf("dumpSchema columns: %v", err)
	}
	for rows.Next() {
		var tbl, col, typ, nullable, def string
		if err := rows.Scan(&tbl, &col, &typ, &nullable, &def); err != nil {
			rows.Close()
			t.Fatalf("dumpSchema scan: %v", err)
		}
		fmt.Fprintf(&sb, "column %s.%s %s null=%s default=%s\n", tbl, col, typ, nullable, def)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("dumpSchema columns: %v", err)
	}

	idx, err := conn.Query(ctx, `
		SELECT tablename, indexname, indexdef
		FROM pg_indexes WHERE schemaname = $1
		ORDER BY tablename, indexname`, schema)
	if err != nil {
		t.Fatalf("dumpSchema indexes: %v", err)
	}
	defer idx.Close()
	for idx.Next() {
		var tbl, name, def string
		if err := idx.Scan(&tbl, &name, &def); err != nil {
			t.Fatalf("dumpSchema index scan: %v", err)
		}
		// Index definitions embed the schema name; strip it so the comparison
		// is about structure.
		def = strings.ReplaceAll(def, schema, "<schema>")
		fmt.Fprintf(&sb, "index %s %s\n", tbl, def)
	}
	return sb.String()
}
