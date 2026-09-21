// Package dqtest provides test isolation for durableq: one PostgreSQL schema
// per test, an injectable clock, and pool plumbing. Tests never share rows and
// never truncate between runs, so the whole suite runs in parallel.
package dqtest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/unilinq/durableq/storage"
	"github.com/unilinq/durableq/storage/postgres"
)

// DefaultDatabaseURL is the local throwaway database the suite expects when
// DURABLEQ_TEST_DATABASE_URL is unset.
const DefaultDatabaseURL = "postgres://postgres:durableq@localhost:45432/durableq_test?sslmode=disable"

// DatabaseURL returns the test database connection string.
func DatabaseURL() string {
	if u := os.Getenv("DURABLEQ_TEST_DATABASE_URL"); u != "" {
		return u
	}
	return DefaultDatabaseURL
}

var (
	poolOnce sync.Once
	poolVal  *pgxpool.Pool
	poolErr  error
)

// Pool returns the process-wide test pool. Every test shares it; isolation
// comes from schemas, not from connections.
func Pool(tb testing.TB) *pgxpool.Pool {
	tb.Helper()
	poolOnce.Do(func() {
		cfg, err := pgxpool.ParseConfig(DatabaseURL())
		if err != nil {
			poolErr = err
			return
		}
		// Room for the contention tests without starving the reclaimer.
		cfg.MaxConns = 32
		cfg.MinConns = 2
		poolVal, poolErr = pgxpool.NewWithConfig(context.Background(), cfg)
	})
	if poolErr != nil {
		tb.Fatalf("dqtest: connecting to %s: %v", DatabaseURL(), poolErr)
	}
	if err := poolVal.Ping(context.Background()); err != nil {
		tb.Fatalf("dqtest: pinging %s: %v\nIs the durableq-test-pg container running?", DatabaseURL(), err)
	}
	return poolVal
}

var schemaSeq struct {
	sync.Mutex
	n int
}

// Schema creates an isolated, migrated schema for one test and drops it when
// the test ends. Two tests calling Schema can never see each other's rows.
func Schema(tb testing.TB) string {
	tb.Helper()
	name := schemaName(tb)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, DatabaseURL())
	if err != nil {
		tb.Fatalf("dqtest: connect for migration: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if err := postgres.MigrateUp(ctx, conn, name); err != nil {
		tb.Fatalf("dqtest: migrating schema %s: %v", name, err)
	}
	tb.Cleanup(func() {
		ctx := context.Background()
		c, err := pgx.Connect(ctx, DatabaseURL())
		if err != nil {
			return
		}
		defer func() { _ = c.Close(ctx) }()
		_ = postgres.DropSchema(ctx, c, name)
	})
	return name
}

func schemaName(tb testing.TB) string {
	schemaSeq.Lock()
	schemaSeq.n++
	n := schemaSeq.n
	schemaSeq.Unlock()

	clean := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			return r
		case r >= 'A' && r <= 'Z':
			return r + 32
		default:
			return '_'
		}
	}, tb.Name())
	// PostgreSQL identifiers cap at 63 bytes; leave room for the suffix.
	const maxBase = 40
	if len(clean) > maxBase {
		clean = clean[:maxBase]
	}
	return fmt.Sprintf("dq_%s_%d_%d", clean, os.Getpid()%100000, n)
}

// StubClock is a Clock a test controls outright. Advancing it is how tests
// observe time-dependent behaviour without sleeping.
type StubClock struct {
	mu  sync.RWMutex
	now time.Time
}

var _ storage.Clock = (*StubClock)(nil)

// NewStubClock returns a clock reading t, truncated to microseconds so values
// survive a PostgreSQL round trip unchanged.
func NewStubClock(t time.Time) *StubClock {
	return &StubClock{now: t.UTC().Truncate(time.Microsecond)}
}

// NewStubClockNow returns a stub clock starting at the current time.
func NewStubClockNow() *StubClock { return NewStubClock(time.Now()) }

// Now returns the stubbed time.
func (c *StubClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

// Set moves the clock to t.
func (c *StubClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t.UTC().Truncate(time.Microsecond)
}

// Advance moves the clock forward by d.
func (c *StubClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// NewStore returns a store on its own schema, driven by clock. A nil clock
// gets a stub started at the current time.
func NewStore(tb testing.TB, clock storage.Clock) *postgres.Store {
	tb.Helper()
	if clock == nil {
		clock = NewStubClockNow()
	}
	st, err := postgres.New(postgres.Config{
		Pool:   Pool(tb),
		Schema: Schema(tb),
		Clock:  clock,
	})
	if err != nil {
		tb.Fatalf("dqtest: building store: %v", err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	return st
}

// NewStoreInSchema returns a store bound to an existing schema.
func NewStoreInSchema(tb testing.TB, schema string, clock storage.Clock) *postgres.Store {
	tb.Helper()
	if clock == nil {
		clock = NewStubClockNow()
	}
	st, err := postgres.New(postgres.Config{Pool: Pool(tb), Schema: schema, Clock: clock})
	if err != nil {
		tb.Fatalf("dqtest: building store: %v", err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	return st
}
