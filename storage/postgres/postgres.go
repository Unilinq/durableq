// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

// Package postgres implements the durableq storage contract on PostgreSQL.
//
// Every statement this package issues is schema-qualified, so a durableq
// installation can live in its own schema alongside an application's tables and
// the conformance suite can give each test its own isolated schema.
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/unilinq/durableq/storage"
)

// Config configures a Store.
type Config struct {
	// Pool is the connection pool the store uses. Required.
	Pool *pgxpool.Pool
	// Schema is the PostgreSQL schema holding the durableq tables.
	// Empty means "public".
	Schema string
	// Clock supplies the current time. Nil means the wall clock.
	Clock storage.Clock
	// DefaultPolicy applies to items enqueued without one. Zero value means
	// storage.DefaultPolicy.
	DefaultPolicy storage.Policy
	// LeasedByMax truncates the worker identity written to leased_by, matching
	// what the column can hold. Zero means 128.
	LeasedByMax int
	// DoneWindow is how far back Stats counts done items a fold swept, by
	// when they finished, and how long PrepareFold keeps their tallies.
	// Done items still in the table are always counted. Zero means 7 days,
	// the default retention, so Stats reports what it did before folding.
	DoneWindow time.Duration
}

// Store is the PostgreSQL implementation of storage.Store.
type Store struct {
	pool          *pgxpool.Pool
	schema        string
	clock         storage.Clock
	defaultPolicy storage.Policy
	leasedByMax   int
	doneWindow    time.Duration

	randMu sync.Mutex
	rand   *rand.Rand
}

var _ storage.Store = (*Store)(nil)

// New builds a Store. It does not run migrations; call MigrateUp for that.
func New(cfg Config) (*Store, error) {
	if cfg.Pool == nil {
		return nil, fmt.Errorf("postgres: Config.Pool is required")
	}
	schema := cfg.Schema
	if schema == "" {
		schema = "public"
	}
	clock := cfg.Clock
	if clock == nil {
		clock = storage.RealClock()
	}
	policy := cfg.DefaultPolicy
	if policy.MaxAttempts == 0 && len(policy.Schedule) == 0 {
		policy = storage.DefaultPolicy()
	}
	max := cfg.LeasedByMax
	if max == 0 {
		max = 128
	}
	doneWindow := cfg.DoneWindow
	if doneWindow <= 0 {
		doneWindow = 7 * 24 * time.Hour
	}
	return &Store{
		pool:          cfg.Pool,
		schema:        schema,
		clock:         clock,
		defaultPolicy: policy,
		leasedByMax:   max,
		doneWindow:    doneWindow,
		rand:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Schema returns the schema the store reads and writes.
func (s *Store) Schema() string { return s.schema }

// Close releases the store. The pool is owned by the caller and is left open.
func (s *Store) Close() error { return nil }

// tbl returns a schema-qualified table name.
func (s *Store) tbl(name string) string {
	return QuoteIdent(s.schema) + "." + name
}

func (s *Store) now(override time.Time) time.Time {
	if !override.IsZero() {
		return override.UTC()
	}
	return s.clock.Now().UTC()
}

// doneSince is the first hour of folded done items Stats counts at now.
func (s *Store) doneSince(now time.Time) time.Time {
	return now.Add(-s.doneWindow).Truncate(time.Hour)
}

// jitter returns d adjusted by up to +/-frac, never negative.
func (s *Store) jitter(d time.Duration, frac float64) time.Duration {
	if frac <= 0 || d <= 0 {
		return d
	}
	s.randMu.Lock()
	r := s.rand.Float64()
	s.randMu.Unlock()
	delta := (r*2 - 1) * frac * float64(d)
	out := time.Duration(float64(d) + delta)
	if out < 0 {
		return 0
	}
	return out
}

func (s *Store) truncateWorker(worker string) string {
	if len(worker) <= s.leasedByMax {
		return worker
	}
	return worker[:s.leasedByMax]
}

// itemColumns is the canonical select list; scanItem reads it in this order.
const itemColumns = `id, queue, payload, state, outcome, attempt, max_attempts,
	available_at, leased_by, lease_until, last_error, last_worker,
	execution_id, step_id, item_id, parent_item_id,
	produced, dropped, produced_capped, policy, created_at, updated_at, finalized_at`

// qualifiedItemColumns renders the canonical select list prefixed with a table
// alias, for statements where a bare column name would be ambiguous.
func qualifiedItemColumns(alias string) string {
	parts := strings.Split(itemColumns, ",")
	for i, p := range parts {
		parts[i] = alias + "." + strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}

type policyJSON struct {
	MaxAttempts int     `json:"max_attempts"`
	ScheduleNS  []int64 `json:"schedule_ns"`
	Jitter      float64 `json:"jitter"`
}

func encodePolicy(p storage.Policy) ([]byte, error) {
	pj := policyJSON{MaxAttempts: p.MaxAttempts, Jitter: p.Jitter}
	for _, d := range p.Schedule {
		pj.ScheduleNS = append(pj.ScheduleNS, int64(d))
	}
	return json.Marshal(pj)
}

func decodePolicy(raw []byte) (storage.Policy, error) {
	var pj policyJSON
	if len(raw) == 0 {
		return storage.DefaultPolicy(), nil
	}
	if err := json.Unmarshal(raw, &pj); err != nil {
		return storage.Policy{}, err
	}
	p := storage.Policy{MaxAttempts: pj.MaxAttempts, Jitter: pj.Jitter}
	for _, ns := range pj.ScheduleNS {
		p.Schedule = append(p.Schedule, time.Duration(ns))
	}
	return p, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanItem(row rowScanner) (storage.Item, error) {
	var (
		it           storage.Item
		outcome      *string
		availableAt  *time.Time
		leasedBy     *string
		leaseUntil   *time.Time
		lastError    *string
		lastWorker   *string
		executionID  *string
		stepID       *string
		itemID       *string
		parentItemID *string
		policyRaw    []byte
		finalizedAt  *time.Time
	)
	err := row.Scan(
		&it.ID, &it.Queue, &it.Payload, &it.State, &outcome, &it.Attempt, &it.MaxAttempts,
		&availableAt, &leasedBy, &leaseUntil, &lastError, &lastWorker,
		&executionID, &stepID, &itemID, &parentItemID,
		&it.Produced, &it.Dropped, &it.ProducedCapped, &policyRaw,
		&it.CreatedAt, &it.UpdatedAt, &finalizedAt,
	)
	if err != nil {
		return storage.Item{}, err
	}
	if outcome != nil {
		it.Outcome = storage.Outcome(*outcome)
	}
	if leasedBy != nil {
		it.LeasedBy = *leasedBy
	}
	if leaseUntil != nil {
		it.LeaseUntil = leaseUntil.UTC()
	}
	if lastError != nil {
		it.LastError = *lastError
	}
	if lastWorker != nil {
		it.LastWorker = *lastWorker
	}
	if executionID != nil {
		it.ExecutionID = *executionID
	}
	if stepID != nil {
		it.StepID = *stepID
	}
	if itemID != nil {
		it.ItemID = *itemID
	}
	if parentItemID != nil {
		it.ParentItemID = *parentItemID
	}
	if finalizedAt != nil {
		it.FinalizedAt = finalizedAt.UTC()
	}
	if availableAt != nil {
		it.AvailableAt = availableAt.UTC()
	}
	// A live item always has a policy; one read back from history has none.
	if policyRaw != nil {
		p, err := decodePolicy(policyRaw)
		if err != nil {
			return storage.Item{}, err
		}
		it.Policy = p
	}
	it.CreatedAt = it.CreatedAt.UTC()
	it.UpdatedAt = it.UpdatedAt.UTC()
	return it, nil
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// Enqueue inserts items and returns them with ids assigned.
func (s *Store) Enqueue(ctx context.Context, items ...storage.NewItem) ([]storage.Item, error) {
	if len(items) == 0 {
		return nil, nil
	}
	var out []storage.Item
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		out, err = s.enqueueTx(ctx, tx, s.now(time.Time{}), items)
		return err
	})
	return out, err
}

// enqueueChunk is the most items one INSERT statement carries.
const enqueueChunk = 5000

// enqueueTx inserts items inside an existing transaction. Complete uses it so
// that acking a step and enqueuing the work it produced are one atomic write.
func (s *Store) enqueueTx(ctx context.Context, tx pgx.Tx, now time.Time, items []storage.NewItem) ([]storage.Item, error) {
	if len(items) == 0 {
		return nil, nil
	}
	// One INSERT carries 9 parameters per item and Postgres allows 65535, so
	// a large batch goes in as several INSERTs inside the same transaction.
	if len(items) > enqueueChunk {
		var all []storage.Item
		for start := 0; start < len(items); start += enqueueChunk {
			end := min(start+enqueueChunk, len(items))
			got, err := s.enqueueTx(ctx, tx, now, items[start:end])
			if err != nil {
				return nil, err
			}
			all = append(all, got...)
		}
		return all, nil
	}
	if err := s.ensureQueuesTx(ctx, tx, queueNames(items)); err != nil {
		return nil, err
	}

	var (
		sb   strings.Builder
		args []any
	)
	sb.WriteString(`INSERT INTO ` + s.tbl("durableq_items") + ` (
		queue, payload, state, attempt, max_attempts, available_at, policy,
		execution_id, step_id, item_id, parent_item_id) VALUES `)
	for i, ni := range items {
		policy := s.defaultPolicy
		if ni.Policy != nil {
			policy = *ni.Policy
		}
		raw, err := encodePolicy(policy)
		if err != nil {
			return nil, err
		}
		payload := ni.Payload
		if len(payload) == 0 {
			payload = []byte(`{}`)
		}
		availableAt := ni.AvailableAt
		if availableAt.IsZero() {
			availableAt = now
		}
		maxAttempts := policy.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = storage.DefaultPolicy().MaxAttempts
		}
		if i > 0 {
			sb.WriteString(", ")
		}
		base := len(args)
		fmt.Fprintf(&sb, "($%d, $%d, 'ready', 0, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9)
		args = append(args,
			ni.Queue, payload, maxAttempts, availableAt.UTC(), raw,
			nullable(ni.ExecutionID), nullable(ni.StepID), nullable(ni.ItemID), nullable(ni.ParentItemID))
	}
	sb.WriteString(" RETURNING " + itemColumns)

	rows, err := tx.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storage.Item
	for rows.Next() {
		it, err := scanItem(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

func queueNames(items []storage.NewItem) []string {
	seen := map[string]bool{}
	var out []string
	for _, ni := range items {
		if !seen[ni.Queue] {
			seen[ni.Queue] = true
			out = append(out, ni.Queue)
		}
	}
	return out
}

func (s *Store) ensureQueuesTx(ctx context.Context, tx pgx.Tx, names []string) error {
	if len(names) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO `+s.tbl("durableq_queues")+` (name) SELECT unnest($1::text[]) ON CONFLICT (name) DO NOTHING`,
		names)
	return err
}

// historyItemColumns reads a durableq_item_history row in itemColumns order.
// A folded item was done, its updated_at was its finalized_at, and its last
// worker is the worker of its last attempt; its payload, policy and
// available_at are not kept.
const historyItemColumns = `h.id, h.queue, NULL::jsonb, 'done', h.outcome, h.attempt, h.max_attempts,
	NULL::timestamptz, NULL::text, NULL::timestamptz, h.last_error, h.attempts -> -1 ->> 'worker',
	h.execution_id, h.step_id, h.item_id, h.parent_item_id,
	h.produced, h.dropped, h.produced_capped, NULL::jsonb, h.created_at, h.finalized_at, h.finalized_at`

// GetItem reads one item by id, from its row or, once a fold swept it, from
// its history. One statement reads both, so an item being folded is found in
// exactly one of them.
func (s *Store) GetItem(ctx context.Context, id int64) (storage.Item, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+itemColumns+` FROM `+s.tbl("durableq_items")+` WHERE id = $1
		UNION ALL
		SELECT `+historyItemColumns+` FROM `+s.tbl("durableq_item_history")+` h WHERE h.id = $1`, id)
	it, err := scanItem(row)
	if err == pgx.ErrNoRows {
		return storage.Item{}, fmt.Errorf("%w: id %d", storage.ErrNotFound, id)
	}
	return it, err
}

// Stats returns per-queue counts. With no queues it reports every known queue.
//
// Done counts the done rows still in the table plus the items a fold swept
// that finished within the store's DoneWindow, read in one statement.
func (s *Store) Stats(ctx context.Context, queues ...string) ([]storage.QueueStat, error) {
	q := `SELECT q.name,
			COALESCE(SUM(CASE WHEN i.state = 'ready' THEN 1 ELSE 0 END), 0)::int,
			COALESCE(SUM(CASE WHEN i.state = 'running' THEN 1 ELSE 0 END), 0)::int,
			(COALESCE(SUM(CASE WHEN i.state = 'done' THEN 1 ELSE 0 END), 0) + COALESCE(t.done, 0))::bigint,
			COALESCE(SUM(CASE WHEN i.state = 'dlq' THEN 1 ELSE 0 END), 0)::int,
			(q.paused_at IS NOT NULL)
		FROM ` + s.tbl("durableq_queues") + ` q
		LEFT JOIN (
			SELECT queue, sum(done) AS done FROM ` + s.tbl("durableq_done_hourly") + `
			WHERE hour >= $1 GROUP BY queue
		) t ON t.queue = q.name
		LEFT JOIN ` + s.tbl("durableq_items") + ` i ON i.queue = q.name`
	args := []any{s.doneSince(s.now(time.Time{}))}
	if len(queues) > 0 {
		q += ` WHERE q.name = ANY($2)`
		args = append(args, queues)
	}
	q += ` GROUP BY q.name, q.paused_at, t.done ORDER BY q.name`

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storage.QueueStat
	for rows.Next() {
		var st storage.QueueStat
		if err := rows.Scan(&st.Queue, &st.Ready, &st.Running, &st.Done, &st.DLQ, &st.Paused); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// PauseQueue stops claims against a queue. Items already leased keep running.
func (s *Store) PauseQueue(ctx context.Context, queue string) error {
	return s.setPaused(ctx, queue, true)
}

// ResumeQueue undoes PauseQueue.
func (s *Store) ResumeQueue(ctx context.Context, queue string) error {
	return s.setPaused(ctx, queue, false)
}

func (s *Store) setPaused(ctx context.Context, queue string, paused bool) error {
	var pausedAt *time.Time
	if paused {
		now := s.now(time.Time{})
		pausedAt = &now
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO `+s.tbl("durableq_queues")+` (name, paused_at) VALUES ($1, $2)
		 ON CONFLICT (name) DO UPDATE SET paused_at = EXCLUDED.paused_at, updated_at = now()`,
		queue, pausedAt)
	return err
}

func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
