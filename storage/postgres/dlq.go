package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/unilinq/durableq/storage"
)

// Replay returns dead-lettered items to their working queue with a fresh
// attempt budget. This is the standard recovery: fix the worker, drain the DLQ.
func (s *Store) Replay(ctx context.Context, opts storage.ReplayOpts) (int, error) {
	if opts.Queue == "" {
		return 0, nil
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	now := s.now(opts.Now)
	target := storage.BaseQueue(opts.Queue)

	var moved int
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := s.ensureQueuesTx(ctx, tx, []string{target}); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
			WITH candidate AS (
				SELECT id FROM `+s.tbl("durableq_items")+`
				WHERE queue = $1
				  AND state = 'dlq'
				  AND ($2::text IS NULL OR last_error ILIKE '%' || $2 || '%')
				ORDER BY id
				LIMIT $3
				FOR UPDATE SKIP LOCKED
			)
			UPDATE `+s.tbl("durableq_items")+` i
			SET queue        = $4,
			    state        = 'ready',
			    outcome      = NULL,
			    attempt      = 0,
			    available_at = $5,
			    updated_at   = $5,
			    finalized_at = NULL,
			    leased_by    = NULL,
			    lease_until  = NULL
			FROM candidate c
			WHERE i.id = c.id`,
			opts.Queue, nullable(opts.ErrorContains), limit, target, now)
		if err != nil {
			return err
		}
		moved = int(tag.RowsAffected())
		return nil
	})
	return moved, err
}

// Sweep deletes terminal successes past their retention.
//
// Dead-lettered items are never swept, whatever the retention: the DLQ is the
// record of what went wrong, and deleting it on a timer would destroy the only
// evidence an operator has. Running and ready items are never touched either,
// so a sweep can never race a live lease.
func (s *Store) Sweep(ctx context.Context, opts storage.SweepOpts) (int, error) {
	if opts.Retention == storage.RetentionNever {
		return 0, nil
	}
	if opts.Retention < 0 {
		return 0, nil
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}
	cutoff := s.now(opts.Now).Add(-opts.Retention)

	tag, err := s.pool.Exec(ctx, `
		DELETE FROM `+s.tbl("durableq_items")+` i
		USING (
			SELECT id FROM `+s.tbl("durableq_items")+`
			WHERE state = 'done'
			  AND finalized_at < $1
			  AND ($2::text IS NULL OR queue = $2)
			ORDER BY finalized_at
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		) c
		WHERE i.id = c.id`,
		cutoff, nullable(opts.Queue), limit)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// ListItems returns items matching a filter, newest first. It backs the CLI's
// dead-letter listing and the lineage queries the job layer needs.
func (s *Store) ListItems(ctx context.Context, f storage.ItemFilter) ([]storage.Item, error) {
	q := `SELECT ` + itemColumns + ` FROM ` + s.tbl("durableq_items") + ` WHERE true`
	var args []any
	add := func(clause string, arg any) {
		args = append(args, arg)
		q += clause
	}
	if f.Queue != "" {
		add(" AND queue = $"+itoa(len(args)+1), f.Queue)
	}
	if f.State != "" {
		add(" AND state = $"+itoa(len(args)+1), string(f.State))
	}
	if f.ExecutionID != "" {
		add(" AND execution_id = $"+itoa(len(args)+1), f.ExecutionID)
	}
	if f.ItemID != "" {
		add(" AND item_id = $"+itoa(len(args)+1), f.ItemID)
	}
	if f.ErrorContains != "" {
		add(" AND last_error ILIKE '%' || $"+itoa(len(args)+1)+" || '%'", f.ErrorContains)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit)
	q += " ORDER BY id DESC LIMIT $" + itoa(len(args))

	rows, err := s.pool.Query(ctx, q, args...)
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

// Attempts returns the attempt history for one item, oldest first.
func (s *Store) Attempts(ctx context.Context, id int64) ([]storage.Attempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT attempt, worker, started_at, ended_at, outcome, error
		FROM `+s.tbl("durableq_attempts")+`
		WHERE item_pk = $1 ORDER BY attempt, id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storage.Attempt
	for rows.Next() {
		var (
			a       storage.Attempt
			worker  *string
			ended   *time.Time
			outcome *string
			errText *string
		)
		if err := rows.Scan(&a.Attempt, &worker, &a.StartedAt, &ended, &outcome, &errText); err != nil {
			return nil, err
		}
		if worker != nil {
			a.Worker = *worker
		}
		if ended != nil {
			a.EndedAt = ended.UTC()
		}
		if outcome != nil {
			a.Outcome = *outcome
		}
		if errText != nil {
			a.Error = *errText
		}
		a.StartedAt = a.StartedAt.UTC()
		out = append(out, a)
	}
	return out, rows.Err()
}

func itoa(n int) string {
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n%10]
		n /= 10
	}
	return string(buf[i:])
}

// TerminateOtherBackends kills every other connection to this database. It
// exists so the conformance suite can prove that acking a step and enqueuing
// its downstream work survive losing a connection mid-transaction: either both
// happened or neither did.
//
// It is destructive by design and is only ever called against a test database.
func (s *Store) TerminateOtherBackends(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT count(*)::int FROM (
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND state = 'active'
		) t`).Scan(&n)
	return n, err
}
