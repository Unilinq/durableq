package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/unilinq/durableq/storage"
)

// Claim atomically takes up to Limit ready items from a queue and leases them.
//
// The candidate scan uses FOR UPDATE SKIP LOCKED so concurrent claimers step
// over each other's rows instead of serialising. The row lock is held only for
// the length of this statement: long-lived ownership is the logical lease in
// lease_until, not a database lock, so a worker never holds a lock while it
// processes.
func (s *Store) Claim(ctx context.Context, opts storage.ClaimOpts) ([]storage.Item, error) {
	if opts.Limit <= 0 {
		return nil, nil
	}
	now := s.now(opts.Now)
	leaseUntil := now.Add(opts.LeaseDuration)
	if opts.LeaseDuration <= 0 {
		leaseUntil = now.Add(time.Minute)
	}
	worker := s.truncateWorker(opts.Worker)

	sql := `
WITH candidate AS (
	SELECT i.id
	FROM ` + s.tbl("durableq_items") + ` i
	WHERE i.queue = $1
	  AND i.state = 'ready'
	  AND i.available_at <= $2
	  AND NOT EXISTS (
	      SELECT 1 FROM ` + s.tbl("durableq_queues") + ` q
	      WHERE q.name = i.queue AND q.paused_at IS NOT NULL
	  )
	ORDER BY i.available_at, i.id
	LIMIT $3
	FOR UPDATE SKIP LOCKED
), claimed AS (
	UPDATE ` + s.tbl("durableq_items") + ` i
	SET state       = 'running',
	    attempt     = i.attempt + 1,
	    leased_by   = $4,
	    last_worker = $4,
	    lease_until = $5,
	    updated_at  = $2
	FROM candidate c
	WHERE i.id = c.id
	RETURNING ` + qualifiedItemColumns("i") + `
), logged AS (
	INSERT INTO ` + s.tbl("durableq_attempts") + ` (item_pk, attempt, worker, started_at)
	SELECT id, attempt, $4, $2 FROM claimed
	RETURNING item_pk
)
SELECT ` + itemColumns + ` FROM claimed ORDER BY available_at, id`

	rows, err := s.pool.Query(ctx, sql, opts.Queue, now, opts.Limit, worker, leaseUntil)
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

// workerArg renders the ownership guard. An empty worker means "no ownership
// check", which administrative paths such as the CLI use deliberately; normal
// worker code always passes its identity so a lapsed lease loses the race.
func (s *Store) workerArg(worker string) *string {
	if worker == "" {
		return nil
	}
	w := s.truncateWorker(worker)
	return &w
}

// classify explains why a state transition matched no rows: either the item is
// gone, or it is not in the state the transition requires.
func (s *Store) classify(ctx context.Context, tx pgx.Tx, id int64) error {
	var state string
	var leasedBy *string
	err := tx.QueryRow(ctx,
		`SELECT state, leased_by FROM `+s.tbl("durableq_items")+` WHERE id = $1`, id).
		Scan(&state, &leasedBy)
	if err == pgx.ErrNoRows {
		return fmt.Errorf("%w: id %d", storage.ErrNotFound, id)
	}
	if err != nil {
		return err
	}
	owner := "<none>"
	if leasedBy != nil {
		owner = *leasedBy
	}
	return fmt.Errorf("%w: id %d is %s (leased_by %s)", storage.ErrWrongState, id, state, owner)
}

// finishAttempt closes the open attempt row for an item.
func (s *Store) finishAttempt(b *pgx.Batch, id int64, now time.Time, outcome, errText string) {
	b.Queue(`UPDATE `+s.tbl("durableq_attempts")+`
		SET ended_at = $2, outcome = $3, error = $4
		WHERE item_pk = $1 AND ended_at IS NULL`,
		id, now, outcome, nullable(errText))
}

// Complete acks running items and enqueues the work they produced in the same
// transaction, so a step can never be marked done without its downstream work
// existing, nor create downstream work twice.
func (s *Store) Complete(ctx context.Context, reqs ...storage.CompleteRequest) ([]storage.Result, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	now := s.now(time.Time{})
	results := make([]storage.Result, len(reqs))
	for i, r := range reqs {
		results[i].ID = r.ID
	}

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		sql := `UPDATE ` + s.tbl("durableq_items") + `
			SET state = 'done', outcome = $2, finalized_at = $3, updated_at = $3,
			    leased_by = NULL, lease_until = NULL,
			    produced = $4, dropped = $5, produced_capped = $6
			WHERE id = $1 AND state = 'running'
			  AND ($7::text IS NULL OR leased_by = $7)
			RETURNING id`

		b := &pgx.Batch{}
		for _, r := range reqs {
			outcome := storage.OutcomeSuccess
			if r.Filtered {
				outcome = storage.OutcomeFiltered
			}
			b.Queue(sql, r.ID, string(outcome), now, len(r.Produced), r.Dropped, r.Capped, s.workerArg(r.Worker))
		}
		ok, err := runGuardedBatch(ctx, tx, b, len(reqs))
		if err != nil {
			return err
		}

		attempts := &pgx.Batch{}
		var downstream []storage.NewItem
		for i, r := range reqs {
			if !ok[i] {
				continue
			}
			outcome := "success"
			if r.Filtered {
				outcome = "filtered"
			}
			s.finishAttempt(attempts, r.ID, now, outcome, "")
			downstream = append(downstream, r.Produced...)
		}
		if attempts.Len() > 0 {
			if err := tx.SendBatch(ctx, attempts).Close(); err != nil {
				return err
			}
		}
		if len(downstream) > 0 {
			if _, err := s.enqueueTx(ctx, tx, now, downstream); err != nil {
				return err
			}
		}
		for i, r := range reqs {
			if ok[i] {
				continue
			}
			results[i].Err = s.classify(ctx, tx, r.ID)
		}
		return nil
	})
	return results, err
}

// Retry returns running items to ready after a failure. The attempt was
// consumed at claim time, so the counter is left alone here; the wait comes
// from the policy snapshotted onto the item.
func (s *Store) Retry(ctx context.Context, reqs ...storage.RetryRequest) ([]storage.Result, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	now := s.now(time.Time{})
	results := make([]storage.Result, len(reqs))
	for i, r := range reqs {
		results[i].ID = r.ID
	}

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// Read the policy snapshot and current attempt so backoff is computed
		// from what the item was enqueued with, not today's default.
		ids := make([]int64, len(reqs))
		for i, r := range reqs {
			ids[i] = r.ID
		}
		type live struct {
			attempt int
			policy  storage.Policy
		}
		current := map[int64]live{}
		rows, err := tx.Query(ctx,
			`SELECT id, attempt, policy FROM `+s.tbl("durableq_items")+` WHERE id = ANY($1)`, ids)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id int64
			var attempt int
			var raw []byte
			if err := rows.Scan(&id, &attempt, &raw); err != nil {
				rows.Close()
				return err
			}
			p, err := decodePolicy(raw)
			if err != nil {
				rows.Close()
				return err
			}
			current[id] = live{attempt: attempt, policy: p}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		sql := `UPDATE ` + s.tbl("durableq_items") + `
			SET state = 'ready', available_at = $2, updated_at = $3,
			    leased_by = NULL, lease_until = NULL, last_error = $4
			WHERE id = $1 AND state = 'running'
			  AND ($5::text IS NULL OR leased_by = $5)
			RETURNING id`

		b := &pgx.Batch{}
		for _, r := range reqs {
			at := r.At
			if at.IsZero() {
				l := current[r.ID]
				at = now.Add(s.jitter(l.policy.Backoff(l.attempt), l.policy.Jitter))
			}
			b.Queue(sql, r.ID, at.UTC(), now, nullable(r.Error), s.workerArg(r.Worker))
		}
		ok, err := runGuardedBatch(ctx, tx, b, len(reqs))
		if err != nil {
			return err
		}

		attempts := &pgx.Batch{}
		for i, r := range reqs {
			if ok[i] {
				s.finishAttempt(attempts, r.ID, now, "error", r.Error)
			}
		}
		if attempts.Len() > 0 {
			if err := tx.SendBatch(ctx, attempts).Close(); err != nil {
				return err
			}
		}
		for i, r := range reqs {
			if !ok[i] {
				results[i].Err = s.classify(ctx, tx, r.ID)
			}
		}
		return nil
	})
	return results, err
}

// DeadLetter moves running items to their queue's dead-letter queue, keeping
// lineage, error, attempt count and worker identity as metadata on the item.
func (s *Store) DeadLetter(ctx context.Context, reqs ...storage.DeadLetterRequest) ([]storage.Result, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	now := s.now(time.Time{})
	results := make([]storage.Result, len(reqs))
	for i, r := range reqs {
		results[i].ID = r.ID
	}

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		sql := `UPDATE ` + s.tbl("durableq_items") + `
			SET state = 'dlq', outcome = 'dlq', queue = queue || $2,
			    finalized_at = $3, updated_at = $3,
			    leased_by = NULL, lease_until = NULL, last_error = $4
			WHERE id = $1 AND state = 'running'
			  AND ($5::text IS NULL OR leased_by = $5)
			RETURNING id, queue`

		b := &pgx.Batch{}
		for _, r := range reqs {
			b.Queue(sql, r.ID, storage.DLQSuffix, now, nullable(r.Error), s.workerArg(r.Worker))
		}
		br := tx.SendBatch(ctx, b)
		ok := make([]bool, len(reqs))
		dlqNames := map[string]bool{}
		var batchErr error
		for i := range reqs {
			var id int64
			var queue string
			switch err := br.QueryRow().Scan(&id, &queue); {
			case err == pgx.ErrNoRows:
			case err != nil:
				if batchErr == nil {
					batchErr = err
				}
			default:
				ok[i] = true
				dlqNames[queue] = true
			}
		}
		if err := br.Close(); err != nil && batchErr == nil {
			batchErr = err
		}
		if batchErr != nil {
			return batchErr
		}

		// The DLQ is a queue in its own right, so it must exist for Stats,
		// pause and replay to see it.
		var names []string
		for n := range dlqNames {
			names = append(names, n)
		}
		if err := s.ensureQueuesTx(ctx, tx, names); err != nil {
			return err
		}

		attempts := &pgx.Batch{}
		for i, r := range reqs {
			if ok[i] {
				s.finishAttempt(attempts, r.ID, now, "dlq", r.Error)
			}
		}
		if attempts.Len() > 0 {
			if err := tx.SendBatch(ctx, attempts).Close(); err != nil {
				return err
			}
		}
		for i, r := range reqs {
			if !ok[i] {
				results[i].Err = s.classify(ctx, tx, r.ID)
			}
		}
		return nil
	})
	return results, err
}

// Release returns running items to ready WITHOUT consuming an attempt. Work
// handed back because a process is shutting down must not count against the
// item's budget, so the attempt increment from Claim is undone here.
func (s *Store) Release(ctx context.Context, reqs ...storage.ReleaseRequest) ([]storage.Result, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	now := s.now(time.Time{})
	results := make([]storage.Result, len(reqs))
	for i, r := range reqs {
		results[i].ID = r.ID
	}

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		sql := `UPDATE ` + s.tbl("durableq_items") + `
			SET state = 'ready', available_at = $2, updated_at = $2,
			    attempt = GREATEST(attempt - 1, 0),
			    leased_by = NULL, lease_until = NULL
			WHERE id = $1 AND state = 'running'
			  AND ($3::text IS NULL OR leased_by = $3)
			RETURNING id`

		b := &pgx.Batch{}
		for _, r := range reqs {
			b.Queue(sql, r.ID, now, s.workerArg(r.Worker))
		}
		ok, err := runGuardedBatch(ctx, tx, b, len(reqs))
		if err != nil {
			return err
		}

		// The attempt never happened as far as the budget is concerned, so its
		// log row is removed rather than closed.
		attempts := &pgx.Batch{}
		for i, r := range reqs {
			if ok[i] {
				attempts.Queue(`DELETE FROM `+s.tbl("durableq_attempts")+`
					WHERE item_pk = $1 AND ended_at IS NULL`, r.ID)
			}
		}
		if attempts.Len() > 0 {
			if err := tx.SendBatch(ctx, attempts).Close(); err != nil {
				return err
			}
		}
		for i, r := range reqs {
			if !ok[i] {
				results[i].Err = s.classify(ctx, tx, r.ID)
			}
		}
		return nil
	})
	return results, err
}

// ExtendLease renews the lease a worker holds on items it still owns. A worker
// whose lease already lapsed and was reclaimed cannot extend it back.
func (s *Store) ExtendLease(ctx context.Context, worker string, until time.Time, ids ...int64) ([]storage.Result, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	now := s.now(time.Time{})
	results := make([]storage.Result, len(ids))
	for i, id := range ids {
		results[i].ID = id
	}

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		sql := `UPDATE ` + s.tbl("durableq_items") + `
			SET lease_until = $2, updated_at = $3
			WHERE id = $1 AND state = 'running'
			  AND ($4::text IS NULL OR leased_by = $4)
			RETURNING id`

		b := &pgx.Batch{}
		for _, id := range ids {
			b.Queue(sql, id, until.UTC(), now, s.workerArg(worker))
		}
		ok, err := runGuardedBatch(ctx, tx, b, len(ids))
		if err != nil {
			return err
		}
		for i, id := range ids {
			if !ok[i] {
				results[i].Err = s.classify(ctx, tx, id)
			}
		}
		return nil
	})
	return results, err
}

// ReclaimExpired returns items whose lease lapsed to ready, or dead-letters
// them if the lapsed attempt was their last. The attempt counter is left as it
// is: the attempt was spent, whatever the worker managed to do with it.
func (s *Store) ReclaimExpired(ctx context.Context, opts storage.ReclaimOpts) (storage.ReclaimResult, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	now := s.now(opts.Now)

	var out storage.ReclaimResult
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, attempt, max_attempts, last_worker
			FROM `+s.tbl("durableq_items")+`
			WHERE state = 'running' AND lease_until < $1
			ORDER BY lease_until
			LIMIT $2
			FOR UPDATE SKIP LOCKED`, now, limit)
		if err != nil {
			return err
		}
		type expired struct {
			id      int64
			attempt int
			max     int
			worker  string
		}
		var items []expired
		for rows.Next() {
			var e expired
			var worker *string
			if err := rows.Scan(&e.id, &e.attempt, &e.max, &worker); err != nil {
				rows.Close()
				return err
			}
			if worker != nil {
				e.worker = *worker
			}
			items = append(items, e)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}

		var reclaim, dead []int64
		for _, e := range items {
			if e.attempt >= e.max {
				dead = append(dead, e.id)
			} else {
				reclaim = append(reclaim, e.id)
			}
		}

		const lapsed = "durableq: lease expired; worker did not finish the attempt"

		if len(reclaim) > 0 {
			tag, err := tx.Exec(ctx, `UPDATE `+s.tbl("durableq_items")+`
				SET state = 'ready', available_at = $2, updated_at = $2,
				    leased_by = NULL, lease_until = NULL, last_error = $3
				WHERE id = ANY($1)`, reclaim, now, lapsed)
			if err != nil {
				return err
			}
			out.Reclaimed = int(tag.RowsAffected())
		}
		if len(dead) > 0 {
			tag, err := tx.Exec(ctx, `UPDATE `+s.tbl("durableq_items")+`
				SET state = 'dlq', outcome = 'dlq', queue = queue || $2,
				    finalized_at = $3, updated_at = $3,
				    leased_by = NULL, lease_until = NULL, last_error = $4
				WHERE id = ANY($1)`, dead, storage.DLQSuffix, now, lapsed)
			if err != nil {
				return err
			}
			out.DeadLettered = int(tag.RowsAffected())

			rows, err := tx.Query(ctx,
				`SELECT DISTINCT queue FROM `+s.tbl("durableq_items")+` WHERE id = ANY($1)`, dead)
			if err != nil {
				return err
			}
			var names []string
			for rows.Next() {
				var n string
				if err := rows.Scan(&n); err != nil {
					rows.Close()
					return err
				}
				names = append(names, n)
			}
			rows.Close()
			if err := s.ensureQueuesTx(ctx, tx, names); err != nil {
				return err
			}
		}

		attempts := &pgx.Batch{}
		for _, e := range items {
			outcome := "reclaimed"
			if e.attempt >= e.max {
				outcome = "dlq"
			}
			s.finishAttempt(attempts, e.id, now, outcome, lapsed)
		}
		return tx.SendBatch(ctx, attempts).Close()
	})
	return out, err
}

// runGuardedBatch sends a batch of guarded UPDATE ... RETURNING id statements
// and reports which ones matched a row. A statement matching nothing is not an
// error: the caller classifies it as missing or wrong-state.
func runGuardedBatch(ctx context.Context, tx pgx.Tx, b *pgx.Batch, n int) ([]bool, error) {
	ok := make([]bool, n)
	br := tx.SendBatch(ctx, b)
	var firstErr error
	for i := 0; i < n; i++ {
		var id int64
		switch err := br.QueryRow().Scan(&id); {
		case err == pgx.ErrNoRows:
		case err != nil:
			if firstErr == nil {
				firstErr = err
			}
		default:
			ok[i] = true
		}
	}
	if err := br.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return ok, firstErr
}
