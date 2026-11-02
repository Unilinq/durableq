// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/unilinq/durableq/storage"
)

var _ storage.Folder = (*Store)(nil)

// defaultHistoryRetention is how long folded items' history is kept when
// FoldOpts or SweepOpts does not say.
const defaultHistoryRetention = 7 * 24 * time.Hour

// historyDaysAhead is how many days of history partitions PrepareFold creates
// past today, so a sweep near midnight never meets a missing partition.
const historyDaysAhead = 2

// historyPartitionPrefix names the per-day partitions of durableq_item_history:
// durableq_item_history_20261029 holds items finalized on 29 October 2026, UTC.
const historyPartitionPrefix = "durableq_item_history_"

// Sweep deletes one batch of done items past their retention, oldest first.
//
// Dead-lettered items are never swept, whatever the retention: the DLQ is the
// record of what went wrong, and deleting it on a timer would destroy the only
// evidence an operator has. Running and ready items are never touched either,
// so a sweep can never race a live lease.
//
// With opts.Fold the same statement adds the deleted items to the saved step
// counts, the per-hour done tally and the item history, so an item is counted
// by its row until the statement commits and by the saved data after it, and
// a batch cut off part-way changes nothing.
func (s *Store) Sweep(ctx context.Context, opts storage.SweepOpts) (storage.SweepResult, error) {
	retention := opts.Retention
	switch {
	case retention == storage.RetentionZero:
		retention = 0
	case retention < 0:
		return storage.SweepResult{}, nil
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}
	now := s.now(opts.Now)
	cutoff := now.Add(-retention)

	var (
		n    int
		last pgtype.Timestamptz
		err  error
	)
	if opts.Fold {
		var labels []byte
		labels, err = encodeFilterLabels(opts.FilterLabels)
		if err != nil {
			return storage.SweepResult{}, err
		}
		err = s.pool.QueryRow(ctx, s.foldSQL(),
			opts.After.UTC(), cutoff, nullable(opts.Queue), limit,
			labels, historyFrom(now, opts.HistoryRetention)).Scan(&n, &last)
	} else {
		err = s.pool.QueryRow(ctx, s.sweepSQL(),
			opts.After.UTC(), cutoff, nullable(opts.Queue), limit).Scan(&n, &last)
	}
	if err != nil {
		return storage.SweepResult{}, err
	}
	res := storage.SweepResult{Deleted: n}
	if last.Valid {
		res.Last = last.Time.UTC()
	}
	return res, nil
}

// dueSQL selects one batch of due items, oldest first: $1 after (inclusive),
// $2 cutoff (exclusive), $3 queue or NULL, $4 limit. SKIP LOCKED leaves an
// item another session holds for the next batch.
func (s *Store) dueSQL() string {
	return `due AS (
	SELECT id FROM ` + s.tbl("durableq_items") + `
	WHERE state = 'done'
	  AND finalized_at >= $1
	  AND finalized_at < $2
	  AND ($3::text IS NULL OR queue = $3)
	ORDER BY finalized_at
	LIMIT $4
	FOR UPDATE SKIP LOCKED
)`
}

func (s *Store) sweepSQL() string {
	return `
WITH ` + s.dueSQL() + `, gone AS (
	DELETE FROM ` + s.tbl("durableq_items") + ` i USING due
	WHERE i.id = due.id
	RETURNING i.finalized_at
)
SELECT count(*)::int, max(finalized_at) FROM gone`
}

// foldSQL deletes one batch like sweepSQL and, in the same statement, folds it:
// $5 is the filter labels as {"job": ["flag", ...]}, $6 the oldest finalized_at
// that still gets history. Attempts go with their items (ON DELETE CASCADE);
// every part of the statement reads the snapshot taken before the delete, so
// the history copies them first.
//
// The upserts take their row locks in key order, so two sweepers folding at
// once wait for each other rather than deadlock.
func (s *Store) foldSQL() string {
	return `
WITH ` + s.dueSQL() + `, gone AS (
	DELETE FROM ` + s.tbl("durableq_items") + ` i USING due
	WHERE i.id = due.id
	RETURNING i.id, i.queue, i.outcome, i.attempt, i.max_attempts, i.last_error,
	          i.execution_id, i.step_id, i.item_id, i.parent_item_id,
	          i.produced, i.dropped, i.produced_capped,
	          i.created_at, i.updated_at, i.finalized_at,
	          CASE WHEN i.outcome = 'filtered' AND $5::jsonb <> '{}'::jsonb THEN
	              ARRAY(SELECT l.flag
	                    FROM ` + s.tbl("durableq_executions") + ` e
	                    CROSS JOIN LATERAL jsonb_array_elements_text($5::jsonb -> e.job) AS l(flag)
	                    WHERE e.id = i.execution_id AND i.payload ->> l.flag = 'true')
	          END AS labels
), steps AS (
	SELECT execution_id, step_id,
	       count(*) FILTER (WHERE outcome = 'success')  AS succeeded,
	       count(*) FILTER (WHERE outcome = 'filtered') AS filtered,
	       sum(produced)                                AS produced,
	       sum(dropped)                                 AS dropped,
	       count(*) FILTER (WHERE produced_capped)      AS capped,
	       max(GREATEST(updated_at, finalized_at))      AS last_activity
	FROM gone
	WHERE execution_id IS NOT NULL AND step_id IS NOT NULL
	GROUP BY execution_id, step_id
), labelled AS (
	SELECT execution_id, step_id, jsonb_object_agg(flag, n) AS filtered_by
	FROM (
		SELECT execution_id, step_id, flag, count(*) AS n
		FROM gone CROSS JOIN LATERAL unnest(labels) AS flag
		WHERE execution_id IS NOT NULL AND step_id IS NOT NULL
		GROUP BY execution_id, step_id, flag
	) f
	GROUP BY execution_id, step_id
), counted AS (
	INSERT INTO ` + s.tbl("durableq_step_counts") + ` AS sc
		(execution_id, step_id, succeeded, filtered, filtered_by,
		 produced, dropped, capped, last_activity)
	SELECT st.execution_id, st.step_id, st.succeeded, st.filtered,
	       COALESCE(lb.filtered_by, '{}'::jsonb),
	       st.produced, st.dropped, st.capped, st.last_activity
	FROM steps st
	LEFT JOIN labelled lb ON lb.execution_id = st.execution_id AND lb.step_id = st.step_id
	ORDER BY st.execution_id, st.step_id
	ON CONFLICT (execution_id, step_id) DO UPDATE SET
		succeeded = sc.succeeded + EXCLUDED.succeeded,
		filtered  = sc.filtered + EXCLUDED.filtered,
		filtered_by = CASE WHEN EXCLUDED.filtered_by = '{}'::jsonb THEN sc.filtered_by ELSE (
			SELECT jsonb_object_agg(k, n) FROM (
				SELECT k, sum(v::bigint) AS n FROM (
					SELECT key AS k, value AS v FROM jsonb_each_text(sc.filtered_by)
					UNION ALL
					SELECT key, value FROM jsonb_each_text(EXCLUDED.filtered_by)
				) u GROUP BY k
			) t) END,
		produced  = sc.produced + EXCLUDED.produced,
		dropped   = sc.dropped + EXCLUDED.dropped,
		capped    = sc.capped + EXCLUDED.capped,
		last_activity = GREATEST(sc.last_activity, EXCLUDED.last_activity)
), tallied AS (
	INSERT INTO ` + s.tbl("durableq_done_hourly") + ` AS t (queue, hour, done)
	SELECT queue, date_trunc('hour', finalized_at, 'UTC'), count(*)
	FROM gone
	GROUP BY 1, 2
	ORDER BY 1, 2
	ON CONFLICT (queue, hour) DO UPDATE SET done = t.done + EXCLUDED.done
), kept AS (
	INSERT INTO ` + s.tbl("durableq_item_history") + `
		(id, queue, outcome, attempt, max_attempts, execution_id, step_id,
		 item_id, parent_item_id, last_error, produced, dropped, produced_capped,
		 created_at, finalized_at, attempts)
	SELECT g.id, g.queue, g.outcome, g.attempt, g.max_attempts, g.execution_id, g.step_id,
	       g.item_id, g.parent_item_id, g.last_error, g.produced, g.dropped, g.produced_capped,
	       g.created_at, g.finalized_at,
	       COALESCE((
	           SELECT jsonb_agg(jsonb_build_object(
	                      'attempt', a.attempt, 'worker', a.worker,
	                      'started_at', a.started_at, 'ended_at', a.ended_at,
	                      'outcome', a.outcome, 'error', a.error) ORDER BY a.id)
	           FROM ` + s.tbl("durableq_attempts") + ` a
	           WHERE a.item_pk = g.id), '[]'::jsonb)
	FROM gone g
	WHERE g.finalized_at >= $6
)
SELECT count(*)::int, max(finalized_at) FROM gone`
}

// encodeFilterLabels renders SweepOpts.FilterLabels for foldSQL, dropping
// empty and repeated flags so no item is counted twice under one label.
func encodeFilterLabels(labels map[string][]string) ([]byte, error) {
	clean := make(map[string][]string, len(labels))
	for job, flags := range labels {
		var keep []string
		for _, f := range flags {
			if f != "" && !slices.Contains(keep, f) {
				keep = append(keep, f)
			}
		}
		if len(keep) > 0 {
			clean[job] = keep
		}
	}
	return json.Marshal(clean)
}

// PrepareFold creates the history partitions for every day from the oldest
// retained one to a couple of days ahead, drops partitions past the history
// retention, and deletes done tallies older than the done window Stats reads.
// Two sweepers preparing at once take turns.
func (s *Store) PrepareFold(ctx context.Context, opts storage.FoldOpts) error {
	now := s.now(opts.Now)
	keepFrom := historyFrom(now, opts.HistoryRetention)
	// Partitions are dropped a day after the history stops being written to
	// them, so a sweeper whose pass began just before midnight never inserts
	// into a partition another sweeper has just dropped.
	dropBefore := keepFrom.AddDate(0, 0, -1)
	lastDay := utcDay(now).AddDate(0, 0, historyDaysAhead)

	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`,
			s.schema+".durableq_item_history"); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT c.relname FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			WHERE i.inhparent = $1::regclass`, s.tbl("durableq_item_history"))
		if err != nil {
			return err
		}
		have := map[time.Time]bool{}
		var drop []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			day, ok := partitionDay(name)
			if !ok {
				continue
			}
			have[day] = true
			if day.Before(dropBefore) {
				drop = append(drop, name)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, name := range drop {
			if _, err := tx.Exec(ctx, `DROP TABLE `+s.tbl(QuoteIdent(name))); err != nil {
				return fmt.Errorf("dropping history partition %s: %w", name, err)
			}
		}
		for day := keepFrom; !day.After(lastDay); day = day.AddDate(0, 0, 1) {
			if have[day] {
				continue
			}
			name := historyPartitionPrefix + day.Format("20060102")
			if _, err := tx.Exec(ctx, `CREATE TABLE `+s.tbl(QuoteIdent(name))+
				` PARTITION OF `+s.tbl("durableq_item_history")+
				` FOR VALUES FROM (`+timestampLiteral(day)+`) TO (`+timestampLiteral(day.AddDate(0, 0, 1))+`)`); err != nil {
				return fmt.Errorf("creating history partition %s: %w", name, err)
			}
		}
		_, err = tx.Exec(ctx, `DELETE FROM `+s.tbl("durableq_done_hourly")+` WHERE hour < $1`,
			s.doneSince(now))
		return err
	})
}

// historyFrom is the start of the oldest UTC day whose history is kept.
func historyFrom(now time.Time, retention time.Duration) time.Time {
	if retention <= 0 {
		retention = defaultHistoryRetention
	}
	return utcDay(now.Add(-retention))
}

func utcDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// partitionDay reads the day a history partition holds from its name.
func partitionDay(name string) (time.Time, bool) {
	suffix, ok := strings.CutPrefix(name, historyPartitionPrefix)
	if !ok {
		return time.Time{}, false
	}
	day, err := time.Parse("20060102", suffix)
	if err != nil {
		return time.Time{}, false
	}
	return day, true
}

// timestampLiteral renders a partition bound, which must be a literal.
func timestampLiteral(t time.Time) string {
	return "'" + t.UTC().Format("2006-01-02 15:04:05") + "+00'"
}
