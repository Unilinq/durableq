package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/unilinq/durableq/storage"
)

var _ storage.ExecutionStore = (*Store)(nil)

// CreateExecution records a job invocation and the shape of its pipeline.
// Writing the steps alongside the execution is what lets a projection be read
// later by a process that has never seen the job definition.
func (s *Store) CreateExecution(ctx context.Context, exec storage.Execution, steps []storage.StepDef) error {
	now := s.now(time.Time{})
	if exec.State == "" {
		exec.State = storage.ExecutionRunning
	}
	input := exec.Input
	if len(input) == 0 {
		input = []byte(`{}`)
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO `+s.tbl("durableq_executions")+
				` (id, job, state, input, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$5)`,
			exec.ID, exec.Job, exec.State, input, now); err != nil {
			return err
		}
		for _, st := range steps {
			if _, err := tx.Exec(ctx,
				`INSERT INTO `+s.tbl("durableq_steps")+
					` (execution_id, step_id, idx, queue) VALUES ($1,$2,$3,$4)`,
				exec.ID, st.StepID, st.Idx, st.Queue); err != nil {
				return err
			}
			for _, to := range st.Next {
				if _, err := tx.Exec(ctx,
					`INSERT INTO `+s.tbl("durableq_step_edges")+
						` (execution_id, from_step_id, to_step_id) VALUES ($1,$2,$3)`,
					exec.ID, st.StepID, to); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// GetExecution reads one execution.
func (s *Store) GetExecution(ctx context.Context, id string) (storage.Execution, error) {
	var e storage.Execution
	err := s.pool.QueryRow(ctx,
		`SELECT id, job, state, input, created_at, updated_at FROM `+
			s.tbl("durableq_executions")+` WHERE id = $1`, id).
		Scan(&e.ID, &e.Job, &e.State, &e.Input, &e.CreatedAt, &e.UpdatedAt)
	if err == pgx.ErrNoRows {
		return storage.Execution{}, fmt.Errorf("%w: execution %s", storage.ErrNotFound, id)
	}
	if err != nil {
		return storage.Execution{}, err
	}
	e.CreatedAt = e.CreatedAt.UTC()
	e.UpdatedAt = e.UpdatedAt.UTC()
	return e, nil
}

// ListExecutions returns executions newest first.
func (s *Store) ListExecutions(ctx context.Context, limit int) ([]storage.Execution, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, job, state, input, created_at, updated_at FROM `+
			s.tbl("durableq_executions")+` ORDER BY created_at DESC, id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storage.Execution
	for rows.Next() {
		var e storage.Execution
		if err := rows.Scan(&e.ID, &e.Job, &e.State, &e.Input, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, err
		}
		e.CreatedAt = e.CreatedAt.UTC()
		e.UpdatedAt = e.UpdatedAt.UTC()
		out = append(out, e)
	}
	return out, rows.Err()
}

// ExecutionSteps returns the pipeline shape recorded for an execution. Edges
// are authoritative for topology, so this is a left join against them:
// idx orders the steps, not the graph.
func (s *Store) ExecutionSteps(ctx context.Context, id string) ([]storage.StepDef, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT st.step_id, st.idx, st.queue, e.to_step_id
		FROM `+s.tbl("durableq_steps")+` st
		LEFT JOIN `+s.tbl("durableq_step_edges")+` e
		       ON e.execution_id = st.execution_id AND e.from_step_id = st.step_id
		WHERE st.execution_id = $1
		ORDER BY st.idx, e.to_step_id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	order := make([]string, 0)
	byID := map[string]*storage.StepDef{}
	for rows.Next() {
		var (
			stepID, queue string
			idx           int
			to            *string
		)
		if err := rows.Scan(&stepID, &idx, &queue, &to); err != nil {
			return nil, err
		}
		def, ok := byID[stepID]
		if !ok {
			def = &storage.StepDef{StepID: stepID, Idx: idx, Queue: queue}
			byID[stepID] = def
			order = append(order, stepID)
		}
		if to != nil {
			def.Next = append(def.Next, *to)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]storage.StepDef, 0, len(order))
	for _, id := range order {
		out = append(out, *byID[id])
	}
	return out, nil
}

// StepCounts derives the per-step tallies for an execution from durable item
// state. Nothing here reads an in-memory counter, so the numbers are the same
// whichever process asks and whenever it asks.
func (s *Store) StepCounts(ctx context.Context, executionID string) ([]storage.StepCount, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT st.step_id, st.idx, st.queue,
		       count(i.id)::int                                                      AS received,
		       count(*) FILTER (WHERE i.state = 'done' AND i.outcome = 'success')::int  AS succeeded,
		       count(*) FILTER (WHERE i.state = 'done' AND i.outcome = 'filtered')::int AS filtered,
		       count(*) FILTER (WHERE i.state = 'dlq')::int                            AS dlq,
		       count(*) FILTER (WHERE i.state IN ('ready', 'running'))::int            AS active,
		       COALESCE(sum(i.produced), 0)::int                                       AS produced,
		       COALESCE(sum(i.dropped), 0)::int                                        AS dropped,
		       count(*) FILTER (WHERE i.produced_capped)::int                          AS capped
		FROM `+s.tbl("durableq_steps")+` st
		LEFT JOIN `+s.tbl("durableq_items")+` i
		       ON i.execution_id = st.execution_id AND i.step_id = st.step_id
		WHERE st.execution_id = $1
		GROUP BY st.step_id, st.idx, st.queue
		ORDER BY st.idx`, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storage.StepCount
	for rows.Next() {
		var c storage.StepCount
		if err := rows.Scan(&c.StepID, &c.Idx, &c.Queue, &c.Received, &c.Succeeded,
			&c.Filtered, &c.DLQ, &c.Active, &c.Produced, &c.Dropped, &c.Capped); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Lineage answers "where is item ABC?" for one execution: every step in order,
// with what happened there, including the steps the item never reached.
func (s *Store) Lineage(ctx context.Context, executionID, itemID string) ([]storage.LineageEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT st.step_id, st.idx,
		       i.id, i.state, i.outcome, i.attempt, i.last_error, i.produced
		FROM `+s.tbl("durableq_steps")+` st
		LEFT JOIN `+s.tbl("durableq_items")+` i
		       ON i.execution_id = st.execution_id
		      AND i.step_id = st.step_id
		      AND i.item_id = $2
		WHERE st.execution_id = $1
		ORDER BY st.idx`, executionID, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storage.LineageEntry
	for rows.Next() {
		var (
			e        storage.LineageEntry
			pk       *int64
			state    *string
			outcome  *string
			attempt  *int
			errText  *string
			produced *int
		)
		if err := rows.Scan(&e.StepID, &e.Idx, &pk, &state, &outcome, &attempt, &errText, &produced); err != nil {
			return nil, err
		}
		if pk != nil {
			e.Entered = true
			e.ItemPK = *pk
		}
		if state != nil {
			e.State = storage.State(*state)
		}
		if outcome != nil {
			e.Outcome = storage.Outcome(*outcome)
		}
		if attempt != nil {
			e.Attempt = *attempt
		}
		if errText != nil {
			e.Error = *errText
		}
		if produced != nil {
			e.Produced = *produced
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
