-- +goose Up
-- Fan-out in Jobs (UENG-621): a step may name several successors, so topology
-- is recorded as edges rather than a single next_queue column. Edges are
-- authoritative for topology; durableq_steps.idx remains but is display
-- ordering only from here on.

CREATE TABLE durableq_step_edges (
    execution_id text NOT NULL,
    from_step_id text NOT NULL,
    to_step_id   text NOT NULL,
    PRIMARY KEY (execution_id, from_step_id, to_step_id),
    CONSTRAINT durableq_step_edges_execution_fk
        FOREIGN KEY (execution_id) REFERENCES durableq_executions (id) ON DELETE CASCADE
);

-- Lookup by (execution_id, from_step_id): a step's successors.
CREATE INDEX durableq_step_edges_from_idx
    ON durableq_step_edges (execution_id, from_step_id);

-- Backfill: one edge per consecutive step pair, from the next_queue ordering
-- every existing (linear) execution was written with.
INSERT INTO durableq_step_edges (execution_id, from_step_id, to_step_id)
SELECT s.execution_id, s.step_id, ns.step_id
FROM durableq_steps s
JOIN durableq_steps ns
  ON ns.execution_id = s.execution_id AND ns.queue = s.next_queue
WHERE s.next_queue IS NOT NULL;

ALTER TABLE durableq_steps DROP COLUMN next_queue;

-- +goose Down
-- Restores next_queue and repopulates it from the edges. This is exact for a
-- linear shape (every step has at most one successor); for a fan-out shape
-- (a step with several successors) at most one edge can be represented in a
-- single column, so an arbitrary one of them is kept. That loss is inherent
-- to downgrading a schema that predates fan-out, not a bug in this migration.

ALTER TABLE durableq_steps ADD COLUMN next_queue text;

UPDATE durableq_steps s
SET next_queue = ns.queue
FROM durableq_step_edges e
JOIN durableq_steps ns
  ON ns.execution_id = e.execution_id AND ns.step_id = e.to_step_id
WHERE e.execution_id = s.execution_id AND e.from_step_id = s.step_id;

DROP TABLE durableq_step_edges;
