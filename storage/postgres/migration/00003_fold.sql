-- Copyright 2026 Unilinq Inc
-- SPDX-License-Identifier: Apache-2.0

-- +goose Up
-- Folding: a sweep with SweepOpts.Fold deletes done items and, in the same
-- statement, adds them to the three tables below, so every reader reports the
-- same numbers after the sweep as before it. An item is counted as a row in
-- durableq_items until the statement that deletes it commits, and here after.
-- Nothing is written to these tables unless a sweep folds.

-- Saved per-step counts of folded items: what StepCounts would have counted
-- from their rows. Every folded item was done, so received is
-- succeeded + filtered.
CREATE TABLE durableq_step_counts (
    execution_id  text        NOT NULL,
    step_id       text        NOT NULL,
    succeeded     bigint      NOT NULL DEFAULT 0,
    filtered      bigint      NOT NULL DEFAULT 0,
    -- Filtered items counted by payload flag (SweepOpts.FilterLabels), as
    -- {"<flag>": <count>}.
    filtered_by   jsonb       NOT NULL DEFAULT '{}'::jsonb,
    produced      bigint      NOT NULL DEFAULT 0,
    dropped       bigint      NOT NULL DEFAULT 0,
    capped        bigint      NOT NULL DEFAULT 0,
    -- The greatest updated_at/finalized_at among the folded items.
    last_activity timestamptz,
    PRIMARY KEY (execution_id, step_id)
);

-- Folded items per queue and hour they finished, so Stats can keep reporting
-- done items for as long as a sweep without fold would have kept them.
CREATE TABLE durableq_done_hourly (
    queue text        NOT NULL,
    hour  timestamptz NOT NULL,
    done  bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (queue, hour)
);

-- What GetItem, Attempts and Lineage need of a folded item, written once and
-- never updated. One partition per UTC day of finalized_at, created ahead by
-- PrepareFold and dropped whole once older than the history retention.
-- No payload: nothing that reads history shows one.
CREATE TABLE durableq_item_history (
    id              bigint      NOT NULL,
    queue           text        NOT NULL,
    outcome         text,
    attempt         integer     NOT NULL,
    max_attempts    integer     NOT NULL,
    execution_id    text,
    step_id         text,
    item_id         text,
    parent_item_id  text,
    last_error      text,
    produced        integer     NOT NULL,
    dropped         integer     NOT NULL,
    produced_capped boolean     NOT NULL,
    created_at      timestamptz NOT NULL,
    finalized_at    timestamptz NOT NULL,
    -- The item's durableq_attempts rows in the order they were written:
    -- [{"attempt", "worker", "started_at", "ended_at", "outcome", "error"}].
    attempts        jsonb       NOT NULL DEFAULT '[]'::jsonb
) PARTITION BY RANGE (finalized_at);

CREATE INDEX durableq_item_history_id_idx
    ON durableq_item_history (id);

CREATE INDEX durableq_item_history_item_idx
    ON durableq_item_history (execution_id, item_id);

-- +goose Down
DROP TABLE durableq_item_history;
DROP TABLE durableq_done_hourly;
DROP TABLE durableq_step_counts;
