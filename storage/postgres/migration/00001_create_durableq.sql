-- Copyright 2026 Unilinq Inc
-- SPDX-License-Identifier: Apache-2.0

-- +goose Up
-- Tables are created unqualified on purpose: the caller sets search_path to the
-- target schema, so the same migration serves the default schema, a dedicated
-- durableq schema, and the per-test schemas the conformance suite creates.

CREATE TABLE durableq_queues (
    name       text        PRIMARY KEY,
    paused_at  timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE durableq_executions (
    id         text        PRIMARY KEY,
    job        text        NOT NULL,
    state      text        NOT NULL DEFAULT 'running',
    input      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT durableq_executions_state_check CHECK (state IN ('running', 'complete'))
);

CREATE TABLE durableq_steps (
    execution_id text    NOT NULL REFERENCES durableq_executions (id) ON DELETE CASCADE,
    step_id      text    NOT NULL,
    idx          integer NOT NULL,
    queue        text    NOT NULL,
    next_queue   text,
    PRIMARY KEY (execution_id, step_id)
);

CREATE TABLE durableq_items (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue           text        NOT NULL,
    payload         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    state           text        NOT NULL,
    outcome         text,
    attempt         integer     NOT NULL DEFAULT 0,
    max_attempts    integer     NOT NULL,
    available_at    timestamptz NOT NULL,
    leased_by       text,
    lease_until     timestamptz,
    last_error      text,
    -- The worker that made the most recent attempt. Unlike leased_by this
    -- survives into terminal states, so a dead-lettered item still names who
    -- was running it.
    last_worker     text,

    execution_id    text,
    step_id         text,
    item_id         text,
    parent_item_id  text,

    produced        integer     NOT NULL DEFAULT 0,
    dropped         integer     NOT NULL DEFAULT 0,
    produced_capped boolean     NOT NULL DEFAULT false,

    policy          jsonb       NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    finalized_at    timestamptz,

    CONSTRAINT durableq_items_state_check
        CHECK (state IN ('ready', 'running', 'done', 'dlq')),
    CONSTRAINT durableq_items_outcome_check
        CHECK (outcome IS NULL OR outcome IN ('success', 'filtered', 'dlq')),
    -- A terminal item is finalized; a live item is not. Keeps retention and
    -- projection queries honest.
    CONSTRAINT durableq_items_finalized_check
        CHECK ((state IN ('done', 'dlq')) = (finalized_at IS NOT NULL)),
    -- A running item always names its owner and its lease deadline.
    CONSTRAINT durableq_items_lease_check
        CHECK ((state = 'running') = (leased_by IS NOT NULL AND lease_until IS NOT NULL))
);

-- The claim path: ready items in one queue, oldest available first.
CREATE INDEX durableq_items_claim_idx
    ON durableq_items (queue, available_at, id)
    WHERE state = 'ready';

-- The reclaim path: running items whose lease has lapsed.
CREATE INDEX durableq_items_lease_idx
    ON durableq_items (lease_until)
    WHERE state = 'running';

-- Lineage and projection queries.
CREATE INDEX durableq_items_lineage_idx
    ON durableq_items (execution_id, step_id);

CREATE INDEX durableq_items_item_idx
    ON durableq_items (execution_id, item_id);

-- Retention sweeping.
CREATE INDEX durableq_items_finalized_idx
    ON durableq_items (finalized_at)
    WHERE state = 'done';

-- Per-queue counts for Stats.
CREATE INDEX durableq_items_queue_state_idx
    ON durableq_items (queue, state);

CREATE TABLE durableq_attempts (
    id         bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    item_pk    bigint      NOT NULL REFERENCES durableq_items (id) ON DELETE CASCADE,
    attempt    integer     NOT NULL,
    worker     text,
    started_at timestamptz NOT NULL,
    ended_at   timestamptz,
    outcome    text,
    error      text
);

CREATE INDEX durableq_attempts_item_idx ON durableq_attempts (item_pk, attempt);

-- +goose Down
DROP TABLE durableq_attempts;
DROP TABLE durableq_items;
DROP TABLE durableq_steps;
DROP TABLE durableq_executions;
DROP TABLE durableq_queues;
