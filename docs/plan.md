# DurableQ implementation plan

Design source: `docs/design.md` (Confluence UE/96174083).
Module: `github.com/unilinq/durableq`. Go 1.26. Postgres first, SQLite second.

## 0. Decisions taken before coding (answers to §20 open questions)

| Question | Decision |
| --- | --- |
| Downstream enqueue + upstream ack in one transaction? | Yes, always. `Store.Complete(ctx, itemID, worker, []NewItem)` is a single transaction in both adapters. Cross-store fan-out is out of scope for v0.1, so there is no case where one transaction cannot cover it. |
| Attempt history retention | Every attempt writes a row to `durableq_attempts` (item_id, attempt, worker, started_at, ended_at, outcome, error). Compaction is a retention job, not a runtime concern: `durableq_attempts` rows are deletable independently of item state. Item keeps `attempt` and `last_error` denormalised so projections never join attempts. |
| Queue retention / cleanup | Terminal items (`done`) are deleted by a sweeper after `retention` (default 7d); DLQ items are never auto-deleted. Sweeper is a library-level goroutine plus a `durableq gc` CLI verb, both driven by the same store method. |
| Retry/DLQ policy owner | Queue owns the default policy; a step may override it. Effective policy = step override ?? queue default ?? package default (5 attempts, exponential 5s→30s→2m→10m, jitter). Policy is stored on the item at enqueue time so a policy change never rewrites in-flight work. |
| Zero / one / many downstream items | Step handler signature returns the downstream payloads: `func(ctx, Item[T]) ([]U, error)`. Returning nil is a legitimate terminal success ("filtered"), recorded as `terminal_filtered`, distinct from success-with-output. A convenience `Step1` wrapper wraps single-output handlers. |
| Projection meaning under fan-out | Per-edge counters only: each step records `received`, `succeeded`, `filtered`, `dlq`, `active`, `produced`. The leak invariant is per-edge (`received == succeeded + filtered + dlq + active`) and edge-continuity (`step[i].produced == step[i+1].received + in_flight`). No global discovered-to-indexed equality is claimed. §21's bounded-output limitation is handled by letting a step report `produced_capped = true` with `dropped` count, which surfaces in the projection instead of silently vanishing. |

## 1. Stages

Each stage lands as one PR on a fresh branch off `origin/main`, with a judge brief in `docs/briefs/`.
Stage N is only started when stage N-1 is judged PASS.

### Stage 1 — Storage contract + Postgres schema
- `storage/storage.go`: `Store` interface (`Enqueue`, `Claim`, `Complete`, `Retry`, `DeadLetter`, `Replay`, `ExtendLease`, `ReclaimExpired`, `Stats`, `Sweep`) and the shared types (`Item`, `NewItem`, `Policy`, `ClaimOpts`).
- `storage/postgres`: goose migrations for `durableq_items`, `durableq_attempts`, `durableq_executions`, `durableq_steps`. Indexes: `(queue, state, available_at)` partial on `state='ready'`; `(state, lease_until)` partial on `state='running'`; `(execution_id, step_id)`.
- No runtime code yet.
- Accept: migrations up/down clean against a throwaway PG; `go vet ./...` clean; no adapter import anywhere outside `storage/postgres`.

### Stage 2 — Postgres claim/ack/retry/lease
- `Claim` = `SELECT … FOR UPDATE SKIP LOCKED` + state/lease/attempt update in one transaction.
- `Complete`, `Retry` (sets `available_at` from policy), `DeadLetter` (moves to `<queue>.dlq` preserving lineage and error), `ReclaimExpired`.
- Accept: concurrency test with N=8 goroutines claiming 1000 items shows zero double-claims; killed-worker test (lease not renewed) shows the item reclaimed exactly once; retry backoff timings within tolerance.

### Stage 3 — Polling worker + public Queue API
- `queue.go`, `worker.go`, `retry.go`: `app.Queue("indexing")`, `Enqueue`, `Work(handler)` with concurrency, lease heartbeat, graceful drain on context cancel, panic recovery → retry.
- Accept: `examples/queue` processes 10k items across 3 worker processes; at-least-once holds under SIGKILL of one worker; nothing stuck in `running` after lease expiry.

### Stage 4 — DLQ + replay
- Per-queue `<queue>.dlq`, `Replay(queue, filter, limit)` resetting attempt and state.
- `cmd/durableq`: `queues`, `dlq ls`, `dlq replay`, `gc`.
- Accept: exhausted item lands in DLQ with error/attempts/worker_version metadata; replay returns it to the working queue and it succeeds against a fixed handler.

### Stage 5 — Job, Execution, lineage
- `job.go`, `execution.go`, `internal/execution`: builder API from §15, execution row per `job.Run`, lineage (`execution_id`, `item_id`, `step_id`, `parent_item_id`) carried across every hand-off.
- Ack-of-step-N + enqueue-into-step-N+1 in one transaction.
- Accept: 4-step example pipeline, item-level lineage query returns the §10 per-step trace; a DLQ'd item has no downstream rows.

### Stage 6 — Projections + leak detection
- `Projection(executionID)` returning the §14 table, derived by query from durable state (no counters cached in memory).
- Edge invariants from decision table; `durableq run <exec_id>` renders it.
- Accept: projection matches hand-counted fixtures for success, retry, DLQ, filtered and capped-output cases; an artificially deleted in-flight row is reported as a leak.

### Stage 7 — Telemetry
- `telemetry.go` + `internal/telemetry`: the §14 metric set via OTel meter, dimensions `queue/job/step/worker` only; `execution_id`/`item_id` go to span attributes and logs.
- Accept: metrics present with expected dimensions under an in-memory exporter; no high-cardinality label appears.

### Stage 8 — SQLite adapter
- `storage/sqlite` implementing the same contract via short write transactions (`BEGIN IMMEDIATE`), WAL mode, busy timeout.
- Accept: the entire stage 2–6 acceptance suite runs green against SQLite via a shared conformance test package (`storage/storagetest`).

### Stage 9 — Docs, examples, v0.1 tag
- `examples/queue`, `examples/pipeline` (discover→crawl→process→index), README with the §15 API, migration guide for embedding into the SharePoint/web crawler data plane.

## 2. Cross-cutting rules
- Core packages never import `storage/postgres` or `storage/sqlite`; enforced by a lint test walking imports.
- `storage/storagetest` is the single conformance suite; a new adapter is "done" when it passes it.
- Every state transition is expressed as one store method; no multi-call transitions in runtime code.
- At-least-once is documented at every public entry point; no exactly-once language anywhere.

## 3. Test strategy
- Unit: retry/backoff maths, policy resolution, projection arithmetic (pure functions, no DB).
- Conformance: `storagetest` against PG (docker) and SQLite (temp file).
- Chaos: worker SIGKILL mid-lease, DB connection drop mid-claim, clock skew on `available_at`.
- Load smoke: 100k items through a 4-step pipeline, asserting the terminal invariant and recording throughput per adapter.

## 4. Sequencing note
Stages 1–4 deliver a queue that is independently useful and shippable; the Job layer (5–6) can be built while the queue is already in use by a first consumer.
