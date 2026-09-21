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

Postgres is the only backend in the first pass. The `Store` interface and `storage/storagetest`
conformance suite still exist from stage 1 — they are what keeps SQLite cheap later — but no SQLite
adapter is written until v0.1 is shipped and in use. Every stage is complete only with its
concurrency criteria green; "works single-threaded" is not a passing stage.

Each stage lands as one PR on a fresh branch off `origin/main`, with a judge brief in `docs/briefs/`.
Stage N starts only when stage N-1 is judged PASS.

### Stage 1 — Storage contract, PG schema, test harness
- `storage/storage.go`: `Store` interface (`Enqueue`, `Claim`, `Complete`, `Retry`, `DeadLetter`, `Replay`, `ExtendLease`, `ReclaimExpired`, `Stats`, `Sweep`, `PauseQueue`, `ResumeQueue`) and shared types (`Item`, `NewItem`, `Policy`, `ClaimOpts`).
- `storage/postgres/migration`: goose migrations for `durableq_items`, `durableq_attempts`, `durableq_executions`, `durableq_steps`, `durableq_queues`. Indexes: partial on `(queue, available_at, id) WHERE state='ready'`; partial on `(lease_until) WHERE state='running'`; `(execution_id, step_id)`. All statements schema-qualified.
- `internal/dqtest`: per-test isolated schema, injectable clock, test signals, `WaitOrTimeout`.
- `storage/storagetest`: the exported conformance suite skeleton, wired to the PG adapter.
- Accept: migrations up/down clean and repeatable; two tests running in parallel against the same database cannot see each other's rows; clock injection proven by a test that advances time without sleeping; no adapter import outside `storage/postgres`.

### Stage 2 — Claim, ack, retry, lease (Postgres)
- `Claim` = `SELECT … FOR UPDATE SKIP LOCKED` plus state/lease/attempt update in one transaction.
- `Complete`, `Retry` (backoff from policy), `DeadLetter`, `ExtendLease`, `ReclaimExpired`.
- Accept (correctness): claim honours limit, queue, `available_at`, injected now, deterministic order; `leased_by` over column width truncates; every mutator is a no-op on an item not in the expected state and returns a typed not-found; interrupted lease returns the item to ready with attempt accounting correct.
- Accept (concurrency): see stage 4a criteria — stage 2 ships with its own claim-contention suite green.

### Stage 3 — Polling worker and public Queue API
- `queue.go`, `worker.go`, `retry.go`: `app.Queue("indexing")`, `Enqueue`, `Work(handler)` with a concurrency cap, lease heartbeat, jittered poll, graceful drain, panic recovery, stuck-handler detection, undecodable-payload path.
- Queue pause/resume honoured by the claim loop.
- Accept: worker cap holds under load; graceful stop returns in-flight work **without consuming an attempt**; panic is a normal failure path; a handler that never returns does not permanently hold a worker slot; unknown queue/step handler fails the item, not the worker; short retries are not lost between poll ticks.

### Stage 4 — DLQ, replay, retention
- Per-queue `<queue>.dlq`, `Replay(queue, filter, limit)`, sweeper with per-state retention (`-1` = never), batch-size circuit breaker.
- `cmd/durableq`: `queues`, `dlq ls`, `dlq replay`, `gc`, `pause`, `resume`.
- Accept: exhausted item lands in DLQ with error/attempts/worker metadata; replay returns it and it succeeds; sweeper only touches terminal items and never races a live lease; DLQ is never auto-swept.

### Stage 4a — Concurrency and race gate (hard gate)
A dedicated stage, not a section of the others. Nothing proceeds past it.
- Whole suite runs under `-race` in CI and locally; `-race` failures are stage-blocking, never quarantined.
- **No double delivery**: 8–32 concurrent workers draining 10k items; every item is claimed by exactly one worker at a time; total successes equal total items; no item observed `running` under two workers.
- **Lease expiry races**: reclaimer and the owning worker act simultaneously — a worker acking after its lease expired must lose to the reclaimer, or win atomically, never both. Assert the item does not end up both done and re-queued.
- **Ack vs reclaim vs cancel**: all three issued against one item within the same millisecond, all orderings exercised by forcing schedules with test signals, not sleeps.
- **Retry-vs-claim**: an item transitioning to ready at `available_at` while a claimer scans must not be claimed twice or skipped forever.
- **Replay vs sweep vs claim** on the same DLQ item.
- **Fan-out atomicity under contention**: ack-of-N + enqueue-of-N+1 with concurrent writers; kill the transaction mid-way and assert neither orphan nor duplicate downstream item.
- **Deadlock freedom**: sustained mixed workload (claim + ack + retry + sweep + replay) for 60s with zero PG deadlock errors; any `40P01` is a defect, not a retry-able blip.
- **Stress**: `go test -race -count=20` on the claim suite, plus `startstop`-style start/stop stress on worker pool, heartbeat, sweeper and reclaimer.
- **Chaos**: worker SIGKILL mid-lease, DB connection dropped mid-claim, clock skew on `available_at`, pool exhaustion.
- Accept: all of the above green, run 20× consecutively without an intermittent. A single flake is a defect with a root cause, never a re-run.

### Stage 5 — Job, Execution, lineage
- `job.go`, `execution.go`, `internal/execution`: the §15 builder, an execution row per `job.Run`, lineage (`execution_id`, `item_id`, `step_id`, `parent_item_id`) on every hand-off, ack-N + enqueue-N+1 in one transaction.
- Accept: 4-step pipeline; per-item lineage query returns the §10 trace; a DLQ'd item produces no downstream rows; concurrent executions of the same job do not cross-contaminate lineage.

### Stage 6 — Projections and leak detection
- `Projection(executionID)` derived by query from durable state, never from in-memory counters.
- Edge invariants from the decision table; `durableq run <exec_id>` renders the §14 table.
- Accept: projection matches hand-counted fixtures for success, retry, DLQ, filtered and capped-output cases; a deleted in-flight row is reported as a leak; projections are correct while the pipeline is actively running, not only at rest.

### Stage 7 — Telemetry
- `telemetry.go` + `internal/telemetry`: the §14 metric set via OTel; dimensions `queue/job/step/worker` only; `execution_id`/`item_id` to spans and logs.
- Accept: expected metrics and dimensions under an in-memory exporter; no high-cardinality label; no metric emitted on an idle poll.

### Stage 8 — Load and soak
- 100k items through the 4-step pipeline: terminal invariant holds, throughput recorded, no growth in `running` items, no connection leak, memory flat across the run.
- 30-minute soak with a 2% induced failure rate and one worker killed every minute.

### Stage 9 — Docs, examples, v0.1 tag
- `examples/queue`, `examples/pipeline`, README with the §15 API, integration notes for the crawler data plane.

### Deferred past v0.1
- `storage/sqlite`, validated by running the unchanged `storagetest` suite. The contract and the suite exist from stage 1 specifically so this is an additive change.
- `LISTEN/NOTIFY` wake-ups, leader election for multi-replica maintenance.

## 2. Cross-cutting rules
- Core packages never import `storage/postgres` or `storage/sqlite`; enforced by a lint test walking imports.
- `storage/storagetest` is the single conformance suite; a new adapter is "done" when it passes it.
- Every state transition is expressed as one store method; no multi-call transitions in runtime code.
- At-least-once is documented at every public entry point; no exactly-once language anywhere.
- Every test binary runs under `-race`; a race is a stage-blocking defect, never a quarantine.
- No `time.Sleep` in tests for synchronisation. Waiting happens on a test signal or an injected clock; a sleep in a test is a review rejection.
- Every concurrent test asserts an invariant, not a count reached after waiting: "exactly one claimer", "never both done and requeued", "no orphan downstream row".
- A flake is a defect with a root cause. Re-running to green is not a resolution.

## 3. Test strategy
- Unit: retry/backoff maths, policy resolution, projection arithmetic (pure functions, no DB).
- Conformance: `storagetest` against PG (docker). SQLite joins the same suite post-v0.1.
- Chaos and race coverage live in stage 4a; see that stage for the full list.
- Load smoke: 100k items through a 4-step pipeline, asserting the terminal invariant and recording throughput per adapter.

## 4. Sequencing note
Stages 1–4a deliver a Postgres-backed queue that is independently useful, shippable, and proven under
contention. The Job layer (5–6) builds on a store contract that is by then hardened rather than assumed.
SQLite is deliberately last: the conformance suite written in stage 1 is what makes it a small change,
and writing it earlier would slow every concurrency decision down to the lowest common denominator of
two backends.
