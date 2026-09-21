# Stage 1 — Storage contract, PG schema, test harness

Read `_common.md` first. Branch: `stage-1-storage-contract`.

## Deliverables
- `storage/storage.go`: `Store` interface — `Enqueue`, `Claim`, `Complete`, `Retry`, `DeadLetter`, `Replay`, `ExtendLease`, `ReclaimExpired`, `Stats`, `Sweep`, `PauseQueue`, `ResumeQueue` — plus `Item`, `NewItem`, `Policy`, `ClaimOpts`, typed errors (`ErrNotFound`, `ErrWrongState`).
- `storage/postgres/migration/`: goose migrations creating `durableq_items`, `durableq_attempts`, `durableq_executions`, `durableq_steps`, `durableq_queues`.
- `internal/dqtest/`: per-test isolated schema, injectable clock, test signals, `WaitOrTimeout`.
- `storage/storagetest/`: exported conformance suite skeleton with the stage-1 cases, run against the PG adapter.

## Schema requirements
- `durableq_items`: id, queue, payload (jsonb), state (`ready|running|done|dlq`), attempt, max_attempts, available_at, leased_by, lease_until, last_error, execution_id, step_id, item_id, parent_item_id, policy snapshot, created_at, updated_at.
- Partial index on `(queue, available_at, id) WHERE state = 'ready'`.
- Partial index on `(lease_until) WHERE state = 'running'`.
- Index on `(execution_id, step_id)`.
- `durableq_attempts` is separate from items; items keep denormalised `attempt` and `last_error` so projections never join attempts.
- Policy is snapshotted onto the item at enqueue time.

## Criteria
1. `goose up` then `goose down` then `goose up` against the test database leaves the schema identical. Evidence: run it, then dump `\d durableq_items` before and after and diff.
2. Two `go test` packages running in parallel against the same database cannot see each other's rows. Evidence: a test that proves schema isolation by asserting a row written in one test schema is absent in another.
3. Clock is injectable: a test advances time and observes an `available_at` transition **without any sleep**. Evidence: `grep -rn "time.Sleep" --include="*_test.go" .` returns nothing for synchronisation.
4. Test signals exist and are used: `WaitOrTimeout` present and exercised.
5. `storagetest` is an exported package (not `internal/`) and the PG adapter runs it.
6. Import discipline: a test walks imports and fails if any package outside `storage/postgres` imports it. Evidence: run the test, then temporarily add a bad import and show the test fails, then restore.
7. All SQL schema-qualified: an `AlternateSchema` case runs the suite against a non-default schema and passes.
8. `go build ./...`, `go vet ./...`, `go test ./... -race -count=1` all clean.

## Not in scope
Claim/ack logic (stage 2), worker loop, job layer. Stubs returning `ErrNotImplemented` are acceptable for methods not yet implemented, but the interface must be complete.
