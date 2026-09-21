# Stage 2 — Claim, ack, retry, lease (Postgres)

Read `_common.md` first. Branch: `stage-2-claim-ack-retry`.

## Deliverables
`storage/postgres` implementations of `Claim`, `Complete`, `Retry`, `DeadLetter`, `ExtendLease`, `ReclaimExpired`, plus the matching `storagetest` cases.

`Claim` = `SELECT ... FOR UPDATE SKIP LOCKED` + state/lease/attempt update, one transaction, no locks held during processing.

## Criteria
1. Claim honours `limit`.
2. Claim is constrained to the requested queue.
3. Claim skips items whose `available_at` is in the future, and honours an injected "now".
4. Claim order is deterministic: `available_at`, then id.
5. `leased_by` longer than the column width is truncated, not an error.
6. Every mutator is a no-op against an item in the wrong state and returns `ErrWrongState`; against a missing item returns `ErrNotFound`. Evidence: one case per mutator.
7. `Complete` only affects a `running` item. A `ready` or `dlq` item is untouched.
8. `Retry` sets `available_at` from the policy snapshot; assert exact backoff values with the injected clock (5s, 30s, 2m, 10m ± jitter bound).
9. An item whose lease expired mid-flight returns to `ready` with attempt accounting correct — not silently re-run, not double-incremented.
10. Batch transitions: many items in one call, mixed outcomes, each item's result reported individually.
11. `DeadLetter` moves the item to `<queue>.dlq` preserving lineage, error, attempts and worker identity.
12. **Contention**: 8 concurrent claimers against 1000 items — every item claimed exactly once, zero duplicates, zero lost. Run with `-race -count=10`.
13. No PG deadlocks under concurrent claim+ack. Any `40P01` is a defect.
14. `go test ./... -race -count=1` clean; the claim suite additionally green at `-count=10`.

## Evidence commands
- `go test ./storage/... -race -count=10 -run 'Claim|Lease|Retry' -v | tail -60`
- `docker exec durableq-test-pg psql -U postgres -d durableq_test -c "select state, count(*) from durableq_items group by 1"` after a contention run.
