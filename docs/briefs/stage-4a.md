# Stage 4a — Concurrency and race gate (HARD GATE)

Read `_common.md` first. Branch: `stage-4a-race-gate`.
Nothing proceeds past this stage. This is a verification stage: it may add tests and fix defects
those tests expose, but it does not add features.

## Criteria
Each is a named test asserting an invariant. All run under `-race`.

1. **No double delivery.** 8, 16 and 32 concurrent workers drain 10,000 items. Every item claimed by exactly one worker at a time; total successes == total items; no item ever observed `running` under two workers.
2. **Ack after lease expiry.** A worker acks after its lease expired while the reclaimer acts on the same item. Assert the item does NOT end up both `done` and re-queued. One side wins atomically.
3. **Reclaimer vs owning worker.** Attempt count is adjusted exactly once — no lost and no double increment.
4. **Ack / retry / cancel interleaved.** All three issued against one item, all orderings forced with test signals (not sleeps). Single terminal state every time.
5. **Retry-vs-claim boundary.** An item becoming ready exactly at `available_at` while a claimer scans is claimed exactly once and never skipped indefinitely.
6. **Replay vs sweep vs claim** on one DLQ item concurrently: one coherent outcome.
7. **Fan-out atomicity.** Ack-of-N + enqueue-of-N+1 with concurrent writers; kill the transaction mid-way; assert neither an orphan nor a duplicate downstream item. (If stage 5 has not landed, exercise this at the store level with a two-statement transaction.)
8. **Deadlock freedom.** Sustained mixed workload (claim + ack + retry + sweep + replay) for 60 seconds, zero `40P01`. Any deadlock is a defect, not a retryable blip.
9. **Start/stop stress.** Worker pool, lease heartbeat, sweeper and reclaimer each started and stopped repeatedly from many goroutines. No race, no leaked goroutine.
10. **Chaos:** worker SIGKILL mid-lease (work reclaimed exactly once); DB connection dropped mid-claim (typed error, item left claimable); clock skew on `available_at`; connection-pool exhaustion.

## Gate
`go test ./... -race -count=1` green, **then 20 consecutive green runs**:
```
for i in $(seq 1 20); do go test ./... -race -count=1 > /tmp/dq-$i.log 2>&1 || echo "RUN $i FAILED"; done
```
A single intermittent across the 20 is a FAIL with a required root cause. Re-running is not a fix.

## Evidence
- Full output of the 20-run loop, with pass/fail per run.
- `go test ./storage/... -race -count=20 -run Contention -v | tail -40`
- Postgres log check for deadlocks: `docker logs durableq-test-pg 2>&1 | grep -ci deadlock` (expect 0).
