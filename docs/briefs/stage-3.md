# Stage 3 — Polling worker and public Queue API

Read `_common.md` first. Branch: `stage-3-worker-queue-api`.

## Deliverables
`queue.go`, `worker.go`, `retry.go`, `internal/runtime`, `internal/leasing`, `internal/scheduler`.

Public API exactly as `docs/design.md` §15:
```go
q := app.Queue("indexing")
q.Enqueue(ctx, payload)
q.Work(func(ctx context.Context, item IndexInput) error { return index(item) })
```
Leases, schedulers, retries and DLQ movement must not appear in worker code.

## Criteria
1. Worker concurrency cap holds under load — never more than N handlers in flight. Assert with an atomic counter and a high-water mark.
2. Ack and claim overlap without deadlock: completing work while fetching more.
3. Graceful stop returns in-flight work to `ready` **without consuming an attempt**. This is an explicit invariant — assert the attempt count.
4. Panic in a handler is a normal failure path: retried, and dead-lettered at max attempts. Not a process crash.
5. A handler that never returns does not permanently hold a worker slot: stuck detection frees it.
6. Stuck detection does not fire on parent-context cancellation (shutdown is not stuck).
7. Unknown queue or missing handler fails the item, not the worker.
8. Undecodable payload: retried per policy, then dead-lettered at max attempts. Never an infinite loop.
9. Short retries are not lost between poll ticks: an item becoming ready between ticks is picked up.
10. Poll interval is jittered so N workers do not stampede. Assert distribution, not a single value.
11. Queue pause stops claiming; resume restarts it. Pause taking effect mid-operation is exercised.
12. Lease heartbeat renews while a handler runs; a long handler does not lose its lease.
13. Start/stop stress: repeated concurrent start/stop of the worker pool, heartbeat and scheduler, no goroutine leak. Assert with `runtime.NumGoroutine()` settling.
14. `go test ./... -race -count=1` clean.

## Evidence commands
- `go test ./... -race -count=1 2>&1 | tail -40`
- `go test -race -count=5 -run 'Worker|Stop|Pause|Stuck' ./... -v | tail -60`
