# Stage 4 — DLQ, replay, retention, CLI

Read `_common.md` first. Branch: `stage-4-dlq-replay-cli`.

## Deliverables
`internal/dlq`, sweeper, and `cmd/durableq` with verbs: `queues`, `dlq ls`, `dlq replay`, `gc`, `pause`, `resume`.

## Criteria
1. An item exhausting its attempts lands in `<queue>.dlq` carrying error, attempt count and worker identity.
2. `Replay` returns a DLQ item to its working queue with attempt reset; it then succeeds against a fixed handler.
3. Replay is filtered and limited: replay by error substring and by limit, both exercised.
4. Sweeper deletes only terminal (`done`) items past retention.
5. Retention `-1` means never delete — per state, with a case for each.
6. **DLQ is never auto-swept**, regardless of retention setting.
7. Sweeper never removes an item under a live lease, and never a `ready` item.
8. Sweeper deletes in bounded batches; a batch-size circuit breaker shrinks the batch on DB error and resets on success.
9. Sweeper respects context cancellation, stops immediately, and can run multiple times.
10. CLI: each verb produces correct output against a seeded database. Evidence: run each verb and show output.
11. **Race**: replay, sweep and claim issued against the same DLQ item concurrently — single coherent outcome, no lost item, no double-replay.
12. `go test ./... -race -count=1` clean.

## Evidence commands
- `go run ./cmd/durableq queues`, `... dlq ls <queue>.dlq`, `... dlq replay <queue>.dlq --limit 5`, `... gc`
- `go test ./... -race -count=1 2>&1 | tail -40`
