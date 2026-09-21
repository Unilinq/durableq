# Stage 5 — Job, Execution, lineage

Read `_common.md` first. Branch: `stage-5-job-execution`.

## Deliverables
`job.go`, `execution.go`, `internal/execution`. Public API exactly as `docs/design.md` §15:
```go
job := app.Job("document-ingestion").
    Step("discover", discover).
    Step("crawl", crawl).
    Step("process", process).
    Step("index", index)
execution, err := job.Run(ctx, input)
```

Per the §0 decisions in `docs/plan.md`:
- Step handler returns downstream payloads: `func(ctx, Item[T]) ([]U, error)`. Returning nil is a legitimate terminal success recorded as `terminal_filtered`.
- `Step1` convenience wrapper for single-output handlers.
- Ack-of-step-N and enqueue-into-step-N+1 are always one transaction.

## Criteria
1. A 4-step job creates the 3 internal queues and one execution row per `Run`.
2. Lineage (`execution_id`, `item_id`, `step_id`, `parent_item_id`) is carried across every hand-off and is durable state, not telemetry.
3. Per-item lineage query returns the §10 trace: per step, outcome and attempt count, with steps never entered shown as such.
4. A dead-lettered item produces **no** downstream rows; other items in the same execution continue independently.
5. Fan-out 1→N: one item producing 50 downstream items is recorded correctly on the edge.
6. Fan-out 1→0: nil return is `terminal_filtered`, distinct from success-with-output.
7. Ack+enqueue atomicity: transaction killed between the two leaves neither orphan nor duplicate.
8. Concurrent executions of the same job do not cross-contaminate lineage.
9. `go test ./... -race -count=1` clean; stage 4a suite still green (regression).

## Evidence
- `go test ./... -race -count=1 2>&1 | tail -40`
- Lineage output for one item from the examples pipeline.
