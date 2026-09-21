# Stage 6 — Projections and leak detection

Read `_common.md` first. Branch: `stage-6-projections`.

## Deliverables
`Projection(executionID)` derived **by query from durable state** — never from in-memory counters —
and `durableq run <exec_id>` rendering the §14 table.

Per-edge counters only: `received`, `succeeded`, `filtered`, `dlq`, `active`, `produced`.

## Criteria
1. Projection matches hand-counted fixtures for: all-success, with-retries, with-DLQ, with-filtered, and capped-output cases.
2. Edge invariant holds at terminal state: `received == succeeded + filtered + dlq + active`.
3. Edge continuity: `step[i].produced == step[i+1].received + in_flight`.
4. A deleted in-flight row is **reported as a leak**, not absorbed silently.
5. Capped discovery (design §21) surfaces `produced_capped` and `dropped` rather than a silently shrinking count.
6. Projections are correct **while the pipeline is running**, not only at rest. Assert mid-flight against a known state.
7. No global discovered-to-indexed equality is claimed anywhere in code or docs.
8. `durableq run <exec_id>` renders the §14 table correctly.
9. `go test ./... -race -count=1` clean; stages 4a and 5 still green (regression).

## Evidence
- `go run ./cmd/durableq run <exec_id>` output against a seeded execution.
- `go test ./... -race -count=1 2>&1 | tail -40`
