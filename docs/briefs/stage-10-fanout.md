# Stage 10 — Fan-out in Jobs (UENG-621)

Read `_common.md` first. Branch: `stage-10-fanout` off `main` at `6d06433`.
All standing rules apply: everything under `-race`, no `time.Sleep` for synchronisation,
concurrent tests assert invariants, a flake is a defect.

## What this is
A Job step may have several successors. Its output is broadcast to every successor, each as its own
item on that successor's queue, enqueued in the transaction that acks the upstream item.

**Fan-out only.** Every step still has exactly one predecessor, so the shape is a tree. A join —
a step named as successor by more than one step — is rejected at build time with a clear error.
No barrier, no waiting, no new delivery mechanism.

## Public API
`After` is a new `StepOption` naming this step's predecessor:

```go
job := app.Job("sharepoint-ingest").
    Step("discover", discover).
    Step("document", doc,  durableq.After("discover")).
    Step("metadata", meta, durableq.After("discover")).
    Step("acl",      acl,  durableq.After("discover"), durableq.StepPolicy(graphPolicy))
```

- A step with no `After` keeps today's behaviour: its predecessor is the step declared before it.
  Every existing linear job must compile and behave identically, unchanged.
- `After` naming an unknown step is a build error.
- `After` with more than one argument is a build error: `durableq: join is not supported` — it is
  reserved for a later story, not silently accepted.
- Two steps naming the same `After` is the fan-out case and is valid.
- A cycle, or a step no path reaches from the first step, is a build error.

Handler shapes are unchanged. A handler returning `k` payloads at a step with `n` successors produces
`k * n` downstream items: the same payload set on each successor's queue.

## Storage
New migration `00002_step_edges.sql`.

- Add `durableq_step_edges (execution_id text, from_step_id text, to_step_id text)`, primary key on all
  three, FK to `durableq_executions` ON DELETE CASCADE, plus an index supporting lookup by
  `(execution_id, from_step_id)`.
- Backfill one edge per consecutive step pair from the existing `durableq_steps.next_queue` ordering,
  then drop `next_queue`. The down migration must restore the column and repopulate it for linear
  shapes. Prove up/down/up round-trips.
- `storage.StepDef` loses `NextQueue` and gains `Next []string` (successor step ids). `CreateExecution`
  writes the edges. `ExecutionSteps` returns them.

Edges are authoritative for topology. `idx` remains, for stable display ordering only — nothing may
infer topology from `idx` any more.

## Projection
- **Terminal steps are the leaves** (steps with no successors), not "the last step by idx".
  `TerminalSuccess` sums `succeeded` across every leaf.
- **Continuity becomes per fan-out group.** For a step `a` with successors `S`:
  `sum(received for s in S) == a.produced + in_flight`. `produced` on an item stays what it is today —
  the number of downstream items created — so this reduces exactly to the current invariant when
  `|S| == 1`. Do not change the meaning of the `produced` column.
- Per-step invariant `received == succeeded + filtered + dlq + active` is unchanged.
- A leak names the from-step and the edge it was detected on. A branch that is merely slower than its
  siblings must never be reported as a leak.
- Every defined step still appears in the projection with zero counts when it has no items.

## Criteria
1. Existing linear jobs are unaffected: the whole pre-existing suite passes untouched.
2. A three-way fan-out delivers every payload to all three branches; counts on each branch match.
3. Ack-of-upstream and enqueue-into-all-successors is one transaction. Kill the transaction mid-way:
   assert no orphan and no duplicate on any branch.
4. Branches are independent: one branch dead-letters while the others complete; one branch paused does
   not stop the others; per-step policy applies per branch.
5. Build-time validation: unknown `After`, multi-arg `After` (join), cycle, unreachable step — each a
   distinct, clear error. One test per case.
6. Projection on a fan-out: per-leaf terminal counts, per-group continuity, per-step invariant, and a
   deliberately deleted in-flight row reported as a leak naming the right edge.
7. A slow branch is not reported as a leak while work is still in flight.
8. Migration up/down/up round-trips; backfill produces the same edges a linear job would write.
9. Lineage across a fan-out: an item's trace shows the branch steps it entered and the ones it did not.
10. Race gate extended: fan-out atomicity across multiple successors under contention, at `-race`.
11. `go build ./...`, `go vet ./...`, `go test ./... -race -count=1` clean, then **20 consecutive green runs**.

## Evidence
- `go test ./... -race -count=1 2>&1 | tail -40`
- `for i in $(seq 1 20); do go test ./... -race -count=1 >/tmp/dq-fo-$i.log 2>&1 || echo "RUN $i FAILED"; done`
- `docker logs durableq-test-pg 2>&1 | grep -ci deadlock` (expect 0)
- Projection output for a seeded three-way fan-out execution.

## Out of scope
Joins, per-branch payload selection, barriers, and anything that makes a step wait.
