# DurableQ: Data Plane Orchestrator

## Context and Scope
Background processing systems generally provide one of two abstractions:


Queues such as River, BullMQ, Faktory, etc., which provide durable item processing, retries, and worker concurrency.

Workflow systems such as Temporal, which provide execution-level state and orchestration but introduce a substantially larger runtime model.
A common data-processing workload sits between these abstractions.
Consider an ingestion pipeline:
```
Trigger
   ↓
Discovery
   ↓
Queue
   ↓
Crawl
   ↓
Queue
   ↓
Process
   ↓
Queue
   ↓
Index
```

A discovery step may produce thousands or millions of independent items. Each item should:


be processed independently,

be retried independently,

eventually succeed or move to the corresponding DLQ.
At the same time, operators need to understand the entire trigger/run:
```
Run #123

Discovered     100,000
Processed       99,950
DLQ                 50
Active                0
Status          Complete
```

The desired abstraction is therefore:
Item-level reliability with run-level observability.
Hence we need something that provides both:
```
Simple Queue semantics
and
Multi-step Job semantics
```

using the same underlying queue engine.

# 2. Goals

### Queue
Provide a lightweight durable queue with:


at-least-once processing,

multiple competing workers,

timed retries,

configurable backoff,

worker leases and crash recovery,

per-queue DLQs,

DLQ replay,

delayed work,

PostgreSQL and SQLite storage.

### Jobs
Provide a higher-level Job abstraction consisting of multiple processing steps.
For:
```
A → B → C → D
```

DurableQ maintains durable queues between steps:
```
A → Q_AB → B → Q_BC → C → Q_CD → D
```

A four-step job therefore normally has three internal queues.

### Executions
Each invocation of a Job creates an Execution.
DurableQ tracks enough information to answer:


What happened to this run?

How many items entered each step?

How many succeeded?

How many are retrying?

How many ended up in DLQ?

Is there still active work?

Did any items disappear between stages?

### Deployment
DurableQ should require: application and a database and nothing else.
Initial storage implementations:


PostgreSQL

SQLite
No Redis, Kafka, RabbitMQ, or dedicated control plane should be necessary.

# 3. Non-goals
DurableQ v0.1 is not intended to be:


a distributed transaction coordinator,

an exactly-once execution system,

a general DAG engine - we are supporting very limited linear pipelines 

a Saga/compensation framework,

a human-approval workflow system - no wait / approval timers yet
The initial Job abstraction is intentionally simpler than a general workflow engine.

# 4. Design Overview
DurableQ has five primary concepts:
```
Queue
Job
Execution
Item
Worker
```

The relationship is:
```
Job
 │
 └── Execution
       │
       ├── Item A
       ├── Item B
       ├── Item C
       └── ...
```

A Job defines processing steps:
```
discover → crawl → process → index
```

Queues provide durable hand-offs:Sample Crawler Processing Job

A user can also use a Queue completely independently of Jobs.

# 5. Queue Semantics
A queue contains independent work items.
Conceptually: A worker claims items which are available. This ensures the items are going to be handled by only worker at a time. If the worker goes down, subsequent worker can claim it as it becomes “claimable” 
```
READY
  │
  │ Claim(worker-17)
  ▼
RUNNING
  claimed_by = worker-17
  lease_until = T
  attempt = 2
  │
  ├── success ───────────────► DONE
  │
  ├── failure, retries left
  │        ▼
  │      READY
  │      available_at = retry_time
  │
  └── failure, exhausted ────► indexing.dlq

RUNNING
  │
  └── lease expires ─────────► claimable by another worker
```

Every working queue has a corresponding DLQ:
```
indexing
indexing.dlq

crawl
crawl.dlq

processing
processing.dlq
```

The DLQ is associated with the queue, not with an error type.
Errors remain metadata on the item.
Example:
```
queue: indexing.dlq

item:
  error = unsupported_pdf_encoding
  attempts = 5
  worker_version = v1.8
```

After fixing the worker means draining DLQ - standard queue pattern. 
```
indexing.dlq
       ↓ replay
indexing
       ↓
worker v1.9
```


# 6. Retry Model
Retries belong to individual queue items.
An item contains approximately:
```
id
queue
payload

state
attempt
max_attempts

available_at

leased_by
lease_until

last_error

execution_id
step_id
item_id
```

available_at answers:
When may this work execute?
It supports:
```
timed retry
delayed jobs
backoff
```

lease_until answers:
Which worker currently owns this attempt, and when can ownership be recovered?
These are intentionally separate concepts.
Example:
```
attempt 1 → error
available_at = now + 5 sec

attempt 2 → error
available_at = now + 30 sec

attempt 3 → error
→ indexing.dlq
```

Retries do not create additional logical items.
Three attempts remain:
```
1 item
3 attempts
```


# 7. Worker Delivery and Claiming
Workers initially use polling.
```
Worker
   ↓
Claim(queue, limit)
   ↓
Process
   ↓
Ack / Retry / DLQ
```

The delivery mechanism is deliberately separated from queue correctness.
Later versions may add wake-up mechanisms such as PostgreSQL LISTEN/NOTIFY, but notifications only indicate:
There may be work available.
Workers must still call Claim().
This preserves the invariant:
The database is the source of truth; notification is only a latency optimization.

## PostgreSQL Claim
PostgreSQL uses short-lived row locking:
```
SELECT candidate rows
FOR UPDATE SKIP LOCKED
```

followed atomically by:
```
state = running
leased_by = worker-X
lease_until = T
attempt++
```

The database transaction then commits.
Workers do not hold database locks while processing.
The long-lived ownership mechanism is the logical lease.
If the worker crashes:
```
lease_until < now()
```

the item becomes eligible to be claimed again.

## SQLite Claim
SQLite implements the same logical Claim() contract using a short write transaction.
The concurrency characteristics may differ, but the externally visible semantics should remain the same.

# 8. Storage Abstraction
The runtime must not depend directly on PostgreSQL or SQLite.
Conceptually:
```
type Store interface {
    Enqueue(...)
    Claim(...)
    Ack(...)
    Retry(...)
    DeadLetter(...)
    Replay(...)
}
```

Job/execution operations extend this interface with execution metadata.
Implementations:
```
storage/postgres
storage/sqlite
```

Storage-specific mechanisms remain private.
For example:
```
Postgres
  SKIP LOCKED

SQLite
  write transaction
```

Both implement the same logical operation:
```
Claim(queue, worker, count, leaseDuration)
```


# 9. Job Model
A Job provides higher-level semantics over queues.
Example:
```
DocumentIngestion

discover
   ↓
crawl
   ↓
process
   ↓
index
```

DurableQ creates or associates the necessary queues:
```
crawl queue
process queue
index queue
```

A worker successfully processing one step causes the next durable work item to be created.
For example:
```
crawl(item)
   ↓ success
enqueue process(item)
```

The transition must be atomic with the completion of the current step where supported by the backing store.
A failed item does not continue downstream:
```
crawl
  ↓ retry
  ↓ retry
  ↓ exhausted
crawl.dlq

STOP
```

Other items in the same execution continue independently.

# 10. Execution and Lineage
A Job invocation creates an Execution:
```
execution_id = exec_123
```

Every work item retains lineage:
```
execution_id
item_id
step_id
```

This enables queries such as:
```
Where is item ABC?

discover      success
crawl         success, 2 attempts
process       DLQ
index         never entered
```

Lineage is durable state, not merely telemetry.

# 11. Fan-out
Fan-out requires no special orchestration primitive.
If Discovery finds another object:
```
Discovery
   ↓
enqueue another crawl item
```

If it finds 50,000 objects:
```
50,000 downstream queue items
```

The queue naturally absorbs dynamic cardinality.
DurableQ does not require an explicit Seal, CloseInput, or producer-completion barrier.

# 12. Run-Level Projections
Run state is derived from durable facts generated by workers and queues.
For example:
```
Discovery:
completed = true
produced = 10,000

Crawl:
received = 10,000
success = 9,990
dlq = 10
active = 0

Processing:
received = 9,990
success = 9,985
dlq = 5
active = 0
```

DurableQ can derive:
```
Execution exec_123
Status: COMPLETE

Terminal successful: 9,985
Terminal DLQ:             15
Active:                    0
```

The projection logic belongs in DurableQ because DurableQ owns the necessary lineage and queue state.
A dashboard does not.

# 13. Leak Detection
One important use case is detecting work that silently disappeared between processing stages.
For an edge:
```
Step A output
      =
Step B received
```

allowing for work still awaiting delivery.
At terminal state, a useful invariant is approximately:
```
items entering a step
=
successful items
+ dead-lettered items
```

For a completed execution:
```
discovered
=
terminal_success
+ terminal_DLQ
```

subject to the pipeline's transformation semantics.
DurableQ should expose the counts necessary to evaluate these invariants.
For v0.1, the system should not attempt to impose a universal business definition of "successful execution."

# 14. Observability
Observability has two separate layers.

### Durable observability
Stored by DurableQ:
```
Executions
Items
Attempts
Step transitions
Queue state
DLQ state
Errors
Worker ownership
```

This supports exact operational queries independent of metrics retention.

### Telemetry
DurableQ also emits metrics such as:
```
queue_depth
items_claimed_total
items_succeeded_total
items_retried_total
items_dead_lettered_total

claim_duration
processing_duration

lease_expirations_total
```

Common dimensions include:
```
queue
job
step
worker
```

High-cardinality identifiers such as execution_id or item_id should generally live in logs/traces/durable state rather than Prometheus labels.

### Visualization
Visualization is not part of the core runtime.
An optional CLI/UI can query DurableQ's projection API:
```
Run exec_123

Step        Received  Success  DLQ  Active
crawl         10,000    9,990   10      0
process        9,990    9,985    5      0
index          9,985    9,983    2      0
```

This allows visualization to evolve independently from the runtime.

# 15. Public API Shape
The public API should remain deliberately small.
Simple queue:
```
q := app.Queue("indexing")

q.Enqueue(ctx, payload)

q.Work(func(ctx context.Context, item IndexInput) error {
    return index(item)
})
```

Job:
```
job := app.Job("document-ingestion").
    Step("discover", discover).
    Step("crawl", crawl).
    Step("process", process).
    Step("index", index)

execution, err := job.Run(ctx, input)
```

Implementation details such as leases, schedulers, retries, and DLQ movement must not leak into normal worker code.

# 16. Repository Structure
The project uses the previously selected minimal public-surface structure:
```
durableq/
├── queue.go
├── job.go
├── worker.go
├── execution.go
├── retry.go
├── telemetry.go
│
├── storage/
│   ├── storage.go
│   ├── postgres/
│   └── sqlite/
│
├── internal/
│   ├── runtime/
│   ├── execution/
│   ├── scheduler/
│   ├── leasing/
│   ├── dlq/
│   └── telemetry/
│
├── cmd/
├── examples/
└── docs/
```

The public API remains small while internal implementation can evolve without breaking users.

# 17. Alternatives Considered

## Use River directly
River already provides a strong PostgreSQL-backed queue with:
```
retries
worker concurrency
scheduled jobs
SKIP LOCKED claiming
```

However, DurableQ's desired abstraction additionally treats:
```
Job
Execution
Step
Lineage
Run-level projection
Per-edge DLQ
```

as core concepts.
Building these semantics entirely above River would couple DurableQ to PostgreSQL and River's internal execution model, making SQLite or other storage implementations significantly harder.
Decision: implement queue semantics behind DurableQ's own storage contract.

## Temporal
Temporal provides durable workflows, retries, timers, and rich execution histories.
However, the primary workload here has very high item cardinality and relatively static pipeline topology:
```
one run
→ hundreds of thousands of independent items
→ simple sequence of processing stages
```

Making each individual item primarily a workflow/activity construct adds significantly more orchestration semantics than this problem requires.
Decision: remain queue-first rather than workflow-first.

## Message broker dependency
Redis, RabbitMQ, Kafka, etc. could provide efficient worker notification and delivery.
However, introducing another system violates one of DurableQ's central operational goals:
```
application + database
```

Decision: database-backed polling initially; optional notification optimizations later.

## Push delivery from the beginning
Workers could receive work over network connections.
This introduces:
```
worker registration
connection state
push routing
failure recovery
delivery acknowledgement
```

while still requiring durable database state.
Decision: polling first. Add push-assisted wakeups later without changing Claim() semantics.

## Explicit run completion barrier
The runtime could require:
```
run.Close()
run.Seal()
```

to indicate that no additional work will arrive.
For the target workloads, worker/step completion and durable queue state already provide enough facts to infer useful run-level projections.
Decision: no explicit completion barrier in v0.1.

# 18. Cross-Cutting Concerns

## Reliability
Primary guarantee:
At-least-once processing.
Workers must therefore assume duplicate execution is possible.
DurableQ does not claim exactly-once side effects.

## Crash recovery
Long-running ownership is represented by leases.
Expired leases allow abandoned work to be reclaimed.

## Atomicity
Where possible, these operations should be atomic:
```
claim + establish lease

ack current step + enqueue downstream item

retry state transition

retry exhaustion + DLQ transition
```


## Observability
Runtime behavior must be understandable using both:
```
durable state
and
metrics/logging/tracing
```

Observability is a design requirement, not an afterthought.

## Storage portability
Core runtime code must never import PostgreSQL or SQLite implementations directly.
Backend-specific behavior lives solely in storage adapters.

# 19. Initial Implementation Plan
The smallest useful implementation is:
```
1. Queue schema + storage contract
2. PostgreSQL implementation
3. Polling worker
4. Atomic Claim()
5. Ack
6. Timed retry
7. Lease expiry/recovery
8. Per-queue DLQ
9. DLQ replay
10. Job/Execution lineage
11. Multi-step Job transitions
12. Execution projections
13. Metrics hooks
14. SQLite adapter
15. CLI / visualization
```

The queue implementation should be independently usable before the Job abstraction is complete.

# 20. Open Design Questions
The major architecture is reasonably well defined. Remaining questions are implementation-level trade-offs rather than fundamental product gaps:


Should downstream enqueue and upstream ack always require the same database transaction?

What history of individual attempts should be retained versus compacted?

How should queue retention/cleanup work?

Should retry/DLQ policy belong to the queue, step, or allow both?

How should an item intentionally produce zero, one, or multiple downstream items?

What should the execution projection mean for pipelines where one item legitimately fans out into many?
Those are the areas I would settle before calling this design v0.1 approved.
The structure follows the design-doc pattern in the article: succinct objective context, explicit goals/non-goals, the actual design and its trade-offs, alternatives considered, and cross-cutting concerns. (Industrial Empathy)

# 21. Known Limitations


If upstream step bounds the output, DurableQ cant tell the difference.


Example : Web Discovery step discovers 5000 pages, but say the config caps it at 200. Then the pipeline would show 200 done for all stages, while silently hiding the signal that 4800 pages were never crawled. 

## Design decisions

| Question | Decision |
| --- | --- |
| Downstream enqueue + upstream ack in one transaction? | Yes, always. `Store.Complete(ctx, itemID, worker, []NewItem)` is a single transaction in both adapters. Cross-store fan-out is out of scope for v0.1, so there is no case where one transaction cannot cover it. |
| Attempt history retention | Every attempt writes a row to `durableq_attempts` (item_id, attempt, worker, started_at, ended_at, outcome, error). Compaction is a retention job, not a runtime concern: `durableq_attempts` rows are deletable independently of item state. Item keeps `attempt` and `last_error` denormalised so projections never join attempts. |
| Queue retention / cleanup | Terminal items (`done`) are deleted by a sweeper after `retention` (default 7d); DLQ items are never auto-deleted. Sweeper is a library-level goroutine plus a `durableq gc` CLI verb, both driven by the same store method. |
| Retry/DLQ policy owner | Queue owns the default policy; a step may override it. Effective policy = step override ?? queue default ?? package default (5 attempts, exponential 5s→30s→2m→10m, jitter). Policy is stored on the item at enqueue time so a policy change never rewrites in-flight work. |
| Zero / one / many downstream items | Step handler signature returns the downstream payloads: `func(ctx, Item[T]) ([]U, error)`. Returning nil is a legitimate terminal success ("filtered"), recorded as `terminal_filtered`, distinct from success-with-output. A convenience `Step1` wrapper wraps single-output handlers. |
| Projection meaning under fan-out | Per-edge counters only: each step records `received`, `succeeded`, `filtered`, `dlq`, `active`, `produced`. The leak invariant is per-edge (`received == succeeded + filtered + dlq + active`) and edge-continuity (`step[i].produced == step[i+1].received + in_flight`). No global discovered-to-indexed equality is claimed. §21's bounded-output limitation is handled by letting a step report `produced_capped = true` with `dropped` count, which surfaces in the projection instead of silently vanishing. |
