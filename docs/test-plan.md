# Test plan — scenarios leveraged from River

Source surveyed: github.com/riverqueue/river @ main (146 test files, ~40k lines).
River is **MPL-2.0** (file-level copyleft): we take the *scenario list and structure*, and write our own
code. No River source file is copied into this repo.

## 1. Structural patterns worth copying outright

| River | What it is | DurableQ equivalent |
| --- | --- | --- |
| `riverdriver/riverdrivertest` | One exported suite (~8k lines) that every storage driver runs: `exerciseJobInsert/Read/Update/Delete/Queue/ExecutorTx/Listener`. Postgres, SQLite, libSQL, Turso all call the same entrypoint. | `storage/storagetest` — already in the plan. This is the single biggest win: it is the reason River can add a backend without re-deriving semantics, and it is exactly the stage-8 gate. |
| `riverdbtest.TestSchema` / `TestTx` | Per-test isolated Postgres schema (or a rolled-back tx) so the whole suite runs in parallel against one database, no truncation between tests. | `internal/dqtest.Schema(t)` / `Tx(t)`. Adopt from stage 1 — retrofitting isolation later is painful. |
| `rivershared/testsignal` | Zero-cost channel wrapper embedded in production structs; tests `WaitOrTimeout()` on specific internal events instead of sleeping. | Needed for lease-expiry, sweeper and claim-loop tests. Prevents the flaky `time.Sleep` suite we would otherwise write. |
| `riversharedtest.WaitOrTimeout/WaitOrTimeoutN`, stubbed clock | Deterministic waits plus injectable time so backoff maths is asserted, not slept through. | `Policy` must take a clock from the archetype, not call `time.Now()` directly. Decide this in stage 1. |
| `startstoptest.Stress` (`StartStopStress` subtest on every service) | Repeatedly start/stop a service from many goroutines to catch shutdown races. | Applies to worker pool, lease heartbeat, sweeper, reclaimer. |
| `rivertest` package (`RequireInserted`, `Worker.Work`) | Public helpers so *users* can test their handlers without standing up a client. | `durableqtest` for stage 9 — a step handler should be testable without a running pipeline. |
| `riverdrivertest/benchmark.go` | Shared throughput benchmark run per driver. | Our 100k-item load smoke, run per adapter. |

## 2. Scenario inventory, mapped to our stages

### Stage 2 — claim / ack / retry / lease (River: `job_read.go` JobGetAvailable, `job_update.go`)
- Claim success; constrained to limit; constrained to queue.
- Claim skips items with `available_at` in the future; honours an injected "now" (`ConstrainedToScheduledAtBeforeCustomNowTime`).
- Claim ordering is deterministic (River: `Prioritized`; ours: `available_at`, then id).
- `leased_by` string longer than the column max is truncated, not an error (`AttemptedByAtMaxTruncated` / `OverMaxTruncated`).
- Ack only affects a `running` item: `CompletesARunningJob`, `DoesNotCompleteARetryableJob`, `UnknownJobIgnored`.
- Retry sets `available_at` from policy: `SetsARunningJobToRetryable`.
- An item whose lease expired mid-flight goes back to ready **with the attempt count adjusted**, not silently re-run (`SetsAnInterruptedRunningJobToAvailableWithUpdatedAttempt`).
- Idempotent transitions: re-applying a transition to an already-retryable item is a no-op except for metadata (`DoesNotTouchAlreadyRetryableJob`, `UpdatesOnlyMetadataForAlreadyRetryableJobs`).
- Batch transitions: many items moved in one call, mixed outcomes (`JobSetStateIfRunningMany_MultipleJobsAtOnce`).
- Not-found returns a typed error, never a silent success (`ReturnsErrNotFoundIfJobDoesNotExist` — repeated across every mutator).
- Concurrency: N goroutines × M items, zero double-claims (our addition; River leans on `SKIP LOCKED` plus the driver suite).

### Stage 2/3 — lease recovery (River: `internal/maintenance/job_rescuer_test.go`)
- `RescuesStuckJobs` — the core lease-expiry reclaim.
- `RescuesInBatches`, `RescuesPastFullBatchOfJobsWithNoTimeout` — reclaim must not stall at batch boundaries.
- `UnmarshalErrorDiscardsAtMaxAttempts` vs `UnmarshalErrorRetriesWithClientPolicy` — an item whose payload no longer decodes must go to DLQ rather than spin forever. **We need this**: worker version skew is a real case for the crawler.
- `CustomizableInterval`, `StopsImmediately`, `RespectsContextCancellation`, `CanRunMultipleTimes`.
- `ReducedBatchSizeBreakerTrips` / `ResetsOnSuccess` — a circuit breaker that shrinks the batch when the DB rejects large statements. Worth adopting for the sweeper and reclaimer; cheap insurance against a wedged maintenance loop.

### Stage 3 — worker loop (River: `producer_test.go`, `internal/jobexecutor/job_executor_test.go`)
- `MaxWorkers` — concurrency cap actually holds.
- `CompletesJobWhileFetchingNewOnes` — ack and claim overlap without deadlock.
- `CancelledWorkContextCancelsJob`, `SoftStopCancelMakesJobAvailableAndDecrementsAttempt` — graceful drain returns in-flight work **without burning an attempt**. This is a subtle rule we would otherwise get wrong.
- `Panic`, `PanicAgainAfterRetry`, `PanicDiscardsJobAfterTooManyAttempts` — panic is a normal failure path, not a crash.
- `JobStuckHandler`, `JobStuckHandlerOpensExecutorSlot` — a handler that never returns must not permanently consume a worker slot.
- `StuckDetectionIgnoresParentContextCancellation` — don't mistake shutdown for a stuck handler.
- `UnknownJobKindErrorsTheJob` — an item for a step/queue with no registered handler fails the item; it does not kill the worker.
- `ErrorSetsJobAvailableBelowSchedulerIntervalThreshold` — short retries must not be lost between maintenance ticks. Directly relevant to our polling design.
- `jitteredFetchPollInterval` — poll jitter so N workers don't stampede.
- `QueuePausedBeforeStart` / `DuringOperation` / `AndResumedDuringOperation` — pausing a queue. We have no pause in v0.1; **recommend adding it**, it is ~20 lines of schema and is the operational lever you want when a downstream system is down.

### Stage 4 — DLQ, replay, retention (River: `job_cleaner_test.go`, `queue_cleaner_test.go`, `job_delete.go`)
- `DeletesCancelledCompletedAndDiscardedJobs` — sweeper only removes terminal states.
- `DoesNotDeleteWhenRetentionMinusOne` (per-state variants) — retention off means off. Maps to our "DLQ is never auto-swept".
- `DeletesInBatches`, `OmmittedQueues` — sweep by queue, in bounded batches.
- `DoesNotDeleteARunningJob`, `IgnoresRunningJobs` — deletion never races an in-flight lease.
- `JobRetry` family for replay semantics: `AltersScheduledAtForAlreadyCompletedJob`, `DoesNotAlterScheduledAtIfInThePastAndJobAlreadyAvailable`, `DoesNotUpdateARunningJob`.

### Stage 5/6 — Job, lineage, projections
No River analogue — River has no execution/step concept. Our own scenarios:
- Ack-of-N + enqueue-of-N+1 atomicity: kill the tx between the two, assert no orphan and no duplicate.
- Fan-out 1→N and 1→0 (`terminal_filtered`) counted correctly per edge.
- DLQ'd item produces no downstream rows.
- Edge invariant `received == succeeded + filtered + dlq + active` across every step.
- Deleted in-flight row is reported as a leak, not absorbed.
- The §21 capped-discovery case surfaces `produced_capped`/`dropped`.
The closest River borrows are `resumable_test.go` (multi-step job state carried across attempts) and `JobCountByQueueAndState` / `JobCountByState` for the counting queries behind projections.

### Stage 7 — telemetry (River: `producer_test.go` MetricEmitHook)
- `EmitsMetricsForFetch`, `SkipsMetricsWhenNoFetchAttempted` — no metric noise on an idle poll.

### Stage 8 — SQLite (River: `driver_test.go`)
- The whole suite runs per backend with a `t.Run` per connection mode (`DefaultMode`, `SimpleProtocol`, `ExecMode`). We need the equivalent for pgx pool vs `database/sql`, and SQLite WAL vs default journal.
- `SQLiteStoresJobJSONAsJSONB` — payload encoding differs per backend; assert the round-trip explicitly.
- `AlternateSchema` subtests — every statement must respect a non-default schema. Cheap now, painful to retrofit; the crawler will want DurableQ tables in their own schema.

### Cross-cutting (River: `riverdrivertest/executor_tx.go`)
- `BasicVisibility`, `NestedTransactions`, `RollbackAfterCommit` — the tx abstraction behaves the same on both backends. Required before we claim ack+enqueue atomicity.

## 3. Deliberately not borrowed
Unique-job/dedup keys (`dbunique`), leader election (`leadership/elector_test.go` — we have no cluster-singleton maintenance yet; revisit when the sweeper runs in multiple replicas), periodic/cron enqueuing, `LISTEN/NOTIFY` (`notifier_test.go`) until we add wake-ups, job snooze, remote cancellation, subscription/event bus, reindexer, and River's pilot/plugin seams.

`elector_test.go` and `notifier_test.go` are the two to come back to: the first the moment more than one process runs the sweeper, the second when polling latency becomes the complaint.

## 4. Changes this survey suggests to `docs/plan.md`
1. **Stage 1 must include the test harness** (schema isolation + injectable clock + test signals), not stage 3. River's whole suite depends on those three being present from the first commit.
2. **Add queue pause/resume to v0.1** — cheap, and the standard operational response to a sick downstream.
3. **Add a payload-decode-failure path** to the retry model: undecodable payload must DLQ at max attempts rather than retry forever.
4. **Add batch-size circuit breakers** to the reclaimer and sweeper.
5. **Schema-qualified statements from day one**, with an `AlternateSchema` subtest in the conformance suite.
6. **Graceful-stop must return work without consuming an attempt** — make it an explicit invariant in the storage contract.

## 5. Concurrency coverage (first-pass requirement)

The first pass is Postgres-only and must be proven under contention, so the race scenarios below are a
gated stage (plan stage 4a) rather than a later hardening pass. River's own suite supplies the shape for
most of them.

| Scenario | Invariant asserted | River precedent |
| --- | --- | --- |
| N workers drain one queue | every item claimed by exactly one worker at a time; successes == items | `SKIP LOCKED` claim path, driver suite |
| Worker acks after its lease expired | item is not both completed and re-queued; one outcome wins atomically | `JobSetStateIfRunningMany` "running only" guards |
| Reclaimer vs owning worker | reclaim adjusts attempt correctly; no lost or double increment | `SetsAnInterruptedRunningJobToAvailableWithUpdatedAttempt` |
| Ack / retry / cancel issued together | all orderings exercised via test signals; single terminal state | `rivershared/testsignal` |
| Item becoming ready while a claimer scans | not claimed twice, not skipped indefinitely | `ErrorSetsJobAvailableBelowSchedulerIntervalThreshold` |
| Replay vs sweep vs claim on one DLQ item | sweeper never removes an item under a live lease | `DoesNotDeleteARunningJob`, `IgnoresRunningJobs` |
| Ack-N + enqueue-N+1 with the tx killed mid-way | no orphan, no duplicate downstream item | none — ours |
| Sustained mixed workload, 60s | zero PG deadlocks (`40P01`) | none — ours |
| Start/stop stress on every background service | no shutdown race, no leaked goroutine | `startstoptest.Stress`, `StartStopStress` subtests |
| Worker SIGKILL mid-lease | work reclaimed exactly once | `RescuesStuckJobs` |
| Pool exhaustion, connection dropped mid-claim | typed error, item left claimable | `CompletionImmediateFailureOnErrClosedPool` |

Gate: the whole suite green under `-race`, run 20× consecutively with no intermittent.
