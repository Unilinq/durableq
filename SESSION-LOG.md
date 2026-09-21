# durableq build log

Started 2026-09-21. Unattended judge-loop run, stages 1 → 6 (7–9 if time allows).
Briefs: `docs/briefs/`. Plan: `docs/plan.md`. Test DB: `durableq-test-pg` on :45432.

## Verification log
(one entry per judge verdict: timestamp, stage, round, verdict, evidence, defects, fix commits)

### 2026-09-21T08:18Z — Stage 1 (storage contract, PG schema, test harness) — round 1 — **PASS**

Branch `stage-1-storage-contract`.

| Criterion | Evidence | Verdict |
| --- | --- | --- |
| 1. up/down/up leaves schema identical | `TestMigrateUpDownUp` diffs an information_schema + pg_indexes dump either side of a full down/up | PASS |
| 2. Parallel tests cannot see each other's rows | `TestSchemaIsolation`, suite `Isolation/StoresDoNotShareRows` (same queue name, two schemas) | PASS |
| 3. Clock injectable, no sleeps | `TestClockIsInjectable` advances 72h with no wall-clock wait; `TestNoSleepForSynchronisation` walks every `_test.go` | PASS |
| 4. Test signals exist and are exercised | `internal/dqsignal` + 5 tests incl. concurrent-emit race test | PASS |
| 5. `storagetest` exported, PG runs it | `storage/storagetest` (not internal); `TestConformance` | PASS |
| 6. Import discipline enforced | `TestCoreDoesNotImportABackend`; negative control added a bad import to `internal/runtime`, test FAILED as required, then restored | PASS |
| 7. All SQL schema-qualified | `TestAlternateSchema` runs in `..._Mixed-Case` and asserts `public` has 0 durableq tables | PASS |
| 8. build / vet / test -race clean | see below | PASS |

```
go build ./...   -> BUILD_OK
go vet ./...     -> VET_OK
go test ./... -race -count=1
  ok  github.com/unilinq/durableq/internal/arch       1.4s
  ok  github.com/unilinq/durableq/internal/dqsignal   1.5s
  ok  github.com/unilinq/durableq/storage/postgres    2.0s
```

Negative controls (proving the rules fire, not just pass):
```
# import rule
--- FAIL: TestCoreDoesNotImportABackend
    package .../internal/runtime imports .../storage/postgres
# sleep rule
--- FAIL: TestNoSleepForSynchronisation
    internal/runtime/sleep_check_test.go:8 uses time.Sleep for synchronisation
```

**Deviations from the brief** (all judged to serve the criteria better; none narrow scope):
1. **Migrations are applied by a small embedded migrator, not the goose CLI.** The files stay goose-format
   (`-- +goose Up` / `Down`) in `storage/postgres/migration/`, but goose cannot target an arbitrary schema
   per test, which criterion 2 requires. The migrator sets `search_path` and tracks versions in
   `durableq_migrations`. This also drops an external binary dependency.
2. **`Release` added to the `Store` interface.** Stage 3 criterion 3 (graceful stop must not consume an
   attempt) has no other home. The brief said the interface must be complete; this completes it.
3. **`last_worker` column added.** The lease CHECK constraint forces `leased_by` NULL outside `running`,
   but stage 4 criterion 1 requires a dead-lettered item to name its worker.
4. **`outcome`, `produced`, `dropped`, `produced_capped` columns added now** so stages 5-6 need no second
   migration.

### 2026-09-21T08:26Z — Stage 2 (claim, ack, retry, lease) — round 1 — **PASS**

Branch `stage-2-claim-ack-retry`. 73 conformance subtests green.

| Criterion | Evidence | Verdict |
| --- | --- | --- |
| 1. Claim honours limit | `Claim/HonoursLimit` (3, then the remaining 7, then 0) | PASS |
| 2. Constrained to queue | `Claim/ConstrainedToQueue` | PASS |
| 3. Skips future `available_at`, honours injected now | `Claim/SkipsItemsNotYetAvailable`, `Claim/HonoursAnInjectedNow` | PASS |
| 4. Deterministic order | `Claim/OrderIsDeterministic` (inserted newest-first), `Claim/TiesBreakByID` | PASS |
| 5. Over-long `leased_by` truncated, not an error | `Claim/OverLongWorkerIdentityIsTruncatedNotRejected` | PASS |
| 6. Typed errors per mutator | `NotFound/*` and `WrongState/*` — 5 mutators x 2 = 10 subtests | PASS |
| 7. Complete only affects running | `Transitions/CompleteDoesNotTouchAReadyItem`, `CompleteByTheWrongWorkerIsRejected` | PASS |
| 8. Retry backoff exact | `Transitions/RetryReschedulesByPolicy` asserts 5s/30s/2m/10m exactly (jitter 0); `RetryJitterStaysInBounds` x20 | PASS |
| 9. Lease expiry attempt accounting | `Lease/ExpiredLeaseIsReclaimedWithAttemptIntact` — attempt stays 1, next claim is 2 | PASS |
| 10. Batch mixed outcomes | `Batch/MixedOutcomesReportedPerItem` (ok / not-found / wrong-state / ok in one call) | PASS |
| 11. DeadLetter preserves metadata | `Transitions/DeadLetterPreservesMetadata` — error, attempts, worker, lineage, DLQ visible in Stats | PASS |
| 12. 8 claimers x 1000 items, zero duplicates, `-count=10` | `Contention/NoDoubleDelivery` + `SingleItemRace` (16 racers, 1 winner) | PASS |
| 13. No deadlocks | `Contention/NoDeadlocksUnderMixedLoad`; server log `deadlock detected|40P01` count = **0** | PASS |
| 14. `-race -count=1` clean, claim suite at `-count=10` | see below | PASS |

```
go test ./storage/... -race -count=10 -run 'TestConformance/(Claim|Lease|Transitions)'  -> ok 5.554s
go test ./storage/... -race -count=10 -run 'TestConformance/Contention'                 -> ok 6.178s
go test ./... -race -count=1
  ok  internal/arch 1.470s   ok internal/dqsignal 1.447s   ok storage/postgres 2.972s
docker logs durableq-test-pg | grep -ciE "deadlock detected|40P01"  -> 0
```

Defect found and fixed during the stage (no fix round needed; caught by the suite before any verdict):
`Claim`'s `UPDATE ... FROM candidate ... RETURNING <cols>` left the column list unqualified, so `id` was
ambiguous (SQLSTATE 42702) and every claim failed. Fixed by `qualifiedItemColumns(alias)`.

**Note on evidence commands.** The brief's `docker logs ... | grep -ci deadlock` is a false-positive trap:
it matches the *schema name* of the `NoDeadlocksUnderMixedLoad` test, which appears in logged statement
text. The real check is `grep -ciE "deadlock detected|40P01"`. Stage 4a uses the precise form.

**Design notes.**
- `leased_by` is `text`, which PostgreSQL does not bound, so the truncation criterion is enforced in code
  at `Config.LeasedByMax` (default 128) rather than by the column. The test asserts the stored value is
  shorter than what was passed and non-empty.
- Ownership guard: every transition carries the worker identity and matches on `leased_by`. This is the
  mechanism by which a worker whose lease already lapsed loses to the reclaimer
  (`Lease/AckAfterLeaseExpiredLosesToTheReclaimer`). An empty worker means "no ownership check" and is
  reserved for administrative paths such as the CLI.
- `Release` deletes the open attempt row rather than closing it: a shutdown hand-back did not consume an
  attempt, so it should leave no trace in the attempt history either.

### 2026-09-21T08:42Z — Stage 3 (polling worker, public Queue API) — round 1 — **PASS**

Branch `stage-3-worker-queue-api`. 16 tests, all green at `-count=5`.

| Criterion | Evidence | Verdict |
| --- | --- | --- |
| 1. Concurrency cap holds under load | `TestConcurrencyCapHolds` — 40 items, cap 4, high-water mark asserted via the pool's concurrency signal | PASS |
| 2. Ack and claim overlap without deadlock | `TestConcurrencyCapHolds` + `TestWorkerProcessesItems`; no deadlocks in the mixed-load suite | PASS |
| 3. Graceful stop returns work **without consuming an attempt** | `TestGracefulStopDoesNotConsumeAnAttempt` — attempt 1 while running, 0 after Stop, state ready | PASS |
| 4. Panic is a normal failure path | `TestPanicIsANormalFailurePath` — 2 retries then DLQ, handler ran exactly 3 times | PASS |
| 5. Stuck handler does not hold its slot | `TestStuckHandlerFreesItsSlot` — wedged handler ignores cancellation, ordinary work still flows | PASS |
| 6. Shutdown is not "stuck" | `TestShutdownIsNotMistakenForAStuckHandler` — stuck signal empty, item released, attempt 0 | PASS |
| 7. Unhandled queue fails the item, not the worker | `TestQueueWithoutAHandlerIsNotClaimed` — producer-only queue left waiting, 0 dead-lettered | PASS |
| 8. Undecodable payload retried then dead-lettered | `TestUndecodablePayloadDeadLettersEventually` — handler body ran 0 times, item reached DLQ with a recorded error | PASS |
| 9. Short retry not lost between poll ticks | `TestRetryBecomingReadyIsPickedUp` — item returns purely because the clock advanced | PASS |
| 10. Poll jitter spreads polls | `TestPollJitterSpreadsPolls` — 21 samples inside +/-20%, >=10 distinct, floor honoured | PASS |
| 11. Pause/resume mid-operation | `TestPauseStopsClaiming` — five consecutive empty polls while paused, flows again after resume | PASS |
| 12. Heartbeat renews a long handler's lease | `TestHeartbeatKeepsALongHandlersLease` — deadline moves by exactly the time that passed; reclaimer takes 0 | PASS |
| 13. Start/stop stress, no goroutine leak | `TestStartStopStress` — 8 cycles, double-Stop safe, goroutine growth bounded | PASS |
| 14. `-race -count=1` clean | see below | PASS |

```
go test ./... -race -count=1
  ok  github.com/unilinq/durableq                 1.850s
  ok  github.com/unilinq/durableq/internal/arch   1.412s
  ok  github.com/unilinq/durableq/internal/dqsignal 1.581s
  ok  github.com/unilinq/durableq/internal/scheduler 1.452s
  ok  github.com/unilinq/durableq/storage/postgres 3.204s
go test . -race -count=5   -> ok 3.609s
```

Three defects found during the stage, all fixed before the verdict:

1. **`App.Stop` could hang for ever.** `Start`'s context watcher waited only on the caller's context, so
   `Stop` called while that context was still live blocked on `a.wg.Wait()`. Caught as a 90s test timeout.
   Fixed: the watcher selects on the caller's context *or* the app's own cancellation.
2. **Data race in `scheduler.Breaker`** (`-race`, `TestHeartbeat...`). `Reclaimer.Pass` is exported so an
   operator can run one pass by hand; doing that while the loop runs raced on the batch size. Fixed with a
   mutex plus `breaker_test.go`, including an explicit concurrency case. This is exactly the class of bug
   the race gate exists to catch, found two stages early.
3. **Missing `Config.HeartbeatInterval`.** Renewal was hard-wired to `LeaseDuration/3`, so a 30s lease
   forced a 10s renewal. Now configurable, defaulting to a third.

One test was wrong rather than the code: the first heartbeat test asserted renewal with a frozen stub
clock, where a no-op renewal is the correct behaviour. Rewritten to advance the clock and assert the
deadline moves by exactly the elapsed time.

**Design note.** `Queue.Work` takes `func(ctx, T) error` via reflection, matching the design doc's API
exactly, and validates the signature at registration (`TestHandlerSignatureIsCheckedAtRegistration`
rejects seven wrong shapes). `T` may be any JSON-decodable type, `[]byte`/`json.RawMessage` for the raw
payload, or `storage.Item` for the whole item.

### 2026-09-21T08:46Z — Stage 4 (DLQ, replay, retention, CLI) — round 1 — **PASS**

Branch `stage-4-dlq-replay-cli`.

| Criterion | Evidence | Verdict |
| --- | --- | --- |
| 1. Exhausted item carries error, attempts, worker | CLI `item 16`: `attempts 2 of 2`, worker `Mac-65403`, error `unsupported_pdf_encoding`, plus the two attempt rows | PASS |
| 2. Replay returns the item and it succeeds | `Replay/ReturnsItemsToTheWorkingQueue` — back to ready on the working queue, attempt reset to 0, claimable again | PASS |
| 3. Replay filtered and limited | `Replay/HonoursItsLimit`, `Replay/FiltersByError` (2 of 3 by error substring); CLI `dlq replay -limit 2` moved exactly 2 | PASS |
| 4. Sweeper deletes only terminal items past retention | `Sweep/DeletesDoneItemsPastRetention` | PASS |
| 5. Retention `-1` means never | `Sweep/RetentionNeverDeletesNothing`, `TestSweeperDoesNothingWhenRetentionIsNever` (store never called) | PASS |
| 6. DLQ is never auto-swept | `Sweep/NeverSweepsTheDLQ` (a year past retention); CLI `gc -retention 0s` deleted 16 done items, left 2 DLQ items | PASS |
| 7. Sweeper never races a live lease | `Sweep/NeverSweepsLiveWork` — ready and running items survive a year-past-retention sweep | PASS |
| 8. Bounded batches, circuit breaker | `Sweep/HonoursBatchLimit`; `TestSweeperBreakerShrinksAndRecovers`; `TestBreakerHalvesAndRecovers` | PASS |
| 9. Respects cancellation, repeatable | `Sweeper.Run` selects on ctx; `Pass` is idempotent and separately exported for the CLI | PASS |
| 10. CLI verbs correct against a seeded database | full transcript below | PASS |
| 11. Replay/sweep/claim race on one item | `DLQContention/ReplaySweepAndClaimOnOneItem` — 4 replayers, 2 sweepers, 4 claimers over 40 items; every item accounted for, done-count matches completions, no deadlock | PASS |
| 12. `-race -count=1` clean | all five packages ok | PASS |

CLI transcript (schema `dq_cli_demo2`, seeded by `examples/queue -count 20`, since dropped):
```
$ durableq queues
QUEUE         READY  RUNNING  DONE  DLQ  PAUSED
indexing      0      0        16    0    false
indexing.dlq  0      0        0     4    false

$ durableq dlq ls indexing.dlq
ID  ATTEMPTS  WORKER     FAILED AT             ERROR
16  2         Mac-65403  2026-09-21T08:45:26Z  unsupported_pdf_encoding
...

$ durableq item 16
item 16 / queue indexing.dlq / state dlq / outcome dlq / attempts 2 of 2
ATTEMPT  WORKER     STARTED               OUTCOME  ERROR
1        Mac-65403  2026-09-21T08:45:26Z  error    unsupported_pdf_encoding
2        Mac-65403  2026-09-21T08:45:26Z  dlq      unsupported_pdf_encoding

$ durableq pause indexing   -> paused    (queues shows PAUSED true)
$ durableq resume indexing  -> resumed
$ durableq dlq replay indexing.dlq -limit 2  -> replayed 2 items
$ durableq dlq replay indexing.dlq -error-contains connection_refused -> replayed 0
$ durableq gc -retention 0s -> deleted 16 completed items; DLQ untouched (2 remain)
$ durableq migrate status -> applied 1, latest 1
```

One test was wrong rather than the code: `Replay/DoesNotTouchRunningOrReadyItems` enqueued the "ready"
item first and then claimed, but claiming takes the *oldest* item, so it claimed the one the test meant to
leave alone. Reordered, with a comment saying why the order matters.

Also added: `examples/queue` (a runnable demo that seeds a schema, used for the CLI evidence above) and
`storage.Lister` (`ListItems` / `Attempts`) so the CLI can show a DLQ and an item's history without
widening the hot-path `Store` interface.

### 2026-09-21T09:05Z — Stage 4a (concurrency and race gate) — round 1 — **PASS (HARD GATE MET)**

Branch `stage-4a-race-gate`.

**Gate: 20 consecutive full-suite runs under `-race`, 20 passed, 0 failed. No intermittents.**

```
$ for i in $(seq 1 20); do go test ./... -race -count=1 -timeout 600s > run-$i.log || echo "RUN $i FAILED"; done
RUN 1 PASS ... RUN 20 PASS
GATE RESULT: 20 passed, 0 failed
```

| Criterion | Test | Verdict |
| --- | --- | --- |
| 1. No double delivery at 8/16/32 workers | `RaceGate/NoDoubleDeliveryAtScale/{8,16,32}Workers` — 2000 items each; a ledger records every claim and release, so "held by two at once" is an observed-history assertion, not an end count | PASS |
| 2. Ack after lease expiry vs reclaimer | `RaceGate/AckRacesReclaim` — 200 items, all leases lapse at once, 4 ackers race 2 reclaimers; asserts each item is in exactly one state, no item is both done and re-queued, and successful acks equal items marked done | PASS |
| 3. Reclaimer vs owning worker, attempt counted once | same test plus `Lease/ExpiredLeaseIsReclaimedWithAttemptIntact` and the SIGKILL test below | PASS |
| 4. Ack / retry / dead-letter / release interleaved | `RaceGate/TransitionsRaceEachOther` — 60 items, four competing transitions fired simultaneously at each; exactly one wins every time | PASS |
| 5. Retry-vs-claim at the availability boundary | `RaceGate/RetryBoundary` — 100 items retried, 8 claimers scanning continuously across the instant the backoff elapses; each claimed exactly once, none skipped | PASS |
| 6. Replay vs sweep vs claim | `DLQContention/ReplaySweepAndClaimOnOneItem` (stage 4) | PASS |
| 7. Fan-out atomicity, transaction torn mid-way | `TestFanOutSurvivesConnectionLoss` — its **own database**, connections terminated every 3ms during `Complete`; 359 acked upstream = 359 downstream across **4257 killed connections**. Non-destructive contention version in the shared suite: `RaceGate/FanOutAtomicity` | PASS |
| 8. Deadlock freedom under sustained load | `RaceGate/SustainedMixedLoad` — claim/ack/retry/release/enqueue/reclaim/sweep/replay all at once; run at the full **60s** with `DURABLEQ_RACE_SOAK=1`; server log `deadlock detected\|40P01` count = **0** | PASS |
| 9. Start/stop stress on every background service | `TestStartStopStress` (worker pool), `TestReclaimerStartStopStress`, `TestSweeperStartStopStress`, `TestHeartbeaterStartStopStress` — 50 cycles each with manual passes and cancellation racing | PASS |
| 10. Chaos | `TestWorkIsRecoveredAfterSIGKILL` (real subprocess, real SIGKILL, work reclaimed **exactly once** — a second pass takes 0), `TestFanOutSurvivesConnectionLoss` (connection dropped mid-transaction), `RaceGate/ClockSkew` (lagging and leading clocks), `TestPoolExhaustionDoesNotCorruptState` (20 workers through a 2-connection pool, all 200 items complete) | PASS |

Full-length soak evidence (separate run):
```
$ DURABLEQ_RACE_SOAK=1 go test ./... -race -count=1 -timeout 600s
ok  github.com/unilinq/durableq                   1.809s
ok  github.com/unilinq/durableq/internal/arch     1.651s
ok  github.com/unilinq/durableq/internal/dqsignal 1.517s
ok  github.com/unilinq/durableq/internal/leasing  1.619s
ok  github.com/unilinq/durableq/internal/scheduler 1.737s
ok  github.com/unilinq/durableq/storage/postgres  65.826s
```

**One structural correction during the stage.** The first version of the connection-killing test lived in
the shared conformance suite and terminated backends on the database every test shares, so it failed five
sibling tests with `57P01`. Destructive chaos now runs in a database of its own
(`dedicatedDatabase`), and the shared suite keeps a non-destructive contention version. The
`TerminateOtherBackends` helper is documented as test-only and destructive by design.

**Soak duration.** `SustainedMixedLoad` runs 3s by default and 60s under `DURABLEQ_RACE_SOAK=1`. The 60s
form was run once for the criterion-8 evidence above; the 20x gate used the default so the gate itself
stays runnable. Both are recorded here rather than one standing in for the other.
