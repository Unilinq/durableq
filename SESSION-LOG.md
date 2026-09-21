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
