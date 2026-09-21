# Common brief context (every durableq stage)

## Repo
`/Users/chitreshdeshpande/code/durableq`, module `github.com/unilinq/durableq`, Go 1.26.3.
Local git only, branch per stage off `main`. No remote — never push, never open a PR.

## Database
Throwaway Postgres 16 in Docker (Colima context), already running:
- container `durableq-test-pg`, host port **45432**
- `DURABLEQ_TEST_DATABASE_URL=postgres://postgres:durableq@localhost:45432/durableq_test?sslmode=disable`
- psql access: `docker exec durableq-test-pg psql -U postgres -d durableq_test -c '<sql>'`
- If the container is gone, recreate:
  `docker run -d --rm --name durableq-test-pg -e POSTGRES_PASSWORD=durableq -e POSTGRES_DB=durableq_test -p 45432:5432 postgres:16`

## Standing rules (apply to every stage; violating one is a FAIL)
- Every test binary runs under `-race`. A data race is a stage-blocking defect, never quarantined.
- No `time.Sleep` for synchronisation in tests. Wait on a test signal or an injected clock.
- Concurrent tests assert invariants ("exactly one claimer", "never both done and requeued"), not counts reached after waiting.
- A flake is a defect with a root cause. Re-running to green is not a resolution.
- Core packages never import `storage/postgres`. An import-walking test enforces this.
- All SQL is schema-qualified; nothing assumes `public`.
- At-least-once only. No exactly-once language anywhere in code or docs.

## Evidence the judge must collect
- `git -C /Users/chitreshdeshpande/code/durableq log --oneline -5` and the commit under test.
- `go build ./...`
- `go vet ./...`
- `go test ./... -race -count=1` (full output tail, not a summary claim)
- Per-stage commands listed in that stage's brief.
- Timestamp each command block.

## Design source
`docs/design.md` (the approved design), `docs/plan.md` (stage list and the §0 decisions),
`docs/test-plan.md` (scenario inventory and the concurrency matrix). Read all three before judging.
