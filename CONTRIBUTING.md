# Contributing to DurableQ

## Dev setup

DurableQ needs Go 1.26.0 (see `go.mod`) and a PostgreSQL 16 to test against:

```bash
docker run -d --rm --name durableq-test-pg \
  -e POSTGRES_PASSWORD=durableq -e POSTGRES_DB=durableq_test \
  -p 45432:5432 postgres:16
```

Tests read `DURABLEQ_TEST_DATABASE_URL` and fall back to
`postgres://postgres:durableq@localhost:45432/durableq_test?sslmode=disable`
if it is unset, which matches the docker command above. Each test gets its
own schema, so the full suite runs in parallel against one database with no
truncation between runs.

```bash
go test ./... -race
```

`-race` is not optional. A race is a defect regardless of whether it flakes
in a given run.

Format and vet before sending a change:

```bash
gofmt -l .    # should print nothing
go vet ./...
```

### Load and soak tests

The load and soak tests are opt-in — they take minutes and are not part of
the normal test run:

```bash
DURABLEQ_LOAD=1 DURABLEQ_LOAD_ITEMS=100000 go test . -run TestLoad -timeout 30m -v
DURABLEQ_SOAK=1 DURABLEQ_SOAK_MINUTES=30 go test . -run TestSoak -timeout 45m -v
```

There's also a longer sustained-load race soak in the storage conformance
suite, off by default:

```bash
DURABLEQ_RACE_SOAK=1 go test ./... -race
```

## Pull requests

- Keep PRs small and focused on one change.
- Every change needs tests. A bug fix needs a test that fails without the
  fix.
- The full suite must pass under `-race`.
- Describe the behaviour change in the PR description, not just the code
  change — what could an application see differently before and after.
- If the change affects design (new public API, a change to delivery
  semantics, leasing, retry, or transactional guarantees), open an issue
  first so the approach can be discussed before you write the code.
- If the change affects storage semantics (anything a `Store` implementation
  must guarantee), extend `storage/storagetest` so every adapter is held to
  the same contract, not just the one you tested against.

## Compatibility

DurableQ is pre-1.0. The exported API may change between minor versions;
breaking changes are called out in the release notes. Once the project
reaches 1.0, this will follow normal semantic versioning.

## Developer Certificate of Origin

Contributions must be signed off under the [Developer Certificate of Origin
1.1](https://developercertificate.org/):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

Sign off every commit with `git commit -s`, which appends:

```
Signed-off-by: Your Name <your.email@example.com>
```

Use your real name and a working email address; anonymous or pseudonymous
sign-offs are not accepted. A PR with an unsigned commit will be asked to
amend before merge.
