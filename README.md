# durableq

A durable queue with multi-step job semantics, on an ordinary database.

DurableQ sits between a plain work queue and a workflow engine. It gives you
**item-level reliability** — at-least-once delivery, independent retries, worker
leases, per-queue dead-letter queues — together with **run-level observability**,
so you can ask what happened to a whole ingestion run and not only to one item.

It needs an application and a database. No Redis, no Kafka, no control plane.

```
Run exec_b79042f5842b25bbea297b97 (document-ingestion)
Status: COMPLETE

STEP      RECEIVED  SUCCESS  FILTERED  DLQ  ACTIVE  PRODUCED
discover  1         1        0         0    0       25
crawl     25        23       0         2    0       23
process   23        20       3         0    0       20
index     20        20       0         0    0       0

Terminal successful: 20
Terminal DLQ:        2
Active:              0
```

## Status

PostgreSQL is the supported backend. SQLite is deliberately not written yet: the
storage contract and its conformance suite exist so adding it is additive rather
than a re-derivation of the semantics.

Processing is **at-least-once**. A handler may see the same item twice, so its
side effects must tolerate that. DurableQ does not claim exactly-once.

## Install

```bash
go get github.com/unilinq/durableq
```

## Migrations

Schema changes are a deploy step, not application startup.

```bash
go build -o durableq ./cmd/durableq
export DURABLEQ_DATABASE_URL='postgres://user:pass@host:5432/db?sslmode=disable'

./durableq -schema durableq migrate up
./durableq -schema durableq migrate status
```

Or from Go, if you prefer to migrate in your own deploy tooling:

```go
conn, _ := pgx.Connect(ctx, databaseURL)
err := postgres.MigrateUp(ctx, conn, "durableq")
```

Every statement durableq issues is schema-qualified, so the tables can live in
their own schema alongside your application's.

## A queue on its own

```go
store, _ := postgres.New(postgres.Config{Pool: pool, Schema: "durableq"})
app, _ := durableq.New(durableq.Config{Store: store})

type IndexInput struct {
    URL string `json:"url"`
}

q := app.Queue("indexing", durableq.WithConcurrency(8))

q.Work(func(ctx context.Context, in IndexInput) error {
    return index(ctx, in.URL)
})

app.Start(ctx)
defer app.Stop(context.Background())

q.Enqueue(ctx, IndexInput{URL: "https://example.com/a"})
```

The handler's only job is to return an error or not. Leases, backoff, retries
and dead-lettering never appear in worker code.

A handler takes `func(ctx context.Context, in T) error` where `T` is any
JSON-decodable type. Use `[]byte` or `json.RawMessage` for the raw payload, or
`storage.Item` for the whole item. The signature is checked when you register
it, not on the first item in production.

## A multi-step job

```go
job := app.Job("document-ingestion").
    Step("discover", func(ctx context.Context, in Site) ([]Page, error) {
        return discover(ctx, in.Root)          // one item in, thousands out
    }).
    Step("crawl", func(ctx context.Context, in Page) (Document, error) {
        return crawl(ctx, in.URL)              // exactly one item out
    }, durableq.StepConcurrency(8)).
    Step("process", func(ctx context.Context, in Document) ([]Record, error) {
        if in.Empty() {
            return nil, nil                    // a deliberate filter, not a failure
        }
        return extract(ctx, in)
    }, durableq.StepConcurrency(8)).
    Step("index", func(ctx context.Context, in Record) error {
        return index(ctx, in)                  // terminal step: no output
    }, durableq.StepConcurrency(8))

app.Start(ctx)

exec, err := job.Run(ctx, Site{Root: "https://example.com"})
```

`Run` returns as soon as the work is durable. Progress is observed through the
execution, not by waiting on the call.

Each step gets its own durable queue, including the first, so the trigger
survives the process that created it. Acking a step and enqueuing the work it
produced happen in one transaction: a step is never marked done without its
downstream work existing, and never creates it twice.

Fan-out needs no special primitive. A step returning 50,000 items simply
enqueues 50,000 items; the queue absorbs the cardinality. A step returning
nothing is a terminal success recorded as *filtered*, so a deliberate filter is
never mistaken for lost work. A step that exhausts its attempts is dead-lettered
and produces nothing downstream, while every other item carries on.

## Asking what happened

```go
p, _ := app.Projection(ctx, exec.ID)

fmt.Println(p.Status)           // RUNNING or COMPLETE
fmt.Println(p.TerminalSuccess)  // items that made it all the way
fmt.Println(p.TerminalDLQ)      // items that did not
for _, leak := range p.Leaks {
    fmt.Println(leak)           // work the counts cannot account for
}
```

The projection is a query over durable state, never an in-memory counter, so the
answer is the same whichever process asks and survives any metrics retention
window.

It checks two invariants per edge: everything that entered a step is finished or
in flight, and what one step produced is what the next one received once the
upstream step settles. Work that disappears between stages is reported rather
than absorbed. A step that deliberately capped its output says so — otherwise
the counts balance perfectly while thousands of discovered items were never
emitted.

To follow one item:

```go
trace, _ := app.Lineage(ctx, exec.ID, itemID)
// discover  success
// crawl     success, 2 attempts
// process   dlq
// index     never entered
```

Lineage is durable state, not telemetry.

## Operating it

```bash
durableq queues                              # counts per queue
durableq runs                                # recent executions, status derived
durableq run <exec-id>                       # the run table above
durableq trace <exec-id> <item-id>           # one item through every step
durableq item <id>                           # one item and its attempt history

durableq dlq ls indexing.dlq                 # what failed and why
durableq dlq replay indexing.dlq -limit 100  # after fixing the worker
durableq dlq replay indexing.dlq -error-contains unsupported_pdf

durableq pause indexing                      # stop claiming; in-flight work finishes
durableq resume indexing
durableq gc -retention 168h                  # delete completed items; the DLQ is never swept
```

Every working queue has a dead-letter queue named after it (`indexing.dlq`). The
DLQ belongs to the queue, not to an error type; the error stays as metadata on
the item, which is what makes `-error-contains` replay useful after a fix.

## Configuration worth knowing

| Field | Default | Why you would change it |
| --- | --- | --- |
| `PollInterval` | 200ms | Latency against database load. Polls are jittered so workers do not stampede. |
| `LeaseDuration` | 30s | How long after a crash before work is recovered. |
| `HeartbeatInterval` | `LeaseDuration/3` | Tolerates two missed beats before an item is reclaimable. |
| `HandlerTimeout` | 5m | A handler that overruns frees its worker slot and the item is retried. |
| `DrainTimeout` | 30s | How long `Stop` waits before handing work back. |
| `Retention` | 7 days | How long completed items are kept. `storage.RetentionNever` keeps them for ever. Dead-lettered items are never swept. |
| `DefaultPolicy` | 5 attempts, 5s / 30s / 2m / 10m, 10% jitter | Per queue with `WithPolicy`, per step with `StepPolicy`. |

The policy is snapshotted onto an item when it is enqueued, so changing a
queue's policy never rewrites work already in flight.

## Shutdown

`app.Stop(ctx)` stops claiming, gives running handlers `DrainTimeout` to finish,
and hands back anything still running **without consuming an attempt**. Work
interrupted by a deploy does not count against its retry budget. A handler that
finished successfully during shutdown is acked rather than thrown away.

## Using it from another repository

DurableQ is a normal Go module. Until it is published, point at the local
checkout with a workspace.

From the consuming repository, for example `~/code/marketing-resolver`:

```bash
cd ~/code/marketing-resolver
go work use ~/code/durableq
go get github.com/unilinq/durableq
```

That adds `~/code/durableq` to `go.work`:

```
go 1.26.0

use (
	.
	./obs
	./sync
	../durableq
)
```

Then in code:

```go
import (
    "github.com/unilinq/durableq"
    "github.com/unilinq/durableq/storage/postgres"
)
```

If you would rather not use a workspace, a `replace` directive in `go.mod` does
the same job:

```
require github.com/unilinq/durableq v0.0.0
replace github.com/unilinq/durableq => ../durableq
```

## Examples

```bash
export DURABLEQ_DATABASE_URL='postgres://...'

go run ./examples/queue    -schema durableq_example   # a queue on its own
go run ./examples/pipeline -schema durableq_pipeline  # the four-step job
```

Both migrate their own schema and print what happened.

## Testing your handlers

The storage conformance suite is exported so a new backend can prove itself
against the same semantics:

```go
storagetest.Run(t, storagetest.Harness{
    New: func(t *testing.T) (storage.Store, storagetest.Clock) { ... },
})
```

## Running durableq's own tests

They need a PostgreSQL to work against:

```bash
docker run -d --rm --name durableq-test-pg \
  -e POSTGRES_PASSWORD=durableq -e POSTGRES_DB=durableq_test \
  -p 45432:5432 postgres:16

go test ./... -race
DURABLEQ_RACE_SOAK=1 go test ./... -race   # the 60s sustained-load soak
```

Tests give themselves an isolated schema, so the whole suite runs in parallel
against one database with no truncation between runs. Nothing sleeps to
synchronise: tests wait on signals or advance an injected clock.
