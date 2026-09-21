// Command durableq inspects and operates a durableq installation: migrations,
// queue state, dead-letter listing and replay, retention, and pausing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/unilinq/durableq/internal/dlq"
	"github.com/unilinq/durableq/storage"
	"github.com/unilinq/durableq/storage/postgres"
)

const usage = `durableq - operate a durableq installation

Usage:
  durableq [global flags] <command> [arguments]

Commands:
  migrate up            apply pending migrations
  migrate down          roll every migration back
  migrate status        show the applied migration version
  queues                list queues with their counts
  dlq ls <queue.dlq>    list dead-lettered items
  dlq replay <queue.dlq>  return dead-lettered items to their working queue
  item <id>             show one item and its attempt history
  gc                    delete completed items past retention
  pause <queue>         stop workers claiming a queue
  resume <queue>        undo pause

Global flags:
  -database-url  PostgreSQL connection string (or DURABLEQ_DATABASE_URL)
  -schema        schema holding the durableq tables (default "public")
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "durableq:", err)
		os.Exit(1)
	}
}

type globals struct {
	databaseURL string
	schema      string
}

func run() error {
	var g globals
	fs := flag.NewFlagSet("durableq", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	fs.StringVar(&g.databaseURL, "database-url", os.Getenv("DURABLEQ_DATABASE_URL"), "PostgreSQL connection string")
	fs.StringVar(&g.schema, "schema", envOr("DURABLEQ_SCHEMA", "public"), "schema holding the durableq tables")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	args := fs.Args()
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return errors.New("no command given")
	}
	if g.databaseURL == "" {
		return errors.New("set -database-url or DURABLEQ_DATABASE_URL")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch args[0] {
	case "migrate":
		return cmdMigrate(ctx, g, args[1:])
	case "queues":
		return cmdQueues(ctx, g)
	case "dlq":
		return cmdDLQ(ctx, g, args[1:])
	case "item":
		return cmdItem(ctx, g, args[1:])
	case "gc":
		return cmdGC(ctx, g, args[1:])
	case "pause":
		return cmdPause(ctx, g, args[1:], true)
	case "resume":
		return cmdPause(ctx, g, args[1:], false)
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func (g globals) connect(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, g.databaseURL)
}

func (g globals) store(ctx context.Context) (*postgres.Store, func(), error) {
	pool, err := pgxpool.New(ctx, g.databaseURL)
	if err != nil {
		return nil, nil, err
	}
	st, err := postgres.New(postgres.Config{Pool: pool, Schema: g.schema})
	if err != nil {
		pool.Close()
		return nil, nil, err
	}
	return st, pool.Close, nil
}

func cmdMigrate(ctx context.Context, g globals, args []string) error {
	if len(args) == 0 {
		return errors.New("migrate needs up, down or status")
	}
	conn, err := g.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()

	switch args[0] {
	case "up":
		if err := postgres.MigrateUp(ctx, conn, g.schema); err != nil {
			return err
		}
		v, err := postgres.AppliedVersion(ctx, conn, g.schema)
		if err != nil {
			return err
		}
		fmt.Printf("schema %s is at version %d\n", g.schema, v)
		return nil
	case "down":
		if err := postgres.MigrateDown(ctx, conn, g.schema, 0); err != nil {
			return err
		}
		fmt.Printf("schema %s rolled back\n", g.schema)
		return nil
	case "status":
		v, err := postgres.AppliedVersion(ctx, conn, g.schema)
		if err != nil {
			return err
		}
		migrations, err := postgres.Migrations()
		if err != nil {
			return err
		}
		latest := 0
		if len(migrations) > 0 {
			latest = migrations[len(migrations)-1].Version
		}
		fmt.Printf("applied %d, latest %d\n", v, latest)
		return nil
	default:
		return fmt.Errorf("unknown migrate subcommand %q", args[0])
	}
}

func cmdQueues(ctx context.Context, g globals) error {
	st, closeFn, err := g.store(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	stats, err := st.Stats(ctx)
	if err != nil {
		return err
	}
	if len(stats) == 0 {
		fmt.Println("no queues")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "QUEUE\tREADY\tRUNNING\tDONE\tDLQ\tPAUSED")
	for _, s := range stats {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%v\n", s.Queue, s.Ready, s.Running, s.Done, s.DLQ, s.Paused)
	}
	return w.Flush()
}

func cmdDLQ(ctx context.Context, g globals, args []string) error {
	if len(args) < 2 {
		return errors.New("dlq needs a subcommand and a queue, e.g. dlq ls indexing.dlq")
	}
	sub, queue := args[0], args[1]
	if !storage.IsDLQ(queue) {
		queue = storage.DLQName(queue)
	}

	fs := flag.NewFlagSet("dlq "+sub, flag.ContinueOnError)
	limit := fs.Int("limit", 20, "maximum items to act on")
	errorContains := fs.String("error-contains", "", "only items whose last error contains this text")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}

	st, closeFn, err := g.store(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	switch sub {
	case "ls":
		items, err := st.ListItems(ctx, storage.ItemFilter{
			Queue: queue, State: storage.StateDLQ,
			ErrorContains: *errorContains, Limit: *limit,
		})
		if err != nil {
			return err
		}
		if len(items) == 0 {
			fmt.Printf("%s is empty\n", queue)
			return nil
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tATTEMPTS\tWORKER\tFAILED AT\tERROR")
		for _, it := range items {
			fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%s\n",
				it.ID, it.Attempt, orDash(it.LastWorker),
				it.FinalizedAt.Format(time.RFC3339), truncate(it.LastError, 60))
		}
		return w.Flush()
	case "replay":
		moved, err := dlq.Drain(ctx, st, dlq.DrainOpts{
			Queue:         queue,
			Max:           *limit,
			ErrorContains: *errorContains,
			OnBatch: func(moved, total int) {
				if moved > 0 {
					fmt.Printf("replayed %d (%d total)\n", moved, total)
				}
			},
		})
		if err != nil {
			return err
		}
		fmt.Printf("replayed %d items from %s to %s\n", moved, queue, storage.BaseQueue(queue))
		return nil
	default:
		return fmt.Errorf("unknown dlq subcommand %q", sub)
	}
}

func cmdItem(ctx context.Context, g globals, args []string) error {
	if len(args) < 1 {
		return errors.New("item needs an id")
	}
	var id int64
	if _, err := fmt.Sscanf(args[0], "%d", &id); err != nil {
		return fmt.Errorf("bad item id %q", args[0])
	}

	st, closeFn, err := g.store(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	it, err := st.GetItem(ctx, id)
	if err != nil {
		return err
	}
	fmt.Printf("item      %d\nqueue     %s\nstate     %s\n", it.ID, it.Queue, it.State)
	if it.Outcome != "" {
		fmt.Printf("outcome   %s\n", it.Outcome)
	}
	fmt.Printf("attempts  %d of %d\n", it.Attempt, it.MaxAttempts)
	if it.ExecutionID != "" {
		fmt.Printf("execution %s\nstep      %s\nitem key  %s\n", it.ExecutionID, it.StepID, it.ItemID)
	}
	if it.LastError != "" {
		fmt.Printf("error     %s\n", it.LastError)
	}

	attempts, err := st.Attempts(ctx, id)
	if err != nil {
		return err
	}
	if len(attempts) == 0 {
		return nil
	}
	fmt.Println()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ATTEMPT\tWORKER\tSTARTED\tOUTCOME\tERROR")
	for _, a := range attempts {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\n",
			a.Attempt, orDash(a.Worker), a.StartedAt.Format(time.RFC3339),
			orDash(a.Outcome), truncate(a.Error, 50))
	}
	return w.Flush()
}

func cmdGC(ctx context.Context, g globals, args []string) error {
	fs := flag.NewFlagSet("gc", flag.ContinueOnError)
	retention := fs.Duration("retention", 7*24*time.Hour, "how long completed items are kept")
	queue := fs.String("queue", "", "limit to one queue")
	batch := fs.Int("batch", 1000, "items deleted per statement")
	if err := fs.Parse(args); err != nil {
		return err
	}

	st, closeFn, err := g.store(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	total := 0
	for {
		n, err := st.Sweep(ctx, storage.SweepOpts{
			Queue: *queue, Retention: *retention, Limit: *batch,
		})
		if err != nil {
			return err
		}
		total += n
		if n == 0 {
			break
		}
		fmt.Printf("deleted %d (%d total)\n", n, total)
	}
	fmt.Printf("deleted %d completed items older than %s\n", total, *retention)
	fmt.Println("dead-lettered items are never swept")
	return nil
}

func cmdPause(ctx context.Context, g globals, args []string, pause bool) error {
	if len(args) < 1 {
		return errors.New("pause and resume need a queue name")
	}
	st, closeFn, err := g.store(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	if pause {
		if err := st.PauseQueue(ctx, args[0]); err != nil {
			return err
		}
		fmt.Printf("paused %s\n", args[0])
		return nil
	}
	if err := st.ResumeQueue(ctx, args[0]); err != nil {
		return err
	}
	fmt.Printf("resumed %s\n", args[0])
	return nil
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
