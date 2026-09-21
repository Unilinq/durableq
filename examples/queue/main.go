// Command queue is a runnable demonstration of durableq used as a plain queue:
// enqueue work, process it with a handler, and let the policy deal with
// failures. It seeds a schema you can then inspect with the durableq CLI.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/unilinq/durableq"
	"github.com/unilinq/durableq/storage/postgres"
)

// IndexInput is the payload this queue carries.
type IndexInput struct {
	URL string `json:"url"`
}

func main() {
	databaseURL := flag.String("database-url", os.Getenv("DURABLEQ_DATABASE_URL"), "PostgreSQL connection string")
	schema := flag.String("schema", "durableq_example", "schema to use")
	count := flag.Int("count", 20, "items to enqueue")
	flag.Parse()

	if *databaseURL == "" {
		log.Fatal("set -database-url or DURABLEQ_DATABASE_URL")
	}
	ctx := context.Background()

	// Migrate. In a real deployment this is a deploy step, not app startup.
	conn, err := pgx.Connect(ctx, *databaseURL)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	if err := postgres.MigrateUp(ctx, conn, *schema); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	_ = conn.Close(ctx)

	pool, err := pgxpool.New(ctx, *databaseURL)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	store, err := postgres.New(postgres.Config{Pool: pool, Schema: *schema})
	if err != nil {
		log.Fatalf("store: %v", err)
	}

	app, err := durableq.New(durableq.Config{
		Store:        store,
		PollInterval: 20 * time.Millisecond,
		Logger:       slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		log.Fatalf("app: %v", err)
	}

	var processed, failed int

	q := app.Queue("indexing",
		durableq.WithConcurrency(4),
		// Two quick attempts, so the example finishes while you watch it.
		durableq.WithPolicy(durableq.RetryPolicy(2, 10*time.Millisecond)),
	)
	if err := q.Work(func(ctx context.Context, in IndexInput) error {
		// Anything under /broken/ fails every time and ends in the DLQ.
		if strings.Contains(in.URL, "/broken/") {
			failed++
			return errors.New("unsupported_pdf_encoding")
		}
		processed++
		return nil
	}); err != nil {
		log.Fatalf("work: %v", err)
	}

	for i := 0; i < *count; i++ {
		url := fmt.Sprintf("https://example.test/page/%d", i)
		if i%5 == 0 {
			url = fmt.Sprintf("https://example.test/broken/%d", i)
		}
		if _, err := q.Enqueue(ctx, IndexInput{URL: url}); err != nil {
			log.Fatalf("enqueue: %v", err)
		}
	}

	if err := app.Start(ctx); err != nil {
		log.Fatalf("start: %v", err)
	}

	// Wait for the queue to settle: nothing ready and nothing running means
	// every item has either succeeded or exhausted its attempts.
	deadline := time.After(30 * time.Second)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
drain:
	for {
		select {
		case <-deadline:
			log.Print("timed out waiting for the queue to drain")
			break drain
		case <-tick.C:
			st, err := q.Stats(ctx)
			if err != nil {
				log.Fatalf("stats: %v", err)
			}
			if st.Ready == 0 && st.Running == 0 {
				break drain
			}
		}
	}

	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := app.Stop(stopCtx); err != nil {
		log.Fatalf("stop: %v", err)
	}

	st, err := q.Stats(ctx)
	if err != nil {
		log.Fatalf("stats: %v", err)
	}
	dlq, err := app.Store().Stats(ctx, q.DLQ())
	if err != nil {
		log.Fatalf("stats: %v", err)
	}
	dead := 0
	if len(dlq) > 0 {
		dead = dlq[0].DLQ
	}
	fmt.Printf("queue %s: ready %d, running %d, done %d, dead-lettered %d\n",
		st.Queue, st.Ready, st.Running, st.Done, dead)
	fmt.Printf("inspect it with:\n  durableq -schema %s -database-url ... queues\n", *schema)
}
