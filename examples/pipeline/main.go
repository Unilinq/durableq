// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

// Command pipeline is a runnable demonstration of the design document's
// example job: discover -> crawl -> process -> index, with a few pages that
// fail permanently so the dead-letter queue and the run projection have
// something to show.
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

type (
	site struct {
		Root  string `json:"root"`
		Pages int    `json:"pages"`
	}
	page struct {
		URL string `json:"url"`
	}
	document struct {
		URL  string `json:"url"`
		Body string `json:"body"`
	}
	record struct {
		URL  string `json:"url"`
		Text string `json:"text"`
	}
)

func main() {
	databaseURL := flag.String("database-url", os.Getenv("DURABLEQ_DATABASE_URL"), "PostgreSQL connection string")
	schema := flag.String("schema", "durableq_pipeline", "schema to use")
	pages := flag.Int("pages", 25, "pages the discovery step finds")
	flag.Parse()

	if *databaseURL == "" {
		log.Fatal("set -database-url or DURABLEQ_DATABASE_URL")
	}
	ctx := context.Background()

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

	// Two quick attempts, so the example finishes while you watch it.
	failFast := durableq.RetryPolicy(2, 10*time.Millisecond)

	job := app.Job("document-ingestion").
		// One item in, many out: the queue absorbs the cardinality.
		Step("discover", func(ctx context.Context, in site) ([]page, error) {
			out := make([]page, 0, in.Pages)
			for i := 0; i < in.Pages; i++ {
				out = append(out, page{URL: fmt.Sprintf("%s/page/%d", in.Root, i)})
			}
			return out, nil
		}).
		Step("crawl", func(ctx context.Context, in page) (document, error) {
			// Every seventh page is a permanent failure.
			if strings.HasSuffix(in.URL, "7") {
				return document{}, errors.New("http 451: unavailable for legal reasons")
			}
			return document{URL: in.URL, Body: "<html>" + in.URL + "</html>"}, nil
		}, durableq.StepConcurrency(4), durableq.StepPolicy(failFast)).
		Step("process", func(ctx context.Context, in document) ([]record, error) {
			text := strings.TrimSuffix(strings.TrimPrefix(in.Body, "<html>"), "</html>")
			// Pages ending in 3 hold nothing worth indexing. Returning no
			// items is a deliberate filter, not a failure and not lost work.
			if strings.HasSuffix(in.URL, "3") {
				return nil, nil
			}
			return []record{{URL: in.URL, Text: text}}, nil
		}, durableq.StepConcurrency(4), durableq.StepPolicy(failFast)).
		Step("index", func(ctx context.Context, in record) error {
			return nil
		}, durableq.StepConcurrency(4), durableq.StepPolicy(failFast))

	if err := app.Start(ctx); err != nil {
		log.Fatalf("start: %v", err)
	}

	exec, err := job.Run(ctx, site{Root: "https://example.test", Pages: *pages})
	if err != nil {
		log.Fatalf("run: %v", err)
	}
	fmt.Printf("started %s\n\n", exec.ID)

	// Wait for the run to settle, then print the projection.
	deadline := time.After(60 * time.Second)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	var p durableq.Projection
settle:
	for {
		select {
		case <-deadline:
			log.Print("timed out waiting for the run to settle")
			break settle
		case <-tick.C:
			p, err = app.Projection(ctx, exec.ID)
			if err != nil {
				log.Fatalf("projection: %v", err)
			}
			if p.Status == durableq.StatusComplete {
				break settle
			}
		}
	}

	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := app.Stop(stopCtx); err != nil {
		log.Fatalf("stop: %v", err)
	}

	fmt.Printf("Run %s\nStatus: %s\n\n", p.Execution.ID, p.Status)
	fmt.Printf("%-10s %9s %9s %9s %6s %7s\n", "STEP", "RECEIVED", "SUCCESS", "FILTERED", "DLQ", "ACTIVE")
	for _, s := range p.Steps {
		fmt.Printf("%-10s %9d %9d %9d %6d %7d\n",
			s.StepID, s.Received, s.Succeeded, s.Filtered, s.DLQ, s.Active)
	}
	fmt.Printf("\nTerminal successful: %d\nTerminal DLQ:        %d\nActive:              %d\n",
		p.TerminalSuccess, p.TerminalDLQ, p.Active)
	for _, l := range p.Leaks {
		fmt.Printf("note: %s\n", l)
	}
	fmt.Printf("\ninspect it with:\n  durableq -schema %s run %s\n", *schema, exec.ID)
}
