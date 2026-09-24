// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

// Command crashworker claims items and then hangs, so a test can SIGKILL it
// and prove that work held by a process that vanished is recovered exactly
// once by the reclaimer.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/unilinq/durableq/storage"
	"github.com/unilinq/durableq/storage/postgres"
)

func main() {
	databaseURL := flag.String("database-url", "", "PostgreSQL connection string")
	schema := flag.String("schema", "public", "schema holding the durableq tables")
	queue := flag.String("queue", "", "queue to claim from")
	worker := flag.String("worker", "crashworker", "worker identity")
	count := flag.Int("count", 1, "items to claim")
	lease := flag.Duration("lease", time.Minute, "lease duration")
	flag.Parse()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *databaseURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pool:", err)
		os.Exit(2)
	}
	store, err := postgres.New(postgres.Config{Pool: pool, Schema: *schema})
	if err != nil {
		fmt.Fprintln(os.Stderr, "store:", err)
		os.Exit(2)
	}

	items, err := store.Claim(ctx, storage.ClaimOpts{
		Queue: *queue, Worker: *worker, Limit: *count, LeaseDuration: *lease,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "claim:", err)
		os.Exit(2)
	}
	for _, it := range items {
		fmt.Printf("CLAIMED %d\n", it.ID)
	}
	fmt.Println("READY")
	os.Stdout.Sync()

	// Hold the lease and wait to be killed. Nothing is ever acked.
	select {}
}
