// Package dlq drains dead-letter queues back into the work they came from.
//
// Replaying is deliberately a batched loop rather than one statement: draining
// a large DLQ in a single transaction would hold locks over every row at once,
// and an operator watching a recovery wants to see it progress.
package dlq

import (
	"context"

	"github.com/unilinq/durableq/storage"
)

// DrainOpts controls a drain.
type DrainOpts struct {
	// Queue is the dead-letter queue to drain, e.g. "indexing.dlq".
	Queue string
	// BatchSize bounds one replay statement.
	BatchSize int
	// Max bounds the whole drain. Zero drains everything that matches.
	Max int
	// ErrorContains replays only items whose last error contains this text,
	// which is how an operator drains one class of failure after a fix.
	ErrorContains string
	// OnBatch, if set, is called after each batch with the running total.
	OnBatch func(moved, total int)
}

// Drain replays a dead-letter queue in batches until it is empty, the Max is
// reached, or the context ends. It returns how many items were moved.
func Drain(ctx context.Context, store storage.Store, opts DrainOpts) (int, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	total := 0
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		limit := opts.BatchSize
		if opts.Max > 0 {
			remaining := opts.Max - total
			if remaining <= 0 {
				return total, nil
			}
			if remaining < limit {
				limit = remaining
			}
		}
		moved, err := store.Replay(ctx, storage.ReplayOpts{
			Queue:         opts.Queue,
			Limit:         limit,
			ErrorContains: opts.ErrorContains,
		})
		if err != nil {
			return total, err
		}
		total += moved
		if opts.OnBatch != nil {
			opts.OnBatch(moved, total)
		}
		if moved == 0 {
			return total, nil
		}
	}
}
