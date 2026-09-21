package postgres

import (
	"context"
	"time"

	"github.com/unilinq/durableq/storage"
)

// The methods below land in stage 2 (claim/ack/retry/lease) and stage 4
// (DLQ/replay/retention). They are declared here so the contract is complete
// and any caller wiring against it compiles today.

// Claim is implemented in stage 2.
func (s *Store) Claim(ctx context.Context, opts storage.ClaimOpts) ([]storage.Item, error) {
	return nil, storage.ErrNotImplemented
}

// Complete is implemented in stage 2.
func (s *Store) Complete(ctx context.Context, reqs ...storage.CompleteRequest) ([]storage.Result, error) {
	return nil, storage.ErrNotImplemented
}

// Retry is implemented in stage 2.
func (s *Store) Retry(ctx context.Context, reqs ...storage.RetryRequest) ([]storage.Result, error) {
	return nil, storage.ErrNotImplemented
}

// DeadLetter is implemented in stage 2.
func (s *Store) DeadLetter(ctx context.Context, reqs ...storage.DeadLetterRequest) ([]storage.Result, error) {
	return nil, storage.ErrNotImplemented
}

// Release is implemented in stage 2.
func (s *Store) Release(ctx context.Context, reqs ...storage.ReleaseRequest) ([]storage.Result, error) {
	return nil, storage.ErrNotImplemented
}

// ExtendLease is implemented in stage 2.
func (s *Store) ExtendLease(ctx context.Context, worker string, until time.Time, ids ...int64) ([]storage.Result, error) {
	return nil, storage.ErrNotImplemented
}

// ReclaimExpired is implemented in stage 2.
func (s *Store) ReclaimExpired(ctx context.Context, opts storage.ReclaimOpts) (storage.ReclaimResult, error) {
	return storage.ReclaimResult{}, storage.ErrNotImplemented
}

// Replay is implemented in stage 4.
func (s *Store) Replay(ctx context.Context, opts storage.ReplayOpts) (int, error) {
	return 0, storage.ErrNotImplemented
}

// Sweep is implemented in stage 4.
func (s *Store) Sweep(ctx context.Context, opts storage.SweepOpts) (int, error) {
	return 0, storage.ErrNotImplemented
}
