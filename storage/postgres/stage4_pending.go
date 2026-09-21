package postgres

import (
	"context"

	"github.com/unilinq/durableq/storage"
)

// Replay is implemented in stage 4 (DLQ, replay, retention).
func (s *Store) Replay(ctx context.Context, opts storage.ReplayOpts) (int, error) {
	return 0, storage.ErrNotImplemented
}

// Sweep is implemented in stage 4 (DLQ, replay, retention).
func (s *Store) Sweep(ctx context.Context, opts storage.SweepOpts) (int, error) {
	return 0, storage.ErrNotImplemented
}
