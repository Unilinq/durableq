// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/unilinq/durableq/storage"
)

// emitTimeout bounds one Emit call. The call does not use the handler's own
// context: the pool cancels that context when a handler overruns its timeout,
// and the store must still be asked, so the fence can refuse the late commit.
const emitTimeout = 30 * time.Second

// Emitter commits part of a running item's output while the handler runs. It
// is bound to one item and one worker.
type Emitter struct {
	store  storage.Store
	worker string
	id     int64
	// attempt is the attempt the item was claimed under. The store refuses an
	// emit from an earlier attempt even when the same worker claimed the next.
	attempt int
}

// Emit commits items to their queues if this worker still holds the item.
// Otherwise it inserts nothing and returns an error wrapping storage.ErrLeaseLost.
func (e *Emitter) Emit(ctx context.Context, items []storage.NewItem) error {
	if len(items) == 0 {
		return nil
	}
	// The pool cancels the handler context before it retries a handler that
	// overran its timeout, so a done context means this attempt is abandoned.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: handler context ended: %v", storage.ErrLeaseLost, err)
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), emitTimeout)
	defer cancel()
	return e.store.Emit(ctx, storage.EmitRequest{ID: e.id, Worker: e.worker, Attempt: e.attempt, Produced: items})
}

type emitterKey struct{}

// WithEmitter returns a context that carries an emitter for the claimed item.
func WithEmitter(ctx context.Context, store storage.Store, worker string, id int64, attempt int) context.Context {
	return context.WithValue(ctx, emitterKey{}, &Emitter{store: store, worker: worker, id: id, attempt: attempt})
}

// EmitterFrom returns the emitter the pool put in a handler context.
func EmitterFrom(ctx context.Context) (*Emitter, bool) {
	e, ok := ctx.Value(emitterKey{}).(*Emitter)
	return e, ok
}
