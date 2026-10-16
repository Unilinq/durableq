// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
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
}

// Emit commits items to their queues if this worker still holds the item.
// Otherwise it inserts nothing and returns an error wrapping storage.ErrLeaseLost.
func (e *Emitter) Emit(ctx context.Context, items []storage.NewItem) error {
	if len(items) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), emitTimeout)
	defer cancel()
	return e.store.Emit(ctx, storage.EmitRequest{ID: e.id, Worker: e.worker, Produced: items})
}

type emitterKey struct{}

// WithEmitter returns a context that carries an emitter for the claimed item.
func WithEmitter(ctx context.Context, store storage.Store, worker string, id int64) context.Context {
	return context.WithValue(ctx, emitterKey{}, &Emitter{store: store, worker: worker, id: id})
}

// EmitterFrom returns the emitter the pool put in a handler context.
func EmitterFrom(ctx context.Context) (*Emitter, bool) {
	e, ok := ctx.Value(emitterKey{}).(*Emitter)
	return e, ok
}
