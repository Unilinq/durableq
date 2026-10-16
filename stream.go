// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package durableq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/unilinq/durableq/internal/runtime"
	"github.com/unilinq/durableq/storage"
)

// Out is the output handle of a streaming step. It is bound to the item the
// step is running: it only commits while this worker still holds that item's lease.
type Out[T any] interface {
	// Add buffers items for every successor step. It flushes the buffer when it
	// holds FlushSize items, or when FlushInterval has passed since the last flush.
	Add(items ...T) error
	// Flush commits the buffered items now. When it returns nil, the items are
	// durable in the successor queues.
	Flush() error
}

// flushConfig is the resolved flush setting of one streaming step.
type flushConfig struct {
	size     int
	interval time.Duration
}

// streamHandler is the value Stream returns. It builds the runtime handler once
// the job knows the step's successors and flush config.
type streamHandler struct {
	// opts are step options given to Stream; Job.Step applies them before the
	// options given to Step itself.
	opts  []StepOption
	build func(nexts []*Step, flush flushConfig, clock storage.Clock) runtime.Handler
}

// Stream wraps a streaming step handler so it can be passed to Job.Step. The
// handler pushes output to the successor steps while it runs, through out.
// Items still buffered when the handler returns nil are committed with the
// ack. When it returns an error the buffer is dropped and the items already
// flushed stay. Processing is at-least-once: a retried streaming step sends
// the items it flushed in the earlier attempt again.
//
// Step options such as StepFlush may follow fn; they apply to the step as if
// they had been passed to Job.Step.
func Stream[T, U any](fn func(ctx context.Context, in T, out Out[U]) error, opts ...StepOption) any {
	argType := reflect.TypeFor[T]()
	return streamHandler{opts: opts, build: func(nexts []*Step, flush flushConfig, clock storage.Clock) runtime.Handler {
		return func(ctx context.Context, item storage.Item) (runtime.HandlerResult, error) {
			emitter, ok := runtime.EmitterFrom(ctx)
			if !ok {
				return runtime.HandlerResult{}, errors.New("durableq: a streaming step must run inside a worker pool")
			}
			arg, err := decodeArg(argType, item)
			if err != nil {
				return runtime.HandlerResult{}, err
			}
			in, _ := arg.Interface().(T)
			o := &streamOut[U]{
				ctx:       ctx,
				emitter:   emitter,
				item:      item,
				nexts:     nexts,
				flush:     flush,
				clock:     clock,
				lastFlush: clock.Now(),
			}
			// However the handler ends (nil, an error or a panic), the Out is
			// closed and its unflushed items are dropped, so a goroutine that
			// kept it cannot commit them later.
			defer o.close()
			if err := fn(ctx, in, o); err != nil {
				return runtime.HandlerResult{}, err
			}
			o.mu.Lock()
			defer o.mu.Unlock()
			o.closed = true
			if o.dead != nil {
				// A flush failed for good and the handler ignored it. The
				// attempt cannot finish cleanly.
				return runtime.HandlerResult{}, o.dead
			}
			rest, err := o.encodeBuffer()
			o.buf = nil
			if err != nil {
				return runtime.HandlerResult{}, err
			}
			if len(rest) == 0 && o.flushed == 0 {
				return runtime.HandlerResult{Filtered: true}, nil
			}
			return runtime.HandlerResult{Produced: rest}, nil
		}
	}}
}

// streamOut is the Out of one running item.
type streamOut[U any] struct {
	ctx     context.Context
	emitter *runtime.Emitter
	item    storage.Item
	nexts   []*Step
	flush   flushConfig
	clock   storage.Clock

	mu        sync.Mutex
	buf       []U
	lastFlush time.Time
	flushed   int   // NewItems committed by earlier flushes
	closed    bool  // the handler has returned
	dead      error // set once the lease is lost; every later call returns it
}

// ErrOutClosed is returned by Out.Add and Out.Flush after the handler that
// owns the Out has returned. Nothing is committed.
var ErrOutClosed = errors.New("durableq: output closed: the streaming handler has returned")

// close marks the Out closed and drops anything still buffered. The success
// path has already taken the buffer by the time it runs.
func (o *streamOut[U]) close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	o.buf = nil
}

// check reports why the Out can no longer take items. The caller holds mu.
func (o *streamOut[U]) check() error {
	if o.closed {
		return ErrOutClosed
	}
	return o.dead
}

func (o *streamOut[U]) Add(items ...U) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.check(); err != nil {
		return err
	}
	o.buf = append(o.buf, items...)
	if len(o.buf) >= o.flush.size || o.clock.Now().Sub(o.lastFlush) >= o.flush.interval {
		return o.flushLocked()
	}
	return nil
}

func (o *streamOut[U]) Flush() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.check(); err != nil {
		return err
	}
	return o.flushLocked()
}

func (o *streamOut[U]) flushLocked() error {
	if len(o.buf) == 0 {
		o.lastFlush = o.clock.Now()
		return nil
	}
	news, err := o.encodeBuffer()
	if err != nil {
		return err
	}
	if err := o.emitter.Emit(o.ctx, news); err != nil {
		err = fmt.Errorf("durableq: flushing streaming output: %w", err)
		if errors.Is(err, storage.ErrLeaseLost) {
			// The lease is gone for good: drop the buffer and refuse all
			// later calls without asking the store again.
			o.dead = err
			o.buf = nil
		}
		return err
	}
	o.flushed += len(news)
	o.buf = o.buf[:0]
	o.lastFlush = o.clock.Now()
	return nil
}

// encodeBuffer turns the buffer into one item per payload per successor. Every
// payload is a new logical item pointing back at the running item; a payload
// sent to several successors shares one ItemID.
func (o *streamOut[U]) encodeBuffer() ([]storage.NewItem, error) {
	news := make([]storage.NewItem, 0, len(o.buf)*len(o.nexts))
	for _, p := range o.buf {
		raw, err := encodePayload(p)
		if err != nil {
			return nil, fmt.Errorf("durableq: encoding step output: %w", err)
		}
		itemID := newItemID()
		for _, next := range o.nexts {
			policy := next.policy
			news = append(news, storage.NewItem{
				Queue:        next.queue,
				Payload:      raw,
				Policy:       &policy,
				ExecutionID:  o.item.ExecutionID,
				StepID:       next.id,
				ItemID:       itemID,
				ParentItemID: o.item.ItemID,
			})
		}
	}
	return news, nil
}
