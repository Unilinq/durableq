// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package durableq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/unilinq/durableq/internal/leasing"
	"github.com/unilinq/durableq/internal/runtime"
	"github.com/unilinq/durableq/storage"
)

// Queue is a named queue of independent work items.
type Queue struct {
	app  *App
	name string

	mu          sync.Mutex
	policy      storage.Policy
	concurrency int
	handler     runtime.Handler
	pool        *runtime.Pool
	heartbeat   *leasing.Heartbeater
	wg          sync.WaitGroup

	// poolReady, when set, is called with the pool and heartbeater the moment
	// they exist and before they run. Tests use it to arm signals; it is nil
	// in production and costs nothing.
	poolReady func(*runtime.Pool, *leasing.Heartbeater)
}

// QueueOption configures a queue at registration.
type QueueOption func(*Queue)

// WithPolicy sets the retry policy for work enqueued onto this queue.
func WithPolicy(p storage.Policy) QueueOption {
	return func(q *Queue) { q.policy = p }
}

// WithConcurrency sets how many handlers this process runs for the queue.
func WithConcurrency(n int) QueueOption {
	return func(q *Queue) { q.concurrency = n }
}

// Queue returns the named queue, creating it on first use. Calling it again
// with the same name returns the same queue.
func (a *App) Queue(name string, opts ...QueueOption) *Queue {
	a.mu.Lock()
	defer a.mu.Unlock()
	if q, ok := a.queues[name]; ok {
		for _, opt := range opts {
			opt(q)
		}
		return q
	}
	q := &Queue{
		app:         a,
		name:        name,
		policy:      a.cfg.DefaultPolicy,
		concurrency: 1,
	}
	for _, opt := range opts {
		opt(q)
	}
	a.queues[name] = q
	return q
}

// Name returns the queue's name.
func (q *Queue) Name() string { return q.name }

// DLQ returns the queue's dead-letter queue name.
func (q *Queue) DLQ() string { return storage.DLQName(q.name) }

// EnqueueOption adjusts one enqueue.
type EnqueueOption func(*storage.NewItem)

// At schedules the item for a specific time instead of immediately.
func At(t time.Time) EnqueueOption {
	return func(ni *storage.NewItem) { ni.AvailableAt = t }
}

// After schedules the item a duration from now.
func (q *Queue) After(d time.Duration) EnqueueOption {
	return func(ni *storage.NewItem) { ni.AvailableAt = q.app.cfg.Clock.Now().Add(d) }
}

// WithItemPolicy overrides the queue policy for one item.
func WithItemPolicy(p storage.Policy) EnqueueOption {
	return func(ni *storage.NewItem) { ni.Policy = &p }
}

// Enqueue adds one item. The payload is encoded as JSON unless it is already
// raw JSON bytes.
func (q *Queue) Enqueue(ctx context.Context, payload any, opts ...EnqueueOption) (storage.Item, error) {
	ni, err := q.newItem(payload, opts...)
	if err != nil {
		return storage.Item{}, err
	}
	items, err := q.app.cfg.Store.Enqueue(ctx, ni)
	if err != nil {
		return storage.Item{}, err
	}
	if len(items) == 0 {
		return storage.Item{}, errors.New("durableq: enqueue returned no item")
	}
	return items[0], nil
}

// EnqueueMany adds several items in one round trip.
func (q *Queue) EnqueueMany(ctx context.Context, payloads ...any) ([]storage.Item, error) {
	items := make([]storage.NewItem, 0, len(payloads))
	for _, p := range payloads {
		ni, err := q.newItem(p)
		if err != nil {
			return nil, err
		}
		items = append(items, ni)
	}
	return q.app.cfg.Store.Enqueue(ctx, items...)
}

func (q *Queue) newItem(payload any, opts ...EnqueueOption) (storage.NewItem, error) {
	raw, err := encodePayload(payload)
	if err != nil {
		return storage.NewItem{}, err
	}
	q.mu.Lock()
	policy := q.policy
	q.mu.Unlock()

	ni := storage.NewItem{Queue: q.name, Payload: raw, Policy: &policy}
	for _, opt := range opts {
		opt(&ni)
	}
	return ni, nil
}

func encodePayload(payload any) ([]byte, error) {
	switch v := payload.(type) {
	case nil:
		return []byte(`{}`), nil
	case json.RawMessage:
		return v, nil
	case []byte:
		if !json.Valid(v) {
			return nil, fmt.Errorf("durableq: []byte payload is not valid JSON; wrap it in a struct or use json.RawMessage")
		}
		return v, nil
	default:
		return json.Marshal(payload)
	}
}

// Work registers the handler for this queue. The handler must have the shape
//
//	func(ctx context.Context, in T) error
//
// where T is any JSON-decodable type, []byte for the raw payload, or
// storage.Item for the whole item.
//
// Leases, retries, backoff and dead-lettering are not the handler's concern:
// returning an error is enough, and the policy decides the rest.
func (q *Queue) Work(handler any, opts ...QueueOption) error {
	h, err := makeHandler(handler)
	if err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.handler != nil {
		return fmt.Errorf("durableq: queue %q already has a handler", q.name)
	}
	q.handler = h
	for _, opt := range opts {
		opt(q)
	}
	return nil
}

// Pause stops this process, and every other, from claiming the queue. Items
// already leased keep running.
func (q *Queue) Pause(ctx context.Context) error {
	return q.app.cfg.Store.PauseQueue(ctx, q.name)
}

// Resume undoes Pause.
func (q *Queue) Resume(ctx context.Context) error {
	return q.app.cfg.Store.ResumeQueue(ctx, q.name)
}

// Stats returns the queue's current counts.
func (q *Queue) Stats(ctx context.Context) (storage.QueueStat, error) {
	stats, err := q.app.cfg.Store.Stats(ctx, q.name)
	if err != nil {
		return storage.QueueStat{}, err
	}
	for _, s := range stats {
		if s.Queue == q.name {
			return s, nil
		}
	}
	return storage.QueueStat{Queue: q.name}, nil
}

// Replay returns dead-lettered items to this queue.
func (q *Queue) Replay(ctx context.Context, opts ...ReplayOption) (int, error) {
	ro := storage.ReplayOpts{Queue: q.DLQ(), Limit: 100}
	for _, opt := range opts {
		opt(&ro)
	}
	return q.app.cfg.Store.Replay(ctx, ro)
}

// ReplayOption narrows a replay.
type ReplayOption func(*storage.ReplayOpts)

// ReplayLimit bounds how many items one replay moves.
func ReplayLimit(n int) ReplayOption {
	return func(o *storage.ReplayOpts) { o.Limit = n }
}

// ReplayErrorContains replays only items whose last error contains s.
func ReplayErrorContains(s string) ReplayOption {
	return func(o *storage.ReplayOpts) { o.ErrorContains = s }
}

// workerPool returns the worker pool so tests can wait on its signals. It is
// nil until the app has started.
func (q *Queue) workerPool() *runtime.Pool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pool
}

// start launches the queue's worker and lease heartbeat.
func (q *Queue) start(ctx context.Context, app *App) error {
	q.mu.Lock()
	handler := q.handler
	concurrency := q.concurrency
	q.mu.Unlock()

	if handler == nil {
		// A queue used only as a producer is perfectly normal.
		return nil
	}

	pool, err := runtime.New(app.poolConfig(q.name, handler, concurrency))
	if err != nil {
		return err
	}
	heartbeat := app.heartbeaterFor(pool)

	q.mu.Lock()
	q.pool = pool
	q.heartbeat = heartbeat
	ready := q.poolReady
	q.mu.Unlock()
	if ready != nil {
		ready(pool, heartbeat)
	}

	q.wg.Add(2)
	go func() {
		defer q.wg.Done()
		_ = pool.Run(ctx, app.cfg.DrainTimeout)
	}()
	go func() {
		defer q.wg.Done()
		_ = heartbeat.Run(ctx)
	}()
	return nil
}

func (q *Queue) wait() { q.wg.Wait() }
