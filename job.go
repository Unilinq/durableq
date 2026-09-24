// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package durableq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/unilinq/durableq/internal/runtime"
	"github.com/unilinq/durableq/storage"
)

// Job is a linear pipeline of steps with a durable queue between each one.
//
//	discover -> crawl -> process -> index
//
// The design doc counts three "internal" queues for a four-step job, meaning
// the hand-offs. DurableQ gives the first step a queue as well, so the trigger
// itself is durable: a job invocation survives the process that made it.
//
// A step that fails all its attempts is dead-lettered and produces nothing
// downstream. Other items in the same execution carry on independently.
type Job struct {
	app  *App
	name string

	mu         sync.Mutex
	steps      []*Step
	wired      bool
	buildErr   error
	successors map[string][]string
}

// Step is one stage of a job.
type Step struct {
	id          string
	queue       string
	raw         any
	concurrency int
	policy      storage.Policy

	// afterSet is true once After has been called, even with no arguments.
	// That distinguishes "no After was given, use the declaration-order
	// default" from "explicitly given no predecessor".
	afterSet bool
	after    []string
}

// StepOption configures one step.
type StepOption func(*Step)

// StepConcurrency sets how many handlers this process runs for the step.
func StepConcurrency(n int) StepOption { return func(s *Step) { s.concurrency = n } }

// StepPolicy overrides the retry policy for work on this step's queue.
func StepPolicy(p Policy) StepOption { return func(s *Step) { s.policy = p } }

// After names this step's predecessor. A step with no After keeps today's
// behaviour: its predecessor is the step declared before it (the first step
// has none). Two steps naming the same After is a fan-out and is valid: each
// is its own successor queue, and the predecessor's output is broadcast to
// both.
//
// After naming an unknown step, or naming more than one, is a build error
// caught by wire(), not accepted silently - a join (more than one
// predecessor) is a different feature with different delivery semantics,
// reserved for later.
func After(steps ...string) StepOption {
	return func(s *Step) {
		s.afterSet = true
		s.after = append([]string(nil), steps...)
	}
}

// Job returns the named job, creating it on first use.
func (a *App) Job(name string) *Job {
	a.mu.Lock()
	defer a.mu.Unlock()
	if j, ok := a.jobs[name]; ok {
		return j
	}
	j := &Job{app: a, name: name}
	a.jobs[name] = j
	return j
}

// Name returns the job's name.
func (j *Job) Name() string { return j.name }

// Step appends a processing step. Handlers take one of three shapes:
//
//	func(ctx, T) ([]U, error)  fan-out: zero, one or many downstream items
//	func(ctx, T) (U, error)    exactly one downstream item
//	func(ctx, T) error         terminal: produces nothing
//
// Returning an empty slice is a legitimate terminal success, recorded as
// "filtered" so a deliberate filter is never mistaken for lost work.
func (j *Job) Step(id string, handler any, opts ...StepOption) *Job {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.wired {
		j.buildErr = errors.New("durableq: cannot add a step after the job has started")
		return j
	}
	for _, existing := range j.steps {
		if existing.id == id {
			j.buildErr = fmt.Errorf("durableq: job %q already has a step %q", j.name, id)
			return j
		}
	}
	s := &Step{
		id:          id,
		queue:       StepQueue(j.name, id),
		raw:         handler,
		concurrency: 1,
		policy:      j.app.cfg.DefaultPolicy,
	}
	for _, opt := range opts {
		opt(s)
	}
	j.steps = append(j.steps, s)
	return j
}

// StepQueue is the queue name durableq uses for a job's step. It is exported
// so operators can find the right queue from the CLI without guessing.
func StepQueue(job, step string) string { return job + "." + step }

// Steps returns the step ids in order.
func (j *Job) Steps() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]string, 0, len(j.steps))
	for _, s := range j.steps {
		out = append(out, s.id)
	}
	return out
}

// wire registers each step's queue and handler. It is called by App.Start and
// by Run, and is safe to call more than once.
func (j *Job) wire() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.buildErr != nil {
		return j.buildErr
	}
	if j.wired {
		return nil
	}
	if len(j.steps) == 0 {
		return fmt.Errorf("durableq: job %q has no steps", j.name)
	}

	nodes := make([]stepNode, len(j.steps))
	for i, step := range j.steps {
		after := step.after
		if !step.afterSet {
			if i > 0 {
				after = []string{j.steps[i-1].id}
			} else {
				after = nil
			}
		}
		nodes[i] = stepNode{id: step.id, after: after}
	}
	successors, err := buildStepGraph(nodes)
	if err != nil {
		return fmt.Errorf("durableq: job %q: %w", j.name, err)
	}

	byID := make(map[string]*Step, len(j.steps))
	for _, s := range j.steps {
		byID[s.id] = s
	}

	for _, step := range j.steps {
		var nexts []*Step
		for _, id := range successors[step.id] {
			nexts = append(nexts, byID[id])
		}
		h, err := makeStepHandler(step.raw, nexts)
		if err != nil {
			return fmt.Errorf("durableq: job %q step %q: %w", j.name, step.id, err)
		}
		q := j.app.Queue(step.queue,
			WithConcurrency(step.concurrency),
			WithPolicy(step.policy))
		if err := q.Work(h); err != nil {
			return fmt.Errorf("durableq: job %q step %q: %w", j.name, step.id, err)
		}
	}
	j.successors = successors
	j.wired = true
	return nil
}

// Run starts one execution of the job: it records the execution and its
// pipeline shape, then enqueues the input onto the first step's queue.
//
// It returns as soon as the work is durable. Progress is observed through the
// execution, not by waiting on this call.
func (j *Job) Run(ctx context.Context, input any) (storage.Execution, error) {
	if err := j.wire(); err != nil {
		return storage.Execution{}, err
	}
	execStore, ok := j.app.cfg.Store.(storage.ExecutionStore)
	if !ok {
		return storage.Execution{}, errors.New(
			"durableq: this store does not support jobs; it implements Store but not ExecutionStore")
	}

	j.mu.Lock()
	steps := make([]storage.StepDef, 0, len(j.steps))
	for i, s := range j.steps {
		def := storage.StepDef{StepID: s.id, Idx: i, Queue: s.queue}
		if next := j.successors[s.id]; len(next) > 0 {
			def.Next = append([]string(nil), next...)
		}
		steps = append(steps, def)
	}
	first := j.steps[0]
	j.mu.Unlock()

	payload, err := encodePayload(input)
	if err != nil {
		return storage.Execution{}, err
	}

	exec := storage.Execution{
		ID:    NewExecutionID(),
		Job:   j.name,
		State: storage.ExecutionRunning,
		Input: payload,
	}
	if err := execStore.CreateExecution(ctx, exec, steps); err != nil {
		return storage.Execution{}, err
	}

	policy := first.policy
	if _, err := j.app.cfg.Store.Enqueue(ctx, storage.NewItem{
		Queue:       first.queue,
		Payload:     payload,
		Policy:      &policy,
		ExecutionID: exec.ID,
		StepID:      first.id,
		ItemID:      newItemID(),
	}); err != nil {
		return storage.Execution{}, err
	}
	return execStore.GetExecution(ctx, exec.ID)
}

// makeStepHandler adapts a step handler to the runtime contract and wires its
// output into every successor's queue - one broadcast item per successor per
// payload - carrying each successor's own retry policy onto the work it
// creates. A downstream item is governed by the step that will run it, not by
// the step that produced it.
func makeStepHandler(handler any, nexts []*Step) (runtime.Handler, error) {
	if handler == nil {
		return nil, errors.New("step handler is nil")
	}
	if h, ok := handler.(runtime.Handler); ok {
		return h, nil
	}
	v := reflect.ValueOf(handler)
	t := v.Type()
	if t.Kind() != reflect.Func {
		return nil, fmt.Errorf("step handler must be a function, got %s", t)
	}
	if t.NumIn() != 2 || t.In(0) != contextType {
		return nil, fmt.Errorf("step handler must take (context.Context, T), got %s", t)
	}
	argType := t.In(1)

	var fanOut, single bool
	switch {
	case t.NumOut() == 1 && t.Out(0) == errorType:
		// terminal step
	case t.NumOut() == 2 && t.Out(1) == errorType && t.Out(0).Kind() == reflect.Slice:
		fanOut = true
	case t.NumOut() == 2 && t.Out(1) == errorType:
		single = true
	default:
		return nil, fmt.Errorf(
			"step handler must return error, (T, error) or ([]T, error), got %s", t)
	}
	if (fanOut || single) && len(nexts) == 0 {
		return nil, fmt.Errorf(
			"the last step produces output but has nowhere to send it; "+
				"add another step or return only error (got %s)", t)
	}

	return func(ctx context.Context, item storage.Item) (runtime.HandlerResult, error) {
		arg, err := decodeArg(argType, item)
		if err != nil {
			return runtime.HandlerResult{}, err
		}
		out := v.Call([]reflect.Value{reflect.ValueOf(ctx), arg})
		if e, _ := out[len(out)-1].Interface().(error); e != nil {
			return runtime.HandlerResult{}, e
		}
		if !fanOut && !single {
			return runtime.HandlerResult{}, nil
		}

		var payloads []reflect.Value
		if single {
			payloads = []reflect.Value{out[0]}
		} else {
			for i := 0; i < out[0].Len(); i++ {
				payloads = append(payloads, out[0].Index(i))
			}
		}
		if len(payloads) == 0 {
			// A deliberate filter: terminal success, and distinguishable from
			// both a failure and a step that produced output.
			return runtime.HandlerResult{Filtered: true}, nil
		}

		produced := make([]storage.NewItem, 0, len(payloads)*len(nexts))
		for _, p := range payloads {
			raw, err := encodePayload(p.Interface())
			if err != nil {
				return runtime.HandlerResult{}, fmt.Errorf("durableq: encoding step output: %w", err)
			}
			// One output means the same logical item moved on, so it keeps its
			// identity and a lineage query follows it end to end. Several
			// outputs are new items pointing back at their parent. That
			// identity is per payload, not per successor: broadcasting one
			// payload to several successors is one logical item entering
			// several branches, which is exactly what lineage should show.
			payloadItemID := newItemID()
			if len(payloads) == 1 {
				payloadItemID = item.ItemID
			}
			for _, next := range nexts {
				policy := next.policy
				produced = append(produced, storage.NewItem{
					Queue:        next.queue,
					Payload:      raw,
					Policy:       &policy,
					ExecutionID:  item.ExecutionID,
					StepID:       next.id,
					ItemID:       payloadItemID,
					ParentItemID: item.ItemID,
				})
			}
		}
		return runtime.HandlerResult{Produced: produced}, nil
	}, nil
}

// NewExecutionID returns a fresh execution identifier.
func NewExecutionID() string { return "exec_" + randomID() }

func newItemID() string { return "item_" + randomID() }

func randomID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is not something a queue can paper over.
		panic("durableq: reading random bytes: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
