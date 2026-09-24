// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package durableq

import (
	"context"
	"errors"

	"github.com/unilinq/durableq/internal/execution"
	"github.com/unilinq/durableq/storage"
)

// errNoExecutionStore explains the one configuration mistake that makes the
// job layer unavailable.
var errNoExecutionStore = errors.New(
	"durableq: this store does not support jobs; it implements Store but not ExecutionStore")

func (a *App) executionStore() (storage.ExecutionStore, error) {
	es, ok := a.cfg.Store.(storage.ExecutionStore)
	if !ok {
		return nil, errNoExecutionStore
	}
	return es, nil
}

// Execution returns one job invocation.
func (a *App) Execution(ctx context.Context, id string) (storage.Execution, error) {
	es, err := a.executionStore()
	if err != nil {
		return storage.Execution{}, err
	}
	return es.GetExecution(ctx, id)
}

// Executions returns recent job invocations, newest first.
func (a *App) Executions(ctx context.Context, limit int) ([]storage.Execution, error) {
	es, err := a.executionStore()
	if err != nil {
		return nil, err
	}
	return es.ListExecutions(ctx, limit)
}

// Lineage answers "where is item ABC?" for one execution: every step in order
// with what happened there, including the steps the item never reached.
//
// Lineage is durable state, not telemetry: it survives metrics retention and
// is the same answer whichever process asks.
func (a *App) Lineage(ctx context.Context, executionID, itemID string) ([]storage.LineageEntry, error) {
	es, err := a.executionStore()
	if err != nil {
		return nil, err
	}
	return es.Lineage(ctx, executionID, itemID)
}

// ExecutionSteps returns the pipeline shape recorded for an execution.
func (a *App) ExecutionSteps(ctx context.Context, executionID string) ([]storage.StepDef, error) {
	es, err := a.executionStore()
	if err != nil {
		return nil, err
	}
	return es.ExecutionSteps(ctx, executionID)
}

// Projection is the run-level answer to "what happened to this run?".
type Projection = execution.Projection

// StepProjection is one row of the run table.
type StepProjection = execution.StepProjection

// Leak is one discrepancy found while projecting a run.
type Leak = execution.Leak

// Run-level status values.
const (
	StatusRunning  = execution.StatusRunning
	StatusComplete = execution.StatusComplete
)

// Projection derives the state of one execution from durable state. Nothing is
// cached: the answer is a query, so it is the same whichever process asks and
// survives any metrics retention window.
func (a *App) Projection(ctx context.Context, executionID string) (Projection, error) {
	es, err := a.executionStore()
	if err != nil {
		return Projection{}, err
	}
	exec, err := es.GetExecution(ctx, executionID)
	if err != nil {
		return Projection{}, err
	}
	counts, err := es.StepCounts(ctx, executionID)
	if err != nil {
		return Projection{}, err
	}
	steps, err := es.ExecutionSteps(ctx, executionID)
	if err != nil {
		return Projection{}, err
	}
	edges := make(map[string][]string, len(steps))
	for _, s := range steps {
		edges[s.StepID] = s.Next
	}
	return execution.Project(exec, counts, edges), nil
}
