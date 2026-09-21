package durableq

import (
	"context"
	"errors"

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
