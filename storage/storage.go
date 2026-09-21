// Package storage defines the durable store contract shared by all adapters.
//
// DurableQ guarantees at-least-once processing. A handler may observe the same
// item more than once; side effects must tolerate that.
package storage

import (
	"context"
	"errors"
	"strings"
	"time"
)

// State is the lifecycle state of a queue item.
type State string

const (
	// StateReady means the item is claimable once AvailableAt has passed.
	StateReady State = "ready"
	// StateRunning means a worker holds a lease on the item.
	StateRunning State = "running"
	// StateDone means the item reached a terminal success (or was filtered).
	StateDone State = "done"
	// StateDLQ means the item exhausted its attempts and sits in a dead-letter queue.
	StateDLQ State = "dlq"
)

// Outcome records how a terminal item finished.
type Outcome string

const (
	// OutcomeSuccess is a step that completed and produced downstream work.
	OutcomeSuccess Outcome = "success"
	// OutcomeFiltered is a step that completed deliberately producing nothing.
	OutcomeFiltered Outcome = "filtered"
	// OutcomeDeadLettered is a step that exhausted its attempts.
	OutcomeDeadLettered Outcome = "dlq"
)

// DLQSuffix is appended to a queue name to form its dead-letter queue.
const DLQSuffix = ".dlq"

// DLQName returns the dead-letter queue belonging to queue.
func DLQName(queue string) string { return queue + DLQSuffix }

// IsDLQ reports whether queue names a dead-letter queue.
func IsDLQ(queue string) bool { return strings.HasSuffix(queue, DLQSuffix) }

// BaseQueue returns the working queue that a dead-letter queue belongs to.
func BaseQueue(dlq string) string { return strings.TrimSuffix(dlq, DLQSuffix) }

// RetentionNever disables retention sweeping for a state.
const RetentionNever = time.Duration(-1)

// Errors returned by Store implementations. Callers compare with errors.Is.
var (
	// ErrNotFound means no item exists with the given id.
	ErrNotFound = errors.New("durableq: item not found")
	// ErrWrongState means the item exists but is not in the state the
	// operation requires; the operation was a no-op.
	ErrWrongState = errors.New("durableq: item not in the expected state")
	// ErrNotImplemented marks a contract method an adapter has not built yet.
	ErrNotImplemented = errors.New("durableq: not implemented")
)

// Clock supplies the current time. Production code uses RealClock; tests inject
// a stub so time-dependent behaviour is asserted rather than slept through.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

// RealClock returns a Clock backed by the wall clock, in UTC.
func RealClock() Clock { return realClock{} }

// Policy governs how many times an item is attempted and how long it waits
// between attempts. A policy is snapshotted onto an item at enqueue time, so
// changing a queue's policy never rewrites work already in flight.
type Policy struct {
	// MaxAttempts is the total number of attempts before dead-lettering.
	MaxAttempts int
	// Schedule is the wait before each retry. Attempt n waits Schedule[n-1];
	// attempts past the end of the schedule repeat the final entry.
	Schedule []time.Duration
	// Jitter is the fraction of the backoff randomly added or subtracted,
	// e.g. 0.1 for +/-10%. Zero makes backoff exactly deterministic.
	Jitter float64
}

// DefaultPolicy is the policy applied when neither step nor queue sets one.
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: 5,
		Schedule: []time.Duration{
			5 * time.Second,
			30 * time.Second,
			2 * time.Minute,
			10 * time.Minute,
		},
		Jitter: 0.1,
	}
}

// Backoff returns the deterministic wait after the given attempt has failed.
// Attempt is 1-based: Backoff(1) is the wait after the first failure.
func (p Policy) Backoff(attempt int) time.Duration {
	if len(p.Schedule) == 0 {
		return DefaultPolicy().Backoff(attempt)
	}
	if attempt < 1 {
		attempt = 1
	}
	i := attempt - 1
	if i >= len(p.Schedule) {
		i = len(p.Schedule) - 1
	}
	return p.Schedule[i]
}

// JitterBounds returns the inclusive range a jittered backoff may fall in.
func (p Policy) JitterBounds(attempt int) (low, high time.Duration) {
	base := p.Backoff(attempt)
	if p.Jitter <= 0 {
		return base, base
	}
	delta := time.Duration(float64(base) * p.Jitter)
	return base - delta, base + delta
}

// Exhausted reports whether an item on its given attempt has no attempts left.
func (p Policy) Exhausted(attempt int) bool {
	max := p.MaxAttempts
	if max <= 0 {
		max = DefaultPolicy().MaxAttempts
	}
	return attempt >= max
}

// Item is a unit of durable work.
type Item struct {
	ID          int64
	Queue       string
	Payload     []byte
	State       State
	Outcome     Outcome
	Attempt     int
	MaxAttempts int
	AvailableAt time.Time
	LeasedBy    string
	LeaseUntil  time.Time
	LastError   string
	// LastWorker is the worker that made the most recent attempt. It survives
	// into terminal states, so a dead-lettered item still names its worker.
	LastWorker string

	// Lineage. Set when the item belongs to a Job execution.
	ExecutionID  string
	StepID       string
	ItemID       string
	ParentItemID string

	// Fan-out accounting, written when the item reaches a terminal state.
	Produced       int
	Dropped        int
	ProducedCapped bool

	Policy      Policy
	CreatedAt   time.Time
	UpdatedAt   time.Time
	FinalizedAt time.Time
}

// NewItem describes work to enqueue.
type NewItem struct {
	Queue   string
	Payload []byte
	// AvailableAt delays the item. Zero means immediately.
	AvailableAt time.Time
	// Policy overrides the queue default. Nil takes the queue's policy.
	Policy *Policy

	ExecutionID  string
	StepID       string
	ItemID       string
	ParentItemID string
}

// ClaimOpts parameterises a claim.
type ClaimOpts struct {
	Queue         string
	Worker        string
	Limit         int
	LeaseDuration time.Duration
	// Now overrides the store clock for this call. Zero uses the clock.
	Now time.Time
}

// CompleteRequest acks one running item and, atomically, enqueues the work it
// produced. Producing nothing is a legitimate terminal success recorded as
// OutcomeFiltered.
type CompleteRequest struct {
	ID       int64
	Worker   string
	Produced []NewItem
	// Filtered marks a step that deliberately produced nothing, so a
	// legitimate filter is distinguishable from a step that produced output.
	// A plain queue handler leaves this false.
	Filtered bool
	// Dropped and Capped record output the step deliberately discarded, so a
	// bounded discovery step does not look like a silent leak.
	Dropped int
	Capped  bool
}

// RetryRequest returns a running item to ready after a failure, consuming the
// attempt it just used.
type RetryRequest struct {
	ID     int64
	Worker string
	Error  string
	// At overrides the policy backoff. Zero uses the policy.
	At time.Time
}

// DeadLetterRequest moves a running item to its queue's DLQ.
type DeadLetterRequest struct {
	ID     int64
	Worker string
	Error  string
}

// ReleaseRequest returns a running item to ready WITHOUT consuming an attempt.
// This is what graceful shutdown uses: work handed back because the process is
// stopping must not count against the item's budget.
type ReleaseRequest struct {
	ID     int64
	Worker string
	Reason string
}

// Result reports the per-item outcome of a batch transition. Err is nil on
// success, or wraps ErrNotFound or ErrWrongState.
type Result struct {
	ID  int64
	Err error
}

// ReplayOpts selects dead-lettered items to return to their working queue.
type ReplayOpts struct {
	// Queue is the dead-letter queue to drain, e.g. "indexing.dlq".
	Queue string
	Limit int
	// ErrorContains filters to items whose last error contains this substring.
	ErrorContains string
	Now           time.Time
}

// SweepOpts controls retention deletion of terminal items. Dead-lettered items
// are never swept, whatever the retention.
type SweepOpts struct {
	// Queue limits the sweep. Empty sweeps every queue.
	Queue string
	// Retention is how long a done item is kept. RetentionNever disables it.
	Retention time.Duration
	// Limit bounds one sweep batch.
	Limit int
	Now   time.Time
}

// ReclaimOpts controls recovery of items whose lease expired.
type ReclaimOpts struct {
	Limit int
	Now   time.Time
}

// ReclaimResult reports what a reclaim pass did. Items past their attempt
// budget are dead-lettered rather than returned to the queue.
type ReclaimResult struct {
	Reclaimed    int
	DeadLettered int
}

// ItemFilter narrows a listing of items.
type ItemFilter struct {
	Queue         string
	State         State
	ExecutionID   string
	ItemID        string
	ErrorContains string
	Limit         int
}

// Attempt is one recorded try at an item.
type Attempt struct {
	Attempt   int
	Worker    string
	StartedAt time.Time
	EndedAt   time.Time
	Outcome   string
	Error     string
}

// Lister is implemented by stores that can enumerate items. It is separate
// from Store so the hot path stays small.
type Lister interface {
	ListItems(ctx context.Context, f ItemFilter) ([]Item, error)
	Attempts(ctx context.Context, id int64) ([]Attempt, error)
}

// Execution is one invocation of a Job.
//
// State is a lifecycle marker, not a verdict. DurableQ has no completion
// barrier: whether a run has finished is derived from the state of its items,
// so callers asking "is this done?" project the run rather than read a column.
type Execution struct {
	ID        string
	Job       string
	State     string
	Input     []byte
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Execution lifecycle markers.
const (
	// ExecutionRunning is set when a run is created.
	ExecutionRunning = "running"
	// ExecutionComplete is available for callers that choose to mark a run
	// finished on their own terms. Nothing in durableq sets it, because what
	// counts as a finished run is a business question durableq does not answer.
	ExecutionComplete = "complete"
)

// StepDef records one step of a job for one execution, so lineage and
// projections can be read back without the job definition being in memory.
//
// Next names this step's successors. A linear job has at most one; a step
// with several is a fan-out, broadcasting its output to every successor as
// its own item on that successor's queue. Edges are authoritative for
// topology: Idx is for stable display ordering only.
type StepDef struct {
	StepID string
	Idx    int
	Queue  string
	Next   []string
}

// LineageEntry is what happened to one item at one step. A step the item never
// reached is reported with Entered false rather than omitted, because "never
// entered" is the answer to a real question.
type LineageEntry struct {
	StepID   string
	Idx      int
	Entered  bool
	ItemPK   int64
	State    State
	Outcome  Outcome
	Attempt  int
	Error    string
	Produced int
}

// StepCount is the per-edge tally a projection is built from. Every field is
// derived from durable item state, never from an in-memory counter.
type StepCount struct {
	StepID    string
	Idx       int
	Queue     string
	Received  int
	Succeeded int
	Filtered  int
	DLQ       int
	Active    int
	Produced  int
	Dropped   int
	Capped    int
}

// ExecutionStore is implemented by stores that carry the job layer. It is
// separate from Store so a plain queue deployment need not provide it.
type ExecutionStore interface {
	CreateExecution(ctx context.Context, exec Execution, steps []StepDef) error
	GetExecution(ctx context.Context, id string) (Execution, error)
	ListExecutions(ctx context.Context, limit int) ([]Execution, error)
	ExecutionSteps(ctx context.Context, id string) ([]StepDef, error)
	StepCounts(ctx context.Context, executionID string) ([]StepCount, error)
	Lineage(ctx context.Context, executionID, itemID string) ([]LineageEntry, error)
}

// QueueStat is a point-in-time count for one queue.
type QueueStat struct {
	Queue   string
	Ready   int
	Running int
	Done    int
	DLQ     int
	Paused  bool
}

// Store is the durable contract every backend implements. All methods are safe
// for concurrent use. Batch methods report per-item results and never fail the
// whole batch because one item was missing or in the wrong state.
type Store interface {
	// Enqueue inserts items and returns them with ids assigned.
	Enqueue(ctx context.Context, items ...NewItem) ([]Item, error)

	// Claim atomically takes up to Limit ready items and leases them. Items
	// whose AvailableAt is in the future, and items on paused queues, are not
	// returned. Claiming increments the attempt counter.
	Claim(ctx context.Context, opts ClaimOpts) ([]Item, error)

	// Complete acks running items and enqueues their downstream work in the
	// same transaction.
	Complete(ctx context.Context, reqs ...CompleteRequest) ([]Result, error)

	// Retry returns running items to ready, scheduled by policy.
	Retry(ctx context.Context, reqs ...RetryRequest) ([]Result, error)

	// DeadLetter moves running items to their dead-letter queue.
	DeadLetter(ctx context.Context, reqs ...DeadLetterRequest) ([]Result, error)

	// Release returns running items to ready without consuming an attempt.
	Release(ctx context.Context, reqs ...ReleaseRequest) ([]Result, error)

	// ExtendLease renews the lease a worker holds on items it still owns.
	ExtendLease(ctx context.Context, worker string, until time.Time, ids ...int64) ([]Result, error)

	// ReclaimExpired returns items whose lease lapsed to ready, or to the DLQ
	// if they have no attempts left.
	ReclaimExpired(ctx context.Context, opts ReclaimOpts) (ReclaimResult, error)

	// Replay returns dead-lettered items to their working queue.
	Replay(ctx context.Context, opts ReplayOpts) (int, error)

	// Sweep deletes terminal items past retention. Never deletes DLQ items,
	// running items, or ready items.
	Sweep(ctx context.Context, opts SweepOpts) (int, error)

	// Stats returns per-queue counts. With no queues, returns every queue.
	Stats(ctx context.Context, queues ...string) ([]QueueStat, error)

	// PauseQueue stops claims against a queue without touching its items.
	PauseQueue(ctx context.Context, queue string) error

	// ResumeQueue undoes PauseQueue.
	ResumeQueue(ctx context.Context, queue string) error

	// GetItem reads one item by id.
	GetItem(ctx context.Context, id int64) (Item, error)

	// Close releases resources held by the store.
	Close() error
}
