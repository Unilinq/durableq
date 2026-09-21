package durableq

import (
	"errors"
	"fmt"
	"time"

	"github.com/unilinq/durableq/internal/runtime"
	"github.com/unilinq/durableq/storage"
)

// Policy governs how many times an item is attempted and how long it waits
// between attempts.
type Policy = storage.Policy

// DefaultPolicy is five attempts with waits of 5s, 30s, 2m and 10m, jittered
// by 10%.
func DefaultPolicy() Policy { return storage.DefaultPolicy() }

// RetryPolicy builds a policy from an explicit schedule. Attempts past the end
// of the schedule repeat its final wait.
func RetryPolicy(maxAttempts int, schedule ...time.Duration) Policy {
	return Policy{MaxAttempts: maxAttempts, Schedule: schedule, Jitter: 0.1}
}

// NoRetry is a policy that gives an item exactly one attempt.
func NoRetry() Policy {
	return Policy{MaxAttempts: 1, Schedule: []time.Duration{0}, Jitter: 0}
}

// Discard wraps an error so the item goes straight to its dead-letter queue
// without spending its remaining attempts. Use it for failures that retrying
// cannot fix, such as a payload this worker will never understand.
func Discard(err error) error {
	if err == nil {
		return runtime.ErrDiscard
	}
	return fmt.Errorf("%w: %w", runtime.ErrDiscard, err)
}

// IsDiscard reports whether an error asks for immediate dead-lettering.
func IsDiscard(err error) bool {
	return err != nil && errors.Is(err, runtime.ErrDiscard)
}
