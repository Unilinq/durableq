// Package scheduler runs durableq's background maintenance: returning work
// whose lease lapsed, and deleting terminal work past its retention.
package scheduler

import "sync"

// Breaker shrinks a batch size when the database rejects the work and grows it
// back on success. A maintenance loop that keeps retrying one oversized batch
// makes no progress at all; halving until something lands is how it recovers
// on its own.
//
// A Breaker is safe for concurrent use: a maintenance loop and an operator
// running a pass by hand share the same one.
type Breaker struct {
	mu      sync.Mutex
	max     int
	min     int
	current int
	tripped int
}

// NewBreaker builds a breaker that starts at max and never shrinks below min.
func NewBreaker(max, min int) *Breaker {
	if min < 1 {
		min = 1
	}
	if max < min {
		max = min
	}
	return &Breaker{max: max, min: min, current: max}
}

// Batch returns the size to attempt next.
func (b *Breaker) Batch() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.current
}

// Trip records a failure and halves the batch.
func (b *Breaker) Trip() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tripped++
	next := b.current / 2
	if next < b.min {
		next = b.min
	}
	b.current = next
}

// Reset records a success and restores the batch to full size.
func (b *Breaker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.current = b.max
}

// Tripped reports how many failures the breaker has absorbed.
func (b *Breaker) Tripped() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tripped
}
