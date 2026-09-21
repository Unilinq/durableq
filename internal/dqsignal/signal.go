// Package dqsignal provides a test signal: a channel wrapper that production
// code can emit into at near-zero cost and that tests wait on, so concurrent
// behaviour is asserted at the moment it happens instead of slept through.
//
// The zero value is safe to signal into and discards everything. A test calls
// Init to start buffering, then WaitOrTimeout to observe events.
package dqsignal

import (
	"fmt"
	"time"
)

// DefaultTimeout bounds how long WaitOrTimeout blocks.
const DefaultTimeout = 10 * time.Second

// defaultBuffer is deliberately generous: a test that misses an event because
// the buffer filled is a worse failure than the memory a few thousand slots
// cost during a test run.
const defaultBuffer = 4096

// Fataler is the subset of testing.TB the signal needs.
type Fataler interface {
	Helper()
	Fatalf(format string, args ...any)
}

// Signal carries values of type T from production code to a waiting test.
type Signal[T any] struct {
	ch chan T
}

// Init starts buffering. Until it is called, Emit is a no-op.
func (s *Signal[T]) Init() {
	s.ch = make(chan T, defaultBuffer)
}

// Initialized reports whether a test has armed this signal.
func (s *Signal[T]) Initialized() bool { return s.ch != nil }

// Emit sends a value if a test is listening. It never blocks and never panics,
// so leaving Emit calls in production paths is free.
func (s *Signal[T]) Emit(v T) {
	if s.ch == nil {
		return
	}
	select {
	case s.ch <- v:
	default:
	}
}

// WaitOrTimeout returns the next value, failing the test if none arrives.
func (s *Signal[T]) WaitOrTimeout(tb Fataler) T {
	tb.Helper()
	return s.WaitOrTimeoutAfter(tb, DefaultTimeout)
}

// WaitOrTimeoutAfter is WaitOrTimeout with an explicit bound.
func (s *Signal[T]) WaitOrTimeoutAfter(tb Fataler, d time.Duration) T {
	tb.Helper()
	if s.ch == nil {
		tb.Fatalf("dqsignal: WaitOrTimeout on a signal that was never Init'd")
		var zero T
		return zero
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case v := <-s.ch:
		return v
	case <-timer.C:
		tb.Fatalf("dqsignal: timed out after %s waiting for a signal", d)
		var zero T
		return zero
	}
}

// WaitN returns the next n values in order.
func (s *Signal[T]) WaitN(tb Fataler, n int) []T {
	tb.Helper()
	out := make([]T, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, s.WaitOrTimeout(tb))
	}
	return out
}

// TryReceive returns the next buffered value without blocking.
func (s *Signal[T]) TryReceive() (T, bool) {
	var zero T
	if s.ch == nil {
		return zero, false
	}
	select {
	case v := <-s.ch:
		return v, true
	default:
		return zero, false
	}
}

// Drain removes and returns everything buffered so far.
func (s *Signal[T]) Drain() []T {
	var out []T
	for {
		v, ok := s.TryReceive()
		if !ok {
			return out
		}
		out = append(out, v)
	}
}

// String makes a Signal printable in test failures.
func (s *Signal[T]) String() string {
	if s.ch == nil {
		return "dqsignal(uninitialised)"
	}
	return fmt.Sprintf("dqsignal(buffered=%d)", len(s.ch))
}
