package dqsignal_test

import (
	"sync"
	"testing"
	"time"

	"github.com/unilinq/durableq/internal/dqsignal"
)

func TestZeroValueDiscards(t *testing.T) {
	t.Parallel()
	var s dqsignal.Signal[int]
	if s.Initialized() {
		t.Fatalf("zero value reports itself initialised")
	}
	// Emitting into an unarmed signal must be safe and free.
	for i := 0; i < 1000; i++ {
		s.Emit(i)
	}
	if v, ok := s.TryReceive(); ok {
		t.Fatalf("unarmed signal produced %v", v)
	}
}

func TestWaitOrTimeoutReceivesInOrder(t *testing.T) {
	t.Parallel()
	var s dqsignal.Signal[string]
	s.Init()

	go func() {
		s.Emit("first")
		s.Emit("second")
	}()

	if got := s.WaitOrTimeout(t); got != "first" {
		t.Fatalf("first value: got %q", got)
	}
	if got := s.WaitOrTimeout(t); got != "second" {
		t.Fatalf("second value: got %q", got)
	}
}

func TestWaitNAndDrain(t *testing.T) {
	t.Parallel()
	var s dqsignal.Signal[int]
	s.Init()
	for i := 0; i < 5; i++ {
		s.Emit(i)
	}
	got := s.WaitN(t, 3)
	if len(got) != 3 || got[0] != 0 || got[2] != 2 {
		t.Fatalf("WaitN: got %v", got)
	}
	rest := s.Drain()
	if len(rest) != 2 {
		t.Fatalf("Drain: got %v, want the remaining 2", rest)
	}
}

// TestWaitOrTimeoutFailsOnSilence proves the timeout path reports rather than
// hanging the suite. It uses a recording stub in place of *testing.T.
func TestWaitOrTimeoutFailsOnSilence(t *testing.T) {
	t.Parallel()
	var s dqsignal.Signal[int]
	s.Init()

	rec := &recorder{}
	s.WaitOrTimeoutAfter(rec, 20*time.Millisecond)
	if !rec.failed() {
		t.Fatalf("waiting on a silent signal did not fail the test")
	}
}

// TestConcurrentEmitIsRaceFree is the reason this type exists: production code
// emits from many goroutines while a test reads.
func TestConcurrentEmitIsRaceFree(t *testing.T) {
	t.Parallel()
	var s dqsignal.Signal[int]
	s.Init()

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				s.Emit(w)
			}
		}(w)
	}
	wg.Wait()

	if got := len(s.Drain()); got != 800 {
		t.Fatalf("drained %d values, want 800", got)
	}
}

type recorder struct {
	mu   sync.Mutex
	msgs []string
}

func (r *recorder) Helper() {}

func (r *recorder) Fatalf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, format)
}

func (r *recorder) failed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs) > 0
}
