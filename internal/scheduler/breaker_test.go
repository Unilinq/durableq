// Copyright 2026 Unilinq Inc
// SPDX-License-Identifier: Apache-2.0

package scheduler_test

import (
	"sync"
	"testing"

	"github.com/unilinq/durableq/internal/scheduler"
)

func TestBreakerHalvesAndRecovers(t *testing.T) {
	t.Parallel()
	b := scheduler.NewBreaker(100, 1)

	if got := b.Batch(); got != 100 {
		t.Fatalf("initial batch: got %d, want 100", got)
	}
	for _, want := range []int{50, 25, 12, 6, 3, 1, 1} {
		b.Trip()
		if got := b.Batch(); got != want {
			t.Fatalf("after trip: got %d, want %d", got, want)
		}
	}
	if got := b.Tripped(); got != 7 {
		t.Fatalf("tripped: got %d, want 7", got)
	}
	b.Reset()
	if got := b.Batch(); got != 100 {
		t.Fatalf("after reset: got %d, want 100", got)
	}
}

func TestBreakerRespectsItsFloor(t *testing.T) {
	t.Parallel()
	b := scheduler.NewBreaker(10, 4)
	for i := 0; i < 10; i++ {
		b.Trip()
	}
	if got := b.Batch(); got != 4 {
		t.Fatalf("batch floor: got %d, want 4", got)
	}
}

// TestBreakerIsConcurrencySafe guards the case that actually happens: a
// maintenance loop and an operator running a pass by hand share one breaker.
func TestBreakerIsConcurrencySafe(t *testing.T) {
	t.Parallel()
	b := scheduler.NewBreaker(64, 1)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				switch j % 3 {
				case 0:
					b.Trip()
				case 1:
					b.Reset()
				default:
					_ = b.Batch()
				}
			}
		}(i)
	}
	wg.Wait()
	if got := b.Batch(); got < 1 || got > 64 {
		t.Fatalf("batch out of range after concurrent use: %d", got)
	}
}
