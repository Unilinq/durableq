package storagetest

import (
	"errors"
	"testing"
	"time"

	"github.com/unilinq/durableq/storage"
)

func requireNoErr(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", what, err)
	}
}

func requireErrIs(t *testing.T, err, target error, what string) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("%s: got error %v, want %v", what, err, target)
	}
}

func requireEqual[T comparable](t *testing.T, got, want T, what string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

func requireLen[T any](t *testing.T, got []T, want int, what string) {
	t.Helper()
	if len(got) != want {
		t.Fatalf("%s: got %d elements, want %d", what, len(got), want)
	}
}

// requireTimeEqual compares times at microsecond resolution, which is what a
// timestamptz round trip preserves.
func requireTimeEqual(t *testing.T, got, want time.Time, what string) {
	t.Helper()
	g := got.UTC().Truncate(time.Microsecond)
	w := want.UTC().Truncate(time.Microsecond)
	if !g.Equal(w) {
		t.Fatalf("%s: got %s, want %s", what, g, w)
	}
}

func mustEnqueue(t *testing.T, store storage.Store, items ...storage.NewItem) []storage.Item {
	t.Helper()
	out, err := store.Enqueue(t.Context(), items...)
	requireNoErr(t, err, "Enqueue")
	requireLen(t, out, len(items), "Enqueue returned items")
	return out
}

func statFor(t *testing.T, store storage.Store, queue string) storage.QueueStat {
	t.Helper()
	stats, err := store.Stats(t.Context(), queue)
	requireNoErr(t, err, "Stats")
	for _, s := range stats {
		if s.Queue == queue {
			return s
		}
	}
	t.Fatalf("Stats: no entry for queue %q (got %+v)", queue, stats)
	return storage.QueueStat{}
}
