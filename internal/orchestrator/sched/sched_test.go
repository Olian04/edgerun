package sched

import (
	"fmt"
	"testing"

	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

func workers(n int) []store.Worker {
	var ws []store.Worker
	for i := range n {
		ws = append(ws, store.Worker{ID: fmt.Sprint("id", i), Endpoint: fmt.Sprintf("http://10.0.0.%d:9000", i), MaxConcurrency: 4})
	}
	return ws
}

func first(key string, ws []store.Worker) string { return Order(key, ws)[0].Endpoint }

func TestDeterministicAndInputOrderIndependent(t *testing.T) {
	ws := workers(8)
	rev := make([]store.Worker, len(ws))
	for i := range ws {
		rev[len(ws)-1-i] = ws[i]
	}
	for k := range 50 {
		key := fmt.Sprint("sha256:", k)
		a, b := Order(key, ws), Order(key, rev)
		for i := range a {
			if a[i].Endpoint != b[i].Endpoint {
				t.Fatalf("key %s: order depends on input order", key)
			}
		}
	}
}

func TestPinnedOrderIsStableAcrossReleases(t *testing.T) {
	// Orchestrator instances of different versions must agree, so the scoring
	// function is part of the contract. These orders were captured from the
	// implementation (they are not derivable by hand). If this fails, the hash
	// changed and a rolling upgrade would reshuffle every module's worker:
	// that needs a deliberate migration, not an edit of this table.
	for key, want := range map[string]string{
		"sha256:abc": "[id0 id1 id2 id3]",
		"sha256:abd": "[id0 id1 id3 id2]",
		"k2":         "[id2 id1 id3 id0]",
		"k3":         "[id3 id2 id0 id1]",
	} {
		var ids []string
		for _, w := range Order(key, workers(4)) {
			ids = append(ids, w.ID)
		}
		if got := fmt.Sprint(ids); got != want {
			t.Errorf("Order(%q) = %s, want %s", key, got, want)
		}
	}
}

func endpoints(ws []store.Worker) []string {
	var out []string
	for _, w := range ws {
		out = append(out, w.Endpoint)
	}
	return out
}

func TestRemovingAWorkerOnlyMovesItsOwnKeys(t *testing.T) {
	ws := workers(10)
	removed := ws[4].Endpoint
	without := append(append([]store.Worker{}, ws[:4]...), ws[5:]...)
	moved, ownedByRemoved := 0, 0
	const keys = 2000
	for k := range keys {
		key := fmt.Sprint("k", k)
		before, after := first(key, ws), first(key, without)
		if before == removed {
			ownedByRemoved++
		}
		if before != after {
			moved++
			if before != removed {
				t.Fatalf("key %s moved from %s although that worker stayed", key, before)
			}
		}
	}
	if moved != ownedByRemoved {
		t.Errorf("moved %d keys, but the removed worker owned %d", moved, ownedByRemoved)
	}
	if ownedByRemoved == 0 {
		t.Error("degenerate: the removed worker owned nothing")
	}
}

func TestSpreadIsRoughlyEven(t *testing.T) {
	ws := workers(10)
	count := map[string]int{}
	const keys = 10000
	for k := range keys {
		count[first(fmt.Sprint("sha256:", k), ws)]++
	}
	for ep, n := range count {
		if n < keys/10*7/10 || n > keys/10*13/10 { // within 30% of the mean
			t.Errorf("%s owns %d of %d keys: uneven", ep, n, keys)
		}
	}
	if len(count) != 10 {
		t.Errorf("only %d of 10 workers ever preferred", len(count))
	}
}

func TestFullWorkersGoLastButStayInTheList(t *testing.T) {
	ws := workers(4)
	key := "sha256:abc"
	preferred := first(key, ws)
	for i := range ws {
		if ws[i].Endpoint == preferred {
			ws[i].InFlight = ws[i].MaxConcurrency // its last report: full
		}
	}
	order := Order(key, ws)
	if len(order) != 4 {
		t.Fatalf("a full worker must stay in the list (its report may be stale): %d", len(order))
	}
	if order[0].Endpoint == preferred || order[3].Endpoint != preferred {
		t.Errorf("full preferred worker should be tried last: %v", endpoints(order))
	}
	// Once it has room again, it is preferred again: affinity is not lost.
	for i := range ws {
		ws[i].InFlight = 0
	}
	if first(key, ws) != preferred {
		t.Error("affinity must return when the worker frees up")
	}
}

func TestEdgeCases(t *testing.T) {
	if got := Order("k", nil); len(got) != 0 {
		t.Errorf("nil: %v", got)
	}
	in := workers(3)
	snapshot := fmt.Sprint(in)
	Order("k", in)
	if fmt.Sprint(in) != snapshot {
		t.Error("input was modified")
	}
}
