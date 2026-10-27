// Package sched orders workers for a module by rendezvous (highest random
// weight) hashing.
//
// The same module always prefers the same worker, so its compiled code stays
// warm, and every orchestrator instance computes the same order from the same
// inputs with no coordination between them. When a worker joins or leaves, only
// the modules that preferred it move: nothing else is reshuffled.
//
// Load is a hint, not a lock. Workers that last reported a free slot come
// first, but full ones stay in the list: their report may be stale, and the
// worker itself is the authority (it answers NO_CAPACITY, never queues).
package sched

import (
	"hash/fnv"
	"sort"

	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

// Order returns candidates in the order they should be tried for key (usually
// the module digest). The input is not modified.
func Order(key string, candidates []store.Worker) []store.Worker {
	type scored struct {
		w     store.Worker
		score uint64
		free  bool
	}
	s := make([]scored, len(candidates))
	for i, w := range candidates {
		s[i] = scored{w: w, score: score(key, w.Endpoint), free: w.InFlight < w.MaxConcurrency}
	}
	sort.SliceStable(s, func(i, j int) bool {
		if s[i].free != s[j].free {
			return s[i].free
		}
		if s[i].score != s[j].score {
			return s[i].score > s[j].score
		}
		return s[i].w.Endpoint < s[j].w.Endpoint
	})
	out := make([]store.Worker, len(s))
	for i := range s {
		out[i] = s[i].w
	}
	return out
}

// score is stable across processes and releases: it is the contract that lets
// separate orchestrator instances agree. The endpoint, not the worker id, is
// hashed, because the id changes every time a worker restarts and the endpoint
// does not, which keeps a module's warm worker across restarts.
func score(key, endpoint string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	h.Write([]byte{0})
	h.Write([]byte(endpoint))
	// FNV is weak in its low bits for similar inputs; finish with a mix.
	x := h.Sum64()
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
