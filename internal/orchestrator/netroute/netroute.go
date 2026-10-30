// Package netroute decides which network service carries each destination a
// function may reach. The orchestrator does it, once per invocation, from the
// services that are registered and alive; a worker is only told the answer.
package netroute

import (
	"hash/fnv"

	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/internal/egress/policy"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

// Resolve returns, for each entry of a function's allow list, the service that
// carries it. An entry no service carries is left out: the function has been
// given a destination nothing can reach, and a guest asking for it is told it
// is not allowed.
//
// A service carries an entry if its coverage names everything the entry can
// match. Of the services that do, the one whose coverage names the entry most
// precisely carries it (an exact host before a wildcard before an address
// range before a catch-all), so a service for one network does not lose its
// traffic to a catch-all in another. Among equally precise services the choice
// is spread by rendezvous hashing on the entry and the service, so it is stable
// while the set of services is, and moves only what it must when it changes.
func Resolve(allow []string, services []store.NetworkService) []*workerv1.EgressGrant {
	var out []*workerv1.EgressGrant
	for _, entry := range allow {
		rule, err := policy.ParseRule(entry)
		if err != nil {
			continue // the orchestrator stores only entries it can read
		}
		if svc, ok := carrier(entry, rule, services); ok {
			out = append(out, &workerv1.EgressGrant{Allow: entry, Service: svc.Endpoint})
		}
	}
	return out
}

func carrier(entry string, rule policy.Rule, services []store.NetworkService) (store.NetworkService, bool) {
	var best store.NetworkService
	var bestRank [2]int
	var bestScore uint64
	found := false
	for _, s := range services {
		cover, ok := s.Covers.BestFor(rule)
		if !ok {
			continue
		}
		rank, score := cover.Rank(), rendezvous(entry, s.Endpoint)
		switch c := policy.CompareRank(rank, bestRank); {
		case !found, c > 0, c == 0 && score > bestScore:
			best, bestRank, bestScore, found = s, rank, score, true
		}
	}
	return best, found
}

func rendezvous(entry, endpoint string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(entry))
	h.Write([]byte{0})
	h.Write([]byte(endpoint))
	return h.Sum64()
}
