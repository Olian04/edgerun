// Package router matches a request's host and path to a route.
//
// Among the routes that match both host and path, the most specific host wins
// (an exact host, then a "*.suffix" wildcard with the longer suffix first, then
// "*"), and among those the longest path prefix. Prefixes match on whole path
// segments, so "/api" matches "/api" and "/api/x" but not "/apiary". A request
// that matches a specific host but none of its path prefixes falls through to
// less specific hosts, so a "*" route works as a catch-all.
package router

import (
	"sort"
	"strings"

	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

// Table is an immutable, indexed set of routes. Build a new one on change.
type Table struct {
	exact    map[string][]*store.RouteEntry // host -> routes, longest prefix first
	wildcard []wildcard                     // longest suffix first
	any      []*store.RouteEntry            // "*" routes, longest prefix first
}

type wildcard struct {
	suffix string // ".example.com"
	routes []*store.RouteEntry
}

// New indexes entries. The entries must outlive the table.
func New(entries []store.RouteEntry) *Table {
	t := &Table{exact: map[string][]*store.RouteEntry{}}
	byWildcard := map[string][]*store.RouteEntry{}
	for i := range entries {
		e := &entries[i]
		switch {
		case e.HostPattern == "*":
			t.any = append(t.any, e)
		case strings.HasPrefix(e.HostPattern, "*."):
			s := e.HostPattern[1:] // ".example.com"
			byWildcard[s] = append(byWildcard[s], e)
		default:
			t.exact[e.HostPattern] = append(t.exact[e.HostPattern], e)
		}
	}
	sortByPrefix(t.any)
	for h := range t.exact {
		sortByPrefix(t.exact[h])
	}
	for s, rs := range byWildcard {
		sortByPrefix(rs)
		t.wildcard = append(t.wildcard, wildcard{suffix: s, routes: rs})
	}
	sort.Slice(t.wildcard, func(i, j int) bool { return len(t.wildcard[i].suffix) > len(t.wildcard[j].suffix) })
	return t
}

func sortByPrefix(rs []*store.RouteEntry) {
	sort.Slice(rs, func(i, j int) bool {
		if len(rs[i].PathPrefix) != len(rs[j].PathPrefix) {
			return len(rs[i].PathPrefix) > len(rs[j].PathPrefix)
		}
		return rs[i].RouteID < rs[j].RouteID // deterministic
	})
}

// Match returns the route for a request, or nil.
func (t *Table) Match(host, path string) *store.RouteEntry {
	host = NormalizeHost(host)
	if r := firstMatching(t.exact[host], path); r != nil {
		return r
	}
	for _, w := range t.wildcard {
		// "*.example.com" matches "a.example.com" but not "example.com".
		if len(host) > len(w.suffix) && strings.HasSuffix(host, w.suffix) {
			if r := firstMatching(w.routes, path); r != nil {
				return r
			}
		}
	}
	return firstMatching(t.any, path)
}

func firstMatching(rs []*store.RouteEntry, path string) *store.RouteEntry {
	for _, r := range rs {
		if PrefixMatches(r.PathPrefix, path) {
			return r
		}
	}
	return nil
}

// PrefixMatches reports whether path is under prefix on a segment boundary.
func PrefixMatches(prefix, path string) bool {
	if prefix == "/" {
		return strings.HasPrefix(path, "/")
	}
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	return len(path) == len(prefix) || path[len(prefix)] == '/'
}

// NormalizeHost lower-cases a Host header value and strips the port and any
// trailing dot, so "API.Example.com.:8080" matches "api.example.com".
func NormalizeHost(h string) string {
	h = strings.ToLower(h)
	if strings.HasPrefix(h, "[") { // IPv6 literal, with or without a port
		if end := strings.IndexByte(h, ']'); end >= 0 {
			return h[:end+1]
		}
		return h
	}
	if i := strings.LastIndexByte(h, ':'); i >= 0 {
		h = h[:i]
	}
	return strings.TrimSuffix(h, ".")
}
