package router

import (
	"fmt"
	"testing"

	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

func table(routes ...string) *Table {
	var es []store.RouteEntry
	for i, r := range routes {
		var host, prefix string
		fmt.Sscanf(r, "%s %s", &host, &prefix)
		es = append(es, store.RouteEntry{RouteID: fmt.Sprint("r", i), HostPattern: host, PathPrefix: prefix, FunctionName: r})
	}
	return New(es)
}

func match(t *Table, host, path string) string {
	if r := t.Match(host, path); r != nil {
		return r.FunctionName
	}
	return "-"
}

func TestSpecificityAndFallThrough(t *testing.T) {
	tb := table(
		"api.example.com /",
		"api.example.com /v1",
		"api.example.com /v1/admin",
		"*.example.com /",
		"*.example.com /static",
		"*.eu.example.com /",
		"* /",
		"* /health",
	)
	for _, tc := range []struct{ host, path, want string }{
		// exact host beats wildcard beats "*"; longest prefix within a host
		{"api.example.com", "/", "api.example.com /"},
		{"api.example.com", "/v1", "api.example.com /v1"},
		{"api.example.com", "/v1/users", "api.example.com /v1"},
		{"api.example.com", "/v1/admin/x", "api.example.com /v1/admin"},
		{"api.example.com", "/other", "api.example.com /"},
		// a wildcard host; the longer suffix is more specific
		{"www.example.com", "/static/a.css", "*.example.com /static"},
		{"www.example.com", "/page", "*.example.com /"},
		{"a.eu.example.com", "/page", "*.eu.example.com /"},
		// the wildcard does not match its own apex
		{"example.com", "/health", "* /health"},
		{"example.com", "/anything", "* /"},
		// catch-all
		{"unrelated.test", "/health/deep", "* /health"},
		{"unrelated.test", "/", "* /"},
	} {
		if got := match(tb, tc.host, tc.path); got != tc.want {
			t.Errorf("%s%s -> %q, want %q", tc.host, tc.path, got, tc.want)
		}
	}
}

func TestFallsThroughWhenASpecificHostHasNoMatchingPath(t *testing.T) {
	tb := table("a.com /api", "* /")
	if got := match(tb, "a.com", "/other"); got != "* /" {
		t.Errorf("got %q: a host-specific route that does not match the path must not shadow the catch-all", got)
	}
	if got := match(table("a.com /api"), "a.com", "/other"); got != "-" {
		t.Errorf("got %q, want no match", got)
	}
}

func TestPrefixesMatchWholeSegments(t *testing.T) {
	for _, tc := range []struct {
		prefix, path string
		want         bool
	}{
		{"/api", "/api", true}, {"/api", "/api/", true}, {"/api", "/api/x", true},
		{"/api", "/apiary", false}, {"/api", "/ap", false}, {"/api", "/", false},
		{"/", "/", true}, {"/", "/anything/at/all", true}, {"/", "", false},
		{"/a/b", "/a/b/c", true}, {"/a/b", "/a/bc", false}, {"/a/b", "/a", false},
	} {
		if got := PrefixMatches(tc.prefix, tc.path); got != tc.want {
			t.Errorf("PrefixMatches(%q, %q) = %v, want %v", tc.prefix, tc.path, got, tc.want)
		}
	}
}

func TestHostNormalisation(t *testing.T) {
	for in, want := range map[string]string{
		"API.Example.COM": "api.example.com", "a.com:8080": "a.com", "a.com.": "a.com", "A.com.:443": "a.com",
		"[::1]:80": "[::1]", "[::1]": "[::1]", "localhost": "localhost", "": "",
	} {
		if got := NormalizeHost(in); got != want {
			t.Errorf("NormalizeHost(%q) = %q, want %q", in, got, want)
		}
	}
	tb := table("api.example.com /")
	if match(tb, "API.EXAMPLE.COM:8443", "/x") != "api.example.com /" {
		t.Error("case and port must not affect matching")
	}
}

func TestEmptyTable(t *testing.T) {
	if New(nil).Match("a.com", "/") != nil {
		t.Error("empty table matched")
	}
}

// The index must agree with the plain definition: of all matching routes, the
// one with the most specific host and then the longest prefix.
func TestAgreesWithBruteForce(t *testing.T) {
	routes := []string{
		"a.com /", "a.com /x", "a.com /x/y", "b.com /", "*.a.com /", "*.a.com /x", "*.b.a.com /", "* /", "* /x/y/z",
	}
	tb := table(routes...)
	hosts := []string{"a.com", "b.com", "c.com", "w.a.com", "x.b.a.com", "b.a.com", "A.COM"}
	paths := []string{"/", "/x", "/x/", "/x/y", "/x/y/z", "/x/yy", "/q", "/x/y/z/w"}
	for _, h := range hosts {
		for _, p := range paths {
			if got, want := match(tb, h, p), brute(routes, NormalizeHost(h), p); got != want {
				t.Errorf("%s%s: index says %q, brute force says %q", h, p, got, want)
			}
		}
	}
}

func brute(routes []string, host, path string) string {
	best, bestRank := "-", [3]int{-1, 0, 0}
	for _, r := range routes {
		var hp, pp string
		fmt.Sscanf(r, "%s %s", &hp, &pp)
		var rank0, rank1 int
		switch {
		case hp == host:
			rank0 = 3
		case len(hp) > 2 && hp[:2] == "*." && len(host) > len(hp)-1 && host[len(host)-(len(hp)-1):] == hp[1:]:
			rank0, rank1 = 2, len(hp)
		case hp == "*":
			rank0 = 1
		default:
			continue
		}
		if !PrefixMatches(pp, path) {
			continue
		}
		rank := [3]int{rank0, rank1, len(pp)}
		if rank[0] > bestRank[0] || rank[0] == bestRank[0] && (rank[1] > bestRank[1] || rank[1] == bestRank[1] && rank[2] > bestRank[2]) {
			best, bestRank = r, rank
		}
	}
	return best
}
