package netroute

import (
	"fmt"
	"testing"

	"github.com/Olian04/edgerun/internal/egress/policy"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

func svc(endpoint string, covers ...string) store.NetworkService {
	cs, err := policy.ParseCovers(covers)
	if err != nil {
		panic(err)
	}
	return store.NetworkService{ID: endpoint, Endpoint: endpoint, Covers: cs}
}

func grants(allow []string, services ...store.NetworkService) map[string]string {
	out := map[string]string{}
	for _, g := range Resolve(allow, services) {
		out[g.Allow] = g.Service
	}
	return out
}

func TestEachDestinationGoesToTheServiceThatCarriesIt(t *testing.T) {
	private := svc("http://private", "10.0.0.0/8", "*.corp.internal")
	public := svc("http://public", "*")
	got := grants([]string{"api.example.com", "10.1.2.3:8080", "db.corp.internal:5432", "*.cdn.example.com"}, private, public)
	want := "map[*.cdn.example.com:http://public 10.1.2.3:8080:http://private api.example.com:http://public db.corp.internal:5432:http://private]"
	if fmt.Sprint(got) != want {
		t.Errorf("got %v, want %s", got, want)
	}
}

func TestTheMorePreciseServiceWins(t *testing.T) {
	catchAll := svc("http://all", "*")
	wild := svc("http://wild", "*.example.com")
	exact := svc("http://exact", "api.example.com")
	for _, c := range []struct {
		entry, want string
		services    []store.NetworkService
	}{
		{"api.example.com", "http://exact", []store.NetworkService{catchAll, wild, exact}},
		{"www.example.com", "http://wild", []store.NetworkService{catchAll, wild, exact}},
		{"elsewhere.test", "http://all", []store.NetworkService{catchAll, wild, exact}},
		{"api.example.com", "http://wild", []store.NetworkService{catchAll, wild}},
	} {
		if got := grants([]string{c.entry}, c.services...)[c.entry]; got != c.want {
			t.Errorf("%s: carried by %q, want %q", c.entry, got, c.want)
		}
	}
}

func TestADestinationNobodyCarriesIsLeftOut(t *testing.T) {
	if got := grants([]string{"x.test", "y.example.com"}, svc("http://a", "*.example.com")); fmt.Sprint(got) != "map[y.example.com:http://a]" {
		t.Errorf("got %v", got)
	}
	if got := Resolve([]string{"x.test"}, nil); len(got) != 0 {
		t.Errorf("with no services nothing can be reached: %v", got)
	}
	if got := Resolve(nil, []store.NetworkService{svc("http://a", "*")}); len(got) != 0 {
		t.Errorf("a function with no allow list gets no grants: %v", got)
	}
	// A service that carries only part of a wildcard does not carry it.
	if got := grants([]string{"*.example.com"}, svc("http://a", "*.cdn.example.com")); len(got) != 0 {
		t.Errorf("partial coverage is no coverage: %v", got)
	}
}

func TestEqualServicesShareTheLoadStably(t *testing.T) {
	a, b, c := svc("http://a", "*"), svc("http://b", "*"), svc("http://c", "*")
	var entries []string
	for i := 0; i < 200; i++ {
		entries = append(entries, fmt.Sprintf("h%d.example.com", i))
	}
	all := grants(entries, a, b, c)
	count := map[string]int{}
	for _, s := range all {
		count[s]++
	}
	for _, s := range []string{"http://a", "http://b", "http://c"} {
		if count[s] < 30 {
			t.Errorf("%s carries %d of 200: the load is not spread: %v", s, count[s], count)
		}
	}
	again := grants(entries, c, a, b) // order of registration does not matter
	if fmt.Sprint(again) != fmt.Sprint(all) {
		t.Error("the choice must not depend on the order services are listed in")
	}
	// Losing one service moves only what it carried.
	without := grants(entries, a, b)
	for e, s := range all {
		if s != "http://c" && without[e] != s {
			t.Errorf("%s moved from %s to %s though its service is still there", e, s, without[e])
		}
	}
}
