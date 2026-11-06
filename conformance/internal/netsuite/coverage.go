package netsuite

import (
	"testing"
	"time"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
)

// coverage: a service carries what it declared, and nothing else, whatever the
// caller allows.
func (h *harness) coverage(t *testing.T) {
	covers := h.registration.Service.GetCovers()
	do := func(host string) fetched {
		return h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: "http://" + host + "/", Allow: []string{host}}, 15*time.Second)
	}
	// Names that do not resolve: where the service carries one, the request gets
	// as far as failing to resolve it, which is not a refusal; where it does not,
	// it is refused before anything is looked up.
	for _, host := range []string{
		"probe.covered.test", "a.b.covered.test", "covered.test", "probe.outside.test", "outside.test",
		"probe.corp.internal", "api.example.com", "elsewhere.invalid",
	} {
		t.Run(host, func(t *testing.T) {
			r := do(host)
			reason, _ := check.ReasonOf(r.err)
			denied := reason == commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED
			if want := !check.Covered(covers, host); denied != want {
				t.Errorf("declared as carrying %v: a request to %s was denied = %v, want %v (error: %v)", covers, host, denied, want, r.err)
			}
		})
	}
}

// requireOriginCovered stops the run, with the reason, if the service does not
// carry the suite's web servers: every later group needs them.
func (h *harness) requireOriginCovered(t *testing.T) {
	covers := h.registration.Service.GetCovers()
	for _, host := range []string{h.origin.host(), "probe.covered.test"} {
		if !check.Covered(covers, host) {
			t.Fatalf("the network service declared it carries %v, which does not include %s: the suite needs it to carry its own web servers (%s) "+
				"and the names under covered.test, so configure it with --covers %s,*.covered.test (or \"*\"), "+
				"and to be allowed to connect to %s", covers, host, h.origin.host(), h.origin.host(), h.origin.host())
		}
	}
}

// requireOriginReachable asks the service for the suite's own web server and
// stops the suite, saying why, if it cannot be had. Without it a service that
// cannot reach the suite (a tunnel, a proxy, another network) makes every case
// run into its deadline, and the suite looks hung for minutes.
func (h *harness) requireOriginReachable(t *testing.T) {
	r := h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: h.origin.url("/echo"), Allow: []string{h.origin.entry()}}, 10*time.Second)
	if r.err != nil || r.head == nil || r.head.GetStatus() != 200 {
		t.Fatalf("the network service cannot reach the suite's web server at %s (%v): every case that fetches from it would only time out. "+
			"The suite's servers must be reachable from where the service makes its requests: see --listen and --advertise "+
			"(for a service behind a tunnel or a proxy, the address its far end can reach).", h.origin.addr(), r.err)
	}
}
