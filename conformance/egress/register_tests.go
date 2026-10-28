package egressconf

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
)

// registrationGroup: how a service makes itself known.
func (h *harness) registrationGroup(t *testing.T) {
	rg := h.registration
	if rg.ID == "" {
		t.Fatal("the service never registered")
	}

	t.Run("it keeps trying", func(t *testing.T) {
		// The first two registrations were refused as "unavailable", as a
		// registry that is not up yet would: the service must not give up.
		h.reg.mu.Lock()
		attempts := h.reg.attempts
		h.reg.mu.Unlock()
		if attempts < 3 {
			t.Errorf("registered after %d attempts, but two were refused: it must retry", attempts)
		}
	})

	t.Run("what it declares", func(t *testing.T) {
		if rg.Info.GetImplementation().GetName() == "" {
			t.Error("the implementation has no name")
		}
		found := false
		for _, p := range rg.Info.GetImplementation().GetProtocols() {
			found = found || p == "edgerun.egress.v1"
		}
		if !found {
			t.Errorf("protocols %v do not include edgerun.egress.v1", rg.Info.GetImplementation().GetProtocols())
		}
		covers := rg.Info.GetCovers()
		if len(covers) == 0 {
			t.Fatal("a service that does not say what it carries is useless: at least one coverage entry")
		}
		if !sort.StringsAreSorted(covers) {
			t.Errorf("covers %v are not in canonical (sorted) order", covers)
		}
		seen := map[string]bool{}
		for _, c := range covers {
			if c == "" || c != strings.ToLower(c) || seen[c] {
				t.Errorf("covers %v: entry %q is empty, not lower-case, or repeated", covers, c)
			}
			seen[c] = true
		}
	})

	t.Run("it is where it says it is", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), settle)
		defer cancel()
		r, err := h.client.Describe(ctx, connect.NewRequest(&egressv1.DescribeRequest{}))
		if err != nil {
			t.Fatalf("Describe at the registered endpoint %s: %v", rg.Endpoint, err)
		}
		if got, want := fmt.Sprint(r.Msg.GetCovers()), fmt.Sprint(rg.Info.GetCovers()); got != want {
			t.Errorf("Describe says it carries %s, it registered as carrying %s", got, want)
		}
	})

	t.Run("it heartbeats, ready", func(t *testing.T) {
		h.reg.waitFor(t, "three heartbeats", settle, func() bool {
			n := 0
			for _, hb := range h.reg.heartbeats {
				if hb.ID == rg.ID {
					n++
				}
			}
			return n >= 3
		})
		for _, hb := range h.reg.heartbeatsOf(rg.ID) {
			if hb.State != egressv1.ServiceState_SERVICE_STATE_READY {
				t.Errorf("heartbeat state %v, want READY while serving", hb.State)
			}
		}
		beats := h.reg.heartbeatsOf(rg.ID)
		gap := beats[len(beats)-1].At.Sub(beats[0].At) / time.Duration(len(beats)-1)
		if gap > 5*h.env.HeartbeatInterval {
			t.Errorf("heartbeats every %v: the registry asked for %v", gap, h.env.HeartbeatInterval)
		}
	})

	t.Run("it presents the network token", func(t *testing.T) {
		h.reg.mu.Lock()
		failures := h.reg.authFailures
		h.reg.mu.Unlock()
		if failures != 0 {
			t.Errorf("%d registry calls were made without the network token", failures)
		}
	})
}

// reregistration: a registry that forgets a service tells it so, and it
// registers again.
func (h *harness) reregistration(t *testing.T) {
	first := h.reg.registrationFor(h.url)
	h.reg.forgetNext(first.ID)
	h.reg.waitFor(t, "the service to register again", settle, func() bool {
		cur := h.reg.byEndpoint[norm(h.url)]
		return cur.ID != "" && cur.ID != first.ID
	})
	second := h.reg.registrationFor(h.url)
	if fmt.Sprint(second.Info.GetCovers()) != fmt.Sprint(first.Info.GetCovers()) {
		t.Errorf("it declared %v the second time, %v the first", second.Info.GetCovers(), first.Info.GetCovers())
	}
	h.reg.waitFor(t, "heartbeats under the new id", settle, func() bool {
		n := 0
		for _, hb := range h.reg.heartbeats {
			if hb.ID == second.ID {
				n++
			}
		}
		return n >= 2
	})
	h.registration = second
}

// deregistration: a service that is asked to stop tells the registry, so that
// nothing more is routed to it.
func (h *harness) deregistration(t *testing.T) {
	if h.stop == nil {
		t.Skip("the suite cannot stop this service (see --stop-cmd), so it cannot see it deregister")
	}
	id := h.registration.ID
	h.stop()
	h.reg.waitFor(t, "the service to deregister", settle, func() bool {
		for _, d := range h.reg.deregistered {
			if d == id {
				return true
			}
		}
		return false
	})
}

// coverage: a service carries what it declared, and nothing else, whatever the
// caller allows.
func (h *harness) coverage(t *testing.T) {
	covers := h.registration.Info.GetCovers()
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
	covers := h.registration.Info.GetCovers()
	for _, host := range []string{h.origin.host(), "probe.covered.test"} {
		if !check.Covered(covers, host) {
			t.Fatalf("the network service declared it carries %v, which does not include %s: the suite needs it to carry its own web servers (%s) "+
				"and the names under covered.test, so configure it with --covers %s,*.covered.test (or \"*\"), "+
				"and to be allowed to connect to %s", covers, host, h.origin.host(), h.origin.host(), h.origin.host())
		}
	}
}
