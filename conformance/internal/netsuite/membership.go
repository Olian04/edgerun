package netsuite

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	membershipv1 "github.com/Olian04/edgerun/gen/edgerun/membership/v1"
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
		h.reg.Lock()
		attempts := h.reg.Attempts
		h.reg.Unlock()
		if attempts < 3 {
			t.Errorf("registered after %d attempts, but two were refused: it must retry", attempts)
		}
	})

	t.Run("what it declares", func(t *testing.T) {
		if rg.Service.GetImplementation().GetName() == "" {
			t.Error("the implementation has no name")
		}
		found := false
		for _, p := range rg.Service.GetImplementation().GetProtocols() {
			found = found || p == "edgerun.egress.v1"
		}
		if !found {
			t.Errorf("protocols %v do not include edgerun.egress.v1", rg.Service.GetImplementation().GetProtocols())
		}
		covers := rg.Service.GetCovers()
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
		if got, want := fmt.Sprint(r.Msg.GetCovers()), fmt.Sprint(rg.Service.GetCovers()); got != want {
			t.Errorf("Describe says it carries %s, it registered as carrying %s", got, want)
		}
	})

	t.Run("it heartbeats, ready", func(t *testing.T) {
		h.reg.WaitFor(t, "three heartbeats", settle, func() bool {
			n := 0
			for _, hb := range h.reg.Beats {
				if hb.ID == rg.ID {
					n++
				}
			}
			return n >= 3
		})
		for _, hb := range beatsOf(h.reg, rg.ID) {
			if hb.State != membershipv1.MemberState_MEMBER_STATE_READY {
				t.Errorf("heartbeat state %v, want READY while serving", hb.State)
			}
		}
		beats := beatsOf(h.reg, rg.ID)
		gap := beats[len(beats)-1].At.Sub(beats[0].At) / time.Duration(len(beats)-1)
		if gap > 5*h.env.HeartbeatInterval {
			t.Errorf("heartbeats every %v: the registry asked for %v", gap, h.env.HeartbeatInterval)
		}
	})

	t.Run("it presents the network token", func(t *testing.T) {
		h.reg.Lock()
		failures := h.reg.AuthFailures
		h.reg.Unlock()
		if failures != 0 {
			t.Errorf("%d registry calls were made without the network token", failures)
		}
	})
}

// reregistration: a registry that forgets a service tells it so, and it
// registers again.
func (h *harness) reregistration(t *testing.T) {
	first := registrationFor(h.reg, h.url)
	h.reg.ForgetNext(first.ID)
	h.reg.WaitFor(t, "the service to register again", settle, func() bool {
		cur, ok := h.reg.MemberAt(h.url, kindNetwork)
		return ok && cur.ID != first.ID
	})
	second := registrationFor(h.reg, h.url)
	if fmt.Sprint(second.Service.GetCovers()) != fmt.Sprint(first.Service.GetCovers()) {
		t.Errorf("it declared %v the second time, %v the first", second.Service.GetCovers(), first.Service.GetCovers())
	}
	h.reg.WaitFor(t, "heartbeats under the new id", settle, func() bool {
		n := 0
		for _, hb := range h.reg.Beats {
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
	h.reg.WaitFor(t, "the service to deregister", settle, func() bool {
		return h.reg.Left(id)
	})
	// Before leaving it told the orchestrator it was draining, so that nothing
	// new was sent its way.
	draining := false
	for _, hb := range beatsOf(h.reg, id) {
		draining = draining || hb.State == membershipv1.MemberState_MEMBER_STATE_DRAINING
	}
	if !draining {
		t.Error("the service left without ever reporting DRAINING: work could still have been routed to it while it was stopping")
	}
}
