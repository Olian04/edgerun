package orchsuite

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	membershipv1 "github.com/Olian04/edgerun/gen/edgerun/membership/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

// Membership judges the orchestrator as the host of MembershipService
// (membership.v1): workers and network services dial in, register, heartbeat and
// leave, and an operator lists who is in. It plays both kinds of member and the
// operator.
func Membership(t *testing.T, target Target) {
	h := start(t, target)
	t.Run("Authentication", h.membershipAuthentication)
	t.Run("Registration", h.membershipRegistration)
	t.Run("Lifecycle", h.membershipLifecycle)
	t.Run("Reregistration", h.membershipReregistration)
	t.Run("ForeignMembers", h.membershipForeign)
	t.Run("Heartbeats", h.membershipHeartbeats)
	t.Run("Deregistration", h.membershipDeregistration)
	t.Run("Listing", h.membershipListing)
	t.Run("WorkerLiveness", h.fleetLiveness)
	t.Run("NetworkServiceLiveness", h.networkLiveness)
}

const (
	kindWorker  = membershipv1.MemberKind_MEMBER_KIND_WORKER
	kindNetwork = membershipv1.MemberKind_MEMBER_KIND_NETWORK_SERVICE
	ready       = membershipv1.MemberState_MEMBER_STATE_READY
	draining    = membershipv1.MemberState_MEMBER_STATE_DRAINING
)

// --- helpers ---------------------------------------------------------------------

func workerInfo(abis ...string) *workerv1.WorkerInfo {
	return &workerv1.WorkerInfo{
		Implementation: &commonv1.Implementation{Name: "conformance-raw-worker", Version: "1"},
		Abis:           abis, MaxConcurrency: 3, Labels: map[string]string{"zone": "z1"},
	}
}

func serviceInfo(covers ...string) *egressv1.ServiceInfo {
	return &egressv1.ServiceInfo{Implementation: &commonv1.Implementation{Name: "conformance-raw-service"}, Covers: covers}
}

func workerReq(endpoint string, info *workerv1.WorkerInfo) *membershipv1.RegisterRequest {
	r := &membershipv1.RegisterRequest{Endpoint: endpoint}
	if info != nil {
		r.Info = &membershipv1.RegisterRequest_Worker{Worker: info}
	}
	return r
}

func serviceReq(endpoint string, info *egressv1.ServiceInfo) *membershipv1.RegisterRequest {
	r := &membershipv1.RegisterRequest{Endpoint: endpoint}
	if info != nil {
		r.Info = &membershipv1.RegisterRequest_NetworkService{NetworkService: info}
	}
	return r
}

// member is one caller of MembershipService, presenting one token.
type member struct {
	h     *harness
	token string
}

func (m member) register(r *membershipv1.RegisterRequest) (*membershipv1.RegisterResponse, error) {
	ctx, cancel := ctxT(settle)
	defer cancel()
	resp, err := m.h.membership(m.token).Register(ctx, connect.NewRequest(r))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (m member) heartbeat(id string, state membershipv1.MemberState, inFlight uint32) (*membershipv1.HeartbeatResponse, error) {
	ctx, cancel := ctxT(settle)
	defer cancel()
	resp, err := m.h.membership(m.token).Heartbeat(ctx, connect.NewRequest(&membershipv1.HeartbeatRequest{MemberId: id, State: state, InFlight: inFlight}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (m member) deregister(id string) error {
	ctx, cancel := ctxT(settle)
	defer cancel()
	_, err := m.h.membership(m.token).Deregister(ctx, connect.NewRequest(&membershipv1.DeregisterRequest{MemberId: id}))
	return err
}

func (h *harness) workerCaller() member  { return member{h, h.env.WorkerToken} }
func (h *harness) serviceCaller() member { return member{h, h.env.NetworkToken} }

// listMembers is what the operator sees, optionally of one kind.
func (h *harness) listMembers(t testing.TB, kind membershipv1.MemberKind) []*membershipv1.Member {
	t.Helper()
	ctx, cancel := ctxT(settle)
	defer cancel()
	r, err := h.mem.ListMembers(ctx, connect.NewRequest(&membershipv1.ListMembersRequest{Kind: kind}))
	if err != nil {
		t.Fatalf("ListMembers: %v", err)
	}
	return r.Msg.GetMembers()
}

// memberAt returns the listed member of a kind with this endpoint, or nil.
func (h *harness) memberAt(t testing.TB, kind membershipv1.MemberKind, endpoint string) *membershipv1.Member {
	t.Helper()
	for _, m := range h.listMembers(t, kind) {
		if m.GetEndpoint() == endpoint {
			return m
		}
	}
	return nil
}

// workerStatus and networkStatus return the listed worker or network service
// with this endpoint, or nil.
func (h *harness) workerStatus(t testing.TB, endpoint string) *membershipv1.Member {
	return h.memberAt(t, kindWorker, endpoint)
}

func (h *harness) networkStatus(t testing.TB, endpoint string) *membershipv1.Member {
	return h.memberAt(t, kindNetwork, endpoint)
}

func (h *harness) aliveCount(t testing.TB, kind membershipv1.MemberKind) int {
	n := 0
	for _, m := range h.listMembers(t, kind) {
		if m.GetAlive() {
			n++
		}
	}
	return n
}

func (h *harness) workerEndpoint(tag string) string {
	return fmt.Sprintf("http://127.0.0.1:1/%s-%s", tag, h.run)
}

func (h *harness) serviceEndpoint(tag string) string {
	return fmt.Sprintf("http://reg-%s-%s.conformance.test:8090", tag, h.run)
}

// --- authentication: the scope matrix --------------------------------------------

// membershipAuthentication: what a caller may do is decided per call by the
// scope of its token. A worker's token registers, heartbeats and deregisters
// workers and nothing else; a network service's the same for network services;
// an operator's lists; no scope can be a member and also list. A missing or
// unknown token is UNAUTHENTICATED, a valid token of a scope that may not make
// the call is PERMISSION_DENIED.
func (h *harness) membershipAuthentication(t *testing.T) {
	scopes := map[string]string{"worker": h.env.WorkerToken, "network": h.env.NetworkToken, "admin": h.env.AdminToken, "ingress": h.env.IngressToken}
	unknown := []string{h.env.ExecutionToken}
	const never = "00000000-0000-4000-8000-0000000000aa"

	t.Run("Register a worker: worker scope only", func(t *testing.T) {
		var made []string
		check.ScopeMatrix(t, scopes, []string{"worker"}, unknown, func(token string) error {
			r, err := member{h, token}.register(workerReq(h.workerEndpoint("auth-w"), workerInfo("a")))
			if err == nil {
				made = append(made, r.GetMemberId())
			}
			return err
		})
		for _, id := range made {
			h.workerCaller().deregister(id)
		}
		if m := h.workerStatus(t, h.workerEndpoint("auth-w")); m != nil {
			t.Errorf("after everything that was let in has left, %+v is still listed: a refused registration left it behind", m)
		}
	})
	t.Run("Register a network service: network scope only", func(t *testing.T) {
		var made []string
		check.ScopeMatrix(t, scopes, []string{"network"}, unknown, func(token string) error {
			r, err := member{h, token}.register(serviceReq(h.serviceEndpoint("auth-n"), serviceInfo("*")))
			if err == nil {
				made = append(made, r.GetMemberId())
			}
			return err
		})
		for _, id := range made {
			h.serviceCaller().deregister(id)
		}
		if m := h.networkStatus(t, h.serviceEndpoint("auth-n")); m != nil {
			t.Errorf("after everything that was let in has left, %+v is still listed: a refused registration left it behind", m)
		}
	})
	t.Run("Heartbeat: a member's scope", func(t *testing.T) {
		check.ScopeMatrix(t, scopes, []string{"worker", "network"}, unknown, func(token string) error {
			_, err := member{h, token}.heartbeat(never, ready, 0)
			return err
		})
	})
	t.Run("Deregister: a member's scope", func(t *testing.T) {
		check.ScopeMatrix(t, scopes, []string{"worker", "network"}, unknown, func(token string) error {
			return member{h, token}.deregister(never)
		})
	})
	t.Run("ListMembers: admin scope only", func(t *testing.T) {
		check.ScopeMatrix(t, scopes, []string{"admin"}, unknown, func(token string) error {
			ctx, cancel := ctxT(settle)
			defer cancel()
			_, err := h.membership(token).ListMembers(ctx, connect.NewRequest(&membershipv1.ListMembersRequest{}))
			return err
		})
	})
}

// --- registration: what is accepted and what is refused --------------------------

func (h *harness) membershipRegistration(t *testing.T) {
	t.Run("a worker needs an endpoint, an ABI and a concurrency", func(t *testing.T) {
		ep := h.workerEndpoint("invalid")
		zero := workerInfo("a")
		zero.MaxConcurrency = 0
		for name, c := range map[string]struct {
			endpoint string
			info     *workerv1.WorkerInfo
		}{
			"no endpoint":    {"", workerInfo("a")},
			"not a URL":      {"not a url", workerInfo("a")},
			"not http":       {"ftp://example.test/x", workerInfo("a")},
			"no host":        {"http:///x", workerInfo("a")},
			"no concurrency": {ep, zero},
			"no ABI":         {ep, workerInfo()},
		} {
			_, err := h.workerCaller().register(workerReq(c.endpoint, c.info))
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("%s: %v, want invalid_argument", name, err)
			}
		}
		if h.workerStatus(t, ep) != nil {
			t.Error("a refused registration must leave no worker behind")
		}
	})

	t.Run("a network service needs an endpoint and what it carries", func(t *testing.T) {
		ep := h.serviceEndpoint("invalid")
		var many []string
		for i := 0; i < 65; i++ {
			many = append(many, fmt.Sprintf("h%d.example.com", i))
		}
		for name, c := range map[string]struct {
			endpoint string
			covers   []string
		}{
			"no endpoint":           {"", []string{"*"}},
			"not a URL":             {"not a url", []string{"*"}},
			"not http":              {"ftp://example.test/x", []string{"*"}},
			"no host":               {"http:///x", []string{"*"}},
			"carries nothing":       {ep, nil},
			"an empty entry":        {ep, []string{""}},
			"a port":                {ep, []string{"example.com:443"}},
			"a URL":                 {ep, []string{"http://example.com"}},
			"a malformed wildcard":  {ep, []string{"*example.com"}},
			"a malformed range":     {ep, []string{"10.0.0.0/33"}},
			"a bad entry beside ok": {ep, []string{"*.example.com", "not an entry"}},
			"too many entries":      {ep, many},
		} {
			_, err := h.serviceCaller().register(serviceReq(c.endpoint, serviceInfo(c.covers...)))
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("%s: %v, want invalid_argument", name, err)
			}
		}
		if h.networkStatus(t, ep) != nil {
			t.Error("a refused registration must leave no service behind")
		}
	})

	t.Run("a registration says what the member is", func(t *testing.T) {
		// No info at all, whichever credential presents it.
		for who, m := range map[string]member{"a worker": h.workerCaller(), "a network service": h.serviceCaller()} {
			_, err := m.register(&membershipv1.RegisterRequest{Endpoint: h.workerEndpoint("noinfo")})
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("%s registering with no info: %v, want invalid_argument", who, err)
			}
		}
		if h.workerStatus(t, h.workerEndpoint("noinfo")) != nil {
			t.Error("a registration with no info left a member behind")
		}
		// An info that is present but empty is judged as its kind.
		_, err := h.workerCaller().register(&membershipv1.RegisterRequest{Endpoint: h.workerEndpoint("emptyinfo"), Info: &membershipv1.RegisterRequest_Worker{}})
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("a worker with no description: %v, want invalid_argument", err)
		}
	})

	t.Run("the kind of the info must agree with the credential", func(t *testing.T) {
		// A worker's token with a network service's info, and the other way round:
		// refused for the scope, and nothing registered.
		_, err := h.workerCaller().register(serviceReq(h.serviceEndpoint("cross"), serviceInfo("*")))
		check.WantCode(t, err, connect.CodePermissionDenied)
		_, err = h.serviceCaller().register(workerReq(h.workerEndpoint("cross"), workerInfo("a")))
		check.WantCode(t, err, connect.CodePermissionDenied)
		if h.networkStatus(t, h.serviceEndpoint("cross")) != nil || h.workerStatus(t, h.workerEndpoint("cross")) != nil {
			t.Error("a registration by the wrong scope left a member behind")
		}
	})
}

// --- the life of a member --------------------------------------------------------

// membershipLifecycle: register, be listed, heartbeat, drain, leave, for each kind.
func (h *harness) membershipLifecycle(t *testing.T) {
	t.Run("a worker", func(t *testing.T) {
		m, ep := h.workerCaller(), h.workerEndpoint("listed")
		r, err := m.register(workerReq(ep, workerInfo("abi-x", "abi-y")))
		if err != nil {
			t.Fatal(err)
		}
		if r.GetMemberId() == "" {
			t.Fatal("no member id")
		}
		if got := r.GetHeartbeatInterval().AsDuration(); got != h.env.HeartbeatInterval {
			t.Errorf("heartbeat interval = %v, want the configured %v", got, h.env.HeartbeatInterval)
		}
		l := h.workerStatus(t, ep)
		if l == nil {
			t.Fatal("a registered worker is not listed")
		}
		if l.GetId() != r.GetMemberId() || l.GetKind() != kindWorker || !l.GetAlive() || l.GetState() != ready {
			t.Errorf("listed as %+v", l)
		}
		if l.GetLastHeartbeatTime() == nil {
			t.Error("last_heartbeat_time is not set on a member that has just registered")
		}
		if got := l.GetWorker(); got.GetImplementation().GetName() != "conformance-raw-worker" ||
			len(got.GetAbis()) != 2 || got.GetMaxConcurrency() != 3 || got.GetLabels()["zone"] != "z1" {
			t.Errorf("the worker's info was not kept: %v", got)
		}
		if l.GetNetworkService() != nil {
			t.Errorf("a worker is listed with a network service's info: %v", l.GetNetworkService())
		}

		// Heartbeats carry state and load, which the operator sees.
		if hb, err := m.heartbeat(r.GetMemberId(), ready, 2); err != nil || hb.GetReregister() {
			t.Fatalf("Heartbeat = %v, %v", hb, err)
		}
		if l := h.workerStatus(t, ep); l.GetInFlight() != 2 || !l.GetAlive() {
			t.Errorf("after a heartbeat with in_flight 2: %+v", l)
		}
		if _, err := m.heartbeat(r.GetMemberId(), draining, 0); err != nil {
			t.Fatal(err)
		}
		if l := h.workerStatus(t, ep); l == nil || l.GetState() != draining {
			t.Errorf("a draining worker must be listed as draining, not removed: %+v", l)
		}

		if err := m.deregister(r.GetMemberId()); err != nil {
			t.Fatal(err)
		}
		if h.workerStatus(t, ep) != nil {
			t.Error("a deregistered worker is still listed")
		}
		if hb, err := m.heartbeat(r.GetMemberId(), ready, 0); err != nil || !hb.GetReregister() {
			t.Errorf("a heartbeat from a worker that left must ask it to register again: %v, %v", hb, err)
		}
	})

	t.Run("a network service", func(t *testing.T) {
		m, ep := h.serviceCaller(), h.serviceEndpoint("listed")
		r, err := m.register(serviceReq(ep, serviceInfo("B.Example.com", "*", "10.1.1.1/8", "b.example.com", "*.Corp.TEST")))
		if err != nil {
			t.Fatal(err)
		}
		if r.GetMemberId() == "" {
			t.Fatal("no member id")
		}
		if got := r.GetHeartbeatInterval().AsDuration(); got != h.env.HeartbeatInterval {
			t.Errorf("heartbeat interval = %v, want the configured %v", got, h.env.HeartbeatInterval)
		}
		l := h.networkStatus(t, ep)
		if l == nil {
			t.Fatal("a registered service is not listed")
		}
		if l.GetId() != r.GetMemberId() || l.GetKind() != kindNetwork || !l.GetAlive() || l.GetState() != ready {
			t.Errorf("listed as %+v", l)
		}
		if got := fmt.Sprint(l.GetNetworkService().GetCovers()); got != "[* *.corp.test 10.0.0.0/8 b.example.com]" {
			t.Errorf("covers = %s, want the canonical form: lower-cased, ranges as networks, deduplicated, sorted", got)
		}
		if l.GetNetworkService().GetImplementation().GetName() != "conformance-raw-service" {
			t.Errorf("implementation = %v", l.GetNetworkService().GetImplementation())
		}
		if l.GetWorker() != nil {
			t.Errorf("a network service is listed with a worker's info: %v", l.GetWorker())
		}

		if hb, err := m.heartbeat(r.GetMemberId(), ready, 0); err != nil || hb.GetReregister() {
			t.Fatalf("Heartbeat = %v, %v", hb, err)
		}
		if _, err := m.heartbeat(r.GetMemberId(), draining, 0); err != nil {
			t.Fatal(err)
		}
		if l := h.networkStatus(t, ep); l == nil || l.GetState() != draining {
			t.Errorf("a draining service must be listed as draining, not removed: %+v", l)
		}

		if err := m.deregister(r.GetMemberId()); err != nil {
			t.Fatal(err)
		}
		if h.networkStatus(t, ep) != nil {
			t.Error("a deregistered service is still listed")
		}
		if hb, err := m.heartbeat(r.GetMemberId(), ready, 0); err != nil || !hb.GetReregister() {
			t.Errorf("a heartbeat from a service that left must ask it to register again: %v, %v", hb, err)
		}
	})

	t.Run("an endpoint is one member", func(t *testing.T) {
		w, ep := h.workerCaller(), h.workerEndpoint("again")
		first, err := w.register(workerReq(ep, workerInfo("abi-x")))
		if err != nil {
			t.Fatal(err)
		}
		second, err := w.register(workerReq(ep, workerInfo("abi-x")))
		if err != nil {
			t.Fatal(err)
		}
		if first.GetMemberId() == second.GetMemberId() {
			t.Error("registering again must issue a new id")
		}
		n := 0
		for _, l := range h.listMembers(t, kindWorker) {
			if l.GetEndpoint() == ep {
				n++
				if l.GetId() != second.GetMemberId() {
					t.Error("the older registration must be replaced")
				}
			}
		}
		if n != 1 {
			t.Errorf("%d workers share one endpoint, want 1", n)
		}
		if hb, _ := w.heartbeat(first.GetMemberId(), ready, 0); !hb.GetReregister() {
			t.Error("the replaced registration's id must be told to register again")
		}
		w.deregister(second.GetMemberId())

		s, sep := h.serviceCaller(), h.serviceEndpoint("again")
		sfirst, err := s.register(serviceReq(sep, serviceInfo("*")))
		if err != nil {
			t.Fatal(err)
		}
		ssecond, err := s.register(serviceReq(sep, serviceInfo("*.example.com")))
		if err != nil {
			t.Fatal(err)
		}
		if sfirst.GetMemberId() == ssecond.GetMemberId() {
			t.Error("registering again must issue a new id")
		}
		n = 0
		for _, l := range h.listMembers(t, kindNetwork) {
			if l.GetEndpoint() == sep {
				n++
				if l.GetId() != ssecond.GetMemberId() || fmt.Sprint(l.GetNetworkService().GetCovers()) != "[*.example.com]" {
					t.Errorf("the older registration must be replaced, with what the newer declared: %+v", l)
				}
			}
		}
		if n != 1 {
			t.Errorf("%d services share one endpoint, want 1", n)
		}
		if hb, _ := s.heartbeat(sfirst.GetMemberId(), ready, 0); !hb.GetReregister() {
			t.Error("the replaced registration's id must be told to register again")
		}
		s.deregister(ssecond.GetMemberId())
	})
}

// membershipReregistration: a member the orchestrator does not know is told to
// register again, whoever it is and whatever it calls itself.
func (h *harness) membershipReregistration(t *testing.T) {
	for name, m := range map[string]member{"worker": h.workerCaller(), "network service": h.serviceCaller()} {
		t.Run("unknown "+name+"s are told to register again", func(t *testing.T) {
			for _, id := range []string{unknownID, "not-an-id", ""} {
				hb, err := m.heartbeat(id, ready, 0)
				if err != nil || !hb.GetReregister() {
					t.Errorf("Heartbeat(%q) = %v, %v, want reregister", id, hb, err)
				}
			}
		})
	}
	t.Run("a known member is not", func(t *testing.T) {
		r, err := h.workerCaller().register(workerReq(h.workerEndpoint("known"), workerInfo("a")))
		if err != nil {
			t.Fatal(err)
		}
		defer h.workerCaller().deregister(r.GetMemberId())
		for _, state := range []membershipv1.MemberState{ready, draining, ready} {
			if hb, err := h.workerCaller().heartbeat(r.GetMemberId(), state, 0); err != nil || hb.GetReregister() {
				t.Errorf("Heartbeat(%v) of a registered worker = %v, %v, want no reregister", state, hb, err)
			}
		}
	})
}

// membershipForeign: a member is only ever known to the scope that registered it.
// A worker's credential cannot heartbeat, drain or remove a network service, nor
// the other way round: the id is simply unknown to it.
func (h *harness) membershipForeign(t *testing.T) {
	w, s := h.workerCaller(), h.serviceCaller()
	wr, err := w.register(workerReq(h.workerEndpoint("foreign"), workerInfo("a")))
	if err != nil {
		t.Fatal(err)
	}
	defer w.deregister(wr.GetMemberId())
	sr, err := s.register(serviceReq(h.serviceEndpoint("foreign"), serviceInfo("*")))
	if err != nil {
		t.Fatal(err)
	}
	defer s.deregister(sr.GetMemberId())

	if hb, err := s.heartbeat(wr.GetMemberId(), ready, 0); err != nil || !hb.GetReregister() {
		t.Errorf("a network service's heartbeat for a worker's id = %v, %v, want reregister: that id is not one of its kind", hb, err)
	}
	if hb, err := w.heartbeat(sr.GetMemberId(), ready, 0); err != nil || !hb.GetReregister() {
		t.Errorf("a worker's heartbeat for a network service's id = %v, %v, want reregister: that id is not one of its kind", hb, err)
	}
	// Removing what is not yours succeeds (it is idempotent) and removes nothing.
	if err := s.deregister(wr.GetMemberId()); err != nil {
		t.Errorf("Deregister of a foreign id = %v, want success", err)
	}
	if err := w.deregister(sr.GetMemberId()); err != nil {
		t.Errorf("Deregister of a foreign id = %v, want success", err)
	}
	if h.workerStatus(t, h.workerEndpoint("foreign")) == nil {
		t.Error("a network service's credential removed a worker")
	}
	if h.networkStatus(t, h.serviceEndpoint("foreign")) == nil {
		t.Error("a worker's credential removed a network service")
	}
	// And the real owner still heartbeats it without being told to start over.
	if hb, err := w.heartbeat(wr.GetMemberId(), ready, 0); err != nil || hb.GetReregister() {
		t.Errorf("the worker's own heartbeat = %v, %v", hb, err)
	}
}

// membershipHeartbeats: states are READY or DRAINING and nothing else, and load
// is only reported by workers.
func (h *harness) membershipHeartbeats(t *testing.T) {
	for name, c := range map[string]struct {
		m    member
		kind membershipv1.MemberKind
		req  *membershipv1.RegisterRequest
	}{
		"worker":          {h.workerCaller(), kindWorker, workerReq(h.workerEndpoint("states"), workerInfo("a"))},
		"network service": {h.serviceCaller(), kindNetwork, serviceReq(h.serviceEndpoint("states"), serviceInfo("*"))},
	} {
		t.Run(name, func(t *testing.T) {
			r, err := c.m.register(c.req)
			if err != nil {
				t.Fatal(err)
			}
			defer c.m.deregister(r.GetMemberId())
			for _, bad := range []membershipv1.MemberState{membershipv1.MemberState_MEMBER_STATE_UNSPECIFIED, membershipv1.MemberState(99)} {
				_, err := c.m.heartbeat(r.GetMemberId(), bad, 0)
				if connect.CodeOf(err) != connect.CodeInvalidArgument {
					t.Errorf("a heartbeat with state %v: %v, want invalid_argument", bad, err)
				}
			}
			// A refused heartbeat changed nothing.
			if _, err := c.m.heartbeat(r.GetMemberId(), ready, 0); err != nil {
				t.Errorf("a valid heartbeat after refused ones: %v", err)
			}
			// The state is the last one reported: draining and back again.
			if _, err := c.m.heartbeat(r.GetMemberId(), draining, 0); err != nil {
				t.Fatal(err)
			}
			if l := h.memberAt(t, c.kind, c.req.GetEndpoint()); l == nil || l.GetState() != draining {
				t.Errorf("after a DRAINING heartbeat: %+v", l)
			}
			if _, err := c.m.heartbeat(r.GetMemberId(), ready, 0); err != nil {
				t.Fatal(err)
			}
			if l := h.memberAt(t, c.kind, c.req.GetEndpoint()); l == nil || l.GetState() != ready {
				t.Errorf("after a READY heartbeat: %+v", l)
			}
		})
	}
}

// membershipDeregistration: leaving is idempotent.
func (h *harness) membershipDeregistration(t *testing.T) {
	for name, m := range map[string]member{"worker": h.workerCaller(), "network service": h.serviceCaller()} {
		t.Run(name, func(t *testing.T) {
			for _, id := range []string{unknownID, "not-an-id", ""} {
				if err := m.deregister(id); err != nil {
					t.Errorf("Deregister(%q) of a member that never was = %v, want success", id, err)
				}
			}
		})
	}
	t.Run("twice", func(t *testing.T) {
		m := h.workerCaller()
		r, err := m.register(workerReq(h.workerEndpoint("twice"), workerInfo("a")))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if err := m.deregister(r.GetMemberId()); err != nil {
				t.Errorf("Deregister #%d = %v, want success", i+1, err)
			}
		}
		if h.workerStatus(t, h.workerEndpoint("twice")) != nil {
			t.Error("a deregistered worker is still listed")
		}
		s := h.serviceCaller()
		sr, err := s.register(serviceReq(h.serviceEndpoint("twice"), serviceInfo("*")))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if err := s.deregister(sr.GetMemberId()); err != nil {
				t.Errorf("Deregister #%d = %v, want success", i+1, err)
			}
		}
		if h.networkStatus(t, h.serviceEndpoint("twice")) != nil {
			t.Error("a deregistered service is still listed")
		}
	})
}

// membershipListing: ListMembers orders by kind (workers, then network services)
// and then by endpoint, and can be limited to one kind.
func (h *harness) membershipListing(t *testing.T) {
	tag := "ord"
	// Registered out of order, so that order of registration cannot be what is seen.
	endpoints := map[membershipv1.MemberKind][]string{
		kindWorker:  {h.workerEndpoint(tag + "-c"), h.workerEndpoint(tag + "-a"), h.workerEndpoint(tag + "-b")},
		kindNetwork: {h.serviceEndpoint(tag + "-z"), h.serviceEndpoint(tag + "-m"), h.serviceEndpoint(tag + "-q")},
	}
	// Interleave the kinds as well.
	var ids []func()
	for i := 0; i < 3; i++ {
		wr, err := h.workerCaller().register(workerReq(endpoints[kindWorker][i], workerInfo("a")))
		if err != nil {
			t.Fatal(err)
		}
		sr, err := h.serviceCaller().register(serviceReq(endpoints[kindNetwork][i], serviceInfo("*")))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, func() { h.workerCaller().deregister(wr.GetMemberId()); h.serviceCaller().deregister(sr.GetMemberId()) })
	}
	defer func() {
		for _, f := range ids {
			f()
		}
	}()
	mine := func(ms []*membershipv1.Member) (out []string) {
		for _, m := range ms {
			if strings.Contains(m.GetEndpoint(), "-"+tag+"-") || strings.Contains(m.GetEndpoint(), "/"+tag+"-") || strings.Contains(m.GetEndpoint(), "reg-"+tag+"-") {
				out = append(out, fmt.Sprintf("%d:%s", m.GetKind(), m.GetEndpoint()))
			}
		}
		return out
	}
	sorted := func(kinds ...membershipv1.MemberKind) (out []string) {
		for _, k := range kinds {
			eps := append([]string(nil), endpoints[k]...)
			sort.Strings(eps)
			for _, e := range eps {
				out = append(out, fmt.Sprintf("%d:%s", k, e))
			}
		}
		return out
	}

	t.Run("by kind, then endpoint", func(t *testing.T) {
		got, want := mine(h.listMembers(t, membershipv1.MemberKind_MEMBER_KIND_UNSPECIFIED)), sorted(kindWorker, kindNetwork)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("ListMembers order:\n got  %v\n want %v (workers before network services, each by endpoint)", got, want)
		}
	})
	t.Run("only workers", func(t *testing.T) {
		ms := h.listMembers(t, kindWorker)
		for _, m := range ms {
			if m.GetKind() != kindWorker {
				t.Errorf("a member of kind %v in a list of workers: %+v", m.GetKind(), m)
			}
		}
		if got, want := mine(ms), sorted(kindWorker); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("workers = %v, want %v", got, want)
		}
	})
	t.Run("only network services", func(t *testing.T) {
		ms := h.listMembers(t, kindNetwork)
		for _, m := range ms {
			if m.GetKind() != kindNetwork {
				t.Errorf("a member of kind %v in a list of network services: %+v", m.GetKind(), m)
			}
		}
		if got, want := mine(ms), sorted(kindNetwork); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("network services = %v, want %v", got, want)
		}
	})
	t.Run("every member says what it is", func(t *testing.T) {
		for _, m := range h.listMembers(t, membershipv1.MemberKind_MEMBER_KIND_UNSPECIFIED) {
			switch m.GetKind() {
			case kindWorker:
				if m.GetWorker() == nil || m.GetNetworkService() != nil {
					t.Errorf("worker %s listed with info %v / %v", m.GetEndpoint(), m.GetWorker(), m.GetNetworkService())
				}
			case kindNetwork:
				if m.GetNetworkService() == nil || m.GetWorker() != nil {
					t.Errorf("network service %s listed with info %v / %v", m.GetEndpoint(), m.GetWorker(), m.GetNetworkService())
				}
			default:
				t.Errorf("%s is listed with kind %v", m.GetEndpoint(), m.GetKind())
			}
			if m.GetId() == "" || m.GetEndpoint() == "" || m.GetState() == membershipv1.MemberState_MEMBER_STATE_UNSPECIFIED {
				t.Errorf("incomplete member %+v", m)
			}
		}
	})
}

// fleetLiveness: a worker that stops heartbeating, or says it is draining,
// receives no new work, and one that comes back receives it again. (The
// consequence for Invoke is judged here because liveness is what Membership is
// for; the scheduling rules themselves are Execution's.)
func (h *harness) fleetLiveness(t *testing.T) {
	abi := h.abi()
	w := h.worker(t, []string{abi}, respond("alive"))
	f := h.deploy(t, abi)
	h.served(t, f.host)
	if st := h.workerStatus(t, w.srv.URL); st == nil || !st.GetAlive() {
		t.Fatalf("a heartbeating worker is not alive: %+v", st)
	}

	t.Run("silence", func(t *testing.T) {
		w.pause(true)
		check.Eventually(t, "a silent worker to be reported not alive", settle, func() bool {
			st := h.workerStatus(t, w.srv.URL)
			return st != nil && !st.GetAlive()
		})
		check.Eventually(t, "a silent worker to stop receiving work", settle, func() bool {
			return h.invoke(t, call{host: f.host}).err != nil
		})
		r := h.invoke(t, call{host: f.host})
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY)

		w.pause(false)
		check.Eventually(t, "the worker to be alive again", settle, func() bool {
			st := h.workerStatus(t, w.srv.URL)
			return st != nil && st.GetAlive()
		})
		h.served(t, f.host)
	})

	t.Run("draining", func(t *testing.T) {
		w.setState(draining)
		check.Eventually(t, "a draining worker to stop receiving work", settle, func() bool {
			return h.invoke(t, call{host: f.host}).err != nil
		})
		check.WantReason(t, h.invoke(t, call{host: f.host}).err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY)
		if st := h.workerStatus(t, w.srv.URL); st == nil || st.GetState() != draining {
			t.Errorf("a draining worker stays listed, as draining: %+v", st)
		}
		w.setState(ready)
		h.served(t, f.host)
	})

	t.Run("leaving", func(t *testing.T) {
		w.stop()
		if h.workerStatus(t, w.srv.URL) != nil {
			t.Error("a deregistered worker is still listed")
		}
		check.Eventually(t, "a departed worker to stop receiving work", settle, func() bool {
			return h.invoke(t, call{host: f.host}).err != nil
		})
	})
}

// networkLiveness: a service that stops heartbeating is not alive.
func (h *harness) networkLiveness(t *testing.T) {
	s := h.netService(t, "*.liveness.test")
	if n := h.networkStatus(t, s.endpoint); n == nil || !n.GetAlive() {
		t.Fatalf("a heartbeating service is not alive: %+v", n)
	}
	s.pause(true)
	check.Eventually(t, "a silent service to be reported not alive", settle, func() bool {
		n := h.networkStatus(t, s.endpoint)
		return n != nil && !n.GetAlive()
	})
	s.pause(false)
	check.Eventually(t, "the service to be alive again", settle, func() bool {
		n := h.networkStatus(t, s.endpoint)
		return n != nil && n.GetAlive()
	})
	s.setState(draining)
	check.Eventually(t, "the service to be listed as draining", settle, func() bool {
		n := h.networkStatus(t, s.endpoint)
		return n != nil && n.GetState() == draining
	})
}
