// Package netsuite holds the conformance suites that need a running network
// service: Egress (the service as the host of egress.v1 EgressService) and the
// service's duties as a client of Membership. They share one harness: a service
// started by the implementation under test, pointed at a scripted orchestrator
// (fakeorch) it registers with, plus web servers of the suite's own for it to
// reach. The public packages conformance/<service> are the API; this package is
// where their tests live.
//
// The suites are black-box. They play the orchestrator (the Membership host a
// service registers with) and the worker (the caller of Fetch) through the
// generated protocol code alone. A service that passes declares what it carries,
// carries only that, enforces the same allow-list grammar and the same refusals as
// any other that passes (docs/protocol.md, "Network access"), so a deployment can
// have services inside a private network, on the public internet and behind a
// proxy, from any implementation, and workers and functions cannot tell.
package netsuite

import (
	"context"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	"github.com/Olian04/edgerun/conformance/internal/fakeorch"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/egress/v1/egressv1connect"
	membershipv1 "github.com/Olian04/edgerun/gen/edgerun/membership/v1"
)

const settle = 20 * time.Second

// Env tells the service under test how to be configured. The suite's defaults
// fill any field left empty in Target.Env.
type Env struct {
	// EgressToken is the bearer token workers present to the service (scope
	// "egress"): the suite presents it when it plays the worker.
	EgressToken string
	// NetworkToken is the token the service must present to the orchestrator
	// when it registers: the suite's registry requires it.
	NetworkToken string
	// OrchestratorURL is where the service must register: the suite's registry.
	OrchestratorURL string
	// HeartbeatInterval is what the registry dictates to the service.
	HeartbeatInterval time.Duration
	// Covers is what the service must be configured to carry. The suite adapts
	// to what a service declares, but needs it to carry the suite's own web
	// servers, and for the second service, everything ("*").
	Covers []string
	// AllowNetworks are address ranges (CIDR) the service must be configured to
	// connect to although it refuses them by default: where the suite's web
	// servers are. Empty for the service in its default configuration.
	AllowNetworks []string
}

func (e Env) withDefaults() Env {
	if e.EgressToken == "" {
		e.EgressToken = "conformance-egress-token"
	}
	if e.NetworkToken == "" {
		e.NetworkToken = "conformance-network-token"
	}
	if e.HeartbeatInterval == 0 {
		e.HeartbeatInterval = 200 * time.Millisecond
	}
	return e
}

// Instance is a running network service.
type Instance struct {
	// URL is the base URL workers reach the service at. Empty means there is no
	// such instance: the groups that need it are skipped.
	URL string
	// Stop asks the service to shut down as SIGTERM does (finish what it has,
	// deregister, exit) and returns once it has, or fails the test if it has
	// not within a bounded time. Nil if the suite cannot stop it.
	Stop func()
}

// Target is the implementation under test, and how to reach the suite.
type Target struct {
	// Launch starts a service configured per env, which must register with
	// env.OrchestratorURL, and returns it once it is ready. It is called twice:
	// for the service the suite's web servers are reachable through (env
	// carries them), and for a service in its default configuration. For an
	// implementation that is already running, Launch returns where it is.
	Launch func(t testing.TB, env Env) Instance
	// Env overrides the suite's defaults for the tokens and intervals it
	// configures the service with.
	Env Env
	// Net says where the suite's registry and web servers listen, and how the
	// service reaches them.
	Net check.Net
}

const kindNetwork = membershipv1.MemberKind_MEMBER_KIND_NETWORK_SERVICE

func newOrchestrator(t *testing.T, env Env, n check.Net) *fakeorch.Orchestrator {
	return fakeorch.New(t, n, fakeorch.Config{NetworkToken: env.NetworkToken, Interval: env.HeartbeatInterval})
}

// waitRegistered waits for the service reachable at endpoint to register, and
// returns what it declared.
func waitRegistered(t testing.TB, o *fakeorch.Orchestrator, endpoint string) fakeorch.Member {
	t.Helper()
	o.WaitFor(t, "the network service at "+endpoint+" to register with the suite's orchestrator (is it configured to register at "+o.URL()+"?)", 90*time.Second,
		func() bool { _, ok := o.MemberAt(endpoint, kindNetwork); return ok })
	return registrationFor(o, endpoint)
}

func registrationFor(o *fakeorch.Orchestrator, endpoint string) fakeorch.Member {
	o.Lock()
	defer o.Unlock()
	m, _ := o.MemberAt(endpoint, kindNetwork)
	return m
}

func beatsOf(o *fakeorch.Orchestrator, id string) []fakeorch.Beat {
	o.Lock()
	defer o.Unlock()
	return o.BeatsOf(id)
}

// launched is a service the suite started (or was pointed at), registered.
func launch(t *testing.T, target Target, o *fakeorch.Orchestrator, env Env, origin *origin, covers []string, networks []string) (*harness, bool) {
	env.Covers, env.AllowNetworks = covers, networks
	inst := target.Launch(t, env)
	if inst.URL == "" {
		return nil, false
	}
	h := newHarness(env, origin, o, inst.URL)
	h.registration = waitRegistered(t, o, inst.URL)
	h.stop = inst.Stop
	return h, true
}

// Egress judges the network service as the host of egress.v1 EgressService: it
// carries what it declared, makes the outbound requests of guests, and refuses what
// the allow list, its coverage and its own address checks do not allow. The suite
// plays the worker that calls Fetch, the orchestrator the service registers with
// (so that what it declared is known), and the web servers it is asked to reach.
func Egress(t *testing.T, target Target) {
	env := target.Env.withDefaults()
	orch := newOrchestrator(t, env, target.Net)
	env.OrchestratorURL = orch.URL()
	origin := newOrigin(t, target.Net)

	// The service the suite's web servers are reachable through.
	open, ok := launch(t, target, orch, env, origin, []string{origin.host(), "*.covered.test"}, []string{hostRange(origin.host())})
	if !ok {
		t.Fatal("no network service to test")
	}
	t.Run("Describe", open.describe)
	t.Run("Authentication", open.authentication)
	t.Run("Coverage", open.coverage)
	open.requireOriginCovered(t)
	open.requireOriginReachable(t)
	t.Run("Requests", open.requests)
	t.Run("Responses", open.responses)
	t.Run("Validation", open.validation)
	t.Run("AllowList", open.allowList)
	t.Run("Limits", open.limits)
	t.Run("Failures", open.failures)
	t.Run("Deadline", open.deadline)
	t.Run("Cancellation", open.cancellation)

	// A service in its default configuration, for what it refuses whatever it
	// is asked to allow.
	closed, ok := launch(t, target, orch, env, origin, []string{"*"}, nil)
	if !ok {
		t.Run("ForbiddenDestinations", func(t *testing.T) {
			t.Skip("no second network service in its default configuration to test (see --target-default)")
		})
		return
	}
	t.Run("ForbiddenDestinations", closed.forbidden)
}

// MembershipClient judges the network service as a client of MembershipService:
// it keeps trying to register until it is let in, says what it carries (in
// canonical form) and where it is, heartbeats READY, registers again when it is
// forgotten, and when asked to stop reports DRAINING and deregisters. The suite
// plays the orchestrator (a scripted Membership host). Needs no web servers: it
// is the registration, not the traffic, that is judged.
func MembershipClient(t *testing.T, target Target) {
	env := target.Env.withDefaults()
	orch := newOrchestrator(t, env, target.Net)
	env.OrchestratorURL = orch.URL()
	host := target.Net.Host()

	orch.FailRegistrations(2) // a service must keep trying
	open, ok := launch(t, target, orch, env, nil, []string{host, "*.covered.test"}, []string{hostRange(host)})
	if !ok {
		t.Fatal("no network service to test")
	}
	t.Run("Registration", open.registrationGroup)
	t.Run("Reregistration", open.reregistration)

	// A service in its default configuration leaves too. (The first's departure
	// is last: the suite must not have it gone while it checks the second.)
	if closed, ok := launch(t, target, orch, env, nil, []string{"*"}, nil); ok {
		t.Run("DeregistrationOfTheDefaultInstance", closed.deregistration)
	}
	open.registration = registrationFor(orch, open.url)
	t.Run("Deregistration", open.deregistration)
}

func newHarness(env Env, o *origin, reg *fakeorch.Orchestrator, url string) *harness {
	h := &harness{env: env, origin: o, reg: reg, url: url}
	h.client = egressv1connect.NewEgressServiceClient(http.DefaultClient, url, check.Bearer(env.EgressToken))
	return h
}

// hostRange is the range (CIDR) holding exactly the host a suite's web servers
// are on, which a service must be allowed to connect to; for a name, the host.
func hostRange(host string) string {
	if a, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil {
		return netip.PrefixFrom(a, a.BitLen()).String()
	}
	return host
}

type harness struct {
	env    Env
	origin *origin
	url    string
	client egressv1connect.EgressServiceClient

	reg          *fakeorch.Orchestrator
	registration fakeorch.Member
	stop         func()
}

// fetch is one call, with everything it produced.
type fetched struct {
	head   *commonv1.HttpResponseHead
	body   []byte
	chunks int
	err    error
}

func (h *harness) fetch(t testing.TB, req *egressv1.FetchRequest, budget time.Duration) fetched {
	t.Helper()
	if budget == 0 {
		budget = settle
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	return h.fetchCtx(t, ctx, req)
}

func (h *harness) fetchCtx(t testing.TB, ctx context.Context, req *egressv1.FetchRequest) fetched {
	t.Helper()
	st, err := h.client.Fetch(ctx, connect.NewRequest(req))
	if err != nil {
		return fetched{err: err}
	}
	defer st.Close()
	var out fetched
	for st.Receive() {
		switch p := st.Msg().GetPart().(type) {
		case *egressv1.FetchResponse_Head:
			if out.head != nil {
				t.Errorf("protocol violation: a second response head")
			}
			out.head = p.Head
		case *egressv1.FetchResponse_Chunk:
			if out.head == nil {
				t.Errorf("protocol violation: a chunk before the head")
			}
			if len(p.Chunk) > 1<<20 {
				t.Errorf("protocol violation: a chunk of %d bytes exceeds 1 MiB", len(p.Chunk))
			}
			out.chunks++
			out.body = append(out.body, p.Chunk...)
		default:
			t.Errorf("protocol violation: an empty FetchResponse")
		}
	}
	out.err = st.Err()
	if out.err == nil && out.head == nil {
		t.Errorf("protocol violation: the stream ended OK without a head")
	}
	return out
}

// get requests path on the suite's origin, allowed by the entry that names it.
func (h *harness) get(t testing.TB, path string) fetched {
	t.Helper()
	return h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: h.origin.url(path), Allow: []string{h.origin.entry()}}, 0)
}

// budgetHeader carries an exchange's wall-clock budget from fetchBudget to
// budgetTransport, which puts it on the wire as Connect-Timeout-Ms. Connect
// clients derive the wire deadline from the call's context, which would make
// the caller's own timer and the service's budget one value, and the caller's
// bare context error could then beat the service's typed one.
const budgetHeader = "X-Conformance-Budget-Ms"

type budgetTransport struct{}

func (budgetTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if b := r.Header.Get(budgetHeader); b != "" {
		r = r.Clone(r.Context())
		r.Header.Del(budgetHeader)
		r.Header.Set("Connect-Timeout-Ms", b)
	}
	return http.DefaultTransport.RoundTrip(r)
}

// fetchBudget is a call whose budget, as the service is told it, is shorter
// than the caller's own patience.
func (h *harness) fetchBudget(t testing.TB, req *egressv1.FetchRequest, budget time.Duration) fetched {
	t.Helper()
	c := egressv1connect.NewEgressServiceClient(&http.Client{Transport: budgetTransport{}}, h.url, check.Bearer(h.env.EgressToken))
	ctx, cancel := context.WithTimeout(context.Background(), budget+15*time.Second)
	defer cancel()
	cr := connect.NewRequest(req)
	cr.Header().Set(budgetHeader, strconv.FormatInt(budget.Milliseconds(), 10))
	st, err := c.Fetch(ctx, cr)
	if err != nil {
		return fetched{err: err}
	}
	defer st.Close()
	var out fetched
	for st.Receive() {
		if hd := st.Msg().GetHead(); hd != nil {
			out.head = hd
		}
	}
	out.err = st.Err()
	return out
}

func hdr(name, value string) *commonv1.Header {
	return &commonv1.Header{Name: name, Value: []byte(value)}
}

func (h *harness) describe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), settle)
	defer cancel()
	r, err := h.client.Describe(ctx, connect.NewRequest(&egressv1.DescribeRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if r.Msg.GetImplementation().GetName() == "" {
		t.Error("the implementation has no name")
	}
	found := false
	for _, p := range r.Msg.GetImplementation().GetProtocols() {
		found = found || p == "edgerun.egress.v1"
	}
	if !found {
		t.Errorf("protocols %v do not include edgerun.egress.v1", r.Msg.GetImplementation().GetProtocols())
	}
	if r.Msg.GetMaxRequestBodyBytes() == 0 || r.Msg.GetMaxResponseBodyBytes() == 0 {
		t.Errorf("limits must be reported: %v", r.Msg)
	}
}

func (h *harness) authentication(t *testing.T) {
	for name, tok := range map[string]string{"no token": "", "wrong token": "not-the-token", "upper-cased": strings.ToUpper(h.env.EgressToken)} {
		c := egressv1connect.NewEgressServiceClient(http.DefaultClient, h.url, check.Bearer(tok))
		ctx, cancel := context.WithTimeout(context.Background(), settle)
		_, err := c.Describe(ctx, connect.NewRequest(&egressv1.DescribeRequest{}))
		cancel()
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("Describe with %s: %v, want unauthenticated", name, err)
		}
		// Nor may an unauthenticated caller reach the network.
		ctx, cancel = context.WithTimeout(context.Background(), settle)
		st, err := c.Fetch(ctx, connect.NewRequest(&egressv1.FetchRequest{Method: "GET", Url: h.origin.url("/echo"), Allow: []string{h.origin.entry()}}))
		if err == nil {
			for st.Receive() {
			}
			err = st.Err()
			st.Close()
		}
		cancel()
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("Fetch with %s: %v, want unauthenticated", name, err)
		}
	}
	if n := h.origin.count("/echo"); n != 0 {
		t.Errorf("an unauthenticated call reached the origin %d times", n)
	}
}
