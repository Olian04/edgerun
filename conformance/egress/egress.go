// Package egressconf is the conformance suite for the network service role:
// whatever serves edgerun.egress.v1 and makes outbound HTTP requests on behalf
// of guests.
//
// The suite starts the service under test through Target.Launch, runs origin
// servers of its own for it to reach, and plays the worker through the
// generated protocol code alone. A service that passes enforces the same
// allow-list grammar and the same refusals as any other that passes
// (docs/protocol.md, "Network access"), so a deployment can replace its network
// service, or put one where its network is, without workers or functions
// noticing.
package egressconf

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/egress/v1/egressv1connect"
)

const (
	token  = "conformance-egress-token"
	settle = 20 * time.Second
)

// Env tells the service under test how to be configured.
type Env struct {
	// Token is the bearer token workers present to it (scope "egress").
	Token string
	// AllowLoopback asks for the service to be configured to connect to
	// loopback addresses, which it refuses by default. The suite needs it to
	// give the service something to reach; how a service is told is its
	// deployment's business. When false, the service must be in its default
	// configuration.
	AllowLoopback bool
}

// Target starts the implementation under test.
type Target struct {
	// Launch starts a service configured per env and returns its base URL once
	// it is ready. It is called more than once, for differently configured
	// services; arranging for each to stop at the end of the test is its job.
	Launch func(t testing.TB, env Env) (url string)
}

// Run runs the suite against target.
func Run(t *testing.T, target Target) {
	origin := newOrigin(t)
	open := &harness{
		origin: origin,
		url:    target.Launch(t, Env{Token: token, AllowLoopback: true}),
	}
	open.client = egressv1connect.NewEgressServiceClient(http.DefaultClient, open.url, check.Bearer(token))

	t.Run("Describe", open.describe)
	t.Run("Authentication", open.authentication)
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
	closed := &harness{
		origin: origin,
		url:    target.Launch(t, Env{Token: token, AllowLoopback: false}),
	}
	closed.client = egressv1connect.NewEgressServiceClient(http.DefaultClient, closed.url, check.Bearer(token))
	t.Run("ForbiddenDestinations", closed.forbidden)
}

type harness struct {
	origin *origin
	url    string
	client egressv1connect.EgressServiceClient
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
	c := egressv1connect.NewEgressServiceClient(&http.Client{Transport: budgetTransport{}}, h.url, check.Bearer(token))
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

func hdr(name, value string) *commonv1.Header { return &commonv1.Header{Name: name, Value: value} }

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
	for name, tok := range map[string]string{"no token": "", "wrong token": "not-the-token", "upper-cased": "CONFORMANCE-EGRESS-TOKEN"} {
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
