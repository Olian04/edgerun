// Package edgeconf is the conformance suite for the Edge platform service: the
// plain-HTTP contract a frontend hosts, written as the numbered checklist E1..E20
// in docs/protocol.md ("The Edge contract"), and the frontend's duties as the
// client of ingress.v1 behind it.
//
// The suite starts the frontend under test through Target.Launch, points it at
// an orchestrator the suite hosts itself (a scripted IngressService whose every
// answer is the test's), and talks to the frontend as an HTTP client. A frontend
// that passes maps HTTP onto the protocol, and the protocol's answers and errors
// back onto HTTP, exactly as the checklist says, so frontends can be swapped
// without the orchestrator or the functions noticing. Because the scripted
// orchestrator is the only thing the frontend can be a client of, this one suite
// also judges the frontend's side of Ingress: Ingress has no client suite of its own.
//
// Tests cite the rule they judge in their name or comment ("E7"). A test that
// judges a rule of "HTTP messages" rather than a numbered one says so.
package edgeconf

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
)

const (
	// bodyLimit is what the fake orchestrator reports as its request body
	// limit: small, so that the tests need not send megabytes.
	bodyLimit = 4096
	settle    = 15 * time.Second
)

// Env tells the frontend under test how to be configured. The suite's defaults
// fill any field left empty in Target.Env.
type Env struct {
	// OrchestratorURL is the base URL of the suite's fake orchestrator.
	OrchestratorURL string
	// IngressToken is the bearer token it must present to the orchestrator.
	IngressToken string
	// RequestTimeout is the whole-request budget it must give each invocation.
	RequestTimeout time.Duration
}

func (e Env) withDefaults() Env {
	if e.IngressToken == "" {
		e.IngressToken = "conformance-ingress-token"
	}
	if e.RequestTimeout == 0 {
		e.RequestTimeout = 3 * time.Second
	}
	return e
}

// Target is the implementation under test, and how to reach the suite.
type Target struct {
	// Launch starts a frontend configured per env and returns the base URL of
	// its public HTTP listener (http://host:port) once it is serving.
	// Arranging for it to stop at the end of the test is Launch's job. For a
	// frontend that is already running, Launch only returns where it is.
	Launch func(t testing.TB, env Env) (url string)
	// Env overrides the suite's defaults for the token and the timeout it
	// configures the frontend with.
	Env Env
	// Net says where the suite's fake orchestrator listens, and how the
	// frontend reaches it. The fake orchestrator is the primary server: its
	// port, if Net.Listen names one, is fixed, so that a frontend started
	// beforehand can be configured with it.
	Net check.Net
	// Wait is how long to wait for the frontend to ask the fake orchestrator
	// for its limits, which a frontend does when it starts. Default 15s.
	Wait time.Duration
}

// Run runs the suite against target.
func Run(t *testing.T, target Target) {
	env := target.Env.withDefaults()
	orch := newFakeOrchestrator(t, target.Net, env.IngressToken)
	env.OrchestratorURL = orch.srv.URL
	h := &harness{orch: orch, url: target.Launch(t, env), env: env}
	wait := target.Wait
	if wait == 0 {
		wait = settle
	}
	// A frontend learns the body limit from Describe when it starts.
	check.Eventually(t, "the frontend to ask the suite's orchestrator at "+orch.srv.URL+" for its limits", wait, func() bool { return orch.describes() > 0 })

	// ... which it may do before it listens: wait until it takes connections too,
	// or the first request of the suite meets a closed port and every case fails.
	check.Eventually(t, "the frontend to accept connections at "+h.url, wait, func() bool {
		host := strings.TrimPrefix(strings.TrimPrefix(h.url, "https://"), "http://")
		if i := strings.IndexByte(host, '/'); i >= 0 {
			host = host[:i]
		}
		c, err := net.DialTimeout("tcp", host, 500*time.Millisecond)
		if err == nil {
			c.Close()
		}
		return err == nil
	})

	t.Run("RequestMapping_E3_E6_E7_E9", h.requestMapping)
	t.Run("RequestIDs_E8", h.requestIDs)
	t.Run("TraceContext_E10", h.traceContext)
	t.Run("HTTPMessages_E3_E4_E5_E6_E11_E12_E13_E14", h.httpMessages)
	t.Run("InvocationIDs_E16", h.invocationIDs)
	t.Run("BodyLimit_E1", h.bodyLimit)
	t.Run("BadRequests_E2", h.badRequests)
	t.Run("ResponseMapping_E13_E14_E17_E19", h.responseMapping)
	t.Run("Streaming_E15", h.streaming)
	t.Run("PlatformErrors_E18", h.platformErrors)
	t.Run("UntypedErrors_E18", h.untypedErrors)
	t.Run("FailureAfterTheHead_E20", h.failureAfterHead)
	t.Run("Deadline_E9_E18", h.deadline)
	t.Run("ClientGoesAway_E9", h.clientGoesAway)
	// Must stay last: it takes the orchestrator away.
	t.Run("OrchestratorUnreachable_E18", h.orchestratorDown)
}

type harness struct {
	orch *fakeOrchestrator
	url  string
	env  Env
}

// --- the fake orchestrator ---------------------------------------------------------

type invokeFunc func(ctx context.Context, req *ingressv1.InvokeRequest, st *connect.ServerStream[ingressv1.InvokeResponse]) error

// seen is one Invoke call as the orchestrator received it.
type seen struct {
	req    *ingressv1.InvokeRequest
	header http.Header
}

type fakeOrchestrator struct {
	token string
	srv   *check.Server

	mu       sync.Mutex
	nDescr   int
	got      []seen
	behave   invokeFunc
	describe http.Header
}

func newFakeOrchestrator(t *testing.T, n check.Net, token string) *fakeOrchestrator {
	o := &fakeOrchestrator{token: token}
	mux := http.NewServeMux()
	mux.Handle(ingressv1connect.NewIngressServiceHandler(ingress{o}))
	o.srv = n.Serve(t, mux, true)
	return o
}

func (o *fakeOrchestrator) describes() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.nDescr
}

// script sets what Invoke does until the next call to script.
func (o *fakeOrchestrator) script(f invokeFunc) {
	o.mu.Lock()
	o.behave = f
	o.mu.Unlock()
}

func (o *fakeOrchestrator) invocations() []seen {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]seen(nil), o.got...)
}

// invocationsWith returns the Invoke calls whose request carried the header
// x-conformance-test: id. Tests mark their requests, so that what earlier
// tests did not clean up cannot be mistaken for what this one caused.
func (o *fakeOrchestrator) invocationsWith(id string) []seen {
	var out []seen
	for _, s := range o.invocations() {
		for _, h := range s.req.GetRequest().GetHeaders() {
			if h.GetName() == "x-conformance-test" && string(h.GetValue()) == id {
				out = append(out, s)
			}
		}
	}
	return out
}

type ingress struct{ o *fakeOrchestrator }

func (i ingress) authenticate(h http.Header) error {
	if h.Get("Authorization") != "Bearer "+i.o.token {
		return connect.NewError(connect.CodeUnauthenticated, nil)
	}
	return nil
}

func (i ingress) Describe(_ context.Context, req *connect.Request[ingressv1.DescribeRequest]) (*connect.Response[ingressv1.DescribeResponse], error) {
	if err := i.authenticate(req.Header()); err != nil {
		return nil, err
	}
	i.o.mu.Lock()
	i.o.nDescr++
	i.o.mu.Unlock()
	return connect.NewResponse(&ingressv1.DescribeResponse{
		Implementation:      &commonv1.Implementation{Name: "conformance-orchestrator", Version: "1", Protocols: []string{"edgerun.ingress.v1"}},
		MaxRequestBodyBytes: bodyLimit,
	}), nil
}

func (i ingress) Invoke(ctx context.Context, req *connect.Request[ingressv1.InvokeRequest], st *connect.ServerStream[ingressv1.InvokeResponse]) error {
	if err := i.authenticate(req.Header()); err != nil {
		return err
	}
	i.o.mu.Lock()
	i.o.got = append(i.o.got, seen{req: req.Msg, header: req.Header().Clone()})
	b := i.o.behave
	i.o.mu.Unlock()
	if b == nil {
		return connect.NewError(connect.CodeInternal, nil)
	}
	return b(ctx, req.Msg, st)
}

// --- scripted answers ------------------------------------------------------------------

func hdr(name, value string) *commonv1.Header {
	return &commonv1.Header{Name: name, Value: []byte(value)}
}

func sendHead(st *connect.ServerStream[ingressv1.InvokeResponse], status uint32, headers ...*commonv1.Header) error {
	return st.Send(&ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Head{Head: &ingressv1.InvokeHead{
		Http: &commonv1.HttpResponseHead{Status: status, Headers: headers}, InvocationId: "inv-conformance"}}})
}

func sendChunk(st *connect.ServerStream[ingressv1.InvokeResponse], body string) error {
	return st.Send(&ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Chunk{Chunk: []byte(body)}})
}

// answer responds with status, headers and a body in one chunk.
func answer(status uint32, body string, headers ...*commonv1.Header) invokeFunc {
	return func(_ context.Context, _ *ingressv1.InvokeRequest, st *connect.ServerStream[ingressv1.InvokeResponse]) error {
		if err := sendHead(st, status, headers...); err != nil {
			return err
		}
		if body != "" {
			return sendChunk(st, body)
		}
		return nil
	}
}

// failWith answers with a platform error and a message the client must never see.
func failWith(reason commonv1.ErrorReason) invokeFunc {
	return func(context.Context, *ingressv1.InvokeRequest, *connect.ServerStream[ingressv1.InvokeResponse]) error {
		return check.Error(reason, leak)
	}
}

// leak is operator-only detail: it must not appear in any response to a client.
const leak = "SECRET-OPERATOR-DETAIL postgres://user:pw@db/internal"
