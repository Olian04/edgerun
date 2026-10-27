// Package frontendconf is the conformance suite for the frontend role: the
// HTTP edge that is an ingress.v1 client.
//
// The suite starts the frontend under test through Target.Launch, points it at
// an orchestrator the suite hosts itself (a fake IngressService whose every
// answer is scripted), and talks to the frontend as an HTTP client. A frontend
// that passes maps HTTP onto the protocol, and the protocol's answers and
// errors back onto HTTP, exactly as the specification says (docs/protocol.md,
// "Frontend behaviour"), so frontends can be swapped without the orchestrator
// or the functions noticing.
package frontendconf

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	ingressToken = "conformance-ingress-token"
	// bodyLimit is what the fake orchestrator reports as its request body
	// limit: small, so that the tests need not send megabytes.
	bodyLimit = 4096
	settle    = 15 * time.Second
)

// Env tells the frontend under test how to be configured.
type Env struct {
	// OrchestratorURL is the base URL of the suite's fake orchestrator.
	OrchestratorURL string
	// IngressToken is the bearer token it must present to the orchestrator.
	IngressToken string
	// RequestTimeout is the whole-request budget it must give each invocation.
	RequestTimeout time.Duration
}

// Target starts the implementation under test.
type Target struct {
	// Launch starts a frontend configured per env and returns the base URL of
	// its public HTTP listener (http://host:port) once it is serving.
	// Arranging for it to stop at the end of the test is Launch's job.
	Launch func(t testing.TB, env Env) (url string)
}

// Run runs the suite against target.
func Run(t *testing.T, target Target) {
	orch := newFakeOrchestrator(t)
	env := Env{OrchestratorURL: orch.srv.URL, IngressToken: ingressToken, RequestTimeout: 3 * time.Second}
	h := &harness{orch: orch, url: target.Launch(t, env), env: env}
	// A frontend learns the body limit from Describe when it starts.
	check.Eventually(t, "the frontend to ask the orchestrator for its limits", settle, func() bool { return orch.describes() > 0 })

	t.Run("RequestMapping", h.requestMapping)
	t.Run("RequestIDs", h.requestIDs)
	t.Run("BodyLimit", h.bodyLimit)
	t.Run("BadRequests", h.badRequests)
	t.Run("ResponseMapping", h.responseMapping)
	t.Run("Streaming", h.streaming)
	t.Run("PlatformErrors", h.platformErrors)
	t.Run("UntypedErrors", h.untypedErrors)
	t.Run("FailureAfterTheHead", h.failureAfterHead)
	t.Run("Deadline", h.deadline)
	t.Run("ClientGoesAway", h.clientGoesAway)
	// Must stay last: it takes the orchestrator away.
	t.Run("OrchestratorUnreachable", h.orchestratorDown)
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
	t   *testing.T
	srv *httptest.Server

	mu       sync.Mutex
	nDescr   int
	got      []seen
	behave   invokeFunc
	describe http.Header
}

func newFakeOrchestrator(t *testing.T) *fakeOrchestrator {
	o := &fakeOrchestrator{t: t}
	mux := http.NewServeMux()
	mux.Handle(ingressv1connect.NewIngressServiceHandler(ingress{o}))
	o.srv = httptest.NewServer(mux)
	t.Cleanup(o.srv.Close)
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
			if h.GetName() == "x-conformance-test" && h.GetValue() == id {
				out = append(out, s)
			}
		}
	}
	return out
}

type ingress struct{ o *fakeOrchestrator }

func (i ingress) authenticate(h http.Header) error {
	if h.Get("Authorization") != "Bearer "+ingressToken {
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

func hdr(name, value string) *commonv1.Header { return &commonv1.Header{Name: name, Value: value} }

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
