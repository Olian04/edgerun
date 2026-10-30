// Package workerconf is the conformance suite for the worker role of the
// edgerun worker seam (edgerun.worker.v1).
//
// It is black-box and implementation-agnostic: the suite starts the worker
// under test through Target.Launch, then talks to it only over the wire, using
// nothing but the generated protocol code. A worker that passes is
// interchangeable with any other that passes, which is what lets a worker be
// replaced without the orchestrator noticing.
//
// Because workers dial in, the suite hosts a fake orchestrator (FleetService
// and ArtifactService) and points the worker at it. That also exercises the
// Fleet and Artifact seams from the worker's side.
package workerconf

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/guest"
	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
)

const (
	abi = "wagi-1"
	// roomy covers the first invocation, which compiles the guest (seconds
	// under the race detector). Once one invocation has succeeded the worker
	// is warm and ordinary calls get steady: a broken worker then fails the
	// suite in minutes rather than hanging each remaining group for ages.
	roomy  = 90 * time.Second
	steady = 30 * time.Second
)

// Env tells the worker under test how to reach the suite. The suite's defaults
// fill any field left empty in Target.Env.
type Env struct {
	// OrchestratorURL is the base URL of the suite's fake orchestrator.
	OrchestratorURL string
	// WorkerToken must be presented to the orchestrator on Fleet and
	// Artifact calls.
	WorkerToken string
	// ExecutionToken must be required on ExecutionService calls.
	ExecutionToken string
	// EgressToken is what the worker must present to network services, which
	// the orchestrator routes a function's outbound requests to. A worker that
	// offers wagi-2 uses it; one that does not has no network to offer.
	EgressToken string
	// MaxConcurrency is the number of invocations the worker must run at once.
	MaxConcurrency uint32
	// HeartbeatInterval is what the fake orchestrator dictates to the worker.
	HeartbeatInterval time.Duration
}

func (e Env) withDefaults() Env {
	if e.WorkerToken == "" {
		e.WorkerToken = "conformance-worker-token"
	}
	if e.ExecutionToken == "" {
		e.ExecutionToken = "conformance-execution-token"
	}
	if e.EgressToken == "" {
		e.EgressToken = "conformance-egress-token"
	}
	if e.MaxConcurrency == 0 {
		e.MaxConcurrency = 2
	}
	if e.HeartbeatInterval == 0 {
		e.HeartbeatInterval = 100 * time.Millisecond
	}
	return e
}

// Target is the implementation under test, and how to reach the suite.
type Target struct {
	// Launch starts a worker configured per env and returns promptly; the
	// worker is expected to register on its own. The returned stop requests a
	// graceful shutdown (what SIGTERM means for the worker: drain, finish
	// in-flight work, deregister, exit) and blocks until the worker has
	// exited, or reports a test failure if it has not within a bounded time:
	// it must never block forever, or a broken worker hangs the suite instead
	// of failing it. Launch must arrange for stop to run at the end of the
	// test if the suite has not already called it. For a worker that is already
	// running, Launch only returns a stop for it, or nil if the suite has no
	// way to stop it, in which case the group that shuts it down is skipped.
	Launch func(t testing.TB, env Env) (stop func())
	// Env overrides the suite's defaults for the tokens and limits it
	// configures the worker with.
	Env Env
	// Net says where the suite's fake orchestrator and network services
	// listen, and how the worker reaches them. The fake orchestrator is the
	// primary server: its port, if Net.Listen names one, is fixed, so that a
	// worker started beforehand can be configured with it.
	Net check.Net
	// Endpoint, host:port or a URL, is the worker to test: the one that
	// registers at that address. Empty means the first to register.
	Endpoint string
	// Wait is how long to wait for the worker to register. Default 60s.
	Wait time.Duration
}

// Run runs the suite against target.
func Run(t *testing.T, target Target) {
	env := target.Env.withDefaults()
	wasm := guest.Build(t)
	orch := newFakeOrchestrator(t, target.Net, env.WorkerToken, env.HeartbeatInterval)
	env.OrchestratorURL = orch.url()
	good := orch.addBlob(wasm)
	netGood := orch.addBlob(guest.BuildNet(t))
	egress := newFakeEgress(t, target.Net, env.EgressToken)

	stop := target.Launch(t, env)
	wait := target.Wait
	if wait == 0 {
		wait = 60 * time.Second
	}
	orch.waitFor(t, "the worker to register with the suite's orchestrator at "+orch.url(), wait, func() bool { return orch.registered(target.Endpoint) != nil })
	orch.mu.Lock()
	reg := *orch.registered(target.Endpoint)
	orch.mu.Unlock()
	h := &harness{
		env: env, net: target.Net,
		orch: orch, reg: reg, good: good, netGood: netGood, egress: egress, stop: stop,
		client: workerv1connect.NewExecutionServiceClient(&http.Client{Transport: budgetTransport{}}, reg.Endpoint),
	}

	h.budget.Store(int64(roomy))

	t.Run("Registration", h.registration)
	t.Run("Heartbeats", h.heartbeats)
	t.Run("Authentication", h.authentication)
	t.Run("Requests", h.requests)
	t.Run("FunctionStatusIsNotAPlatformError", h.functionStatus)
	t.Run("Summary", h.summary)
	t.Run("ABIMismatch", h.abiMismatch)
	t.Run("Artifacts", h.artifacts)
	// A worker that does not stop a runaway guest is in an unknown state: its
	// slots stay occupied, and a spinning guest can even wedge the whole
	// process (the Go runtime cannot preempt native JIT code, so every garbage
	// collection waits for it). Nothing after this point can be trusted, so
	// stop here instead of reporting a cascade of meaningless failures.
	if !t.Run("Deadline", h.deadline) {
		t.Fatal("deadline enforcement failed: the worker is in an unknown state, skipping the remaining groups")
	}
	t.Run("Traps", h.traps)
	t.Run("MemoryLimit", h.memoryLimit)
	t.Run("ResponseLimit", h.responseLimit)
	t.Run("ABIViolations", h.abiViolations)
	t.Run("MalformedRequests", h.malformedRequests)
	t.Run("HTTPMessages", h.httpMessages)
	t.Run("TraceContext", h.traceContext)
	t.Run("RealTimeAndEntropy", h.timeAndEntropy)
	t.Run("Network", h.network)
	t.Run("Capacity", h.capacity)
	t.Run("ReregistersWhenForgotten", h.reregister)
	// Must stay last: it shuts the worker down.
	t.Run("GracefulShutdown", h.gracefulShutdown)
}

type harness struct {
	env    Env
	net    check.Net
	budget atomic.Int64 // default per-call budget in ns; roomy until the first success
	orch   *fakeOrchestrator
	reg    registration
	good   string
	// netGood is the digest of the network test guest, and egress the fake
	// network service the worker was told to use.
	netGood string
	egress  *fakeEgress
	stop    func()
	client  workerv1connect.ExecutionServiceClient
}

// budgetHeader carries an invocation's wall-clock budget from call to
// budgetTransport, which puts it on the wire as Connect-Timeout-Ms.
const budgetHeader = "X-Conformance-Budget-Ms"

// budgetTransport lets the suite give the worker a budget independent of the
// client's own context deadline.
type budgetTransport struct{}

func (budgetTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if b := r.Header.Get(budgetHeader); b != "" {
		r = r.Clone(r.Context())
		r.Header.Del(budgetHeader)
		r.Header.Set("Connect-Timeout-Ms", b)
	}
	return http.DefaultTransport.RoundTrip(r)
}

// canonicalCode is the Connect code each reason must travel with
// (docs/protocol.md). Kept independent of any implementation's copy.
var canonicalCode = map[commonv1.ErrorReason]connect.Code{
	commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND: connect.CodeNotFound,
	commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY:        connect.CodeResourceExhausted,
	commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED:  connect.CodeDeadlineExceeded,
	commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED:   connect.CodeAborted,
	commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH:       connect.CodeFailedPrecondition,
	commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE:  connect.CodeInvalidArgument,
	commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE: connect.CodeResourceExhausted,
	commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND: connect.CodeNotFound,
	commonv1.ErrorReason_ERROR_REASON_INTERNAL:           connect.CodeInternal,
	commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED:      connect.CodePermissionDenied,
	commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED:      connect.CodeUnavailable,
}

// reasonOf extracts the ErrorInfo reason from a Connect error.
func reasonOf(err error) (commonv1.ErrorReason, string) {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED, ""
	}
	for _, d := range ce.Details() {
		if v, verr := d.Value(); verr == nil {
			if info, ok := v.(*commonv1.ErrorInfo); ok {
				return info.GetReason(), info.GetMessage()
			}
		}
	}
	return commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED, ""
}

// outcome is everything one Execute produced.
type outcome struct {
	head    *commonv1.HttpResponseHead
	body    []byte
	summary *workerv1.ExecutionSummary
	err     error
	elapsed time.Duration
}

func (o outcome) headers(name string) []string {
	var out []string
	for _, h := range o.head.GetHeaders() {
		if h.Name == name {
			out = append(out, string(h.Value))
		}
	}
	return out
}

// invocation describes one Execute call.
type invocation struct {
	path, query, method string
	body                string
	digest              string
	abi                 string
	limits              *commonv1.Limits
	env                 []*commonv1.EnvVar
	timeout             time.Duration
	token               *string                 // nil: the correct token; pointer to "": none
	traceparent         string                  // the call carries this trace context, if set
	clientHeaders       []*commonv1.Header      // more headers on the request the guest is given
	egress              []*workerv1.EgressGrant // the function's network grants
}

func (h *harness) call(t *testing.T, inv invocation) outcome {
	t.Helper()
	if inv.digest == "" {
		inv.digest = h.good
	}
	if inv.abi == "" {
		inv.abi = abi
	}
	if inv.method == "" {
		inv.method = "GET"
	}
	if inv.timeout == 0 {
		inv.timeout = time.Duration(h.budget.Load())
	}
	// The budget is what the worker is told (Connect-Timeout-Ms, see
	// budgetTransport); the local guard is longer, so the worker's own typed
	// error arrives before our timer could replace it with a bare context
	// error. Connect clients derive the wire deadline from the context, which
	// would make the two the same value.
	ctx, cancel := context.WithTimeout(context.Background(), inv.timeout+15*time.Second)
	defer cancel()

	req := connect.NewRequest(&workerv1.ExecuteRequest{
		InvocationId: "conformance-" + t.Name(), ArtifactDigest: inv.digest, Abi: inv.abi,
		Limits: inv.limits, Env: inv.env, Egress: inv.egress,
		Request: &commonv1.HttpRequest{
			Method: inv.method, Host: "conformance.test", Path: inv.path, RawQuery: inv.query,
			Body: []byte(inv.body), RemoteAddr: "192.0.2.1:1234",
			Headers: append([]*commonv1.Header{{Name: "content-type", Value: []byte("text/x-conformance")}, {Name: "x-multi", Value: []byte("a")}, {Name: "x-multi", Value: []byte("b")}}, inv.clientHeaders...),
		},
	})
	if inv.traceparent != "" {
		req.Header().Set("Traceparent", inv.traceparent)
	}
	req.Header().Set(budgetHeader, strconv.FormatInt(inv.timeout.Milliseconds(), 10))
	switch {
	case inv.token == nil:
		req.Header().Set("Authorization", "Bearer "+h.env.ExecutionToken)
	case *inv.token != "":
		req.Header().Set("Authorization", "Bearer "+*inv.token)
	}

	start := time.Now()
	out := outcome{}
	st, err := h.client.Execute(ctx, req)
	if err != nil {
		out.err, out.elapsed = err, time.Since(start)
		return out
	}
	defer st.Close()

	// The stream shape is part of the protocol: head, chunks*, summary, end.
	sawSummary := false
	for st.Receive() {
		m := st.Msg()
		switch {
		case sawSummary:
			t.Errorf("protocol violation: message after the summary")
		case m.GetHead() != nil:
			if out.head != nil {
				t.Errorf("protocol violation: second head")
			}
			out.head = m.GetHead()
		case m.GetChunk() != nil:
			if out.head == nil {
				t.Errorf("protocol violation: chunk before head")
			}
			out.body = append(out.body, m.GetChunk()...)
		case m.GetSummary() != nil:
			if out.head == nil {
				t.Errorf("protocol violation: summary before head")
			}
			sawSummary = true
			out.summary = m.GetSummary()
		default:
			t.Errorf("protocol violation: empty ExecuteResponse")
		}
	}
	out.err, out.elapsed = st.Err(), time.Since(start)
	if out.err == nil {
		h.budget.Store(int64(steady)) // the worker has compiled and run a module
	}
	if out.err == nil && (out.head == nil || !sawSummary) {
		t.Errorf("protocol violation: stream ended OK without head and summary (head=%v summary=%v)", out.head != nil, sawSummary)
	}
	return out
}

func (h *harness) ok(t *testing.T, inv invocation) outcome {
	t.Helper()
	out := h.call(t, inv)
	if out.err != nil {
		t.Fatalf("%s %s: unexpected error: %v", inv.method, inv.path, out.err)
	}
	return out
}

func wantReason(t *testing.T, err error, want commonv1.ErrorReason) string {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %v, got success", want)
	}
	got, msg := reasonOf(err)
	if got != want {
		t.Fatalf("reason = %v (%v), want %v", got, err, want)
	}
	if code, ok := canonicalCode[want]; ok && connect.CodeOf(err) != code {
		t.Errorf("code = %v, want canonical %v for %v", connect.CodeOf(err), code, want)
	}
	return msg
}

func str(s string) *string { return &s }

// --- Fleet seam, from the worker's side -----------------------------------

func (h *harness) registration(t *testing.T) {
	info := h.reg.Info
	if h.reg.Endpoint == "" {
		t.Error("worker registered without an endpoint")
	}
	if info.GetImplementation().GetName() == "" {
		t.Error("implementation name is empty")
	}
	if !contains(info.GetImplementation().GetProtocols(), "edgerun.worker.v1") {
		t.Errorf("implementation must declare edgerun.worker.v1, got %v", info.GetImplementation().GetProtocols())
	}
	if !contains(info.GetAbis(), abi) {
		t.Errorf("worker must advertise %q, got %v", abi, info.GetAbis())
	}
	if info.GetMaxConcurrency() != h.env.MaxConcurrency {
		t.Errorf("max_concurrency = %d, want the configured %d", info.GetMaxConcurrency(), h.env.MaxConcurrency)
	}

	// Describe on the worker itself must agree with what it registered.
	req := connect.NewRequest(&workerv1.DescribeRequest{})
	req.Header().Set("Authorization", "Bearer "+h.env.ExecutionToken)
	resp, err := h.client.Describe(context.Background(), req)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	d := resp.Msg.GetInfo()
	if d.GetMaxConcurrency() != info.GetMaxConcurrency() || strings.Join(d.GetAbis(), ",") != strings.Join(info.GetAbis(), ",") {
		t.Errorf("Describe %v disagrees with Register %v", d, info)
	}
}

func (h *harness) heartbeats(t *testing.T) {
	h.orch.waitFor(t, "three heartbeats", 15*time.Second, func() bool { return len(h.orch.heartbeats) >= 3 })
	h.orch.mu.Lock()
	defer h.orch.mu.Unlock()
	for _, hb := range h.orch.heartbeats[:3] {
		if hb.ID != h.reg.ID {
			t.Errorf("heartbeat for %q, want %q", hb.ID, h.reg.ID)
		}
		if hb.State != workerv1.WorkerState_WORKER_STATE_READY {
			t.Errorf("idle worker reports %v, want READY", hb.State)
		}
	}
	if h.orch.authFailures != 0 {
		t.Errorf("worker failed to authenticate to the orchestrator %d times", h.orch.authFailures)
	}
}

// --- Authentication --------------------------------------------------------

func (h *harness) authentication(t *testing.T) {
	for name, tok := range map[string]*string{"no token": str(""), "wrong token": str("not-the-token"), "the worker token": str(h.env.WorkerToken)} {
		out := h.call(t, invocation{path: "/echo", token: tok, timeout: 10 * time.Second})
		if connect.CodeOf(out.err) != connect.CodeUnauthenticated {
			t.Errorf("%s: code = %v (%v), want unauthenticated", name, connect.CodeOf(out.err), out.err)
		}
	}
	// Describe is guarded too.
	_, err := h.client.Describe(context.Background(), connect.NewRequest(&workerv1.DescribeRequest{}))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("Describe without a token: %v", err)
	}
}

// --- Execution -------------------------------------------------------------

func (h *harness) requests(t *testing.T) {
	out := h.ok(t, invocation{method: "POST", path: "/echo", body: "hello, worker",
		env: []*commonv1.EnvVar{{Name: "MY_VAR", Value: "v1"}}})
	if out.head.Status != 200 || string(out.body) != "hello, worker" {
		t.Errorf("echo: status %d body %q", out.head.Status, out.body)
	}
	if got := out.headers("content-type"); len(got) != 1 || got[0] != "text/x-conformance" {
		t.Errorf("response content-type = %v", got)
	}

	// A body larger than one chunk must come back intact and in order.
	big := strings.Repeat("0123456789abcdef", 20_000) // 320 KB
	out = h.ok(t, invocation{method: "POST", path: "/echo", body: big})
	if string(out.body) != big {
		t.Errorf("large body corrupted: got %d bytes, want %d", len(out.body), len(big))
	}

	// The wagi-1 environment.
	out = h.ok(t, invocation{method: "POST", path: "/env", query: "a=b&c=d", body: "xyz",
		env: []*commonv1.EnvVar{{Name: "MY_VAR", Value: "v1"}}})
	env := string(out.body)
	for _, want := range []string{
		"GATEWAY_INTERFACE=CGI/1.1", "SERVER_PROTOCOL=HTTP/1.1", "REQUEST_METHOD=POST",
		"SERVER_NAME=conformance.test", "PATH_INFO=/env", "QUERY_STRING=a=b&c=d",
		"REMOTE_ADDR=192.0.2.1:1234", "CONTENT_LENGTH=3", "CONTENT_TYPE=text/x-conformance",
		"HTTP_X_MULTI=a, b", "MY_VAR=v1",
	} {
		if !strings.Contains(env, want+"\n") {
			t.Errorf("guest environment lacks %q; got:\n%s", want, env)
		}
	}
}

func (h *harness) functionStatus(t *testing.T) {
	for _, code := range []string{"201", "404", "500", "503"} {
		out := h.call(t, invocation{path: "/status", query: "code=" + code})
		if out.err != nil {
			t.Errorf("a function answering %s is a successful invocation, got %v", code, out.err)
			continue
		}
		if strconv.Itoa(int(out.head.Status)) != code {
			t.Errorf("status = %d, want %s", out.head.Status, code)
		}
	}
	out := h.ok(t, invocation{path: "/status", query: "code=200"})
	if got := out.headers("set-cookie"); strings.Join(got, "|") != "a=1|b=2" {
		t.Errorf("repeated headers must keep their order: %v", got)
	}
	if got := out.headers("x-test"); len(got) != 1 || got[0] != "1" {
		t.Errorf("x-test = %v", got)
	}
	for _, hd := range out.head.Headers {
		if hd.Name != strings.ToLower(hd.Name) {
			t.Errorf("header name %q must be lower-case", hd.Name)
		}
		if hd.Name == "status" {
			t.Error("the CGI Status header must not be forwarded as a header")
		}
	}
}

func (h *harness) summary(t *testing.T) {
	out := h.ok(t, invocation{path: "/stderr"})
	s := out.summary
	if string(s.Stderr) != "log line one\nlog line two\n" || s.StderrTruncated {
		t.Errorf("stderr = %q truncated=%v", s.Stderr, s.StderrTruncated)
	}
	if s.DurationMicros == 0 {
		t.Error("duration_micros not reported")
	}
	if s.PeakMemoryBytes == 0 {
		t.Error("peak_memory_bytes not reported")
	}
}

func (h *harness) abiMismatch(t *testing.T) {
	out := h.call(t, invocation{path: "/echo", abi: "wagi-99"})
	wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
}

func (h *harness) artifacts(t *testing.T) {
	t.Run("unknown digest", func(t *testing.T) {
		d := "sha256:" + strings.Repeat("0", 64)
		out := h.call(t, invocation{path: "/echo", digest: d})
		wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND)
	})
	t.Run("malformed digest", func(t *testing.T) {
		out := h.call(t, invocation{path: "/echo", digest: "md5:nope"})
		if connect.CodeOf(out.err) != connect.CodeInvalidArgument {
			t.Errorf("code = %v (%v), want invalid_argument", connect.CodeOf(out.err), out.err)
		}
	})
	t.Run("tampered bytes are never executed", func(t *testing.T) {
		// Two valid, runnable modules that differ by one byte. The orchestrator
		// serves the wrong one under the right one's digest. Garbage would be
		// refused by any worker; only digest verification can refuse a module
		// that would run perfectly well.
		right, wrong := padded(h.blob(), 'R'), padded(h.blob(), 'W')
		d := h.orch.addBlob(right)
		defer h.orch.removeBlob(d)

		h.orch.setBlob(d, wrong)
		out := h.call(t, invocation{path: "/echo", digest: d})
		wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_INTERNAL)

		// The bad bytes must not have been cached or remembered: once the
		// orchestrator serves the right ones, the same digest works.
		h.orch.setBlob(d, right)
		h.ok(t, invocation{path: "/echo", digest: d})
	})
	t.Run("not a wasm module", func(t *testing.T) {
		d := h.orch.addBlob([]byte("this is plainly not WebAssembly"))
		defer h.orch.removeBlob(d)
		out := h.call(t, invocation{path: "/echo", digest: d})
		wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
	})
	t.Run("a module that imports non-WASI functions", func(t *testing.T) {
		// (module (import "env" "f" (func)) (func (export "_start")))
		d := h.orch.addBlob([]byte{0, 'a', 's', 'm', 1, 0, 0, 0,
			1, 4, 1, 0x60, 0, 0, 2, 9, 1, 3, 'e', 'n', 'v', 1, 'f', 0, 0,
			3, 2, 1, 0, 7, 10, 1, 6, '_', 's', 't', 'a', 'r', 't', 0, 1, 10, 4, 1, 2, 0, 0x0b})
		defer h.orch.removeBlob(d)
		out := h.call(t, invocation{path: "/echo", digest: d})
		wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
	})
}

// padded returns wasm with a trailing custom section holding 16 copies of tag:
// still a valid module, but with a different digest for each tag.
func padded(wasm []byte, tag byte) []byte {
	payload := append([]byte{3}, "pad"...)
	payload = append(payload, bytes.Repeat([]byte{tag}, 16)...)
	out := append([]byte{}, wasm...)
	out = append(out, 0, byte(len(payload)))
	return append(out, payload...)
}

func (h *harness) blob() []byte {
	h.orch.mu.Lock()
	defer h.orch.mu.Unlock()
	return h.orch.blobs[h.good]
}

func (h *harness) deadline(t *testing.T) {
	h.ok(t, invocation{path: "/echo"}) // warm: the budget below is for execution
	for name, inv := range map[string]invocation{
		"a spinning guest": {path: "/spin"},
		"a sleeping guest": {path: "/sleep", query: "ms=30000"},
	} {
		inv.timeout = 500 * time.Millisecond
		out := h.call(t, inv)
		wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED)
		if out.elapsed > 10*time.Second {
			t.Errorf("%s: deadline of 500ms enforced after %v", name, out.elapsed)
		}
		// The guest must actually have been stopped, not just abandoned: its
		// slot has to come back, or a few hung functions would starve the worker.
		h.orch.waitFor(t, name+" to release its slot", 10*time.Second, func() bool {
			n := len(h.orch.heartbeats)
			return n > 0 && h.orch.heartbeats[n-1].InFlight == 0
		})
	}
}

func (h *harness) traps(t *testing.T) {
	out := h.call(t, invocation{path: "/exit", query: "code=3"})
	msg := wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED)
	if !strings.Contains(msg, "exiting on request") {
		t.Errorf("a FUNCTION_TRAPPED message should end with the guest's stderr tail, got %q", msg)
	}
	out = h.call(t, invocation{path: "/panic"})
	wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED)
	// Exit code 0 from a guest that wrote a valid response is fine, and the
	// worker survives traps.
	h.ok(t, invocation{path: "/echo"})
}

func (h *harness) memoryLimit(t *testing.T) {
	small := &commonv1.Limits{MemoryLimitBytes: 32 << 20}
	large := &commonv1.Limits{MemoryLimitBytes: 256 << 20}
	out := h.call(t, invocation{path: "/memhog", query: "mb=100", limits: small})
	wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED)
	// The same module on the same worker, with a larger limit, must fit.
	out = h.ok(t, invocation{path: "/memhog", query: "mb=100", limits: large})
	if out.summary.PeakMemoryBytes < 100<<20 {
		t.Errorf("peak_memory_bytes = %d, want >= 100 MiB", out.summary.PeakMemoryBytes)
	}
}

func (h *harness) responseLimit(t *testing.T) {
	lim := &commonv1.Limits{MaxResponseBytes: 64 << 10}
	out := h.call(t, invocation{path: "/big", query: "bytes=1048576", limits: lim})
	wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE)
	if out.head != nil || len(out.body) != 0 {
		t.Error("nothing of an over-limit response may be streamed")
	}
	out = h.ok(t, invocation{path: "/big", query: "bytes=16384", limits: lim})
	if len(out.body) < 16384 {
		t.Errorf("under the limit: got %d bytes", len(out.body))
	}
}

func (h *harness) abiViolations(t *testing.T) {
	for _, path := range []string{"/noseparator", "/empty"} {
		out := h.call(t, invocation{path: path})
		wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
	}
}

// malformedRequests: a request the worker cannot make sense of is the caller's
// bug, so it is invalid_argument with no reason (docs/protocol.md).
func (h *harness) malformedRequests(t *testing.T) {
	for _, name := range []string{"1BAD", "BAD-NAME", "BAD NAME", ""} {
		out := h.call(t, invocation{path: "/echo", env: []*commonv1.EnvVar{{Name: name, Value: "x"}}})
		if out.err == nil {
			t.Errorf("environment variable %q: expected invalid_argument, got success", name)
			continue
		}
		if got := connect.CodeOf(out.err); got != connect.CodeInvalidArgument {
			t.Errorf("environment variable %q: code = %v (%v), want invalid_argument", name, got, out.err)
		}
		if r, _ := reasonOf(out.err); r != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
			t.Errorf("environment variable %q: a malformed request carries no reason, got %v", name, r)
		}
	}
}

// httpMessages: the bytes of an HTTP message are carried as they are
// (docs/protocol.md, "HTTP messages").
func (h *harness) httpMessages(t *testing.T) {
	t.Run("request header values reach the guest as bytes", func(t *testing.T) {
		out := h.ok(t, invocation{path: "/env", clientHeaders: []*commonv1.Header{
			{Name: "x-latin", Value: []byte("caf\xe9")}, {Name: "x-junk", Value: []byte("\xff\xfe\x80")},
			{Name: "x-dup", Value: []byte("one")}, {Name: "x-dup", Value: []byte("\xe9two")}}})
		for _, want := range []string{"HTTP_X_LATIN=caf\xe9\n", "HTTP_X_JUNK=\xff\xfe\x80\n", "HTTP_X_DUP=one, \xe9two\n"} {
			if !bytes.Contains(out.body, []byte(want)) {
				t.Errorf("the guest's environment lacks %q:\n%q", want, out.body)
			}
		}
	})

	t.Run("the path reaches the guest as given, decoded once", func(t *testing.T) {
		// PATH_INFO is the path as the protocol carries it: decoded once, by the ingress, and not again.
		for _, path := range []string{"/pathinfo/caf\u00e9/\u65e5\u672c", "/pathinfo/a b", "/pathinfo/a%b", "/pathinfo/a%2Fb", "/pathinfo/a/./b", "/pathinfo//x", "/pathinfo/%41", "/pathinfo/a?b#c"} {
			if got := string(h.ok(t, invocation{path: path}).body); got != path {
				t.Errorf("PATH_INFO = %q, want %q", got, path)
			}
		}
	})

	t.Run("response header values are bytes, with surrounding whitespace trimmed", func(t *testing.T) {
		out := h.ok(t, invocation{path: "/rawheaders"})
		got := map[string][]string{}
		for _, hd := range out.head.GetHeaders() {
			got[hd.GetName()] = append(got[hd.GetName()], string(hd.GetValue()))
		}
		for name, want := range map[string][]string{"x-latin": {"caf\xe9"}, "x-junk": {"\xff\xfe\x80"}, "x-dup": {"one", "\xe9two"}, "x-nbsp": {"\u00a0v\u00a0"}} {
			if fmt.Sprintf("%q", got[name]) != fmt.Sprintf("%q", want) {
				t.Errorf("%s = %q, want %q", name, got[name], want)
			}
		}
	})

	t.Run("a control character in a response header is an ABI violation", func(t *testing.T) {
		wantReason(t, h.call(t, invocation{path: "/ctlheader"}).err, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
	})

	t.Run("a request that is not well formed is invalid_argument", func(t *testing.T) {
		for what, inv := range map[string]invocation{
			"no path":                           {path: ""},
			"a path without its slash":          {path: "echo"},
			"a NUL in the path":                 {path: "/echo\x00"},
			"a newline in the path":             {path: "/echo\n"},
			"a newline in the query":            {path: "/echo", query: "a\nb"},
			"a header with a space in its name": {path: "/echo", clientHeaders: []*commonv1.Header{{Name: "bad name", Value: []byte("x")}}},
			"a header value with CRLF":          {path: "/echo", clientHeaders: []*commonv1.Header{{Name: "x-a", Value: []byte("a\r\nb: c")}}},
			"a header value with NUL":           {path: "/echo", clientHeaders: []*commonv1.Header{{Name: "x-a", Value: []byte("a\x00b")}}},
		} {
			out := h.call(t, inv)
			if out.err == nil || connect.CodeOf(out.err) != connect.CodeInvalidArgument {
				t.Errorf("%s: %v, want invalid_argument", what, out.err)
			} else if r, _ := reasonOf(out.err); r != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
				t.Errorf("%s: carries reason %v, want none", what, r)
			}
		}
	})
}

// traceContext: the guest is given the platform's trace context, never the
// client's (docs/protocol.md, "Observability").
func (h *harness) traceContext(t *testing.T) {
	const callTrace, clientTrace = "4bf92f3577b34da6a3ce929d0e0e4736", "0af7651916cd43dd8448eb211c80319c"
	client := []*commonv1.Header{
		{Name: "traceparent", Value: []byte("00-" + clientTrace + "-b7ad6b7169203331-01")},
		{Name: "tracestate", Value: []byte("vendor=client")},
		{Name: "baggage", Value: []byte("user=somebody")},
	}

	// The call carries a trace: the guest is given that trace, with a span id of the worker's own.
	env := string(h.ok(t, invocation{path: "/env", traceparent: "00-" + callTrace + "-00f067aa0ba902b7-01", clientHeaders: client}).body)
	var tp string
	for _, line := range strings.Split(env, "\n") {
		if v, ok := strings.CutPrefix(line, "HTTP_TRACEPARENT="); ok {
			tp = v
		}
	}
	if parts := strings.Split(tp, "-"); len(parts) != 4 || parts[1] != callTrace {
		t.Errorf("HTTP_TRACEPARENT = %q: the guest is given the trace of the Execute call (trace id %s)", tp, callTrace)
	}
	if strings.Contains(env, clientTrace) || strings.Contains(env, "vendor=client") || strings.Contains(env, "HTTP_BAGGAGE") {
		t.Errorf("the guest was given trace context the client sent: it is replaced by the platform's:\n%s", env)
	}

	// The call carries none: the client's is still not handed on.
	env = string(h.ok(t, invocation{path: "/env", clientHeaders: client}).body)
	if strings.Contains(env, clientTrace) || strings.Contains(env, "vendor=client") || strings.Contains(env, "HTTP_BAGGAGE") {
		t.Errorf("with no trace in the call, the guest was given the client's:\n%s", env)
	}
}

func (h *harness) timeAndEntropy(t *testing.T) {
	a := h.ok(t, invocation{path: "/random"})
	b := h.ok(t, invocation{path: "/random"})
	if len(bytes.TrimSpace(a.body)) != 32 || bytes.Equal(a.body, b.body) {
		t.Errorf("guest randomness must be real and differ between invocations: %q %q", a.body, b.body)
	}
	before := time.Now().Unix()
	out := h.ok(t, invocation{path: "/time"})
	var got int64
	for _, c := range bytes.TrimSpace(out.body) {
		got = got*10 + int64(c-'0')
	}
	if d := got - before; d < -5 || d > 5 {
		t.Errorf("guest clock reads %s, %ds from real time", bytes.TrimSpace(out.body), d)
	}
	start := time.Now()
	h.ok(t, invocation{path: "/sleep", query: "ms=400"})
	if time.Since(start) < 400*time.Millisecond {
		t.Error("sleeping must really sleep")
	}
}

// --- Capacity --------------------------------------------------------------

func (h *harness) capacity(t *testing.T) {
	h.ok(t, invocation{path: "/echo"}) // warm
	// Only heartbeats sent after the sleepers have been launched may say that the
	// slots are busy. One sent while the warm-up request was still running reports
	// a full house that no longer exists (with a limit of 1 it is enough for the
	// probe below to be admitted), so first wait for a heartbeat that follows the
	// warm-up, and look only at those that follow that.
	h.orch.mu.Lock()
	n0 := len(h.orch.heartbeats)
	h.orch.mu.Unlock()
	h.orch.waitFor(t, "a heartbeat after the warm-up request", 10*time.Second, func() bool { return len(h.orch.heartbeats) > n0 })
	h.orch.mu.Lock()
	from := len(h.orch.heartbeats)
	h.orch.mu.Unlock()
	done := make(chan outcome, h.env.MaxConcurrency)
	for range h.env.MaxConcurrency {
		go func() { done <- h.call(t, invocation{path: "/sleep", query: "ms=1500"}) }()
	}
	h.orch.waitFor(t, "all slots to be reported busy", 10*time.Second, func() bool {
		for _, hb := range h.orch.heartbeats[min(from, len(h.orch.heartbeats)):] {
			if hb.InFlight == h.env.MaxConcurrency {
				return true
			}
		}
		return false
	})

	// Capacity is never queued: with every slot busy the answer is immediate.
	out := h.call(t, invocation{path: "/echo"})
	wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY)
	if out.elapsed > time.Second {
		t.Errorf("NO_CAPACITY took %v: workers must not queue", out.elapsed)
	}

	for range h.env.MaxConcurrency {
		if o := <-done; o.err != nil {
			t.Errorf("an admitted invocation failed: %v", o.err)
		}
	}
	h.ok(t, invocation{path: "/echo"}) // slots are released again
}

// --- Fleet: recovery and shutdown -----------------------------------------

func (h *harness) reregister(t *testing.T) {
	h.orch.mu.Lock()
	n := len(h.orch.registrations)
	last := h.orch.registrations[n-1].ID
	h.orch.mu.Unlock()

	h.orch.update(func() { h.orch.forget = last })
	h.orch.waitFor(t, "a new registration after reregister=true", 15*time.Second, func() bool { return len(h.orch.registrations) > n })

	h.orch.mu.Lock()
	next := h.orch.registrations[len(h.orch.registrations)-1]
	h.orch.mu.Unlock()
	if next.ID == last {
		t.Error("re-registration must yield a fresh worker id")
	}
	h.orch.waitFor(t, "heartbeats under the new id", 15*time.Second, func() bool {
		for _, hb := range h.orch.heartbeats {
			if hb.ID == next.ID {
				return true
			}
		}
		return false
	})
	h.reg = next
	h.ok(t, invocation{path: "/echo"}) // still serving
}

func (h *harness) gracefulShutdown(t *testing.T) {
	if h.stop == nil {
		t.Skip("the suite cannot stop this worker (see --stop-cmd), so it cannot see it drain and leave")
	}
	h.ok(t, invocation{path: "/echo"}) // warm
	inflight := make(chan outcome, 1)
	go func() { inflight <- h.call(t, invocation{path: "/sleep", query: "ms=1200"}) }()
	h.orch.waitFor(t, "the invocation to be in flight", 10*time.Second, func() bool {
		n := len(h.orch.heartbeats)
		return n > 0 && h.orch.heartbeats[n-1].InFlight == 1
	})

	stopped := make(chan struct{})
	go func() { h.stop(); close(stopped) }()

	h.orch.waitFor(t, "a DRAINING heartbeat", 10*time.Second, func() bool {
		n := len(h.orch.heartbeats)
		return n > 0 && h.orch.heartbeats[n-1].State == workerv1.WorkerState_WORKER_STATE_DRAINING
	})
	// A draining worker takes no new work, so the orchestrator can retry elsewhere.
	out := h.call(t, invocation{path: "/echo", timeout: 10 * time.Second})
	wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY)

	// The invocation that was already running is allowed to finish.
	if o := <-inflight; o.err != nil {
		t.Errorf("in-flight invocation was cut off by shutdown: %v", o.err)
	}
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not exit after draining")
	}
	h.orch.waitFor(t, "Deregister", 5*time.Second, func() bool { return contains(h.orch.deregistered, h.reg.ID) })
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
