package invoke

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/orchestrator/sched"
	"github.com/Olian04/edgerun/internal/orchestrator/snapshot"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
	"github.com/Olian04/edgerun/internal/telemetry/telemetrytest"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// --- fakes -----------------------------------------------------------------

type behaviour func(ctx context.Context, req *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error

// okBehaviour answers 200 with "ok:<path>" and a summary.
func okBehaviour(_ context.Context, req *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
	st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Head{Head: &commonv1.HttpResponseHead{
		Status: 201, Headers: []*commonv1.Header{{Name: "x-from", Value: "worker"}}}}})
	st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Chunk{Chunk: []byte("ok:")}})
	st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Chunk{Chunk: []byte(req.Request.Path)}})
	return st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Summary{Summary: &workerv1.ExecutionSummary{
		DurationMicros: 1234, PeakMemoryBytes: 4096, Stderr: []byte("guest log")}}})
}

func failWith(reason commonv1.ErrorReason, msg string) behaviour {
	return func(context.Context, *workerv1.ExecuteRequest, *connect.ServerStream[workerv1.ExecuteResponse]) error {
		return errs.New(reason, "%s", msg)
	}
}

type fakeWorker struct {
	workerv1connect.UnimplementedExecutionServiceHandler
	behave behaviour

	mu      sync.Mutex
	calls   []*workerv1.ExecuteRequest
	headers []http.Header
}

func (f *fakeWorker) Execute(ctx context.Context, req *connect.Request[workerv1.ExecuteRequest], st *connect.ServerStream[workerv1.ExecuteResponse]) error {
	f.mu.Lock()
	f.calls = append(f.calls, req.Msg)
	f.headers = append(f.headers, req.Header().Clone())
	f.mu.Unlock()
	return f.behave(ctx, req.Msg, st)
}

func (f *fakeWorker) callCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }

func startWorker(t *testing.T, b behaviour, execToken string) (string, *fakeWorker) {
	t.Helper()
	fw := &fakeWorker{behave: b}
	mux := http.NewServeMux()
	mux.Handle(workerv1connect.NewExecutionServiceHandler(fw,
		connect.WithInterceptors(authn.Server(authn.Verifier{execToken: "execute"}, "execute"))))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL, fw
}

func deadEndpoint(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // now nothing listens there: connections are refused
	return url
}

type source struct {
	routes  []store.RouteEntry
	workers []store.Worker
}

func (s *source) LoadRouting(context.Context) ([]store.RouteEntry, error) { return s.routes, nil }
func (s *source) ReadyNetworkServices(context.Context, time.Duration) ([]store.NetworkService, error) {
	return nil, nil
}

func (s *source) ReadyWorkers(context.Context, time.Duration) ([]store.Worker, error) {
	return s.workers, nil
}

type memRecorder struct {
	mu   sync.Mutex
	recs []*controlv1.Invocation
}

func (m *memRecorder) Record(i *controlv1.Invocation) {
	m.mu.Lock()
	m.recs = append(m.recs, i)
	m.mu.Unlock()
}
func (m *memRecorder) all() []*controlv1.Invocation {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*controlv1.Invocation(nil), m.recs...)
}

// env wires a Service to routes and workers and exposes it on a real Connect
// server, the way a frontend would reach it.
type env struct {
	t      *testing.T
	svc    *Service
	rec    *memRecorder
	url    string
	client *edgerun.Ingress
}

const (
	execToken    = "exec-secret"
	ingressToken = "ingress-secret"
)

func route(digest string) store.RouteEntry {
	return store.RouteEntry{
		RouteID: "route-1", HostPattern: "a.com", PathPrefix: "/", FunctionID: "fn-id", FunctionName: "fn",
		Version: &store.VersionRef{ID: "ver-id", Digest: digest, ABI: "wagi-1",
			Limits: &commonv1.Limits{MemoryLimitBytes: 1 << 26, MaxResponseBytes: 4096}},
		Env: []*commonv1.EnvVar{{Name: "MY_VAR", Value: "v1"}},
	}
}

func worker(endpoint string, abis ...string) store.Worker {
	if len(abis) == 0 {
		abis = []string{"wagi-1"}
	}
	return store.Worker{ID: "id-" + endpoint, Endpoint: endpoint, ABIs: abis, MaxConcurrency: 4}
}

func newEnv(t *testing.T, routes []store.RouteEntry, workers []store.Worker, tweak func(*Config)) *env {
	t.Helper()
	cache := snapshot.New(&source{routes: routes, workers: workers}, time.Minute, quiet)
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec := &memRecorder{}
	cfg := Config{Cache: cache, Recorder: rec, ExecutionToken: execToken, Log: quiet, DownFor: time.Minute}
	if tweak != nil {
		tweak(&cfg)
	}
	svc := New(cfg)
	mux := http.NewServeMux()
	mux.Handle(svc.Handler(authn.Verifier{ingressToken: IngressScope}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &env{t: t, svc: svc, rec: rec, url: srv.URL,
		client: edgerun.NewIngress(srv.URL, ingressToken, edgerun.WithHTTPClient(&http.Client{Transport: budgetTransport{}}))}
}

// budgetTransport lets a test give the orchestrator a short budget while the
// test client's own timer stays long: Connect derives the wire deadline from
// the client's context, which would make the two race. The budget travels in
// the call's context.
type budgetTransport struct{}

type budgetKey struct{}

func withBudget(ctx context.Context, d time.Duration) context.Context {
	return context.WithValue(ctx, budgetKey{}, d)
}

func (budgetTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if d, ok := r.Context().Value(budgetKey{}).(time.Duration); ok {
		r = r.Clone(r.Context())
		r.Header.Set("Connect-Timeout-Ms", strconv.FormatInt(d.Milliseconds(), 10))
	}
	return http.DefaultTransport.RoundTrip(r)
}

type result struct {
	head *ingressv1.InvokeHead
	body []byte
	err  error
}

func (e *env) invoke(host, path string, body string, budget time.Duration) result {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if budget > 0 {
		ctx = withBudget(ctx, budget)
	}
	res, err := e.client.Invoke(ctx, &ingressv1.InvokeRequest{
		RequestId: "req-1",
		Request: &commonv1.HttpRequest{Method: "POST", Host: host, Path: path, RawQuery: "q=1", Body: []byte(body),
			Headers: []*commonv1.Header{{Name: "x-in", Value: "1"}}},
	})
	if err != nil {
		return result{err: err}
	}
	out := result{head: &ingressv1.InvokeHead{Http: res.Head, InvocationId: res.ID}}
	out.body, out.err = res.ReadAll()
	return out
}

func wantReason(t *testing.T, err error, want commonv1.ErrorReason) *commonv1.ErrorInfo {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %v, got success", want)
	}
	info := errs.Info(err)
	if info == nil || info.Reason != want {
		t.Fatalf("reason = %v (%v), want %v", info.GetReason(), err, want)
	}
	if connect.CodeOf(err) != errs.Code(want) {
		t.Errorf("code = %v, want canonical %v", connect.CodeOf(err), errs.Code(want))
	}
	return info
}

// digestPreferring returns a digest for which want is the scheduler's first
// choice among endpoints.
func digestPreferring(t *testing.T, want string, endpoints ...string) string {
	t.Helper()
	var ws []store.Worker
	for _, e := range endpoints {
		ws = append(ws, worker(e))
	}
	for i := range 1000 {
		d := fmt.Sprintf("sha256:%064d", i)
		if sched.Order(d, ws)[0].Endpoint == want {
			return d
		}
	}
	t.Fatal("no digest prefers the wanted endpoint")
	return ""
}

// --- tests -----------------------------------------------------------------

func TestHappyPathForwardsEverythingAndRecords(t *testing.T) {
	ep, fw := startWorker(t, okBehaviour, execToken)
	e := newEnv(t, []store.RouteEntry{route("sha256:" + strings.Repeat("a", 64))}, []store.Worker{worker(ep)}, nil)

	res := e.invoke("A.com:8080", "/some/path", "hello", 0)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if res.head.Http.Status != 201 || res.head.Http.Headers[0].Name != "x-from" || string(res.body) != "ok:/some/path" {
		t.Errorf("response = %+v %q", res.head, res.body)
	}
	if res.head.InvocationId == "" {
		t.Error("the invocation id must reach the frontend")
	}

	// What the worker was asked.
	if fw.callCount() != 1 {
		t.Fatalf("worker calls = %d", fw.callCount())
	}
	got := fw.calls[0]
	if got.ArtifactDigest != "sha256:"+strings.Repeat("a", 64) || got.Abi != "wagi-1" || got.InvocationId != res.head.InvocationId ||
		got.Limits.MemoryLimitBytes != 1<<26 || got.Limits.MaxResponseBytes != 4096 ||
		len(got.Env) != 1 || got.Env[0].Name != "MY_VAR" || got.Env[0].Value != "v1" ||
		got.Request.Path != "/some/path" || got.Request.RawQuery != "q=1" || string(got.Request.Body) != "hello" ||
		got.Request.Host != "A.com:8080" || got.Request.Headers[0].Name != "x-in" {
		t.Errorf("ExecuteRequest = %+v", got)
	}
	if h := fw.headers[0].Get("Authorization"); h != "Bearer "+execToken {
		t.Errorf("worker credentials = %q", h)
	}
	if fw.headers[0].Get("Connect-Timeout-Ms") == "" {
		t.Error("the deadline must be propagated to the worker")
	}

	// What was recorded.
	recs := e.rec.all()
	if len(recs) != 1 {
		t.Fatalf("records = %d", len(recs))
	}
	r := recs[0]
	if r.Id != res.head.InvocationId || r.RequestId != "req-1" || r.FunctionId != "fn-id" || r.VersionId != "ver-id" ||
		r.WorkerId != "id-"+ep || r.Method != "POST" || r.Path != "/some/path" || r.Status != 201 ||
		r.ErrorReason != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED ||
		r.DurationMicros != 1234 || r.PeakMemoryBytes != 4096 || string(r.Stderr) != "guest log" || r.StartTime == nil {
		t.Errorf("record = %+v", r)
	}
}

func TestRoutingFailuresAreNotFoundAndNotRecorded(t *testing.T) {
	ep, fw := startWorker(t, okBehaviour, execToken)
	noVersion := route("x")
	noVersion.Version = nil
	noVersion.HostPattern = "nover.com"
	e := newEnv(t, []store.RouteEntry{route("sha256:" + strings.Repeat("a", 64)), noVersion}, []store.Worker{worker(ep)}, nil)

	wantReason(t, e.invoke("other.com", "/", "", 0).err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)
	info := wantReason(t, e.invoke("nover.com", "/", "", 0).err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)
	if !strings.Contains(info.Message, "no active version") {
		t.Errorf("message = %q", info.Message)
	}
	if fw.callCount() != 0 {
		t.Error("an unrouted request must not reach a worker")
	}
	if n := len(e.rec.all()); n != 0 {
		t.Errorf("%d records: unrouted requests are not recorded (a scan would flood the table)", n)
	}
}

func TestUnreadableEnvironmentIsInternalAndDoesNotLeak(t *testing.T) {
	ep, _ := startWorker(t, okBehaviour, execToken)
	r := route("sha256:" + strings.Repeat("a", 64))
	r.EnvErr = errors.New("cipher: message authentication failed (secret detail)")
	e := newEnv(t, []store.RouteEntry{r}, []store.Worker{worker(ep)}, nil)
	info := wantReason(t, e.invoke("a.com", "/", "", 0).err, commonv1.ErrorReason_ERROR_REASON_INTERNAL)
	if strings.Contains(info.Message, "cipher") || strings.Contains(info.Message, "secret detail") {
		t.Errorf("internal detail leaked: %q", info.Message)
	}
	if len(e.rec.all()) != 1 {
		t.Error("a routed invocation that failed must still be recorded")
	}
}

func TestRequestTooLarge(t *testing.T) {
	ep, fw := startWorker(t, okBehaviour, execToken)
	e := newEnv(t, []store.RouteEntry{route("sha256:" + strings.Repeat("a", 64))}, []store.Worker{worker(ep)},
		func(c *Config) { c.MaxRequestBodyBytes = 10 })
	wantReason(t, e.invoke("a.com", "/", strings.Repeat("x", 11), 0).err, commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE)
	if e.invoke("a.com", "/", strings.Repeat("x", 10), 0).err != nil {
		t.Error("a body exactly at the limit must be accepted")
	}
	if fw.callCount() != 1 {
		t.Errorf("worker calls = %d", fw.callCount())
	}
}

func TestNoWorkerAvailableVersusNoWorkerForTheABI(t *testing.T) {
	r := route("sha256:" + strings.Repeat("a", 64))

	none := newEnv(t, []store.RouteEntry{r}, nil, nil)
	wantReason(t, none.invoke("a.com", "/", "", 0).err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY)

	ep, fw := startWorker(t, okBehaviour, execToken)
	wrongABI := newEnv(t, []store.RouteEntry{r}, []store.Worker{worker(ep, "v8-1")}, nil)
	wantReason(t, wrongABI.invoke("a.com", "/", "", 0).err, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
	if fw.callCount() != 0 {
		t.Error("a worker that does not support the ABI must never be asked")
	}
}

func TestRetriesNoCapacityOnTheNextWorker(t *testing.T) {
	a, b := "A", "B"
	epA, fwA := startWorker(t, failWith(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, "all slots busy"), execToken)
	epB, fwB := startWorker(t, okBehaviour, execToken)
	_, _ = a, b
	digest := digestPreferring(t, epA, epA, epB)
	e := newEnv(t, []store.RouteEntry{route(digest)}, []store.Worker{worker(epA), worker(epB)}, nil)

	res := e.invoke("a.com", "/", "", 0)
	if res.err != nil || string(res.body) != "ok:/" {
		t.Fatalf("%v %q", res.err, res.body)
	}
	if fwA.callCount() != 1 || fwB.callCount() != 1 {
		t.Errorf("calls: preferred=%d next=%d, want 1 and 1", fwA.callCount(), fwB.callCount())
	}
	if r := e.rec.all()[0]; r.WorkerId != "id-"+epB || r.ErrorReason != 0 {
		t.Errorf("the record must name the worker that served it: %+v", r)
	}
}

// How often an invocation had to be moved is a distribution, like how many
// attempts it took: a histogram, with a point for every invocation (zero too),
// so that a percentile of it means something (docs/observability.md).
func TestRefusalsAndAttemptsAreHistogramsWithAPointPerInvocation(t *testing.T) {
	rec := telemetrytest.Install(t)
	epA, _ := startWorker(t, failWith(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, "all slots busy"), execToken)
	epB, _ := startWorker(t, okBehaviour, execToken)
	digest := digestPreferring(t, epA, epA, epB)
	e := newEnv(t, []store.RouteEntry{route(digest)}, []store.Worker{worker(epA), worker(epB)}, nil)
	if res := e.invoke("a.com", "/", "", 0); res.err != nil {
		t.Fatal(res.err)
	}

	m := rec.Metrics(t)
	hist := func(name string) metricdata.Histogram[int64] {
		h, ok := m[name].Data.(metricdata.Histogram[int64])
		if !ok {
			t.Fatalf("%s is %T, want an int64 histogram", name, m[name].Data)
		}
		return h
	}
	if a := hist("edgerun.invocation.attempts"); len(a.DataPoints) != 1 || a.DataPoints[0].Count != 1 || a.DataPoints[0].Sum != 2 {
		t.Errorf("attempts: %+v, want one invocation that took 2", a.DataPoints)
	}
	got := map[string]int64{}
	for _, dp := range hist("edgerun.invocation.refusals").DataPoints {
		kind, _ := dp.Attributes.Value("edgerun.refusal.kind")
		if dp.Count != 1 {
			t.Errorf("kind %s: %d points, want exactly one per invocation", kind.AsString(), dp.Count)
		}
		got[kind.AsString()] = dp.Sum
	}
	if len(got) != 2 || got["no_capacity"] != 1 || got["unreachable"] != 0 {
		t.Errorf("refusals by kind: %v, want no_capacity=1 and unreachable=0 (zero is recorded)", got)
	}
}

func TestGivesUpAfterMaxAttemptsWithNoCapacity(t *testing.T) {
	var eps []string
	var fws []*fakeWorker
	var ws []store.Worker
	for range 5 {
		ep, fw := startWorker(t, failWith(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, "busy"), execToken)
		eps, fws, ws = append(eps, ep), append(fws, fw), append(ws, worker(ep))
	}
	e := newEnv(t, []store.RouteEntry{route("sha256:" + strings.Repeat("a", 64))}, ws, nil) // MaxAttempts defaults to 3
	wantReason(t, e.invoke("a.com", "/", "", 0).err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY)
	total := 0
	for _, fw := range fws {
		total += fw.callCount()
	}
	if total != 3 {
		t.Errorf("attempts = %d, want exactly MaxAttempts (3)", total)
	}
	if rec := e.rec.all(); len(rec) != 1 || rec[0].ErrorReason != commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY {
		t.Errorf("one record for the invocation, not one per attempt: %+v", rec)
	}
}

func TestNeverRetriesWhenTheFunctionMayHaveRun(t *testing.T) {
	for name, tc := range map[string]struct {
		reason commonv1.ErrorReason
		msg    string
	}{
		"trapped":  {commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, "guest exited with code 3\nstderr (tail):\nboom"},
		"deadline": {commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, "deadline exceeded while running guest"},
		"internal": {commonv1.ErrorReason_ERROR_REASON_INTERNAL, "something odd"},
		"too big":  {commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE, "guest output exceeded 4096 bytes"},
		"abi":      {commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH, "malformed wagi-1 response"},
	} {
		t.Run(name, func(t *testing.T) {
			epA, fwA := startWorker(t, failWith(tc.reason, tc.msg), execToken)
			epB, fwB := startWorker(t, okBehaviour, execToken)
			digest := digestPreferring(t, epA, epA, epB)
			e := newEnv(t, []store.RouteEntry{route(digest)}, []store.Worker{worker(epA), worker(epB)}, nil)

			info := wantReason(t, e.invoke("a.com", "/", "", 0).err, tc.reason)
			if info.Message != tc.msg {
				t.Errorf("the worker's message must pass through for operators: %q", info.Message)
			}
			if fwA.callCount() != 1 || fwB.callCount() != 0 {
				t.Errorf("calls: %d and %d; a function that may have run must not be run again", fwA.callCount(), fwB.callCount())
			}
			r := e.rec.all()[0]
			if r.ErrorReason != tc.reason || string(r.Stderr) != tc.msg {
				t.Errorf("record = reason %v stderr %q", r.ErrorReason, r.Stderr)
			}
		})
	}
}

func TestUnreachableWorkerIsRetriedAndSkippedAfterwards(t *testing.T) {
	dead := deadEndpoint(t)
	live, fw := startWorker(t, okBehaviour, execToken)
	digest := digestPreferring(t, dead, dead, live)
	e := newEnv(t, []store.RouteEntry{route(digest)}, []store.Worker{worker(dead), worker(live)}, nil)

	if res := e.invoke("a.com", "/", "", 0); res.err != nil || string(res.body) != "ok:/" {
		t.Fatalf("a dead worker must not fail the request: %v", res.err)
	}
	if !e.svc.isDown(dead) {
		t.Error("an unreachable worker must be remembered as down")
	}
	if e.svc.isDown(live) {
		t.Error("the live worker must not be marked down")
	}
	// Next request goes straight to the live worker.
	e.invoke("a.com", "/", "", 0)
	if fw.callCount() != 2 {
		t.Errorf("live worker calls = %d", fw.callCount())
	}
}

func TestDownMarkExpires(t *testing.T) {
	e := newEnv(t, nil, nil, func(c *Config) { c.DownFor = 50 * time.Millisecond })
	e.svc.markDown("http://x")
	if !e.svc.isDown("http://x") {
		t.Fatal("not marked")
	}
	time.Sleep(80 * time.Millisecond)
	if e.svc.isDown("http://x") {
		t.Error("the down mark must expire so a recovered worker is used again")
	}
}

func TestAllWorkersUnreachableIsNoCapacity(t *testing.T) {
	d1, d2 := deadEndpoint(t), deadEndpoint(t)
	e := newEnv(t, []store.RouteEntry{route("sha256:" + strings.Repeat("a", 64))}, []store.Worker{worker(d1), worker(d2)}, nil)
	wantReason(t, e.invoke("a.com", "/", "", 0).err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY)
	// And now that both are marked down, the answer is immediate.
	wantReason(t, e.invoke("a.com", "/", "", 0).err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY)
}

func TestFailureAfterTheHeadIsForwardedAndNeverRetried(t *testing.T) {
	// Including NO_CAPACITY, which is retryable *before* the head: once bytes
	// have gone out, a retry would send the frontend a second head and a
	// duplicated body, so no error is retryable any more.
	for _, reason := range []commonv1.ErrorReason{
		commonv1.ErrorReason_ERROR_REASON_INTERNAL,
		commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY,
	} {
		t.Run(reason.String(), func(t *testing.T) {
			broken := func(_ context.Context, _ *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
				st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Head{Head: &commonv1.HttpResponseHead{Status: 200}}})
				st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Chunk{Chunk: []byte("partial")}})
				return errs.New(reason, "worker died")
			}
			epA, fwA := startWorker(t, broken, execToken)
			epB, fwB := startWorker(t, okBehaviour, execToken)
			digest := digestPreferring(t, epA, epA, epB)
			e := newEnv(t, []store.RouteEntry{route(digest)}, []store.Worker{worker(epA), worker(epB)}, nil)

			res := e.invoke("a.com", "/", "", 0)
			wantReason(t, res.err, reason)
			if res.head == nil || string(res.body) != "partial" {
				t.Errorf("what was already sent must have been forwarded, once: %+v %q", res.head, res.body)
			}
			if fwA.callCount() != 1 || fwB.callCount() != 0 {
				t.Errorf("calls %d/%d: after the head, never retry", fwA.callCount(), fwB.callCount())
			}
			if r := e.rec.all()[0]; r.Status != 200 || r.ErrorReason != reason {
				t.Errorf("the record keeps the status that was sent and the failure: %+v", r)
			}
		})
	}
}

func TestWorkerProtocolViolationsAreInternal(t *testing.T) {
	for name, b := range map[string]behaviour{
		"no summary": func(_ context.Context, _ *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
			return st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Head{Head: &commonv1.HttpResponseHead{Status: 200}}})
		},
		"empty stream": func(context.Context, *workerv1.ExecuteRequest, *connect.ServerStream[workerv1.ExecuteResponse]) error {
			return nil
		},
		"chunk first": func(_ context.Context, _ *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
			return st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Chunk{Chunk: []byte("x")}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			ep, _ := startWorker(t, b, execToken)
			e := newEnv(t, []store.RouteEntry{route("sha256:" + strings.Repeat("a", 64))}, []store.Worker{worker(ep)}, nil)
			wantReason(t, e.invoke("a.com", "/", "", 0).err, commonv1.ErrorReason_ERROR_REASON_INTERNAL)
		})
	}
}

func TestOurOwnDeadlineIsDeadlineExceeded(t *testing.T) {
	hang := func(ctx context.Context, _ *workerv1.ExecuteRequest, _ *connect.ServerStream[workerv1.ExecuteResponse]) error {
		<-ctx.Done()
		return ctx.Err()
	}
	ep, _ := startWorker(t, hang, execToken)
	e := newEnv(t, []store.RouteEntry{route("sha256:" + strings.Repeat("a", 64))}, []store.Worker{worker(ep)}, nil)

	start := time.Now()
	res := e.invoke("a.com", "/", "", 300*time.Millisecond)
	wantReason(t, res.err, commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED)
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("took %v for a 300ms budget", d)
	}
	if r := e.rec.all(); len(r) != 1 || r[0].ErrorReason != commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED {
		t.Errorf("records = %+v", r)
	}
}

func TestWorkersBareDeadlineErrorIsDeadlineExceededEvenBeforeOurTimer(t *testing.T) {
	// Deterministic version of a race seen under load: the worker's deadline
	// error (no ErrorInfo) can arrive before this process's own timer has
	// fired. The orchestrator's context is nowhere near expiry here.
	bare := func(context.Context, *workerv1.ExecuteRequest, *connect.ServerStream[workerv1.ExecuteResponse]) error {
		return connect.NewError(connect.CodeDeadlineExceeded, errors.New("context deadline exceeded"))
	}
	ep, fw := startWorker(t, bare, execToken)
	e := newEnv(t, []store.RouteEntry{route("sha256:" + strings.Repeat("a", 64))}, []store.Worker{worker(ep)}, nil)
	res := e.invoke("a.com", "/", "", 0)
	wantReason(t, res.err, commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED)
	if fw.callCount() != 1 {
		t.Errorf("a deadline error must not be retried: %d calls", fw.callCount())
	}
}

func TestDefaultTimeoutWhenNoDeadlineIsSent(t *testing.T) {
	hang := func(ctx context.Context, _ *workerv1.ExecuteRequest, _ *connect.ServerStream[workerv1.ExecuteResponse]) error {
		<-ctx.Done()
		return ctx.Err()
	}
	ep, _ := startWorker(t, hang, execToken)
	e := newEnv(t, []store.RouteEntry{route("sha256:" + strings.Repeat("a", 64))}, []store.Worker{worker(ep)},
		func(c *Config) { c.DefaultTimeout = 200 * time.Millisecond })
	// A request carrying no Connect-Timeout-Ms: the orchestrator must still bound it.
	res, err := e.client.Invoke(context.Background(), &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: "a.com", Path: "/"}})
	if err == nil {
		_, err = res.ReadAll()
	}
	wantReason(t, err, commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED)
}

func TestWorkerCredentialProblemsDoNotLeakToTheFrontend(t *testing.T) {
	ep, _ := startWorker(t, okBehaviour, "a-different-secret") // the orchestrator presents execToken
	e := newEnv(t, []store.RouteEntry{route("sha256:" + strings.Repeat("a", 64))}, []store.Worker{worker(ep)}, nil)
	info := wantReason(t, e.invoke("a.com", "/", "", 0).err, commonv1.ErrorReason_ERROR_REASON_INTERNAL)
	for _, leak := range []string{"token", "bearer", "unauthenticated", ep} {
		if strings.Contains(strings.ToLower(info.Message), leak) {
			t.Errorf("message %q leaks %q", info.Message, leak)
		}
	}
}

func TestIngressAuthentication(t *testing.T) {
	ep, fw := startWorker(t, okBehaviour, execToken)
	e := newEnv(t, []store.RouteEntry{route("sha256:" + strings.Repeat("a", 64))}, []store.Worker{worker(ep)}, nil)
	for name, tok := range map[string]string{"none": "", "wrong": "nope", "the execution token": execToken} {
		res, err := edgerun.NewIngress(e.url, tok).Invoke(context.Background(), &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: "a.com", Path: "/"}})
		if err == nil {
			_, err = res.ReadAll()
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("%s: %v", name, err)
		}
	}
	if fw.callCount() != 0 {
		t.Error("unauthenticated requests must not reach workers")
	}
	// Describe is guarded too, and reports the limit the frontend must respect.
	d, err := e.client.Describe(context.Background(), &ingressv1.DescribeRequest{})
	if err != nil || d.MaxRequestBodyBytes != 6<<20 || d.Implementation.Name == "" {
		t.Errorf("Describe: %+v %v", d, err)
	}
}

func TestConcurrentInvocationsAcrossWorkers(t *testing.T) {
	var ws []store.Worker
	for range 3 {
		ep, _ := startWorker(t, okBehaviour, execToken)
		ws = append(ws, worker(ep))
	}
	e := newEnv(t, []store.RouteEntry{route("sha256:" + strings.Repeat("a", 64))}, ws, nil)
	var wg sync.WaitGroup
	for i := range 60 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			path := "/p" + strconv.Itoa(i)
			res := e.invoke("a.com", path, "", 0)
			if res.err != nil || string(res.body) != "ok:"+path {
				t.Errorf("%s: %v %q", path, res.err, res.body)
			}
		}()
	}
	wg.Wait()
	if n := len(e.rec.all()); n != 60 {
		t.Errorf("records = %d, want 60", n)
	}
}

// A caller whose own timer is the invocation's budget gives up at about the
// moment the orchestrator would have: the record must still say the budget ran
// out, not leave the reason empty.
func TestACallerThatGivesUpAtTheDeadlineIsRecordedAsTheDeadline(t *testing.T) {
	hang := func(ctx context.Context, _ *workerv1.ExecuteRequest, _ *connect.ServerStream[workerv1.ExecuteResponse]) error {
		<-ctx.Done()
		return ctx.Err()
	}
	ep, _ := startWorker(t, hang, execToken)
	e := newEnv(t, []store.RouteEntry{route("sha256:" + strings.Repeat("a", 64))}, []store.Worker{worker(ep)}, nil)
	const runs = 6
	for i := 0; i < runs; i++ {
		// The orchestrator is told 400ms; the caller leaves at 390.
		ctx, cancel := context.WithTimeout(withBudget(context.Background(), 400*time.Millisecond), 390*time.Millisecond)
		res, err := e.client.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: "a.com", Path: "/"}})
		if err == nil {
			res.ReadAll()
		}
		cancel()
	}
	eventually := func() []*controlv1.Invocation {
		deadline := time.Now().Add(5 * time.Second)
		for {
			if recs := e.rec.all(); len(recs) >= runs || time.Now().After(deadline) {
				return recs
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	recs := eventually()
	if len(recs) != runs {
		t.Fatalf("%d records, want %d", len(recs), runs)
	}
	for i, r := range recs {
		if r.ErrorReason != commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED {
			t.Errorf("record %d: reason %v, want DEADLINE_EXCEEDED: the budget was spent", i, r.ErrorReason)
		}
	}
}

// A worker that dies mid-call is reported by the transport as "cancelled". That
// is only the caller's cancellation if the caller's context has ended: otherwise
// it is a failure of the worker, and must be answered as one (the frontend
// reads "cancelled" as "nobody is waiting" and writes nothing).
func TestAWorkerThatDiesIsNotTheCallersCancellation(t *testing.T) {
	e := newEnv(t, nil, nil, nil)
	reset := connect.NewError(connect.CodeCanceled, errors.New(`read tcp 127.0.0.1:1->127.0.0.1:2: read: connection reset by peer`))

	err := e.svc.translate(context.Background(), reset)
	if got := errs.Reason(err); got != commonv1.ErrorReason_ERROR_REASON_INTERNAL {
		t.Errorf("a cancellation while the caller is waiting: reason %v (%v), want INTERNAL", got, err)
	}
	if connect.CodeOf(err) == connect.CodeCanceled {
		t.Error("it must not be passed on as cancelled")
	}

	gone, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.svc.translate(gone, reset); connect.CodeOf(err) != connect.CodeCanceled {
		t.Errorf("a caller that has gone: %v, want cancelled", err)
	}
}
