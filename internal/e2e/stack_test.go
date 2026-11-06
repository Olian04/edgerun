// Package e2e runs the whole system: a real orchestrator on a real PostgreSQL,
// real workers running real WebAssembly, and a test that plays both the
// operator (control API) and the frontend (ingress API).
package e2e

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Olian04/edgerun"
	"github.com/Olian04/edgerun/conformance/guest"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/internal/errs"
	orchapp "github.com/Olian04/edgerun/internal/orchestrator/app"
	"github.com/Olian04/edgerun/internal/orchestrator/orchtest"
	"github.com/Olian04/edgerun/internal/testpg"
	workerapp "github.com/Olian04/edgerun/internal/worker/app"
)

func TestMain(m *testing.M) { testpg.Main(m) }

const (
	ingressToken   = "ingress-token"
	workerToken    = "worker-token"
	adminToken     = "admin-token"
	executionToken = "execution-token"
	networkToken   = "network-token"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// stack is a running orchestrator plus the clients to drive it.
type stack struct {
	t    *testing.T
	dsn  string
	addr string
	orch *orchapp.Server

	ctl     *edgerun.Control
	ingress *edgerun.Ingress
	wasm    []byte
	// workerLog, if set, is the logger workers started afterwards use.
	workerLog *slog.Logger
}

func orchConfig(dsn, listen string) orchapp.Config {
	return orchapp.Config{
		Listen: listen, DatabaseURL: dsn,
		IngressToken: ingressToken, WorkerToken: workerToken, AdminToken: adminToken, ExecutionToken: executionToken, NetworkToken: networkToken,
		SecretKey:         orchtest.Key,
		HeartbeatInterval: 200 * time.Millisecond, RefreshEvery: 100 * time.Millisecond,
		MaxRequestBodyBytes: 4 << 20, ShutdownTimeout: 20 * time.Second, Log: quiet,
	}
}

func newStack(t *testing.T) *stack {
	t.Helper()
	s := &stack{t: t, dsn: testpg.DSN(t), wasm: guest.Build(t)}
	s.startOrchestrator("127.0.0.1:0")
	t.Cleanup(func() { s.orch.Stop(context.Background()) })
	return s
}

func (s *stack) startOrchestrator(listen string) {
	s.t.Helper()
	srv, err := orchapp.Start(context.Background(), orchConfig(s.dsn, listen))
	if err != nil {
		s.t.Fatal(err)
	}
	s.orch, s.addr = srv, srv.Addr()
	url := "http://" + s.addr
	s.ctl = edgerun.NewControl(url, adminToken)
	s.ingress = edgerun.NewIngress(url, ingressToken, edgerun.WithHTTPClient(&http.Client{Transport: budgetTransport{}}))
}

// budgetTransport gives the orchestrator a short budget while the test's own
// timer stays long (Connect would otherwise make the two the same value). The
// budget travels in the call's context.
type budgetTransport struct{}

type budgetKey struct{}

func (budgetTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if d, ok := r.Context().Value(budgetKey{}).(time.Duration); ok {
		r = r.Clone(r.Context())
		r.Header.Set("Connect-Timeout-Ms", strconv.FormatInt(d.Milliseconds(), 10))
	}
	return http.DefaultTransport.RoundTrip(r)
}

// worker is a real worker process-equivalent (app.Run) that can be stopped
// gracefully, the way SIGTERM stops cmd/worker.
type worker struct {
	stop func()
}

func (s *stack) startWorker(maxConcurrency uint32) *worker {
	s.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- workerapp.Run(ctx, workerapp.Config{
			Listen: "127.0.0.1:0", OrchestratorURL: "http://" + s.addr,
			WorkerToken: workerToken, ExecutionToken: executionToken,
			MaxConcurrency: maxConcurrency, DrainTimeout: 10 * time.Second, Log: s.logOr(quiet),
		})
	}()
	var once sync.Once
	w := &worker{stop: func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					s.t.Errorf("worker exited with error: %v", err)
				}
			case <-time.After(60 * time.Second):
				s.t.Error("worker did not exit")
			}
		})
	}}
	s.t.Cleanup(w.stop)
	return w
}

var ctx = context.Background()

func (s *stack) createFunction(name string) string {
	s.t.Helper()
	r, err := s.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: name})
	if err != nil {
		s.t.Fatal(err)
	}
	return r.Function.Id
}

func (s *stack) uploadVersion(fid string, module []byte, limits *commonv1.Limits, activate bool) string {
	s.t.Helper()
	r, err := s.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{
		FunctionId: fid, Module: module, Abi: "wagi-1", Limits: limits, Activate: activate})
	if err != nil {
		s.t.Fatal(err)
	}
	return r.Version.Id
}

func (s *stack) route(host, prefix, fid string) {
	s.t.Helper()
	if _, err := s.ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: prefix, FunctionId: fid}); err != nil {
		s.t.Fatal(err)
	}
}

func (s *stack) setEnv(fid, name, value string, secret bool) {
	s.t.Helper()
	if _, err := s.ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fid,
		Entry: &controlv1.EnvEntry{Name: name, Value: value, Secret: secret}}); err != nil {
		s.t.Fatal(err)
	}
}

// result is what a frontend would get back.
type result struct {
	head    *ingressv1.InvokeHead
	body    []byte
	err     error
	elapsed time.Duration
}

func (r result) status() uint32 { return r.head.GetHttp().GetStatus() }

type req struct {
	host, path, query, method, body string
	budget                          time.Duration
}

func (s *stack) call(r req) result {
	s.t.Helper()
	if r.method == "" {
		r.method = "GET"
	}
	if r.host == "" {
		r.host = "e2e.test"
	}
	cctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	if r.budget > 0 {
		cctx = context.WithValue(cctx, budgetKey{}, r.budget)
	}
	start := time.Now()
	res, err := s.ingress.Invoke(cctx, &ingressv1.InvokeRequest{
		RequestId: "e2e-" + s.t.Name(),
		Request: &commonv1.HttpRequest{Method: r.method, Host: r.host, Path: r.path, RawQuery: r.query, Body: []byte(r.body),
			Headers: []*commonv1.Header{{Name: "content-type", Value: []byte("text/x-e2e")}}},
	})
	if err != nil {
		return result{err: err, elapsed: time.Since(start)}
	}
	out := result{head: &ingressv1.InvokeHead{Http: res.Head, InvocationId: res.ID}}
	out.body, out.err = res.ReadAll()
	out.elapsed = time.Since(start)
	return out
}

func (s *stack) mustCall(r req) result {
	s.t.Helper()
	res := s.call(r)
	if res.err != nil {
		s.t.Fatalf("%s %s: %v", r.method, r.path, res.err)
	}
	return res
}

func reason(err error) commonv1.ErrorReason { return errs.Reason(err) }

// eventually polls cond for up to timeout.
func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

// serving waits until the orchestrator can route to a live worker.
func (s *stack) waitServing(host, path string) {
	s.t.Helper()
	eventually(s.t, "the stack to serve "+host+path, 120*time.Second, func() bool {
		r := s.call(req{host: host, path: path})
		return r.err == nil
	})
}

func (s *stack) aliveWorkers() int {
	r, err := s.ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
	if err != nil {
		return -1
	}
	n := 0
	for _, w := range r.Workers {
		if w.Alive {
			n++
		}
	}
	return n
}

func (s *stack) invocations(fid string) []*controlv1.Invocation {
	s.t.Helper()
	r, err := s.ctl.ListInvocations(ctx, &controlv1.ListInvocationsRequest{FunctionId: fid, PageSize: 500})
	if err != nil {
		s.t.Fatal(err)
	}
	return r.Invocations
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func testpgDSN(t *testing.T) string { t.Helper(); return testpg.DSN(t) }
func wasmBytes(t *testing.T) []byte { t.Helper(); return guest.Build(t) }

func (s *stack) logOr(l *slog.Logger) *slog.Logger {
	if s.workerLog != nil {
		return s.workerLog
	}
	return l
}
