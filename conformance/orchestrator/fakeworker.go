package orchestratorconf

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
)

// behaviour is what a fake worker does with an Execute call.
type behaviour func(ctx context.Context, req *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error

// received is one Execute call as the worker saw it.
type received struct {
	req    *workerv1.ExecuteRequest
	header http.Header
}

// fakeWorker is a worker written from the protocol alone: it serves
// ExecutionService, registers with the orchestrator under test, and keeps
// heartbeating. What it answers is whatever the test says.
type fakeWorker struct {
	t   testing.TB
	h   *harness
	srv *check.Server

	mu      sync.Mutex
	id      string
	got     []received
	behave  behaviour
	abis    []string
	state   workerv1.WorkerState
	paused  bool
	stopped chan struct{}
	done    sync.WaitGroup
}

// worker starts a fake worker that supports abis, registers it, and removes it
// when the test ends.
func (h *harness) worker(t testing.TB, abis []string, b behaviour) *fakeWorker {
	t.Helper()
	w := &fakeWorker{t: t, h: h, behave: b, abis: abis, state: workerv1.WorkerState_WORKER_STATE_READY, stopped: make(chan struct{})}
	mux := http.NewServeMux()
	mux.Handle(workerv1connect.NewExecutionServiceHandler(execution{w}))
	w.srv = h.net.Serve(t, mux, false)
	interval := w.register()
	w.done.Add(1)
	go w.heartbeat(interval)
	t.Cleanup(w.stop)
	return w
}

func (w *fakeWorker) info() *workerv1.WorkerInfo {
	return &workerv1.WorkerInfo{
		Implementation: &commonv1.Implementation{Name: "conformance-worker", Version: "1", Protocols: []string{"edgerun.worker.v1"}},
		Abis:           w.abis, MaxConcurrency: 4,
	}
}

func (w *fakeWorker) register() time.Duration {
	ctx, cancel := ctxT(settle)
	defer cancel()
	r, err := w.h.fleet(w.h.env.WorkerToken).Register(ctx, connect.NewRequest(&workerv1.RegisterRequest{Endpoint: w.srv.URL, Info: w.info()}))
	if err != nil {
		w.t.Fatalf("Register: %v", err)
	}
	w.mu.Lock()
	w.id = r.Msg.GetWorkerId()
	w.mu.Unlock()
	if w.id == "" {
		w.t.Fatal("Register returned no worker id")
	}
	d := r.Msg.GetHeartbeatInterval().AsDuration()
	if d <= 0 {
		w.t.Fatal("Register returned no heartbeat interval")
	}
	return d
}

func (w *fakeWorker) heartbeat(every time.Duration) {
	defer w.done.Done()
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-w.stopped:
			return
		case <-tick.C:
		}
		w.mu.Lock()
		id, state, paused := w.id, w.state, w.paused
		w.mu.Unlock()
		if paused {
			continue
		}
		ctx, cancel := ctxT(5 * time.Second)
		r, err := w.h.fleet(w.h.env.WorkerToken).Heartbeat(ctx, connect.NewRequest(&workerv1.HeartbeatRequest{WorkerId: id, State: state}))
		cancel()
		if err == nil && r.Msg.GetReregister() {
			w.register()
		}
	}
}

func (w *fakeWorker) stop() {
	select {
	case <-w.stopped:
		return
	default:
	}
	close(w.stopped)
	w.done.Wait()
	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()
	w.mu.Lock()
	id := w.id
	w.mu.Unlock()
	w.h.fleet(w.h.env.WorkerToken).Deregister(ctx, connect.NewRequest(&workerv1.DeregisterRequest{WorkerId: id}))
	w.srv.Close()
}

// pause stops heartbeating, as a crashed or partitioned worker does.
func (w *fakeWorker) pause(p bool) {
	w.mu.Lock()
	w.paused = p
	w.mu.Unlock()
}

// setState is what the worker reports on its next heartbeats.
func (w *fakeWorker) setState(s workerv1.WorkerState) {
	w.mu.Lock()
	w.state = s
	w.mu.Unlock()
}

// workerID is the id the orchestrator currently knows the worker by.
func (w *fakeWorker) workerID() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.id
}

// closeListener makes the endpoint unreachable while the worker keeps
// heartbeating: the connection is refused, so a request cannot have run.
func (w *fakeWorker) closeListener() { w.srv.Close() }

func (w *fakeWorker) calls() []received {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]received(nil), w.got...)
}

func (w *fakeWorker) callCount() int { return len(w.calls()) }

type execution struct{ w *fakeWorker }

func (e execution) Describe(context.Context, *connect.Request[workerv1.DescribeRequest]) (*connect.Response[workerv1.DescribeResponse], error) {
	return connect.NewResponse(&workerv1.DescribeResponse{Info: e.w.info()}), nil
}

func (e execution) Execute(ctx context.Context, req *connect.Request[workerv1.ExecuteRequest], st *connect.ServerStream[workerv1.ExecuteResponse]) error {
	w := e.w
	if req.Header().Get("Authorization") != "Bearer "+w.h.env.ExecutionToken {
		return connect.NewError(connect.CodeUnauthenticated, nil)
	}
	w.mu.Lock()
	w.got = append(w.got, received{req: req.Msg, header: req.Header().Clone()})
	b := w.behave
	w.mu.Unlock()
	return b(ctx, req.Msg, st)
}

// --- behaviours -----------------------------------------------------------------

func send(st *connect.ServerStream[workerv1.ExecuteResponse], part *workerv1.ExecuteResponse) error {
	return st.Send(part)
}

func head(status uint32, headers ...*commonv1.Header) *workerv1.ExecuteResponse {
	return &workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Head{Head: &commonv1.HttpResponseHead{Status: status, Headers: headers}}}
}

func chunk(b string) *workerv1.ExecuteResponse {
	return &workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Chunk{Chunk: []byte(b)}}
}

func summary(stderr string) *workerv1.ExecuteResponse {
	return &workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Summary{Summary: &workerv1.ExecutionSummary{
		DurationMicros: 1234, PeakMemoryBytes: 1 << 20, Stderr: []byte(stderr),
	}}}
}

func hdr(name, value string) *commonv1.Header { return &commonv1.Header{Name: name, Value: value} }

// respond answers 200 with body.
func respond(body string) behaviour { return respondWith(200, body, "", nil) }

// respondWith answers with the given status, body, stderr and headers.
func respondWith(status uint32, body, stderr string, headers []*commonv1.Header) behaviour {
	return func(_ context.Context, _ *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
		if err := send(st, head(status, headers...)); err != nil {
			return err
		}
		if body != "" {
			if err := send(st, chunk(body)); err != nil {
				return err
			}
		}
		return send(st, summary(stderr))
	}
}

// echoEnv answers with the value of one environment variable it was given.
func echoEnv(name string) behaviour {
	return func(_ context.Context, req *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
		val := ""
		for _, e := range req.GetEnv() {
			if e.GetName() == name {
				val = e.GetValue()
			}
		}
		if err := send(st, head(200)); err != nil {
			return err
		}
		if err := send(st, chunk(val)); err != nil {
			return err
		}
		return send(st, summary(""))
	}
}

// fail answers with a platform error before any response.
func fail(reason commonv1.ErrorReason, message string) behaviour {
	return func(context.Context, *workerv1.ExecuteRequest, *connect.ServerStream[workerv1.ExecuteResponse]) error {
		return check.Error(reason, message)
	}
}

// headThenFail sends a response head and then fails, as a worker that dies
// mid-response does.
func headThenFail(reason commonv1.ErrorReason) behaviour {
	return func(_ context.Context, _ *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
		if err := send(st, head(200)); err != nil {
			return err
		}
		if err := send(st, chunk("partial")); err != nil {
			return err
		}
		return check.Error(reason, "failed after the head")
	}
}
