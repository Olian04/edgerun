package e2e

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
)

// staticWorker is a complete, independent worker implementation: it is built
// from the generated protocol code only and shares nothing with the real
// worker (no wazero, no internal/worker). It speaks a different guest ABI,
// "static-1", whose "module" is simply the response body. It exists to prove
// the architecture's central promise: the orchestrator and frontend cannot
// tell it from any other worker, because the protocol is all they know.
type staticWorker struct {
	workerv1connect.UnimplementedExecutionServiceHandler
	t       *testing.T
	orchURL string
	srv     *httptest.Server
	fleet   *edgerun.Fleet
	art     *edgerun.Artifacts

	mu       sync.Mutex
	workerID string
	served   int
	stopped  chan struct{}
	once     sync.Once
}

func (sw *staticWorker) Describe(_ context.Context, req *connect.Request[workerv1.DescribeRequest]) (*connect.Response[workerv1.DescribeResponse], error) {
	if req.Header().Get("Authorization") != "Bearer "+executionToken {
		return nil, connect.NewError(connect.CodeUnauthenticated, nil)
	}
	return connect.NewResponse(&workerv1.DescribeResponse{Info: sw.info()}), nil
}

func (sw *staticWorker) info() *workerv1.WorkerInfo {
	return &workerv1.WorkerInfo{
		Implementation: &commonv1.Implementation{Name: "static-worker", Version: "1", Protocols: []string{"edgerun.worker.v1"}},
		Abis:           []string{"static-1"}, MaxConcurrency: 8,
	}
}

func (sw *staticWorker) Execute(ctx context.Context, req *connect.Request[workerv1.ExecuteRequest], st *connect.ServerStream[workerv1.ExecuteResponse]) error {
	if req.Header().Get("Authorization") != "Bearer "+executionToken {
		return connect.NewError(connect.CodeUnauthenticated, nil)
	}
	if req.Msg.Abi != "static-1" {
		return reasoned(commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH, connect.CodeFailedPrecondition, "only static-1")
	}
	// Fetch the "module" from the orchestrator, as any worker does.
	body, err := sw.art.GetArtifact(ctx, req.Msg.ArtifactDigest)
	if edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND) {
		return reasoned(commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND, connect.CodeNotFound, "artifact")
	}
	if err != nil {
		return reasoned(commonv1.ErrorReason_ERROR_REASON_INTERNAL, connect.CodeInternal, "artifact: "+err.Error())
	}
	sw.mu.Lock()
	sw.served++
	sw.mu.Unlock()
	st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Head{Head: &commonv1.HttpResponseHead{Status: 200,
		Headers: []*commonv1.Header{{Name: "content-type", Value: "text/plain"}, {Name: "x-served-by", Value: "static-worker"}}}}})
	st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Chunk{Chunk: body}})
	return st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Summary{Summary: &workerv1.ExecutionSummary{DurationMicros: 1}}})
}

func reasoned(reason commonv1.ErrorReason, code connect.Code, msg string) error {
	err := connect.NewError(code, nil)
	if d, derr := connect.NewErrorDetail(&commonv1.ErrorInfo{Reason: reason, Message: msg}); derr == nil {
		err.AddDetail(d)
	}
	return err
}

// startStaticWorker runs the independent worker and registers it.
func (s *stack) startStaticWorker() *staticWorker {
	s.t.Helper()
	url := "http://" + s.addr
	sw := &staticWorker{t: s.t, orchURL: url, stopped: make(chan struct{}),
		fleet: edgerun.NewFleet(url, workerToken),
		art:   edgerun.NewArtifacts(url, workerToken)}
	mux := http.NewServeMux()
	mux.Handle(workerv1connect.NewExecutionServiceHandler(sw))
	sw.srv = httptest.NewServer(mux)
	s.t.Cleanup(func() { sw.stop() })

	resp, err := sw.fleet.Register(context.Background(), &workerv1.RegisterRequest{Endpoint: sw.srv.URL, Info: sw.info()})
	if err != nil {
		s.t.Fatal(err)
	}
	sw.workerID = resp.WorkerId
	interval := resp.HeartbeatInterval.AsDuration()

	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-sw.stopped:
				return
			case <-t.C:
				sw.mu.Lock()
				id := sw.workerID
				sw.mu.Unlock()
				if r, err := sw.fleet.Heartbeat(context.Background(), &workerv1.HeartbeatRequest{WorkerId: id, State: workerv1.WorkerState_WORKER_STATE_READY}); err == nil && r.Reregister {
					if rr, err := sw.fleet.Register(context.Background(), &workerv1.RegisterRequest{Endpoint: sw.srv.URL, Info: sw.info()}); err == nil {
						sw.mu.Lock()
						sw.workerID = rr.WorkerId
						sw.mu.Unlock()
					}
				}
			}
		}
	}()
	return sw
}

func (sw *staticWorker) stop() {
	sw.once.Do(func() {
		close(sw.stopped)
		sw.fleet.Deregister(context.Background(), &workerv1.DeregisterRequest{WorkerId: sw.workerID})
		sw.srv.Close()
	})
}

func (sw *staticWorker) servedCount() int { sw.mu.Lock(); defer sw.mu.Unlock(); return sw.served }

// TestSwappingAWorker is the architecture's acceptance test: a worker written
// from scratch, speaking another ABI, runs beside the real one behind an
// unmodified orchestrator and frontend, and every byte of HTTP is correct.
func TestSwappingAWorker(t *testing.T) {
	s := newStack(t)
	real := s.startWorker(4)
	foreign := s.startStaticWorker()
	w := s.startFrontend(5 * time.Second)

	wasmFn := s.createFunction("wasm")
	s.uploadVersion(wasmFn, s.wasm, nil, true)
	s.route("wasm.test", "/", wasmFn)

	staticFn := s.createFunction("static")
	if _, err := s.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{
		FunctionId: staticFn, Module: []byte("hello from a completely different worker\n"), Abi: "static-1", Activate: true}); err != nil {
		t.Fatal(err)
	}
	s.route("static.test", "/", staticFn)

	eventually(t, "both workers alive", 60*time.Second, func() bool { return s.aliveWorkers() == 2 })
	eventually(t, "the wasm function to serve", 120*time.Second, func() bool {
		return w.do("GET", "wasm.test", "/echo", "").status == 200
	})

	t.Run("each ABI is routed to a worker that supports it", func(t *testing.T) {
		for i := range 5 {
			if r := w.do("GET", "wasm.test", "/echo", ""); r.status != 200 || r.header.Get("X-Served-By") != "" {
				t.Fatalf("wasm call %d: %+v", i, r)
			}
			r := w.do("GET", "static.test", "/anything", "")
			if r.status != 200 || r.body != "hello from a completely different worker\n" || r.header.Get("X-Served-By") != "static-worker" {
				t.Fatalf("static call %d: %+v", i, r)
			}
		}
		if foreign.servedCount() != 5 {
			t.Errorf("the foreign worker served %d requests, want 5", foreign.servedCount())
		}
	})

	t.Run("the fleet view shows both implementations", func(t *testing.T) {
		ws, err := s.ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
		if err != nil {
			t.Fatal(err)
		}
		names := map[string]bool{}
		for _, wk := range ws.Workers {
			names[wk.Info.Implementation.Name] = true
		}
		if !names["static-worker"] || !names["edgerun-worker-wazero"] {
			t.Errorf("implementations: %v", names)
		}
	})

	t.Run("losing the foreign worker affects only its own ABI", func(t *testing.T) {
		foreign.stop()
		eventually(t, "one worker left", 10*time.Second, func() bool { return s.aliveWorkers() == 1 })
		if r := w.do("GET", "wasm.test", "/echo", ""); r.status != 200 {
			t.Errorf("the wasm function must be unaffected: %d", r.status)
		}
		// Ready workers exist but none supports static-1: an operator problem (502),
		// distinct from "nothing is running" (503).
		eventually(t, "static-1 to be unservable", 10*time.Second, func() bool {
			return w.do("GET", "static.test", "/", "").status == 502
		})
	})

	t.Run("losing the real worker too leaves nothing: 503", func(t *testing.T) {
		real.stop()
		eventually(t, "no workers", 10*time.Second, func() bool { return s.aliveWorkers() == 0 })
		eventually(t, "503", 10*time.Second, func() bool { return w.do("GET", "wasm.test", "/echo", "").status == 503 })
	})
}
