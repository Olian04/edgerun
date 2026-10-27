package fleetclient

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
)

type fakeFleet struct {
	workerv1connect.UnimplementedFleetServiceHandler
	interval time.Duration

	mu           sync.Mutex
	failRegister int // fail this many Register calls first
	registers    int
	heartbeats   []*workerv1.HeartbeatRequest
	forget       string // heartbeats for this id get reregister=true, once
	deregistered []string
}

func (f *fakeFleet) Register(context.Context, *connect.Request[workerv1.RegisterRequest]) (*connect.Response[workerv1.RegisterResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failRegister > 0 {
		f.failRegister--
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("not yet"))
	}
	f.registers++
	return connect.NewResponse(&workerv1.RegisterResponse{
		WorkerId:          "w" + string(rune('0'+f.registers)),
		HeartbeatInterval: durationpb.New(f.interval),
	}), nil
}

func (f *fakeFleet) Heartbeat(_ context.Context, r *connect.Request[workerv1.HeartbeatRequest]) (*connect.Response[workerv1.HeartbeatResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeats = append(f.heartbeats, r.Msg)
	re := f.forget != "" && r.Msg.WorkerId == f.forget
	if re {
		f.forget = ""
	}
	return connect.NewResponse(&workerv1.HeartbeatResponse{Reregister: re}), nil
}

func (f *fakeFleet) Deregister(_ context.Context, r *connect.Request[workerv1.DeregisterRequest]) (*connect.Response[workerv1.DeregisterResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deregistered = append(f.deregistered, r.Msg.WorkerId)
	return connect.NewResponse(&workerv1.DeregisterResponse{}), nil
}

func (f *fakeFleet) snapshot() (registers int, hbs []*workerv1.HeartbeatRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registers, append([]*workerv1.HeartbeatRequest(nil), f.heartbeats...)
}

type source struct {
	mu    sync.Mutex
	state workerv1.WorkerState
	n     uint32
}

func (s *source) State() workerv1.WorkerState { s.mu.Lock(); defer s.mu.Unlock(); return s.state }
func (s *source) InFlight() uint32            { s.mu.Lock(); defer s.mu.Unlock(); return s.n }

func setup(t *testing.T, f *fakeFleet, src *source) (*Client, context.CancelFunc, <-chan error) {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(workerv1connect.NewFleetServiceHandler(f))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := New(Config{
		Endpoint: "http://worker.test", Info: &workerv1.WorkerInfo{},
		Fleet:  workerv1connect.NewFleetServiceClient(srv.Client(), srv.URL),
		Source: src, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		RetryMin: 5 * time.Millisecond, RetryMax: 20 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(cancel)
	return c, cancel, done
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestRegistersRetriesAndHeartbeatsWithLoad(t *testing.T) {
	f := &fakeFleet{interval: 10 * time.Millisecond, failRegister: 3}
	src := &source{state: workerv1.WorkerState_WORKER_STATE_READY, n: 2}
	_, cancel, done := setup(t, f, src)

	eventually(t, "registration after retries", func() bool { r, _ := f.snapshot(); return r == 1 })
	eventually(t, "heartbeats", func() bool { _, h := f.snapshot(); return len(h) >= 2 })
	_, hbs := f.snapshot()
	if hbs[0].WorkerId != "w1" || hbs[0].InFlight != 2 || hbs[0].State != workerv1.WorkerState_WORKER_STATE_READY {
		t.Errorf("heartbeat = %v", hbs[0])
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
}

func TestReregistersWhenForgotten(t *testing.T) {
	f := &fakeFleet{interval: 10 * time.Millisecond, forget: "w1"}
	_, _, _ = setup(t, f, &source{})
	eventually(t, "second registration", func() bool { r, _ := f.snapshot(); return r == 2 })
	eventually(t, "heartbeats under the new id", func() bool {
		_, h := f.snapshot()
		for _, x := range h {
			if x.WorkerId == "w2" {
				return true
			}
		}
		return false
	})
}

func TestPokeHeartbeatsImmediately(t *testing.T) {
	f := &fakeFleet{interval: time.Hour}
	src := &source{state: workerv1.WorkerState_WORKER_STATE_READY}
	c, _, _ := setup(t, f, src)
	eventually(t, "registration", func() bool { r, _ := f.snapshot(); return r == 1 })
	time.Sleep(50 * time.Millisecond)
	if _, h := f.snapshot(); len(h) != 0 {
		t.Fatalf("no heartbeat expected within an hour-long interval, got %d", len(h))
	}

	src.mu.Lock()
	src.state = workerv1.WorkerState_WORKER_STATE_DRAINING
	src.mu.Unlock()
	c.Poke()
	c.Poke() // coalesces; must not block
	eventually(t, "poked heartbeat", func() bool { _, h := f.snapshot(); return len(h) >= 1 })
	_, h := f.snapshot()
	if h[0].State != workerv1.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("poked heartbeat state = %v", h[0].State)
	}
}

func TestDeregister(t *testing.T) {
	f := &fakeFleet{interval: time.Hour}
	c, _, _ := setup(t, f, &source{})
	// Wait for the client's own view: the server counting a registration does
	// not mean the client has received its id yet.
	eventually(t, "the client to hold its worker id", func() bool { return c.WorkerID() == "w1" })
	if err := c.Deregister(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.deregistered) != 1 || f.deregistered[0] != "w1" {
		t.Errorf("deregistered = %v", f.deregistered)
	}
}

func TestDeregisterBeforeRegistrationIsANoOp(t *testing.T) {
	f := &fakeFleet{interval: time.Hour}
	c := New(Config{Fleet: nil, Source: &source{}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}) // never started
	if c.WorkerID() != "" {
		t.Error("a client that never registered has no id")
	}
	if err := c.Deregister(context.Background()); err != nil {
		t.Errorf("nothing to deregister must not be an error (and must not call the orchestrator): %v", err)
	}
	if len(f.deregistered) != 0 {
		t.Error("unexpected deregistration")
	}
}
