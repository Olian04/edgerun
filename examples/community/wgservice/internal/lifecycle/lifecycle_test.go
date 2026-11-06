package lifecycle

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
)

type fake struct {
	mu        sync.Mutex
	events    []string
	failReg   int
	regErr    error
	forget    bool
	nextID    int
	inflight  atomic.Int64
	deregDone chan struct{}
}

func (f *fake) rec(s string) { f.mu.Lock(); f.events = append(f.events, s); f.mu.Unlock() }
func (f *fake) log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}

func (f *fake) Register(context.Context, *egressv1.RegisterRequest) (*egressv1.RegisterResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failReg > 0 {
		f.failReg--
		f.events = append(f.events, "register-failed")
		if f.regErr != nil {
			return nil, f.regErr
		}
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("not up"))
	}
	f.nextID++
	f.events = append(f.events, "register")
	return &egressv1.RegisterResponse{ServiceId: string(rune('a' + f.nextID)), HeartbeatInterval: durationpb.New(20 * time.Millisecond)}, nil
}

func (f *fake) Heartbeat(_ context.Context, r *egressv1.HeartbeatRequest) (*egressv1.HeartbeatResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "heartbeat:"+r.State.String()[len("SERVICE_STATE_"):])
	if f.forget {
		f.forget = false
		return &egressv1.HeartbeatResponse{Reregister: true}, nil
	}
	return &egressv1.HeartbeatResponse{}, nil
}

func (f *fake) Deregister(context.Context, *egressv1.DeregisterRequest) (*egressv1.DeregisterResponse, error) {
	f.rec("deregister")
	return &egressv1.DeregisterResponse{}, nil
}

func count(ev []string, s string) (n int) {
	for _, e := range ev {
		if e == s {
			n++
		}
	}
	return
}

func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func newLoop(f *fake) *Loop {
	return &Loop{Reg: f, Endpoint: "http://x", Info: &egressv1.ServiceInfo{Covers: []string{"*"}},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), InFlight: func() int { return int(f.inflight.Load()) },
		DrainTimeout: 2 * time.Second, MinRetry: 5 * time.Millisecond, MaxRetry: 20 * time.Millisecond}
}

func TestRegistrationRetriesHeartbeatsAndReregisters(t *testing.T) {
	f := &fake{failReg: 3}
	l := newLoop(f)
	ctx, stop := context.WithCancel(context.Background())
	l.Start(ctx)
	eventually(t, "three heartbeats", func() bool { return count(f.log(), "heartbeat:READY") >= 3 })
	if n := count(f.log(), "register-failed"); n != 3 {
		t.Errorf("%d failed registrations: %v", n, f.log())
	}
	first := l.ID()
	f.mu.Lock()
	f.forget = true
	f.mu.Unlock()
	eventually(t, "to register again", func() bool { return l.ID() != first && l.ID() != "" })
	l.Drain(stop)
}

func TestRefusedRegistrationIsReported(t *testing.T) {
	f := &fake{failReg: 2, regErr: connect.NewError(connect.CodeUnauthenticated, errors.New("bad token"))}
	l := newLoop(f)
	var refused atomic.Int64
	l.Refused = func(error) { refused.Add(1) }
	ctx, stop := context.WithCancel(context.Background())
	l.Start(ctx)
	eventually(t, "registered after the refusals", func() bool { return l.ID() != "" })
	if refused.Load() != 2 {
		t.Errorf("reported %d times", refused.Load())
	}
	l.Drain(stop)
}

func TestDrainReportsDrainingWaitsAndDeregisters(t *testing.T) {
	f := &fake{}
	l := newLoop(f)
	ctx, stop := context.WithCancel(context.Background())
	l.Start(ctx)
	eventually(t, "a heartbeat", func() bool { return count(f.log(), "heartbeat:READY") >= 1 })
	f.inflight.Store(1)
	done := make(chan struct{})
	go func() { l.Drain(stop); close(done) }()
	eventually(t, "DRAINING", func() bool { return count(f.log(), "heartbeat:DRAINING") >= 1 })
	time.Sleep(150 * time.Millisecond)
	if count(f.log(), "deregister") != 0 {
		t.Fatal("deregistered with a request in flight")
	}
	f.inflight.Store(0)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return")
	}
	ev := f.log()
	// The last READY heartbeat comes before the first DRAINING, and deregistering is last.
	if ev[len(ev)-1] != "deregister" || count(ev, "deregister") != 1 {
		t.Errorf("events %v", ev)
	}
	seenDraining := false
	for _, e := range ev {
		if e == "heartbeat:DRAINING" {
			seenDraining = true
		} else if e == "heartbeat:READY" && seenDraining {
			t.Errorf("READY after DRAINING: %v", ev)
		}
	}
}

func TestDrainTimeoutLeavesAnyway(t *testing.T) {
	f := &fake{}
	l := newLoop(f)
	l.DrainTimeout = 100 * time.Millisecond
	ctx, stop := context.WithCancel(context.Background())
	l.Start(ctx)
	eventually(t, "registered", func() bool { return l.ID() != "" })
	f.inflight.Store(1)
	l.Drain(stop)
	if count(f.log(), "deregister") != 1 {
		t.Errorf("events %v", f.log())
	}
}
