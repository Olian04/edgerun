package server

import (
	"io"
	"log/slog"
	"testing"
	"time"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/internal/errs"
)

func newTestServer(max uint32) *Server {
	return New(&workerv1.WorkerInfo{Abis: []string{"wagi-1"}, MaxConcurrency: max}, nil, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestSlotsNeverQueue(t *testing.T) {
	s := newTestServer(2)
	for range 2 {
		if err := s.acquire(); err != nil {
			t.Fatal(err)
		}
	}
	if s.InFlight() != 2 {
		t.Errorf("in flight = %d", s.InFlight())
	}
	err := s.acquire()
	if errs.Reason(err) != commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY {
		t.Fatalf("third acquire: %v", err)
	}
	s.release()
	if err := s.acquire(); err != nil {
		t.Errorf("slot not reusable after release: %v", err)
	}
}

func TestDrain(t *testing.T) {
	s := newTestServer(2)
	if s.State() != workerv1.WorkerState_WORKER_STATE_READY {
		t.Fatal("new server must be READY")
	}
	s.acquire()
	s.Drain()
	s.Drain() // idempotent

	if s.State() != workerv1.WorkerState_WORKER_STATE_DRAINING {
		t.Error("state after Drain")
	}
	if errs.Reason(s.acquire()) != commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY {
		t.Error("draining worker must refuse new work")
	}
	select {
	case <-s.Idle():
		t.Fatal("Idle fired with an invocation still running")
	case <-time.After(20 * time.Millisecond):
	}
	s.release()
	select {
	case <-s.Idle():
	case <-time.After(time.Second):
		t.Fatal("Idle did not fire after the last invocation finished")
	}
}

func TestDrainWhenAlreadyIdle(t *testing.T) {
	s := newTestServer(1)
	s.Drain()
	select {
	case <-s.Idle():
	case <-time.After(time.Second):
		t.Fatal("an idle server must be idle immediately after Drain")
	}
}
