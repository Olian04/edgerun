package workerconf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
)

// fakeOrchestrator is the orchestrator side of the worker seam: the Fleet and
// Artifact services a worker dials into. It records everything the worker
// does so the suite can assert on it.
type fakeOrchestrator struct {
	token    string
	interval time.Duration
	server   *check.Server

	mu            sync.Mutex
	registrations []registration
	heartbeats    []heartbeat
	deregistered  []string
	blobs         map[string][]byte
	forget        string // answer the next heartbeat from this id with reregister
	authFailures  int
	changed       chan struct{} // closed and replaced on every state change
}

type registration struct {
	ID       string
	Endpoint string
	Info     *workerv1.WorkerInfo
}

type heartbeat struct {
	ID       string
	State    workerv1.WorkerState
	InFlight uint32
}

func newFakeOrchestrator(t testing.TB, n check.Net, token string, interval time.Duration) *fakeOrchestrator {
	o := &fakeOrchestrator{token: token, interval: interval, blobs: map[string][]byte{}, changed: make(chan struct{})}
	mux := http.NewServeMux()
	mux.Handle(workerv1connect.NewFleetServiceHandler(fleetHandler{o}))
	mux.Handle(workerv1connect.NewArtifactServiceHandler(artifactHandler{o}))
	o.server = n.Serve(t, mux, true)
	return o
}

func (o *fakeOrchestrator) url() string { return o.server.URL }

// registered returns the registration of the worker at endpoint (host:port or
// a URL), or the first if endpoint is empty, or nil. Call with the lock held.
func (o *fakeOrchestrator) registered(endpoint string) *registration {
	want := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"), "/")
	for i := range o.registrations {
		got := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(o.registrations[i].Endpoint, "http://"), "https://"), "/")
		if want == "" || got == want {
			return &o.registrations[i]
		}
	}
	return nil
}

// update runs f under the lock and wakes everyone waiting for a change.
func (o *fakeOrchestrator) update(f func()) {
	o.mu.Lock()
	defer o.mu.Unlock()
	f()
	close(o.changed)
	o.changed = make(chan struct{})
}

// waitFor blocks until cond (evaluated under the lock) holds.
func (o *fakeOrchestrator) waitFor(t testing.TB, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		o.mu.Lock()
		ok, ch := cond(), o.changed
		o.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-ch:
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
	}
}

// addBlob stores bytes under their true digest and returns it.
func (o *fakeOrchestrator) addBlob(b []byte) string {
	sum := sha256.Sum256(b)
	d := "sha256:" + hex.EncodeToString(sum[:])
	o.setBlob(d, b)
	return d
}

// setBlob stores bytes under an arbitrary digest, true or not.
func (o *fakeOrchestrator) setBlob(digest string, b []byte) {
	o.update(func() { o.blobs[digest] = b })
}

func (o *fakeOrchestrator) removeBlob(digest string) {
	o.update(func() { delete(o.blobs, digest) })
}

func (o *fakeOrchestrator) authorised(h http.Header) error {
	if h.Get("Authorization") == "Bearer "+o.token {
		return nil
	}
	o.update(func() { o.authFailures++ })
	return connect.NewError(connect.CodeUnauthenticated, errors.New("bad worker token"))
}

type fleetHandler struct{ o *fakeOrchestrator }

func (f fleetHandler) Register(_ context.Context, r *connect.Request[workerv1.RegisterRequest]) (*connect.Response[workerv1.RegisterResponse], error) {
	if err := f.o.authorised(r.Header()); err != nil {
		return nil, err
	}
	var id string
	f.o.update(func() {
		id = "worker-" + strconv.Itoa(len(f.o.registrations)+1)
		f.o.registrations = append(f.o.registrations, registration{id, r.Msg.Endpoint, r.Msg.Info})
	})
	return connect.NewResponse(&workerv1.RegisterResponse{WorkerId: id, HeartbeatInterval: durationpb.New(f.o.interval)}), nil
}

func (f fleetHandler) Heartbeat(_ context.Context, r *connect.Request[workerv1.HeartbeatRequest]) (*connect.Response[workerv1.HeartbeatResponse], error) {
	if err := f.o.authorised(r.Header()); err != nil {
		return nil, err
	}
	var again bool
	f.o.update(func() {
		f.o.heartbeats = append(f.o.heartbeats, heartbeat{r.Msg.WorkerId, r.Msg.State, r.Msg.InFlight})
		if f.o.forget != "" && f.o.forget == r.Msg.WorkerId {
			f.o.forget, again = "", true
		}
	})
	return connect.NewResponse(&workerv1.HeartbeatResponse{Reregister: again}), nil
}

func (f fleetHandler) Deregister(_ context.Context, r *connect.Request[workerv1.DeregisterRequest]) (*connect.Response[workerv1.DeregisterResponse], error) {
	if err := f.o.authorised(r.Header()); err != nil {
		return nil, err
	}
	f.o.update(func() { f.o.deregistered = append(f.o.deregistered, r.Msg.WorkerId) })
	return connect.NewResponse(&workerv1.DeregisterResponse{}), nil
}

type artifactHandler struct{ o *fakeOrchestrator }

func (a artifactHandler) GetArtifact(_ context.Context, r *connect.Request[workerv1.GetArtifactRequest], st *connect.ServerStream[workerv1.GetArtifactResponse]) error {
	if err := a.o.authorised(r.Header()); err != nil {
		return err
	}
	a.o.mu.Lock()
	b, ok := a.o.blobs[r.Msg.Digest]
	a.o.mu.Unlock()
	if !ok {
		return reasonError(commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND, connect.CodeNotFound, "unknown digest")
	}
	const chunk = 32 << 10
	for len(b) > 0 {
		n := min(chunk, len(b))
		if err := st.Send(&workerv1.GetArtifactResponse{Chunk: b[:n]}); err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

// reasonError builds an error the way the protocol says an orchestrator
// reports a platform failure: a canonical code plus an ErrorInfo detail.
func reasonError(reason commonv1.ErrorReason, code connect.Code, msg string) error {
	err := connect.NewError(code, errors.New(msg))
	if d, derr := connect.NewErrorDetail(&commonv1.ErrorInfo{Reason: reason, Message: msg}); derr == nil {
		err.AddDetail(d)
	}
	return err
}
