package fleet_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/orchestrator/fleet"
	"github.com/Olian04/edgerun/internal/orchestrator/orchtest"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
	"github.com/Olian04/edgerun/internal/testpg"
	"github.com/Olian04/edgerun/internal/worker/cache"
)

func TestMain(m *testing.M) { testpg.Main(m) }

const (
	workerToken = "worker-secret"
	adminToken  = "admin-secret"
)

var ctx = context.Background()

type rig struct {
	st      *store.Store
	fleet   workerv1connect.FleetServiceClient
	art     workerv1connect.ArtifactServiceClient
	changes *atomic.Int32
	srv     *httptest.Server
}

func newRig(t *testing.T) *rig {
	t.Helper()
	st := orchtest.Store(t)
	changes := &atomic.Int32{}
	svc := fleet.New(fleet.Config{Store: st, HeartbeatInterval: 2 * time.Second, OnChange: func() { changes.Add(1) },
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	mux := http.NewServeMux()
	svc.Mount(mux, authn.Verifier{workerToken: fleet.WorkerScope, adminToken: "admin"})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	auth := connect.WithInterceptors(authn.Client(workerToken))
	return &rig{st: st, changes: changes, srv: srv,
		fleet: workerv1connect.NewFleetServiceClient(srv.Client(), srv.URL, auth),
		art:   workerv1connect.NewArtifactServiceClient(srv.Client(), srv.URL, auth)}
}

func info() *workerv1.WorkerInfo {
	return &workerv1.WorkerInfo{Abis: []string{"wagi-1"}, MaxConcurrency: 4,
		Implementation: &commonv1.Implementation{Name: "t", Protocols: []string{"edgerun.worker.v1"}}}
}

func TestRegisterHeartbeatDeregister(t *testing.T) {
	r := newRig(t)
	reg, err := r.fleet.Register(ctx, connect.NewRequest(&workerv1.RegisterRequest{Endpoint: "http://10.0.0.1:9000", Info: info()}))
	if err != nil {
		t.Fatal(err)
	}
	if reg.Msg.WorkerId == "" || reg.Msg.HeartbeatInterval.AsDuration() != 2*time.Second {
		t.Errorf("register response = %+v: the orchestrator dictates the interval", reg.Msg)
	}
	if r.changes.Load() != 1 {
		t.Error("registering must notify so the invoke path sees the worker at once")
	}

	hb, err := r.fleet.Heartbeat(ctx, connect.NewRequest(&workerv1.HeartbeatRequest{
		WorkerId: reg.Msg.WorkerId, State: workerv1.WorkerState_WORKER_STATE_READY, InFlight: 2}))
	if err != nil || hb.Msg.Reregister {
		t.Fatalf("heartbeat: %v %+v", err, hb)
	}
	if r.changes.Load() != 1 {
		t.Error("heartbeats are frequent and must not trigger refreshes")
	}
	ws, _ := r.st.ReadyWorkers(ctx, time.Minute)
	if len(ws) != 1 || ws[0].InFlight != 2 {
		t.Errorf("ready workers = %+v", ws)
	}

	if _, err := r.fleet.Deregister(ctx, connect.NewRequest(&workerv1.DeregisterRequest{WorkerId: reg.Msg.WorkerId})); err != nil {
		t.Fatal(err)
	}
	if r.changes.Load() != 2 {
		t.Error("deregistering must notify")
	}
	hb, _ = r.fleet.Heartbeat(ctx, connect.NewRequest(&workerv1.HeartbeatRequest{
		WorkerId: reg.Msg.WorkerId, State: workerv1.WorkerState_WORKER_STATE_READY}))
	if !hb.Msg.Reregister {
		t.Error("a heartbeat from an unknown worker must tell it to re-register")
	}
}

func TestReregistrationInvalidatesTheOldID(t *testing.T) {
	r := newRig(t)
	a, _ := r.fleet.Register(ctx, connect.NewRequest(&workerv1.RegisterRequest{Endpoint: "http://w:1", Info: info()}))
	b, _ := r.fleet.Register(ctx, connect.NewRequest(&workerv1.RegisterRequest{Endpoint: "http://w:1", Info: info()}))
	if a.Msg.WorkerId == b.Msg.WorkerId {
		t.Fatal("same endpoint must get a fresh id")
	}
	hb, _ := r.fleet.Heartbeat(ctx, connect.NewRequest(&workerv1.HeartbeatRequest{WorkerId: a.Msg.WorkerId, State: workerv1.WorkerState_WORKER_STATE_READY}))
	if !hb.Msg.Reregister {
		t.Error("the replaced worker id must be told to re-register")
	}
}

func TestValidationAndErrors(t *testing.T) {
	r := newRig(t)
	_, err := r.fleet.Register(ctx, connect.NewRequest(&workerv1.RegisterRequest{Endpoint: "not a url", Info: info()}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("bad endpoint: %v", err)
	}
	_, err = r.fleet.Register(ctx, connect.NewRequest(&workerv1.RegisterRequest{Endpoint: "http://w:1"}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("no info: %v", err)
	}
	for _, st := range []workerv1.WorkerState{workerv1.WorkerState_WORKER_STATE_UNSPECIFIED, 99} {
		_, err = r.fleet.Heartbeat(ctx, connect.NewRequest(&workerv1.HeartbeatRequest{WorkerId: "x", State: st}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("heartbeat state %v: %v", st, err)
		}
	}
	if _, err := r.fleet.Deregister(ctx, connect.NewRequest(&workerv1.DeregisterRequest{WorkerId: "never-existed"})); err != nil {
		t.Errorf("deregister must be idempotent: %v", err)
	}
}

func TestOnlyWorkerScopeMayCall(t *testing.T) {
	r := newRig(t)
	for name, opts := range map[string][]connect.ClientOption{
		"none":  nil,
		"admin": {connect.WithInterceptors(authn.Client(adminToken))},
	} {
		f := workerv1connect.NewFleetServiceClient(r.srv.Client(), r.srv.URL, opts...)
		a := workerv1connect.NewArtifactServiceClient(r.srv.Client(), r.srv.URL, opts...)
		want := connect.CodeUnauthenticated
		if name == "admin" {
			want = connect.CodePermissionDenied
		}
		_, e1 := f.Register(ctx, connect.NewRequest(&workerv1.RegisterRequest{Endpoint: "http://w:1", Info: info()}))
		st, e2 := a.GetArtifact(ctx, connect.NewRequest(&workerv1.GetArtifactRequest{Digest: store.Digest([]byte("x"))}))
		if e2 == nil {
			for st.Receive() {
			}
			e2 = st.Err()
		}
		if connect.CodeOf(e1) != want || connect.CodeOf(e2) != want {
			t.Errorf("%s: register %v, artifact %v; want %v", name, e1, e2, want)
		}
	}
}

// The artifact stream is consumed here by the worker's own fetcher: both sides
// of the seam must agree, not just each be self-consistent.
func TestArtifactsStreamInChunksAndTheWorkersFetcherVerifiesThem(t *testing.T) {
	r := newRig(t)
	module := bytes.Repeat([]byte("0123456789abcdef"), 220_000) // ~3.4 MiB: several 1 MiB chunks
	f, _ := r.st.CreateFunction(ctx, "fn")
	v, err := r.st.CreateVersion(ctx, store.NewVersion{FunctionID: f.Id, Module: module, ABI: "wagi-1"})
	if err != nil {
		t.Fatal(err)
	}

	// Raw stream: more than one chunk, none over 1 MiB.
	st, err := r.art.GetArtifact(ctx, connect.NewRequest(&workerv1.GetArtifactRequest{Digest: v.ArtifactDigest}))
	if err != nil {
		t.Fatal(err)
	}
	chunks, total := 0, 0
	for st.Receive() {
		chunks++
		total += len(st.Msg().Chunk)
		if len(st.Msg().Chunk) > 1<<20 {
			t.Errorf("chunk of %d bytes exceeds the protocol's 1 MiB", len(st.Msg().Chunk))
		}
	}
	if st.Err() != nil || chunks < 4 || total != len(module) {
		t.Errorf("chunks=%d total=%d err=%v", chunks, total, st.Err())
	}

	// The worker's cache fetches and verifies it.
	cs := cache.New(cache.ConnectFetcher{Client: r.art, MaxBytes: 64 << 20}, 64<<20)
	got, err := cs.Get(ctx, v.ArtifactDigest)
	if err != nil || !bytes.Equal(got, module) {
		t.Fatalf("worker-side fetch: %v", err)
	}

	// An unknown digest carries the typed reason the worker relies on.
	_, err = cs.Get(ctx, store.Digest([]byte("never uploaded")))
	if errs.Reason(err) != commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND {
		t.Errorf("unknown artifact: reason %v (%v)", errs.Reason(err), err)
	}
}
