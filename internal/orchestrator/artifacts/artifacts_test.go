package artifacts_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/orchestrator/artifacts"
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
	st  *store.Store
	art *edgerun.Artifacts
	// raw is for the one test that is about the shape of the artifact stream
	// (its chunking), which the library's client exists to hide.
	raw workerv1connect.ArtifactServiceClient
	srv *httptest.Server
}

func newRig(t *testing.T) *rig {
	t.Helper()
	st := orchtest.Store(t)
	svc := artifacts.New(artifacts.Config{Store: st, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	mux := http.NewServeMux()
	svc.Mount(mux, authn.Verifier{workerToken: artifacts.WorkerScope, adminToken: "admin"})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &rig{st: st, srv: srv,
		art: edgerun.NewArtifacts(srv.URL, workerToken, edgerun.WithHTTPClient(srv.Client())),
		raw: workerv1connect.NewArtifactServiceClient(srv.Client(), srv.URL, connect.WithInterceptors(authn.Client(workerToken)))}
}

func TestOnlyWorkerScopeMayCall(t *testing.T) {
	r := newRig(t)
	for name, tok := range map[string]string{"none": "", "admin": adminToken} {
		a := edgerun.NewArtifacts(r.srv.URL, tok, edgerun.WithHTTPClient(r.srv.Client()))
		want := connect.CodeUnauthenticated
		if name == "admin" {
			want = connect.CodePermissionDenied
		}
		if _, err := a.GetArtifact(ctx, store.Digest([]byte("x"))); connect.CodeOf(err) != want {
			t.Errorf("%s: %v; want %v", name, err, want)
		}
	}
}

// The artifact stream is consumed here by the worker's own fetcher: both sides
// of the seam must agree, not just each be self-consistent.
func TestArtifactsStreamInChunksAndTheWorkersFetcherVerifiesThem(t *testing.T) {
	r := newRig(t)
	module := bytes.Repeat([]byte("0123456789abcdef"), 220_000) // ~3.4 MiB: several 1 MiB chunks
	f, _ := r.st.CreateFunction(ctx, "fn", nil)
	v, err := r.st.CreateVersion(ctx, store.NewVersion{FunctionID: f.Id, Module: module, ABI: "wagi-1"})
	if err != nil {
		t.Fatal(err)
	}

	// Raw stream: more than one chunk, none over 1 MiB.
	st, err := r.raw.GetArtifact(ctx, connect.NewRequest(&workerv1.GetArtifactRequest{Digest: v.ArtifactDigest}))
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
	cs := cache.New(cache.ArtifactFetcher{Client: r.art}, 64<<20)
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
