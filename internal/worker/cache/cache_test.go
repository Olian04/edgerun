package cache

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	"github.com/Olian04/edgerun/internal/errs"
)

type fakeFetcher struct {
	blobs   map[string][]byte
	calls   atomic.Int32
	gate    chan struct{} // if set, Fetch blocks until closed
	corrupt bool
	fail    atomic.Bool
}

func (f *fakeFetcher) Fetch(ctx context.Context, d string) ([]byte, error) {
	f.calls.Add(1)
	if f.gate != nil {
		<-f.gate
	}
	if f.fail.Load() {
		return nil, errors.New("backend down")
	}
	b, ok := f.blobs[d]
	if !ok {
		return nil, errs.New(commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND, "no such artifact")
	}
	if f.corrupt {
		return append([]byte("evil"), b...), nil
	}
	return b, nil
}

func blob(s string) ([]byte, string) { b := []byte(s); return b, Digest(b) }

func TestDigestAndValidation(t *testing.T) {
	_, d := blob("hello")
	if d != "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" {
		t.Errorf("digest = %s", d)
	}
	for _, bad := range []string{"", "sha256:", "sha256:ABC", "md5:" + strings.Repeat("a", 64),
		"sha256:" + strings.Repeat("A", 64), "sha256:" + strings.Repeat("a", 63), d + "0"} {
		if ValidDigest(bad) {
			t.Errorf("ValidDigest(%q) = true", bad)
		}
	}
	if !ValidDigest(d) {
		t.Error("valid digest rejected")
	}
}

func TestHitMissAndInvalid(t *testing.T) {
	a, da := blob("aaa")
	f := &fakeFetcher{blobs: map[string][]byte{da: a}}
	s := New(f, 1<<20)
	for range 3 {
		got, err := s.Get(context.Background(), da)
		if err != nil || !bytes.Equal(got, a) {
			t.Fatalf("got %q, %v", got, err)
		}
	}
	if f.calls.Load() != 1 {
		t.Errorf("fetched %d times, want 1", f.calls.Load())
	}
	if _, err := s.Get(context.Background(), "nonsense"); connectCode(err) != connect.CodeInvalidArgument {
		t.Errorf("malformed digest: %v", err)
	}
	if f.calls.Load() != 1 {
		t.Error("malformed digest must not reach the fetcher")
	}
}

func connectCode(err error) connect.Code { return connect.CodeOf(err) }

func TestTamperedBytesAreRejectedAndNotCached(t *testing.T) {
	a, da := blob("aaa")
	f := &fakeFetcher{blobs: map[string][]byte{da: a}, corrupt: true}
	s := New(f, 1<<20)
	if _, err := s.Get(context.Background(), da); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("err = %v, want ErrDigestMismatch", err)
	}
	f.corrupt = false
	got, err := s.Get(context.Background(), da)
	if err != nil || !bytes.Equal(got, a) {
		t.Fatalf("after server recovers: %q, %v", got, err)
	}
}

func TestErrorsAreNotCachedAndKeepTheirReason(t *testing.T) {
	_, d := blob("missing")
	f := &fakeFetcher{blobs: map[string][]byte{}}
	s := New(f, 1<<20)
	_, err := s.Get(context.Background(), d)
	if errs.Reason(err) != commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND {
		t.Errorf("reason lost: %v", err)
	}
	s.Get(context.Background(), d)
	if f.calls.Load() != 2 {
		t.Errorf("failures must not be cached: %d calls", f.calls.Load())
	}
}

func TestConcurrentGetsShareOneFetch(t *testing.T) {
	a, da := blob("aaa")
	f := &fakeFetcher{blobs: map[string][]byte{da: a}, gate: make(chan struct{})}
	s := New(f, 1<<20)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, err := s.Get(context.Background(), da); err != nil || !bytes.Equal(got, a) {
				t.Errorf("got %q %v", got, err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(f.gate)
	wg.Wait()
	if n := f.calls.Load(); n != 1 {
		t.Errorf("%d fetches for 20 concurrent callers, want 1", n)
	}
}

func TestOneCallersCancellationDoesNotFailOthers(t *testing.T) {
	a, da := blob("aaa")
	f := &fakeFetcher{blobs: map[string][]byte{da: a}, gate: make(chan struct{})}
	s := New(f, 1<<20)

	ctx, cancel := context.WithCancel(context.Background())
	impatient := make(chan error, 1)
	go func() { _, err := s.Get(ctx, da); impatient <- err }()
	patient := make(chan error, 1)
	go func() { _, err := s.Get(context.Background(), da); patient <- err }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-impatient; !errors.Is(err, context.Canceled) {
		t.Errorf("impatient caller: %v", err)
	}
	close(f.gate)
	if err := <-patient; err != nil {
		t.Errorf("patient caller failed because another cancelled: %v", err)
	}
}

func TestEvictionByBytes(t *testing.T) {
	a, da := blob(strings.Repeat("a", 40))
	b, db := blob(strings.Repeat("b", 40))
	c, dc := blob(strings.Repeat("c", 40))
	f := &fakeFetcher{blobs: map[string][]byte{da: a, db: b, dc: c}}
	s := New(f, 100) // fits two
	ctx := context.Background()
	s.Get(ctx, da)
	s.Get(ctx, db)
	s.Get(ctx, da) // a is now most recent
	s.Get(ctx, dc) // evicts b
	calls := f.calls.Load()
	s.Get(ctx, da)
	s.Get(ctx, dc)
	if f.calls.Load() != calls {
		t.Error("a and c should still be cached")
	}
	s.Get(ctx, db)
	if f.calls.Load() != calls+1 {
		t.Error("b should have been evicted")
	}
}

func TestOversizedArtifactIsServedButNotKept(t *testing.T) {
	a, da := blob(strings.Repeat("a", 200))
	f := &fakeFetcher{blobs: map[string][]byte{da: a}}
	s := New(f, 100)
	for range 2 {
		got, err := s.Get(context.Background(), da)
		if err != nil || !bytes.Equal(got, a) {
			t.Fatal(err)
		}
	}
	if f.calls.Load() != 2 {
		t.Errorf("calls = %d, want 2 (never cached)", f.calls.Load())
	}
}

// artifactServer serves blobs over the real ArtifactService, in small chunks.
type artifactServer struct {
	workerv1connect.UnimplementedArtifactServiceHandler
	blobs map[string][]byte
}

func (a artifactServer) GetArtifact(_ context.Context, req *connect.Request[workerv1.GetArtifactRequest], st *connect.ServerStream[workerv1.GetArtifactResponse]) error {
	b, ok := a.blobs[req.Msg.Digest]
	if !ok {
		return errs.New(commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND, "unknown digest")
	}
	for len(b) > 0 {
		n := min(7, len(b))
		if err := st.Send(&workerv1.GetArtifactResponse{Chunk: b[:n]}); err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

func TestArtifactFetcher(t *testing.T) {
	a, da := blob("0123456789abcdefghijklmnopqrstuvwxyz")
	mux := http.NewServeMux()
	mux.Handle(workerv1connect.NewArtifactServiceHandler(artifactServer{blobs: map[string][]byte{da: a}}))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client := edgerun.NewArtifacts(srv.URL, "", edgerun.WithHTTPClient(srv.Client()))

	f := ArtifactFetcher{Client: client}
	got, err := f.Fetch(context.Background(), da)
	if err != nil || !bytes.Equal(got, a) {
		t.Fatalf("reassembly: %q %v", got, err)
	}

	_, err = f.Fetch(context.Background(), Digest([]byte("nope")))
	if errs.Reason(err) != commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND {
		t.Errorf("not-found reason lost over the wire: %v", err)
	}

	small := ArtifactFetcher{Client: edgerun.NewArtifacts(srv.URL, "", edgerun.WithHTTPClient(srv.Client()))}
	small.Client.MaxBytes = 10
	if _, err := small.Fetch(context.Background(), da); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("size cap not enforced: %v", err)
	}
}

// stuckThenFine blocks its first fetch until its context ends and serves every
// later one at once: a peer that stalled, then recovered.
type stuckThenFine struct {
	b     []byte
	calls atomic.Int32
}

func (f *stuckThenFine) Fetch(ctx context.Context, d string) ([]byte, error) {
	if f.calls.Add(1) == 1 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.b, nil
}

// A fetch that nobody waits for any more is abandoned: it must not keep the
// digest unservable until its own timeout (the first invocation fails at its
// deadline; the next one must not join a fetch that is stuck).
func TestAnAbandonedFetchDoesNotBlockTheDigest(t *testing.T) {
	b, d := blob("module")
	f := &stuckThenFine{b: b}
	s := New(f, 1<<20)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := s.Get(ctx, d); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the first Get: %v, want its deadline", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	got, err := s.Get(ctx2, d)
	if err != nil || !bytes.Equal(got, b) {
		t.Fatalf("the next Get joined the stuck fetch: %v", err)
	}
	if f.calls.Load() != 2 {
		t.Errorf("%d fetches, want 2 (the stuck one, abandoned, and a fresh one)", f.calls.Load())
	}
}

// One impatient caller must not take the fetch from the others.
func TestOneImpatientCallerDoesNotAbandonTheFetch(t *testing.T) {
	b, d := blob("module")
	f := &fakeFetcher{blobs: map[string][]byte{d: b}, gate: make(chan struct{})}
	s := New(f, 1<<20)

	impatient, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	patient := make(chan error, 1)
	go func() { _, err := s.Get(context.Background(), d); patient <- err }()
	time.Sleep(20 * time.Millisecond)
	if _, err := s.Get(impatient, d); err == nil {
		t.Fatal("the impatient caller got an answer before the gate opened")
	}
	close(f.gate)
	if err := <-patient; err != nil {
		t.Errorf("the patient caller: %v", err)
	}
	if f.calls.Load() != 1 {
		t.Errorf("%d fetches, want 1 shared", f.calls.Load())
	}
}
