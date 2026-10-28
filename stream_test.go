package edgerun_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
)

// scripted serves an ingress whose Invoke is whatever the test says, so that
// the client's handling of streams can be pushed to the edges no real
// orchestrator would let it reach.
type scripted struct {
	ingressv1connect.UnimplementedIngressServiceHandler
	invoke func(context.Context, *connect.Request[ingressv1.InvokeRequest], *connect.ServerStream[ingressv1.InvokeResponse]) error
}

func (s scripted) Invoke(ctx context.Context, r *connect.Request[ingressv1.InvokeRequest], st *connect.ServerStream[ingressv1.InvokeResponse]) error {
	return s.invoke(ctx, r, st)
}

func serve(t *testing.T, s scripted) *edgerun.Ingress {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(ingressv1connect.NewIngressServiceHandler(s, connect.WithInterceptors(edgerun.Authenticate(edgerun.Verifier{"tok": edgerun.ScopeIngress}, edgerun.ScopeIngress))))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return edgerun.NewIngress(srv.URL, "tok")
}

func head(status uint32, id string, hs ...string) *ingressv1.InvokeResponse {
	h := &commonv1.HttpResponseHead{Status: status}
	for i := 0; i+1 < len(hs); i += 2 {
		h.Headers = append(h.Headers, &commonv1.Header{Name: hs[i], Value: hs[i+1]})
	}
	return &ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Head{Head: &ingressv1.InvokeHead{Http: h, InvocationId: id}}}
}

func chunk(s string) *ingressv1.InvokeResponse {
	return &ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Chunk{Chunk: []byte(s)}}
}

func TestAResponseIsItsHeadThenItsBody(t *testing.T) {
	in := serve(t, scripted{invoke: func(_ context.Context, _ *connect.Request[ingressv1.InvokeRequest], st *connect.ServerStream[ingressv1.InvokeResponse]) error {
		for _, m := range []*ingressv1.InvokeResponse{head(201, "inv-1", "X-Thing", "a", "x-thing", "b"), chunk("hel"), chunk(""), chunk("lo")} {
			if err := st.Send(m); err != nil {
				return err
			}
		}
		return nil
	}})
	res, err := in.Invoke(context.Background(), &ingressv1.InvokeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status() != 201 || res.ID != "inv-1" {
		t.Errorf("status %d, id %q", res.Status(), res.ID)
	}
	if got := res.Header().Values("X-Thing"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("repeated headers must all arrive, in order: %q", got)
	}
	b, err := res.ReadAll()
	if err != nil || string(b) != "hello" {
		t.Errorf("body %q, %v", b, err)
	}
	if _, err := res.Body.Read(make([]byte, 1)); !errors.Is(err, http.ErrBodyReadAfterClose) {
		t.Errorf("reading a closed body: %v", err)
	}
}

func TestAFailureBeforeTheHeadIsTheCallsTypedError(t *testing.T) {
	in := serve(t, scripted{invoke: func(context.Context, *connect.Request[ingressv1.InvokeRequest], *connect.ServerStream[ingressv1.InvokeResponse]) error {
		return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND, "no route for %s", "x.test")
	}})
	res, err := in.Invoke(context.Background(), &ingressv1.InvokeRequest{})
	if res != nil || err == nil {
		t.Fatalf("got %v, %v", res, err)
	}
	if !edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND) || connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("reason %v, code %v", edgerun.Reason(err), connect.CodeOf(err))
	}
	if info := edgerun.Info(err); info == nil || info.GetMessage() != "no route for x.test" {
		t.Errorf("info = %v", info)
	}
}

// A body that ends cleanly must be a body that is complete: the failure of a
// worker dying mid-response may not look like the end of the data.
func TestAFailureAfterTheHeadIsNotTheEndOfTheBody(t *testing.T) {
	in := serve(t, scripted{invoke: func(_ context.Context, _ *connect.Request[ingressv1.InvokeRequest], st *connect.ServerStream[ingressv1.InvokeResponse]) error {
		st.Send(head(200, "inv-2"))
		st.Send(chunk("partial"))
		return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "the worker died")
	}})
	res, err := in.Invoke(context.Background(), &ingressv1.InvokeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(res.Body)
	if string(b) != "partial" || err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("read %q, %v", b, err)
	}
	if !edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_INTERNAL) {
		t.Errorf("the body's error must keep its reason: %v", err)
	}
	if _, err2 := res.Body.Read(make([]byte, 1)); err2 == nil || errors.Is(err2, io.EOF) {
		t.Errorf("the failure must stay failed: %v", err2)
	}
}

func TestAStreamThatDoesNotBeginWithAHeadIsRefused(t *testing.T) {
	for name, send := range map[string][]*ingressv1.InvokeResponse{"nothing at all": nil, "a chunk first": {chunk("x")}} {
		t.Run(name, func(t *testing.T) {
			in := serve(t, scripted{invoke: func(_ context.Context, _ *connect.Request[ingressv1.InvokeRequest], st *connect.ServerStream[ingressv1.InvokeResponse]) error {
				for _, m := range send {
					st.Send(m)
				}
				return nil
			}})
			res, err := in.Invoke(context.Background(), &ingressv1.InvokeRequest{})
			if err == nil {
				res.Body.Close()
				t.Fatal("a stream with no head is a protocol violation")
			}
			if !errors.Is(err, edgerun.ErrProtocol) {
				t.Errorf("the peer's fault must be said to be: %v", err)
			}
		})
	}
}

func TestClosingABodyEarlyCancelsTheCall(t *testing.T) {
	cancelled := make(chan struct{})
	in := serve(t, scripted{invoke: func(ctx context.Context, _ *connect.Request[ingressv1.InvokeRequest], st *connect.ServerStream[ingressv1.InvokeResponse]) error {
		st.Send(head(200, "inv-3"))
		st.Send(chunk("first"))
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	}})
	res, err := in.Invoke(context.Background(), &ingressv1.InvokeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(res.Body, buf); err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	<-cancelled // the handler saw the call go away
}

func TestTheTokenIsPresentedAndItsScopeMatters(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(ingressv1connect.NewIngressServiceHandler(scripted{invoke: func(_ context.Context, _ *connect.Request[ingressv1.InvokeRequest], st *connect.ServerStream[ingressv1.InvokeResponse]) error {
		return st.Send(head(200, "x"))
	}}, connect.WithInterceptors(edgerun.Authenticate(edgerun.Verifier{"good": edgerun.ScopeIngress, "other": edgerun.ScopeAdmin}, edgerun.ScopeIngress))))
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	for token, want := range map[string]connect.Code{"": connect.CodeUnauthenticated, "wrong": connect.CodeUnauthenticated, "other": connect.CodePermissionDenied} {
		_, err := edgerun.NewIngress(ts.URL, token).Invoke(context.Background(), &ingressv1.InvokeRequest{})
		if connect.CodeOf(err) != want {
			t.Errorf("token %q: %v, want %v", token, err, want)
		}
	}
	if _, err := edgerun.NewIngress(ts.URL, "good").Invoke(context.Background(), &ingressv1.InvokeRequest{}); err != nil {
		t.Errorf("the right token: %v", err)
	}
}

type artifactServer struct {
	workerv1connect.UnimplementedArtifactServiceHandler
	send func(*connect.ServerStream[workerv1.GetArtifactResponse]) error
}

func (a artifactServer) GetArtifact(_ context.Context, _ *connect.Request[workerv1.GetArtifactRequest], st *connect.ServerStream[workerv1.GetArtifactResponse]) error {
	return a.send(st)
}

// A client that runs whatever it is handed defeats content addressing, so the
// library refuses bytes that are not what their digest says, however they
// arrive.
func TestArtifactBytesAreVerifiedAgainstTheirDigest(t *testing.T) {
	content := []byte("the real module")
	sum := sha256.Sum256(content)
	digest := "sha256:" + hex.EncodeToString(sum[:])

	get := func(send func(*connect.ServerStream[workerv1.GetArtifactResponse]) error) ([]byte, error) {
		mux := http.NewServeMux()
		mux.Handle(workerv1connect.NewArtifactServiceHandler(artifactServer{send: send}))
		srv := httptest.NewServer(mux)
		defer srv.Close()
		return edgerun.NewArtifacts(srv.URL, "").GetArtifact(context.Background(), digest)
	}
	chunks := func(parts ...string) func(*connect.ServerStream[workerv1.GetArtifactResponse]) error {
		return func(st *connect.ServerStream[workerv1.GetArtifactResponse]) error {
			for _, p := range parts {
				if err := st.Send(&workerv1.GetArtifactResponse{Chunk: []byte(p)}); err != nil {
					return err
				}
			}
			return nil
		}
	}

	if got, err := get(chunks("the real ", "module")); err != nil || string(got) != string(content) {
		t.Errorf("honest, in pieces: %q, %v", got, err)
	}
	if _, err := get(chunks("the fake module")); !errors.Is(err, edgerun.ErrDigestMismatch) {
		t.Errorf("other bytes: %v", err)
	}
	if _, err := get(chunks("the real ")); !errors.Is(err, edgerun.ErrDigestMismatch) {
		t.Errorf("a truncated artifact: %v", err)
	}
	if _, err := get(chunks()); !errors.Is(err, edgerun.ErrDigestMismatch) {
		t.Errorf("an empty artifact: %v", err)
	}
	if _, err := get(func(st *connect.ServerStream[workerv1.GetArtifactResponse]) error {
		st.Send(&workerv1.GetArtifactResponse{Chunk: content})
		return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "the orchestrator fell over")
	}); !edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_INTERNAL) {
		t.Errorf("a stream that fails after all the bytes is still a failure: %v", err)
	}
}

// A second head in the middle of a body is the peer's fault, and the body must
// not end cleanly after it.
func TestASecondHeadIsAProtocolViolation(t *testing.T) {
	in := serve(t, scripted{invoke: func(_ context.Context, _ *connect.Request[ingressv1.InvokeRequest], st *connect.ServerStream[ingressv1.InvokeResponse]) error {
		st.Send(head(200, "inv-4"))
		st.Send(chunk("a"))
		st.Send(head(200, "inv-4"))
		st.Send(chunk("b"))
		return nil
	}})
	res, err := in.Invoke(context.Background(), &ingressv1.InvokeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(res.Body)
	if !errors.Is(err, edgerun.ErrProtocol) || string(b) != "a" {
		t.Errorf("read %q, %v", b, err)
	}
}
