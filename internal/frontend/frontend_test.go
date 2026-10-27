package frontend

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
	"github.com/Olian04/edgerun/internal/errs"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeOrchestrator scripts the IngressService.
type fakeOrchestrator struct {
	ingressv1connect.UnimplementedIngressServiceHandler
	maxBody uint64
	behave  func(ctx context.Context, req *ingressv1.InvokeRequest, st *connect.ServerStream[ingressv1.InvokeResponse]) error

	mu    sync.Mutex
	calls []*ingressv1.InvokeRequest
}

func (f *fakeOrchestrator) Describe(context.Context, *connect.Request[ingressv1.DescribeRequest]) (*connect.Response[ingressv1.DescribeResponse], error) {
	return connect.NewResponse(&ingressv1.DescribeResponse{MaxRequestBodyBytes: f.maxBody}), nil
}

func (f *fakeOrchestrator) Invoke(ctx context.Context, req *connect.Request[ingressv1.InvokeRequest], st *connect.ServerStream[ingressv1.InvokeResponse]) error {
	f.mu.Lock()
	f.calls = append(f.calls, req.Msg)
	f.mu.Unlock()
	return f.behave(ctx, req.Msg, st)
}

func (f *fakeOrchestrator) called() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }
func (f *fakeOrchestrator) last() *ingressv1.InvokeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[len(f.calls)-1]
}

func head(status uint32, headers ...string) *ingressv1.InvokeResponse {
	h := &commonv1.HttpResponseHead{Status: status}
	for i := 0; i+1 < len(headers); i += 2 {
		h.Headers = append(h.Headers, &commonv1.Header{Name: headers[i], Value: headers[i+1]})
	}
	return &ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Head{Head: &ingressv1.InvokeHead{Http: h, InvocationId: "inv-1"}}}
}

func chunk(s string) *ingressv1.InvokeResponse {
	return &ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Chunk{Chunk: []byte(s)}}
}

func respond(status uint32, body string, headers ...string) func(context.Context, *ingressv1.InvokeRequest, *connect.ServerStream[ingressv1.InvokeResponse]) error {
	return func(_ context.Context, _ *ingressv1.InvokeRequest, st *connect.ServerStream[ingressv1.InvokeResponse]) error {
		st.Send(head(status, headers...))
		if body != "" {
			st.Send(chunk(body))
		}
		return nil
	}
}

func fail(reason commonv1.ErrorReason, msg string) func(context.Context, *ingressv1.InvokeRequest, *connect.ServerStream[ingressv1.InvokeResponse]) error {
	return func(context.Context, *ingressv1.InvokeRequest, *connect.ServerStream[ingressv1.InvokeResponse]) error {
		return errs.New(reason, "%s", msg)
	}
}

type rig struct {
	orch *fakeOrchestrator
	fe   *Frontend
	srv  *httptest.Server // the frontend, as a client would reach it
}

func newRig(t *testing.T, behave func(context.Context, *ingressv1.InvokeRequest, *connect.ServerStream[ingressv1.InvokeResponse]) error, tweak func(*Config)) *rig {
	t.Helper()
	orch := &fakeOrchestrator{behave: behave, maxBody: 1 << 20}
	mux := http.NewServeMux()
	mux.Handle(ingressv1connect.NewIngressServiceHandler(orch))
	osrv := httptest.NewServer(mux)
	t.Cleanup(osrv.Close)

	cfg := Config{Ingress: ingressv1connect.NewIngressServiceClient(osrv.Client(), osrv.URL), Log: quiet}
	if tweak != nil {
		tweak(&cfg)
	}
	fe := New(cfg)
	srv := httptest.NewServer(fe)
	t.Cleanup(srv.Close)
	return &rig{orch: orch, fe: fe, srv: srv}
}

func (r *rig) do(t *testing.T, method, path string, body string, hdr ...string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(method, r.srv.URL+path, strings.NewReader(body))
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Add(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func headerMap(req *ingressv1.InvokeRequest) map[string][]string {
	m := map[string][]string{}
	for _, h := range req.Request.Headers {
		m[h.Name] = append(m[h.Name], h.Value)
	}
	return m
}

// --- request translation ----------------------------------------------------

func TestRequestIsTranslatedFaithfully(t *testing.T) {
	r := newRig(t, respond(200, "ok"), nil)
	resp := r.do(t, "POST", "/a%2Fb/c%20d?x=1&y=%26", "payload",
		"Content-Type", "text/plain", "X-Custom", "1", "X-Custom", "2",
		"Connection", "close", "Te", "trailers", "Upgrade", "websocket")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	got := r.orch.last().Request
	if got.Method != "POST" || got.Path != "/a/b/c d" || got.RawQuery != "x=1&y=%26" || string(got.Body) != "payload" {
		t.Errorf("request = %+v", got)
	}
	if got.Host == "" || got.RemoteAddr == "" {
		t.Errorf("host/remote_addr missing: %+v", got)
	}
	h := headerMap(r.orch.last())
	if h["content-type"][0] != "text/plain" || strings.Join(h["x-custom"], ",") != "1,2" {
		t.Errorf("headers = %v: names are lower-cased and repeated values kept in order", h)
	}
	for _, hop := range []string{"connection", "te", "upgrade", "transfer-encoding", "keep-alive"} {
		if _, ok := h[hop]; ok {
			t.Errorf("hop-by-hop header %q was forwarded", hop)
		}
	}
	if r.orch.last().RequestId == "" || resp.Header.Get("X-Request-Id") != r.orch.last().RequestId {
		t.Errorf("request id: sent %q, returned %q", r.orch.last().RequestId, resp.Header.Get("X-Request-Id"))
	}
}

func TestClientRequestIDIsKeptOnlyIfSane(t *testing.T) {
	r := newRig(t, respond(200, ""), nil)
	r.do(t, "GET", "/", "", "X-Request-Id", "abc-123")
	if r.orch.last().RequestId != "abc-123" {
		t.Errorf("a sane client id should be kept: %q", r.orch.last().RequestId)
	}
	for _, bad := range []string{strings.Repeat("x", 200), "has space", "new\tline"} {
		r.do(t, "GET", "/", "", "X-Request-Id", bad)
		if got := r.orch.last().RequestId; got == bad || got == "" {
			t.Errorf("id %q should have been replaced, got %q", bad, got)
		}
	}
}

func TestForwardedHeadersAreNotTrustedByDefault(t *testing.T) {
	r := newRig(t, respond(200, ""), nil)
	r.do(t, "GET", "/", "", "X-Forwarded-For", "6.6.6.6", "X-Forwarded-Proto", "https", "X-Real-Ip", "6.6.6.6", "Forwarded", "for=6.6.6.6")
	h := headerMap(r.orch.last())
	if xff := strings.Join(h["x-forwarded-for"], ","); strings.Contains(xff, "6.6.6.6") || !strings.HasPrefix(xff, "127.0.0.1") {
		t.Errorf("a spoofed X-Forwarded-For must be replaced by the real peer: %q", xff)
	}
	if h["x-forwarded-proto"][0] != "http" {
		t.Errorf("proto = %v: this connection was plain http", h["x-forwarded-proto"])
	}
	for _, spoof := range []string{"x-real-ip", "forwarded"} {
		if _, ok := h[spoof]; ok {
			t.Errorf("client-supplied %q must be dropped", spoof)
		}
	}
}

func TestTrustedForwardedHeadersAreAppendedTo(t *testing.T) {
	r := newRig(t, respond(200, ""), func(c *Config) { c.TrustForwarded = true })
	r.do(t, "GET", "/", "", "X-Forwarded-For", "203.0.113.9", "X-Forwarded-Proto", "https")
	h := headerMap(r.orch.last())
	if got := strings.Join(h["x-forwarded-for"], ","); got != "203.0.113.9, 127.0.0.1" {
		t.Errorf("x-forwarded-for = %q: behind a trusted proxy, append the peer", got)
	}
	if h["x-forwarded-proto"][0] != "https" {
		t.Errorf("a trusted proto must be kept: %v", h["x-forwarded-proto"])
	}
}

// rawExchange sends bytes over a plain TCP connection and returns the status
// line, for request forms Go's HTTP client will not produce.
func rawExchange(t *testing.T, addr, request string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	line, _ := bufio.NewReader(conn).ReadString('\n')
	return strings.TrimSpace(line)
}

func TestRequestsWithoutAPathNeverReachTheOrchestrator(t *testing.T) {
	r := newRig(t, respond(200, ""), nil)
	addr := strings.TrimPrefix(r.srv.URL, "http://")

	// Authority-form targets (CONNECT) have no path to route.
	if got := rawExchange(t, addr, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"); !strings.Contains(got, "400") {
		t.Errorf("CONNECT: %q, want 400", got)
	}
	// Go's server answers "OPTIONS *" itself, before any handler: also fine, as
	// long as nothing is forwarded.
	if got := rawExchange(t, addr, "OPTIONS * HTTP/1.1\r\nHost: x\r\n\r\n"); !strings.Contains(got, "200") {
		t.Errorf("OPTIONS *: %q", got)
	}
	if r.orch.called() != 0 {
		t.Errorf("%d orchestrator calls for requests that name no resource", r.orch.called())
	}
}

// --- body limit -------------------------------------------------------------

func TestBodyLimitIsEnforcedHere(t *testing.T) {
	r := newRig(t, respond(200, "ok"), func(c *Config) { c.MaxBodyBytes = 100 })

	if resp := r.do(t, "POST", "/", strings.Repeat("x", 100)); resp.StatusCode != 200 {
		t.Errorf("a body exactly at the limit: %d", resp.StatusCode)
	}
	before := r.orch.called()
	if resp := r.do(t, "POST", "/", strings.Repeat("x", 101)); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("declared length over the limit: %d", resp.StatusCode)
	}
	// Chunked upload with no declared length.
	req, _ := http.NewRequest("POST", r.srv.URL+"/", io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("x"), 500))))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("undeclared length over the limit: %d", resp.StatusCode)
	}
	if r.orch.called() != before {
		t.Error("an oversized body must never reach the orchestrator")
	}
}

func TestLimitFollowsTheOrchestrator(t *testing.T) {
	r := newRig(t, respond(200, ""), nil)
	r.orch.maxBody = 10
	if err := r.fe.RefreshLimits(context.Background()); err != nil {
		t.Fatal(err)
	}
	if resp := r.do(t, "POST", "/", strings.Repeat("x", 11)); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("after learning a 10 byte limit, 11 bytes: %d", resp.StatusCode)
	}
	if resp := r.do(t, "POST", "/", strings.Repeat("x", 10)); resp.StatusCode != 200 {
		t.Errorf("10 bytes: %d", resp.StatusCode)
	}
}

// --- response ---------------------------------------------------------------

func TestResponseIsRelayedAndFramingHeadersAreOurs(t *testing.T) {
	r := newRig(t, respond(201, "hello", "content-type", "text/x", "set-cookie", "a=1", "set-cookie", "b=2",
		"content-length", "9999", "connection", "close", "transfer-encoding", "gzip", "keep-alive", "x"), nil)
	resp := r.do(t, "GET", "/", "")
	if resp.StatusCode != 201 || readAll(t, resp) != "hello" {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if got := resp.Header.Values("Set-Cookie"); len(got) != 2 || got[0] != "a=1" || got[1] != "b=2" {
		t.Errorf("repeated headers must be preserved: %v", got)
	}
	if resp.Header.Get("Content-Type") != "text/x" {
		t.Error("content-type lost")
	}
	if resp.Header.Get("X-Edgerun-Invocation-Id") != "inv-1" {
		t.Error("the invocation id should be exposed for support")
	}
	if cl := resp.Header.Get("Content-Length"); cl == "9999" {
		t.Error("a guest-supplied Content-Length must not be trusted: this server frames the response")
	}
	if resp.Header.Get("Keep-Alive") != "" {
		t.Error("hop-by-hop headers must not be relayed")
	}
}

func TestHeadRequestsGetHeadersOnly(t *testing.T) {
	r := newRig(t, respond(200, "this body must not be sent", "content-type", "text/x"), nil)
	resp := r.do(t, "HEAD", "/", "")
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/x" || readAll(t, resp) != "" {
		t.Errorf("HEAD: status %d", resp.StatusCode)
	}
}

func TestBodylessStatusesDoNotBreakTheStream(t *testing.T) {
	r := newRig(t, respond(204, "ignored"), nil)
	if resp := r.do(t, "GET", "/", ""); resp.StatusCode != 204 {
		t.Errorf("status %d", resp.StatusCode)
	}
}

func TestInvalidStatusFromTheOrchestratorIsABadGateway(t *testing.T) {
	r := newRig(t, respond(99, "x"), nil)
	if resp := r.do(t, "GET", "/", ""); resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status %d", resp.StatusCode)
	}
}

func TestResponsesStreamWithoutBuffering(t *testing.T) {
	release := make(chan struct{})
	behave := func(ctx context.Context, _ *ingressv1.InvokeRequest, st *connect.ServerStream[ingressv1.InvokeResponse]) error {
		st.Send(head(200))
		st.Send(chunk("first"))
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return st.Send(chunk("second"))
	}
	r := newRig(t, behave, nil)
	resp := r.do(t, "GET", "/", "")
	buf := make([]byte, 5)
	if _, err := io.ReadFull(resp.Body, buf); err != nil || string(buf) != "first" {
		t.Fatalf("the first chunk must arrive while the second is still being produced: %q %v", buf, err)
	}
	close(release)
	rest, _ := io.ReadAll(resp.Body)
	if string(rest) != "second" {
		t.Errorf("rest = %q", rest)
	}
}

// --- errors -----------------------------------------------------------------

func TestPlatformErrorsMapToTheProtocolTable(t *testing.T) {
	for _, tc := range []struct {
		reason commonv1.ErrorReason
		status int
		retry  bool
	}{
		{commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND, 404, false},
		{commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, 503, true},
		{commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, 504, false},
		{commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, 502, false},
		{commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH, 502, false},
		{commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE, 413, false},
		{commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE, 502, false},
		{commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND, 502, false},
		{commonv1.ErrorReason_ERROR_REASON_INTERNAL, 500, false},
	} {
		t.Run(tc.reason.String(), func(t *testing.T) {
			const secret = "OPERATOR-ONLY: worker 10.1.2.3 stderr: password=hunter2"
			r := newRig(t, fail(tc.reason, secret), nil)
			resp := r.do(t, "GET", "/", "")
			body := readAll(t, resp)
			if resp.StatusCode != tc.status {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.status)
			}
			if (resp.Header.Get("Retry-After") != "") != tc.retry {
				t.Errorf("Retry-After = %q, want present=%v", resp.Header.Get("Retry-After"), tc.retry)
			}
			if strings.Contains(body, "OPERATOR") || strings.Contains(body, "hunter2") || strings.Contains(body, "10.1.2.3") {
				t.Errorf("the client saw operator detail: %q", body)
			}
			if resp.Header.Get("X-Request-Id") == "" {
				t.Error("error responses carry the request id too")
			}
		})
	}
}

func TestUntypedErrorsAreClassifiedByWhatWeCanObserve(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		status int
	}{
		"transport message cap": {connect.NewError(connect.CodeResourceExhausted, errors.New("message size is larger than configured max")), 413},
		"orchestrator down":     {connect.NewError(connect.CodeUnavailable, errors.New("connection refused")), 503},
		"bare deadline":         {connect.NewError(connect.CodeDeadlineExceeded, errors.New("context deadline exceeded")), 504},
		"our credentials wrong": {connect.NewError(connect.CodeUnauthenticated, errors.New("bad token")), 502},
		"forbidden":             {connect.NewError(connect.CodePermissionDenied, errors.New("scope")), 502},
		"anything else":         {connect.NewError(connect.CodeUnknown, errors.New("???")), 500},
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t, func(context.Context, *ingressv1.InvokeRequest, *connect.ServerStream[ingressv1.InvokeResponse]) error {
				return tc.err
			}, nil)
			if resp := r.do(t, "GET", "/", ""); resp.StatusCode != tc.status {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.status)
			}
		})
	}
}

func TestFailureAfterTheHeadAbortsTheConnection(t *testing.T) {
	behave := func(_ context.Context, _ *ingressv1.InvokeRequest, st *connect.ServerStream[ingressv1.InvokeResponse]) error {
		st.Send(head(200))
		st.Send(chunk("partial"))
		return errs.New(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "worker died")
	}
	r := newRig(t, behave, nil)
	resp := r.do(t, "GET", "/", "")
	if resp.StatusCode != 200 {
		t.Fatalf("the status line was already out: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err == nil {
		t.Fatalf("the body ended cleanly with %q: a truncated response must not look complete", body)
	}
}

func TestOwnTimeoutIs504(t *testing.T) {
	hang := func(ctx context.Context, _ *ingressv1.InvokeRequest, _ *connect.ServerStream[ingressv1.InvokeResponse]) error {
		<-ctx.Done()
		return ctx.Err()
	}
	r := newRig(t, hang, func(c *Config) { c.RequestTimeout = 300 * time.Millisecond })
	start := time.Now()
	resp := r.do(t, "GET", "/", "")
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("status = %d", resp.StatusCode)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("a 300ms budget took %v", d)
	}
}

func TestClientDisconnectCancelsTheInvocation(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	hang := func(ctx context.Context, _ *ingressv1.InvokeRequest, st *connect.ServerStream[ingressv1.InvokeResponse]) error {
		st.Send(head(200))
		st.Send(chunk("x"))
		close(started)
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	}
	r := newRig(t, hang, nil)
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", r.srv.URL+"/", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	cancel() // the client hangs up
	resp.Body.Close()
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the invocation kept running after the client left: a worker slot would be wasted on nobody")
	}
}

func TestStatusFunctionDirectly(t *testing.T) {
	if s, _ := Status(context.Canceled, context.Canceled); s != 0 {
		t.Errorf("a cancelled client needs no response, got %d", s)
	}
	// Our own deadline wins even if the error looks unrelated.
	if s, _ := Status(connect.NewError(connect.CodeUnknown, errors.New("x")), context.DeadlineExceeded); s != 504 {
		t.Errorf("status = %d, want 504", s)
	}
}
