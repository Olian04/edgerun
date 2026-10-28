package frontendconf

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

// response is what an HTTP client saw.
type response struct {
	status  int
	header  http.Header
	body    []byte
	bodyErr error
}

func client() *http.Client {
	return &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   60 * time.Second,
		// Redirects are answers too.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// send makes a request to the frontend, marked so that the orchestrator's
// record of it can be told from any other.
func (h *harness) send(t testing.TB, id, method, target string, body io.Reader, mod func(*http.Request)) response {
	t.Helper()
	req, err := http.NewRequest(method, h.url+target, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Conformance-Test", id)
	if mod != nil {
		mod(req)
	}
	resp, err := client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, target, err)
	}
	defer resp.Body.Close()
	b, berr := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, header: resp.Header, body: b, bodyErr: berr}
}

func (h *harness) get(t testing.TB, id string) response {
	t.Helper()
	return h.send(t, id, "GET", "/", nil, nil)
}

func (h *harness) only(t testing.TB, id string) seen {
	t.Helper()
	got := h.orch.invocationsWith(id)
	if len(got) != 1 {
		t.Fatalf("the orchestrator saw %d Invoke calls for this request, want 1", len(got))
	}
	return got[0]
}

func headerValues(r *commonv1.HttpRequest, name string) []string {
	var out []string
	for _, h := range r.GetHeaders() {
		if h.GetName() == name {
			out = append(out, h.GetValue())
		}
	}
	return out
}

func (h *harness) requestMapping(t *testing.T) {
	h.orch.script(answer(200, "ok"))
	body := "request \x00 body"
	h.send(t, "map", "POST", "/a/b/../c?x=1&y=%20z&x=2", strings.NewReader(body), func(r *http.Request) {
		r.Host = "App.Example.Test"
		r.Header.Add("X-One", "a")
		r.Header.Add("X-One", "b")
		r.Header.Set("Content-Type", "text/x-conformance")
		// Describe one connection, not the message.
		r.Header.Set("Keep-Alive", "timeout=5")
		r.Header.Set("Proxy-Authorization", "Basic c2VjcmV0")
		r.Header.Set("Proxy-Connection", "keep-alive")
		r.Header.Set("TE", "trailers")
		r.Header.Set("Upgrade", "x-conformance")
		// Claims about who the client is: not believed.
		r.Header.Set("X-Forwarded-For", "6.6.6.6")
		r.Header.Set("X-Forwarded-Host", "evil.test")
		r.Header.Set("X-Forwarded-Proto", "https")
		r.Header.Set("X-Real-Ip", "6.6.6.6")
		r.Header.Set("Forwarded", "for=6.6.6.6")
	})
	got := h.only(t, "map").req.GetRequest()

	if got.GetMethod() != "POST" {
		t.Errorf("method = %q", got.GetMethod())
	}
	if got.GetHost() != "App.Example.Test" {
		t.Errorf("host = %q, want it as the Host header arrived", got.GetHost())
	}
	if got.GetPath() != "/a/b/../c" {
		t.Errorf("path = %q, want it as received, not cleaned", got.GetPath())
	}
	if got.GetRawQuery() != "x=1&y=%20z&x=2" {
		t.Errorf("raw_query = %q, want it as received, without the '?'", got.GetRawQuery())
	}
	if string(got.GetBody()) != body {
		t.Errorf("body = %q", got.GetBody())
	}
	for _, hd := range got.GetHeaders() {
		if hd.GetName() != strings.ToLower(hd.GetName()) {
			t.Errorf("header name %q is not lower-cased", hd.GetName())
		}
	}
	if v := headerValues(got, "x-one"); fmt.Sprint(v) != "[a b]" {
		t.Errorf("x-one = %v, want every value, in order", v)
	}
	if v := headerValues(got, "content-type"); fmt.Sprint(v) != "[text/x-conformance]" {
		t.Errorf("content-type = %v", v)
	}
	for _, name := range []string{"keep-alive", "proxy-authorization", "proxy-connection", "te", "upgrade", "connection", "transfer-encoding", "trailer", "proxy-authenticate"} {
		if v := headerValues(got, name); len(v) > 0 {
			t.Errorf("hop-by-hop header %q was forwarded: %v", name, v)
		}
	}

	host, port, err := net.SplitHostPort(got.GetRemoteAddr())
	if err != nil || host == "" || port == "" {
		t.Fatalf("remote_addr = %q, want the peer's ip:port", got.GetRemoteAddr())
	}
	if v := headerValues(got, "x-forwarded-for"); fmt.Sprint(v) != "["+host+"]" {
		t.Errorf("x-forwarded-for = %v, want only the peer %s: a client's own claim is not believed", v, host)
	}
	if v := headerValues(got, "x-forwarded-host"); fmt.Sprint(v) != "[App.Example.Test]" && fmt.Sprint(v) != "[app.example.test]" {
		t.Errorf("x-forwarded-host = %v, want the Host the client used", v)
	}
	if v := headerValues(got, "x-forwarded-proto"); fmt.Sprint(v) != "[http]" {
		t.Errorf("x-forwarded-proto = %v, want [http]", v)
	}
	for _, name := range []string{"x-real-ip", "forwarded"} {
		if v := headerValues(got, name); len(v) > 0 {
			t.Errorf("client-supplied %q was forwarded: %v", name, v)
		}
	}

	t.Run("the call", func(t *testing.T) {
		call := h.only(t, "map")
		if call.header.Get("Authorization") != "Bearer "+h.env.IngressToken {
			t.Errorf("Authorization = %q, want the ingress token", call.header.Get("Authorization"))
		}
	})

	t.Run("no body", func(t *testing.T) {
		h.send(t, "map-nobody", "GET", "/plain", nil, nil)
		got := h.only(t, "map-nobody").req.GetRequest()
		if got.GetMethod() != "GET" || got.GetPath() != "/plain" || got.GetRawQuery() != "" || len(got.GetBody()) != 0 {
			t.Errorf("GET /plain = %+v", got)
		}
	})
}

func (h *harness) requestIDs(t *testing.T) {
	h.orch.script(answer(200, "ok"))

	t.Run("generated", func(t *testing.T) {
		seenIDs := map[string]bool{}
		for i := 0; i < 5; i++ {
			id := "gen-" + strconv.Itoa(i)
			r := h.get(t, id)
			rid := h.only(t, id).req.GetRequestId()
			if rid == "" {
				t.Fatal("request_id is empty")
			}
			if r.header.Get("X-Request-Id") != rid {
				t.Errorf("X-Request-Id = %q, want the id given to the orchestrator (%q)", r.header.Get("X-Request-Id"), rid)
			}
			if seenIDs[rid] {
				t.Errorf("request id %q was issued twice", rid)
			}
			seenIDs[rid] = true
		}
	})
	t.Run("honoured", func(t *testing.T) {
		r := h.send(t, "id-given", "GET", "/", nil, func(r *http.Request) { r.Header.Set("X-Request-Id", "client-id-123") })
		if got := h.only(t, "id-given").req.GetRequestId(); got != "client-id-123" {
			t.Errorf("request_id = %q, want the client's printable id", got)
		}
		if r.header.Get("X-Request-Id") != "client-id-123" {
			t.Errorf("X-Request-Id = %q", r.header.Get("X-Request-Id"))
		}
	})
	t.Run("refused", func(t *testing.T) {
		for name, bad := range map[string]string{"too long": strings.Repeat("a", 129), "has space": "has space", "control": "tab\there"} {
			id := "id-bad-" + strings.ReplaceAll(name, " ", "-")
			h.send(t, id, "GET", "/", nil, func(r *http.Request) { r.Header.Set("X-Request-Id", bad) })
			if got := h.only(t, id).req.GetRequestId(); got == bad || got == "" {
				t.Errorf("%s: request_id = %q, want a fresh one", name, got)
			}
		}
	})
	t.Run("on errors too", func(t *testing.T) {
		h.orch.script(failWith(commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND))
		r := h.get(t, "id-error")
		if r.status != 404 || r.header.Get("X-Request-Id") == "" {
			t.Errorf("status %d, X-Request-Id %q: an error response carries the request id too", r.status, r.header.Get("X-Request-Id"))
		}
	})
}

func (h *harness) bodyLimit(t *testing.T) {
	h.orch.script(answer(200, "ok"))
	// The frontend may be slow to adopt the limit it learned: allow for it.
	check.Eventually(t, "the body limit to be enforced", settle, func() bool {
		return h.send(t, "limit-probe", "POST", "/", strings.NewReader(strings.Repeat("x", bodyLimit+1)), nil).status == 413
	})

	t.Run("at the limit", func(t *testing.T) {
		r := h.send(t, "limit-at", "POST", "/", strings.NewReader(strings.Repeat("x", bodyLimit)), nil)
		if r.status != 200 || len(h.only(t, "limit-at").req.GetRequest().GetBody()) != bodyLimit {
			t.Errorf("a body of exactly the reported limit (%d) must be forwarded: status %d", bodyLimit, r.status)
		}
	})
	t.Run("over the limit", func(t *testing.T) {
		r := h.send(t, "limit-over", "POST", "/", strings.NewReader(strings.Repeat("x", bodyLimit+1)), nil)
		if r.status != 413 {
			t.Errorf("status = %d, want 413", r.status)
		}
		if n := len(h.orch.invocationsWith("limit-over")); n != 0 {
			t.Errorf("a request over the limit was forwarded %d times: it must be refused at the edge", n)
		}
	})
	t.Run("over the limit, length not declared", func(t *testing.T) {
		r := h.send(t, "limit-chunked", "POST", "/", io.NopCloser(strings.NewReader(strings.Repeat("x", 8*bodyLimit))), func(r *http.Request) {
			r.ContentLength = -1 // chunked
		})
		if r.status != 413 {
			t.Errorf("status = %d, want 413: a body must be bounded even when its length is not declared", r.status)
		}
		if n := len(h.orch.invocationsWith("limit-chunked")); n != 0 {
			t.Errorf("an oversized chunked body was forwarded %d times", n)
		}
	})
}

func (h *harness) badRequests(t *testing.T) {
	h.orch.script(answer(200, "ok"))
	before := len(h.orch.invocations())
	addr := strings.TrimPrefix(h.url, "http://")
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	fmt.Fprintf(conn, "OPTIONS * HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", addr)
	line, _ := io.ReadAll(conn)
	status := ""
	if f := strings.Fields(string(line)); len(f) > 1 {
		status = f[1]
	}
	if len(status) != 3 || (status[0] != '2' && status[0] != '4') {
		t.Errorf("OPTIONS * answered %q, want a 2xx or 4xx given by the frontend itself", status)
	}
	if got := len(h.orch.invocations()); got != before {
		t.Errorf("OPTIONS * was forwarded")
	}
}

func (h *harness) responseMapping(t *testing.T) {
	t.Run("status, headers and body", func(t *testing.T) {
		h.orch.script(answer(201, "hello", hdr("set-cookie", "a=1"), hdr("set-cookie", "b=2"), hdr("x-custom", "yes"), hdr("content-type", "text/x-conformance")))
		r := h.get(t, "resp-ok")
		if r.status != 201 || string(r.body) != "hello" || r.bodyErr != nil {
			t.Fatalf("status %d, body %q, %v", r.status, r.body, r.bodyErr)
		}
		if fmt.Sprint(r.header.Values("Set-Cookie")) != "[a=1 b=2]" {
			t.Errorf("Set-Cookie = %v: repeated headers must all be forwarded", r.header.Values("Set-Cookie"))
		}
		if r.header.Get("X-Custom") != "yes" || r.header.Get("Content-Type") != "text/x-conformance" {
			t.Errorf("headers = %v", r.header)
		}
	})
	t.Run("the frontend frames the response", func(t *testing.T) {
		h.orch.script(answer(200, "short", hdr("content-length", "999"), hdr("keep-alive", "timeout=1"), hdr("transfer-encoding", "gzip"), hdr("connection", "upgrade"), hdr("upgrade", "x-conformance")))
		r := h.get(t, "resp-framing")
		if r.status != 200 || string(r.body) != "short" || r.bodyErr != nil {
			t.Fatalf("a function that lies about its length must still be served correctly: %d %q %v", r.status, r.body, r.bodyErr)
		}
		for _, name := range []string{"Keep-Alive", "Upgrade"} {
			if v := r.header.Get(name); v != "" {
				t.Errorf("connection-level header %s = %q reached the client", name, v)
			}
		}
	})
	t.Run("a function's own errors are not platform errors", func(t *testing.T) {
		for _, status := range []uint32{404, 500, 503} {
			h.orch.script(answer(status, "the function says so"))
			r := h.get(t, "resp-fn-"+strconv.Itoa(int(status)))
			if r.status != int(status) || string(r.body) != "the function says so" {
				t.Errorf("a function answering %d came out as %d %q", status, r.status, r.body)
			}
		}
	})
	t.Run("a status that is not a final status", func(t *testing.T) {
		for _, status := range []uint32{0, 99, 100, 199, 600, 1000} {
			h.orch.script(answer(status, "x"))
			if r := h.get(t, "resp-bad-"+strconv.Itoa(int(status))); r.status != 502 {
				t.Errorf("a response head with status %d came out as %d, want 502", status, r.status)
			}
		}
	})
	t.Run("an empty body", func(t *testing.T) {
		h.orch.script(answer(204, ""))
		if r := h.get(t, "resp-empty"); r.status != 204 || len(r.body) != 0 {
			t.Errorf("%d %q", r.status, r.body)
		}
	})
	t.Run("a stream with no head", func(t *testing.T) {
		h.orch.script(func(context.Context, *ingressv1.InvokeRequest, *connect.ServerStream[ingressv1.InvokeResponse]) error {
			return nil
		})
		if r := h.get(t, "resp-nohead"); r.status != 502 {
			t.Errorf("an orchestrator that ends its answer without a head is a gateway fault: %d, want 502", r.status)
		}
	})
}

// streaming: chunks reach the client as they are produced.
func (h *harness) streaming(t *testing.T) {
	release := make(chan struct{})
	h.orch.script(func(_ context.Context, _ *ingressv1.InvokeRequest, st *connect.ServerStream[ingressv1.InvokeResponse]) error {
		if err := sendHead(st, 200, hdr("content-type", "text/plain")); err != nil {
			return err
		}
		if err := sendChunk(st, "one,"); err != nil {
			return err
		}
		select {
		case <-release:
		case <-time.After(10 * time.Second):
			return check.Error(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "the first chunk never reached the client")
		}
		return sendChunk(st, "two")
	})
	req, _ := http.NewRequest("GET", h.url+"/", nil)
	req.Header.Set("X-Conformance-Test", "stream")
	resp, err := client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, 4)
	if _, err := io.ReadFull(resp.Body, first); err != nil || string(first) != "one," {
		t.Fatalf("the first chunk did not arrive while the response was still being produced: %q, %v", first, err)
	}
	close(release)
	rest, err := io.ReadAll(resp.Body)
	if err != nil || string(rest) != "two" {
		t.Errorf("the rest = %q, %v", rest, err)
	}
}

func (h *harness) platformErrors(t *testing.T) {
	for _, c := range []struct {
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
		t.Run(c.reason.String(), func(t *testing.T) {
			h.orch.script(failWith(c.reason))
			r := h.get(t, "err-"+c.reason.String())
			if r.status != c.status {
				t.Errorf("status = %d, want %d", r.status, c.status)
			}
			if got := r.header.Get("Retry-After"); (got != "") != c.retry || (c.retry && got != "1") {
				t.Errorf("Retry-After = %q, want %v", got, map[bool]string{true: "1", false: "none"}[c.retry])
			}
			if strings.Contains(string(r.body), "SECRET-OPERATOR-DETAIL") || strings.Contains(fmt.Sprint(r.header), "SECRET-OPERATOR-DETAIL") {
				t.Errorf("operator detail reached the client: %q", r.body)
			}
		})
	}
}

// untypedErrors: failures that carry no ErrorInfo are classified by what the
// frontend can observe.
func (h *harness) untypedErrors(t *testing.T) {
	for _, c := range []struct {
		code   connect.Code
		status int
		retry  bool
	}{
		{connect.CodeResourceExhausted, 413, false}, // the transport's size cap
		{connect.CodeUnavailable, 503, true},
		{connect.CodeDeadlineExceeded, 504, false},
		{connect.CodeUnauthenticated, 502, false}, // the frontend's own credentials
		{connect.CodePermissionDenied, 502, false},
		{connect.CodeInternal, 500, false},
		{connect.CodeUnknown, 500, false},
		{connect.CodeInvalidArgument, 500, false},
	} {
		t.Run(c.code.String(), func(t *testing.T) {
			h.orch.script(func(context.Context, *ingressv1.InvokeRequest, *connect.ServerStream[ingressv1.InvokeResponse]) error {
				return connect.NewError(c.code, fmt.Errorf("%s", leak))
			})
			r := h.get(t, "untyped-"+c.code.String())
			if r.status != c.status {
				t.Errorf("status = %d, want %d", r.status, c.status)
			}
			if got := r.header.Get("Retry-After"); (got != "") != c.retry {
				t.Errorf("Retry-After = %q", got)
			}
			if strings.Contains(string(r.body), "SECRET-OPERATOR-DETAIL") {
				t.Errorf("operator detail reached the client: %q", r.body)
			}
		})
	}
}

// failureAfterHead: once the status line is out, the only honest signal left
// is a broken transfer.
func (h *harness) failureAfterHead(t *testing.T) {
	h.orch.script(func(_ context.Context, _ *ingressv1.InvokeRequest, st *connect.ServerStream[ingressv1.InvokeResponse]) error {
		if err := sendHead(st, 200, hdr("content-type", "text/plain")); err != nil {
			return err
		}
		if err := sendChunk(st, "partial"); err != nil {
			return err
		}
		return check.Error(commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, leak)
	})
	r := h.get(t, "after-head")
	if r.status != 200 {
		t.Errorf("status = %d: the head had been sent", r.status)
	}
	if r.bodyErr == nil {
		t.Errorf("the client read %q to a clean end: a response that failed after it began must be aborted, not finished", r.body)
	}
	if strings.Contains(string(r.body), "SECRET-OPERATOR-DETAIL") {
		t.Error("operator detail reached the client")
	}
}

// deadline: the invocation has a budget, and the client gets an answer when it
// is spent.
func (h *harness) deadline(t *testing.T) {
	h.orch.script(func(ctx context.Context, _ *ingressv1.InvokeRequest, _ *connect.ServerStream[ingressv1.InvokeResponse]) error {
		<-ctx.Done()
		return ctx.Err()
	})
	begin := time.Now()
	r := h.get(t, "deadline")
	took := time.Since(begin)
	if r.status != 504 {
		t.Errorf("status = %d, want 504", r.status)
	}
	if took > h.env.RequestTimeout+10*time.Second {
		t.Errorf("answered after %v with a budget of %v", took, h.env.RequestTimeout)
	}
	ms, err := strconv.Atoi(h.only(t, "deadline").header.Get("Connect-Timeout-Ms"))
	if err != nil || ms <= 0 || time.Duration(ms)*time.Millisecond > h.env.RequestTimeout {
		t.Errorf("Connect-Timeout-Ms = %q, want the request budget (at most %v)", h.only(t, "deadline").header.Get("Connect-Timeout-Ms"), h.env.RequestTimeout)
	}
}

// clientGoesAway: nobody is waiting for the answer, so the invocation is
// cancelled rather than left to run.
func (h *harness) clientGoesAway(t *testing.T) {
	started := make(chan struct{}, 1)
	ended := make(chan error, 1)
	h.orch.script(func(ctx context.Context, _ *ingressv1.InvokeRequest, _ *connect.ServerStream[ingressv1.InvokeResponse]) error {
		started <- struct{}{}
		select {
		case <-ctx.Done():
			ended <- ctx.Err()
		case <-time.After(h.env.RequestTimeout / 2):
			ended <- nil // our own deadline is not the cancellation under test
		}
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", h.url+"/", nil)
	req.Header.Set("X-Conformance-Test", "gone")
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := client().Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the request never reached the orchestrator")
	}
	cancel()
	<-done
	select {
	case err := <-ended:
		if err == nil {
			t.Error("the invocation went on after the client had gone")
		}
	case <-time.After(10 * time.Second):
		t.Error("the orchestrator was never told the client had gone")
	}
}

// orchestratorDown: with nobody to forward to, the answer is "try again".
func (h *harness) orchestratorDown(t *testing.T) {
	h.orch.script(answer(200, "ok"))
	h.orch.srv.CloseClientConnections()
	h.orch.srv.Close()
	r := h.get(t, "down")
	if r.status != 503 || r.header.Get("Retry-After") != "1" {
		t.Errorf("status %d, Retry-After %q, want 503 and 1", r.status, r.header.Get("Retry-After"))
	}
}
