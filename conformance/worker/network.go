package workerconf

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/egress/v1/egressv1connect"
)

// fakeEgress is the network service side of the egress seam. It records every
// call the worker makes and answers as the test scripts it.
type fakeEgress struct {
	srv *httptest.Server

	mu     sync.Mutex
	calls  []egressCall
	behave func(ctx context.Context, req *egressv1.FetchRequest, st *connect.ServerStream[egressv1.FetchResponse]) error
	gone   chan struct{} // receives when a call is abandoned by the worker
}

type egressCall struct {
	req    *egressv1.FetchRequest
	header http.Header
}

func newFakeEgress(t testing.TB) *fakeEgress {
	e := &fakeEgress{gone: make(chan struct{}, 16)}
	mux := http.NewServeMux()
	mux.Handle(egressv1connect.NewEgressServiceHandler(egressHandler{e}))
	e.srv = httptest.NewServer(mux)
	t.Cleanup(e.srv.Close)
	return e
}

func (e *fakeEgress) script(f func(ctx context.Context, req *egressv1.FetchRequest, st *connect.ServerStream[egressv1.FetchResponse]) error) {
	e.mu.Lock()
	e.calls, e.behave = nil, f
	e.mu.Unlock()
}

func (e *fakeEgress) received() []egressCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]egressCall(nil), e.calls...)
}

type egressHandler struct{ e *fakeEgress }

func (h egressHandler) Describe(context.Context, *connect.Request[egressv1.DescribeRequest]) (*connect.Response[egressv1.DescribeResponse], error) {
	return connect.NewResponse(&egressv1.DescribeResponse{
		Implementation:      &commonv1.Implementation{Name: "conformance-egress", Protocols: []string{"edgerun.egress.v1"}},
		MaxRequestBodyBytes: 1 << 20, MaxResponseBodyBytes: 4 << 20,
	}), nil
}

func (h egressHandler) Fetch(ctx context.Context, r *connect.Request[egressv1.FetchRequest], st *connect.ServerStream[egressv1.FetchResponse]) error {
	if r.Header().Get("Authorization") != "Bearer "+egressToken {
		return connect.NewError(connect.CodeUnauthenticated, nil)
	}
	h.e.mu.Lock()
	h.e.calls = append(h.e.calls, egressCall{req: r.Msg, header: r.Header().Clone()})
	b := h.e.behave
	h.e.mu.Unlock()
	if b == nil {
		return reasonError(commonv1.ErrorReason_ERROR_REASON_INTERNAL, connect.CodeInternal, "no script")
	}
	return b(ctx, r.Msg, st)
}

// reply answers with a status, headers and a body in chunks.
func reply(status uint32, headers []*commonv1.Header, body ...[]byte) func(context.Context, *egressv1.FetchRequest, *connect.ServerStream[egressv1.FetchResponse]) error {
	return func(_ context.Context, _ *egressv1.FetchRequest, st *connect.ServerStream[egressv1.FetchResponse]) error {
		if err := st.Send(&egressv1.FetchResponse{Part: &egressv1.FetchResponse_Head{Head: &commonv1.HttpResponseHead{Status: status, Headers: headers}}}); err != nil {
			return err
		}
		for _, c := range body {
			if err := st.Send(&egressv1.FetchResponse{Part: &egressv1.FetchResponse_Chunk{Chunk: c}}); err != nil {
				return err
			}
		}
		return nil
	}
}

func refuse(reason commonv1.ErrorReason, code connect.Code) func(context.Context, *egressv1.FetchRequest, *connect.ServerStream[egressv1.FetchResponse]) error {
	return func(context.Context, *egressv1.FetchRequest, *connect.ServerStream[egressv1.FetchResponse]) error {
		if reason == commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
			return connect.NewError(code, nil)
		}
		return reasonError(reason, code, "scripted")
	}
}

func hdr(name, value string) *commonv1.Header { return &commonv1.Header{Name: name, Value: value} }

// netCall runs the network guest's /fetch path with the given request text and
// allow list, and returns the guest's answer split into the code it got and
// the response it read.
func (h *harness) netCall(t *testing.T, path, request string, allow []string, mod func(*invocation)) (code int, response string, out outcome) {
	t.Helper()
	inv := invocation{path: path, method: "POST", body: request, digest: h.netGood, abi: "wagi-2", egress: allow}
	if mod != nil {
		mod(&inv)
	}
	out = h.ok(t, inv)
	text := string(out.body)
	first, rest, _ := strings.Cut(text, "\n")
	if n, ok := strings.CutPrefix(first, "code="); ok {
		code, _ = strconv.Atoi(n)
		return code, rest, out
	}
	return 0, text, out
}

// network checks wagi-2: a guest's outbound requests go through the network
// service, only for the destinations its function allows, and the host module
// behaves as the ABI says.
func (h *harness) network(t *testing.T) {
	offered := false
	for _, a := range h.reg.Info.GetAbis() {
		offered = offered || a == "wagi-2"
	}
	if !offered {
		t.Skip("the worker does not offer wagi-2, so it offers guests no network")
	}
	ok := reply(200, nil, []byte("ok"))

	t.Run("a wagi-2 module that uses no network runs", func(t *testing.T) {
		h.egress.script(ok)
		out := h.ok(t, invocation{path: "/noop", digest: h.netGood, abi: "wagi-2"})
		if !strings.Contains(string(out.body), "no network used") {
			t.Errorf("body = %q", out.body)
		}
	})

	t.Run("wagi-1 may not import the network", func(t *testing.T) {
		out := h.call(t, invocation{path: "/noop", digest: h.netGood, abi: "wagi-1"})
		wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
	})

	t.Run("closed by default", func(t *testing.T) {
		h.egress.script(ok)
		code, _, _ := h.netCall(t, "/fetch", "GET http://api.test/\n\n", nil, nil)
		if code != -1 {
			t.Errorf("code = %d, want -1 (denied) for a function with an empty allow list", code)
		}
		if n := len(h.egress.received()); n != 0 {
			t.Errorf("the worker called the network service %d times for a function that may reach nothing", n)
		}
	})

	t.Run("a request and its response", func(t *testing.T) {
		long := strings.Repeat("0123456789", 25) // more than one 100-byte read
		h.egress.script(reply(201, []*commonv1.Header{hdr("x-a", "1"), hdr("x-a", "2"), hdr("content-type", "text/x-conformance")}, []byte(long[:100]), []byte(long[100:])))
		allow := []string{"api.test", "*.cdn.test:8443"}
		code, resp, out := h.netCall(t, "/fetch", "POST http://api.test/v1/items?x=1\nContent-Type: text/plain\nX-Two: 2\nX-Two: 3\n\nthe body", allow, func(inv *invocation) {
			inv.timeout = 20 * time.Second
		})
		want := "Status: 201\nx-a: 1\nx-a: 2\ncontent-type: text/x-conformance\n\n" + long
		if code != len(want) || resp != want {
			t.Errorf("code %d, response %q; want %d and %q", code, resp, len(want), want)
		}
		calls := h.egress.received()
		if len(calls) != 1 {
			t.Fatalf("%d calls to the network service, want 1", len(calls))
		}
		got := calls[0]
		if got.req.GetMethod() != "POST" || got.req.GetUrl() != "http://api.test/v1/items?x=1" || string(got.req.GetBody()) != "the body" {
			t.Errorf("request: %s %s %q", got.req.GetMethod(), got.req.GetUrl(), got.req.GetBody())
		}
		var two []string
		for _, hd := range got.req.GetHeaders() {
			if strings.EqualFold(hd.GetName(), "x-two") {
				two = append(two, hd.GetValue())
			}
		}
		if len(two) != 2 || two[0] != "2" || two[1] != "3" {
			t.Errorf("headers = %v: every header line the guest wrote must be passed on", got.req.GetHeaders())
		}
		if strings.Join(got.req.GetAllow(), "|") != strings.Join(allow, "|") {
			t.Errorf("allow = %v, want the function's list as the orchestrator gave it: %v", got.req.GetAllow(), allow)
		}
		if got.req.GetInvocationId() != "conformance-"+t.Name() && got.req.GetInvocationId() == "" {
			t.Error("invocation_id is empty")
		}
		if got.header.Get("Authorization") != "Bearer "+egressToken {
			t.Errorf("Authorization = %q, want the egress token", got.header.Get("Authorization"))
		}
		ms, err := strconv.Atoi(got.header.Get("Connect-Timeout-Ms"))
		if err != nil || ms <= 0 || ms > 20_000 {
			t.Errorf("Connect-Timeout-Ms = %q: the invocation's remaining budget must be the network call's", got.header.Get("Connect-Timeout-Ms"))
		}
		if m := got.req.GetMaxResponseBytes(); m == 0 || m > 4<<20 {
			t.Errorf("max_response_bytes = %d, want a bound of at most 4 MiB", m)
		}
		_ = out
	})

	t.Run("errors the guest sees", func(t *testing.T) {
		for name, c := range map[string]struct {
			script func(context.Context, *egressv1.FetchRequest, *connect.ServerStream[egressv1.FetchResponse]) error
			want   int
		}{
			"denied":                {refuse(commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED, connect.CodePermissionDenied), -1},
			"failed":                {refuse(commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED, connect.CodeUnavailable), -2},
			"response too large":    {refuse(commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE, connect.CodeResourceExhausted), -3},
			"request too large":     {refuse(commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE, connect.CodeInvalidArgument), -4},
			"malformed (no reason)": {refuse(commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED, connect.CodeInvalidArgument), -4},
			"service down":          {refuse(commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED, connect.CodeUnavailable), -2},
			"credentials refused":   {refuse(commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED, connect.CodeUnauthenticated), -2},
			"internal":              {refuse(commonv1.ErrorReason_ERROR_REASON_INTERNAL, connect.CodeInternal), -2},
		} {
			h.egress.script(c.script)
			if code, _, _ := h.netCall(t, "/fetch", "GET http://api.test/\n\n", []string{"api.test"}, nil); code != c.want {
				t.Errorf("%s: the guest got %d, want %d", name, code, c.want)
			}
		}
	})

	t.Run("requests the worker refuses itself", func(t *testing.T) {
		h.egress.script(ok)
		for name, req := range map[string]string{
			"no URL":                 "GET\n\n",
			"no method":              " http://api.test/\n\n",
			"a header with no colon": "GET http://api.test/\nnot a header\n\n",
			"empty":                  "",
			"too large":              "POST http://api.test/\n\n" + strings.Repeat("x", 1<<20),
		} {
			if code, _, _ := h.netCall(t, "/fetch", req, []string{"api.test"}, nil); code != -4 {
				t.Errorf("%s: code = %d, want -4 (invalid)", name, code)
			}
		}
		if code := h.rawCode(t, "/bad-pointer"); code != -4 {
			t.Errorf("a pointer outside memory: code = %d, want -4", code)
		}
		if n := len(h.egress.received()); n != 0 {
			t.Errorf("%d malformed requests reached the network service", n)
		}
	})

	t.Run("at most 16 requests an invocation", func(t *testing.T) {
		h.egress.script(ok)
		_, _, out := h.netCall(t, "/fetch-many", "GET http://api.test/\n\n", []string{"api.test"}, func(inv *invocation) { inv.query = "n=18" })
		fields := strings.Fields(strings.TrimPrefix(strings.TrimSpace(string(out.body)), ""))
		if len(fields) != 18 {
			t.Fatalf("answer %q", out.body)
		}
		for i, f := range fields {
			n, _ := strconv.Atoi(f)
			if i < 16 && n < 0 || i >= 16 && n != -5 {
				t.Errorf("request %d: result %d (all of the first 16 succeed, the rest are -5)", i+1, n)
			}
		}
		if got := len(h.egress.received()); got != 16 {
			t.Errorf("%d calls to the network service, want 16", got)
		}
	})

	t.Run("reading", func(t *testing.T) {
		h.egress.script(reply(200, nil, bytes.Repeat([]byte("r"), 500)))
		if got := h.rawText(t, "/read-without-fetch", ""); strings.TrimSpace(got) != "read=0" {
			t.Errorf("fetch_read with nothing pending: %q, want read=0", got)
		}
		got := h.rawText(t, "/fetch-discards", "GET http://api.test/\n\n")
		// 500 bytes of body plus the status line and the blank line.
		want := "first=" + strconv.Itoa(len("Status: 200\n\n")+500) + " second=" + strconv.Itoa(len("Status: 200\n\n")+500) + " read=" + strconv.Itoa(len("Status: 200\n\n")+500)
		if strings.TrimSpace(got) != want {
			t.Errorf("a new fetch must discard what the last left unread: %q, want %q", strings.TrimSpace(got), want)
		}
	})

	t.Run("a failed fetch leaves nothing to read", func(t *testing.T) {
		h.egress.script(func(ctx context.Context, req *egressv1.FetchRequest, st *connect.ServerStream[egressv1.FetchResponse]) error {
			if strings.Contains(req.GetUrl(), "deny.test") {
				return refuse(commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED, connect.CodePermissionDenied)(ctx, req, st)
			}
			return reply(200, nil, bytes.Repeat([]byte("r"), 300))(ctx, req, st)
		})
		got := strings.TrimSpace(h.rawText(t, "/fetch-fail-discards", "GET http://api.test/\n\n"))
		first := len("Status: 200\n\n") + 300
		if want := "first=" + strconv.Itoa(first) + " second=-1 read=0"; got != want {
			t.Errorf("%q, want %q: after a failed fetch there is no response to read", got, want)
		}
	})

	t.Run("response limits", func(t *testing.T) {
		h.egress.script(reply(200, nil, bytes.Repeat([]byte("x"), 2000)))
		code, _, _ := h.netCall(t, "/fetch", "GET http://api.test/\n\n", []string{"api.test"}, func(inv *invocation) {
			inv.limits = &commonv1.Limits{MaxResponseBytes: 1000}
		})
		if code != -3 {
			t.Errorf("a response larger than the function's response limit: code = %d, want -3", code)
		}
		if calls := h.egress.received(); len(calls) != 1 || calls[0].req.GetMaxResponseBytes() == 0 || calls[0].req.GetMaxResponseBytes() > 1000 {
			t.Errorf("the function's limit must be passed on as max_response_bytes: %v", calls)
		}

		// A service that exceeds the bound it was given is not believed.
		h.egress.script(reply(200, nil, bytes.Repeat([]byte("x"), 1<<20), bytes.Repeat([]byte("x"), 1<<20), bytes.Repeat([]byte("x"), 1<<20), bytes.Repeat([]byte("x"), 1<<20), []byte("x")))
		if code, _, _ := h.netCall(t, "/fetch", "GET http://api.test/\n\n", []string{"api.test"}, func(inv *invocation) {
			inv.limits = &commonv1.Limits{MaxResponseBytes: 32 << 20}
		}); code != -3 {
			t.Errorf("a response over 4 MiB: code = %d, want -3", code)
		}
	})

	t.Run("the deadline ends a request in flight", func(t *testing.T) {
		for len(h.egress.gone) > 0 {
			<-h.egress.gone
		}
		h.egress.script(func(ctx context.Context, _ *egressv1.FetchRequest, _ *connect.ServerStream[egressv1.FetchResponse]) error {
			select {
			case <-ctx.Done():
				h.egress.gone <- struct{}{}
			case <-time.After(30 * time.Second):
			}
			return ctx.Err()
		})
		start := time.Now()
		out := h.call(t, invocation{path: "/fetch", method: "POST", body: "GET http://slow.test/\n\n", digest: h.netGood, abi: "wagi-2",
			egress: []string{"slow.test"}, timeout: 2 * time.Second})
		wantReason(t, out.err, commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED)
		if took := time.Since(start); took > 15*time.Second {
			t.Errorf("answered after %v with a budget of 2s", took)
		}
		select {
		case <-h.egress.gone:
		case <-time.After(10 * time.Second):
			t.Error("the network call was still going 10s after the invocation ended: it must be abandoned")
		}
	})
}

// rawText runs a net guest path and returns what it printed after the headers.
func (h *harness) rawText(t *testing.T, path, body string) string {
	t.Helper()
	out := h.ok(t, invocation{path: path, method: "POST", body: body, digest: h.netGood, abi: "wagi-2", egress: []string{"api.test"}})
	return string(out.body)
}

// rawCode runs a net guest path that prints "code=N".
func (h *harness) rawCode(t *testing.T, path string) int {
	t.Helper()
	first, _, _ := strings.Cut(h.rawText(t, path, ""), "\n")
	n, _ := strconv.Atoi(strings.TrimPrefix(first, "code="))
	return n
}
