package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

// connCount counts the TCP connections a server saw.
type connCount struct {
	opened, closed atomic.Int64
}

func (c *connCount) hook(_ net.Conn, st http.ConnState) {
	switch st {
	case http.StateNew:
		c.opened.Add(1)
	case http.StateClosed, http.StateHijacked:
		c.closed.Add(1)
	}
}

// startFake serves a FakeIngress in this process. The library was easy to
// stand a pretend orchestrator up against: the generated handler is the only
// thing needed.
func startFake(t testing.TB, f *FakeIngress) (string, *connCount) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cc := &connCount{}
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: f.Handler(ingressToken), Protocols: &p, ConnState: cc.hook}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return "http://" + ln.Addr().String(), cc
}

func newFake() *FakeIngress { return &FakeIngress{MaxBody: 6 << 20} }

func TestMidBodyFailureIsCountedSeparately(t *testing.T) {
	url, _ := startFake(t, newFake())
	code, rep, out := tm(t, "-scenario", writeFile(t, "s.yaml", `
defaults: {host: mock.test, budget: 3s}
requests:
  - {name: good, weight: 1, path: /bytes/100000}
  - {name: torn, weight: 1, path: /midfail/100000}
load: {model: constant, rate: 20, duration: 1s}
`), "-url", url, "-token", ingressToken, "-expect", "stream_failures>0", "-expect", "errors>0%")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	tt := rep.Totals
	torn := rep.Templates["torn"]
	if tt.StreamFailures != torn.Sent || torn.Sent == 0 || tt.OK != rep.Templates["good"].Sent {
		t.Errorf("stream failures %d, torn sent %d, ok %d: %+v", tt.StreamFailures, torn.Sent, tt.OK, rep.Templates)
	}
	if tt.PlatformErrors != 0 {
		t.Errorf("a failure after the head is not a platform error before it: %+v", tt)
	}
	if rep.Status["aborted"] != torn.Sent {
		t.Errorf("a frontend aborts the client connection: status %v", rep.Status)
	}
	if rs := rep.Reasons["STREAM:INTERNAL"]; rs.Count != torn.Sent {
		t.Errorf("reasons %+v", rep.Reasons)
	}
	// The bytes before the failure did arrive.
	if want := (tt.OK + tt.StreamFailures) * 100000; tt.BytesReceived != want {
		t.Errorf("bytes %d, want %d", tt.BytesReceived, want)
	}
}

func TestMappingTable(t *testing.T) {
	// docs/protocol.md, "Frontend mapping of platform errors"
	cases := []struct {
		r      commonv1.ErrorReason
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
		{commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED, 500, false},
		{commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED, 500, false},
		{commonv1.ErrorReason(99), 500, false}, // a reason from the future
	}
	for _, c := range cases {
		m := mapError(edgerun.NewError(c.r, "x"), nil)
		if m.Status != c.status || m.RetryAfter != c.retry || !m.Typed {
			t.Errorf("%v: %+v, want %d retry=%v", c.r, m, c.status, c.retry)
		}
	}
	untyped := []struct {
		err    error
		ctxErr error
		status int
	}{
		{connect.NewError(connect.CodeResourceExhausted, errors.New("big")), nil, 413},
		{connect.NewError(connect.CodeUnavailable, errors.New("down")), nil, 503},
		{connect.NewError(connect.CodeUnauthenticated, errors.New("tok")), nil, 502},
		{connect.NewError(connect.CodePermissionDenied, errors.New("tok")), nil, 502},
		{connect.NewError(connect.CodeInternal, errors.New("?")), nil, 500},
		{connect.NewError(connect.CodeDeadlineExceeded, errors.New("late")), nil, 504},
		{connect.NewError(connect.CodeCanceled, errors.New("gone")), context.Canceled, 0},
		{edgerun.ErrProtocol, nil, 502},
		{errors.New("plain"), context.DeadlineExceeded, 504},
	}
	for _, c := range untyped {
		if m := mapError(c.err, c.ctxErr); m.Status != c.status || m.Typed {
			t.Errorf("%v: %+v, want %d", c.err, m, c.status)
		}
	}
}

// TestInvokeErrorsAgainstTheFake looks at what errors the library really
// returns for the cases a frontend has to classify.
func TestInvokeErrorsAgainstTheFake(t *testing.T) {
	url, _ := startFake(t, newFake())
	ing := edgerun.NewIngress(url, ingressToken)
	req := func(path string) *ingressv1.InvokeRequest {
		return &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: mockHost, Path: path}}
	}
	// A deadline that expires before the head: what does the caller see?
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, err := ing.Invoke(ctx, req("/spin"))
	cancel()
	t.Logf("deadline before the head: code=%v reason=%v ctx=%v err=%v", connect.CodeOf(err), edgerun.Reason(err), ctx.Err(), err)
	if m := mapError(err, ctx.Err()); m.Status != 504 {
		t.Errorf("a spent budget must read as 504, got %+v", m)
	}
	// A deadline that expires in the body.
	ctx, cancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
	res, err := ing.Invoke(ctx, req("/drip/10"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(io.Discard, res.Body)
	cancel()
	t.Logf("deadline in the body: code=%v reason=%v err=%v", connect.CodeOf(err), edgerun.Reason(err), err)
	if err == nil || errors.Is(err, io.EOF) {
		t.Errorf("a body cut short must not end as EOF: %v", err)
	}
	// Cancellation in the body.
	ctx, cancel = context.WithCancel(context.Background())
	res, err = ing.Invoke(ctx, req("/drip/10"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	_, err = io.Copy(io.Discard, res.Body)
	t.Logf("cancel in the body: code=%v err=%v ctx=%v", connect.CodeOf(err), err, ctx.Err())
	if err == nil || connect.CodeOf(err) != connect.CodeCanceled {
		t.Errorf("want a canceled error, got %v", err)
	}
	// A server that is not there.
	dead, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := dead.Addr().String()
	dead.Close()
	_, err = edgerun.NewIngress("http://"+addr, ingressToken).Invoke(context.Background(), req("/"))
	t.Logf("refused connection: code=%v reason=%v err=%v", connect.CodeOf(err), edgerun.Reason(err), err)
	if m := mapError(err, nil); m.Status != 503 || !m.RetryAfter {
		t.Errorf("%+v", m)
	}
	// Wrong token.
	_, err = edgerun.NewIngress(url, "wrong").Invoke(context.Background(), req("/"))
	t.Logf("wrong token: code=%v reason=%v", connect.CodeOf(err), edgerun.Reason(err))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("%v", err)
	}
	// Far over the body limit: the transport refuses before ErrorInfo exists.
	f := newFake()
	f.MaxBody = 1 << 10
	url2, _ := startFake(t, f)
	ing2 := edgerun.NewIngress(url2, ingressToken)
	r := req("/status/200")
	r.Request.Body = make([]byte, 2<<20)
	_, err = ing2.Invoke(context.Background(), r)
	t.Logf("far over the limit: code=%v reason=%v", connect.CodeOf(err), edgerun.Reason(err))
	if m := mapError(err, nil); m.Status != 413 {
		t.Errorf("%+v / %v", m, err)
	}
	r.Request.Body = make([]byte, 2<<10)
	_, err = ing2.Invoke(context.Background(), r)
	t.Logf("just over the limit: code=%v reason=%v", connect.CodeOf(err), edgerun.Reason(err))
	if edgerun.Reason(err) != commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE {
		t.Errorf("%v", err)
	}
}

// TestCancellationLeavesNothingBehind: abandoning bodies, by closing early and
// by cancelling, must free the connection and the goroutines.
func TestCancellationLeavesNothingBehind(t *testing.T) {
	url, cc := startFake(t, newFake())
	ing := edgerun.NewIngress(url, ingressToken, edgerun.WithHTTPClient(newHTTPClient("1", 0)))
	settle := func() int {
		var n int
		for i := 0; i < 50; i++ {
			runtime.GC()
			time.Sleep(50 * time.Millisecond)
			n = runtime.NumGoroutine()
			if n <= before+2 {
				break
			}
		}
		return n
	}
	// warm up the runtime's own goroutines
	res, _ := ing.Invoke(context.Background(), &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: mockHost, Path: "/status/200"}})
	res.ReadAll()
	time.Sleep(100 * time.Millisecond)
	before = runtime.NumGoroutine()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: mockHost, Path: "/drip/5"}})
			if err != nil {
				return
			}
			buf := make([]byte, 512)
			r.Body.Read(buf)
			if i%2 == 0 {
				r.Body.Close() // early close, context still alive
			} else {
				cancel() // cancel, and never call Close
			}
		}()
	}
	wg.Wait()
	after := settle()
	t.Logf("goroutines before %d, after 100 abandoned streams %d; connections opened %d closed %d", before, after, cc.opened.Load(), cc.closed.Load())
	if after > before+2 {
		t.Errorf("goroutine leak: %d -> %d", before, after)
	}
	// A connection that carried an abandoned stream is closed, not parked.
	for i := 0; i < 50 && cc.opened.Load()-cc.closed.Load() > 2; i++ {
		time.Sleep(100 * time.Millisecond)
	}
	if open := cc.opened.Load() - cc.closed.Load(); open > 2 {
		t.Errorf("%d connections still open on the server", open)
	}
}

var before int

func TestConnectionReuse(t *testing.T) {
	const total, conc = 3000, 64
	for _, c := range []struct {
		name string
		opts []edgerun.Option
		max  int64
	}{
		{"library default client", nil, -1},
		{"tuned http/1.1", []edgerun.Option{edgerun.WithHTTPClient(newHTTPClient("1", 0))}, 2 * conc},
		{"h2c", []edgerun.Option{edgerun.WithHTTPClient(newHTTPClient("2", 0))}, 8},
	} {
		t.Run(c.name, func(t *testing.T) {
			url, cc := startFake(t, newFake())
			ing := edgerun.NewIngress(url, ingressToken, c.opts...)
			// one request first, so that h2 has a connection to share
			r0, _ := ing.Invoke(context.Background(), &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: mockHost, Path: "/status/200"}})
			r0.ReadAll()
			var wg sync.WaitGroup
			var next atomic.Int64
			t0 := time.Now()
			for range conc {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for next.Add(1) <= total {
						r, err := ing.Invoke(context.Background(), &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: mockHost, Path: "/bytes/1000"}})
						if err != nil {
							t.Error(err)
							return
						}
						io.Copy(io.Discard, r.Body)
						r.Body.Close()
					}
				}()
			}
			wg.Wait()
			t.Logf("%d requests at concurrency %d: %d connections opened, %.0f req/s", total, conc, cc.opened.Load(), total/time.Since(t0).Seconds())
			if c.max >= 0 && cc.opened.Load() > c.max {
				t.Errorf("%d connections for %d requests", cc.opened.Load(), total)
			}
		})
	}
}

func TestStreamWithoutAHeadIsAProtocolError(t *testing.T) {
	url, _ := startFake(t, newFake())
	code, rep, out := tm(t, "-scenario", writeFile(t, "s.yaml", "defaults: {host: mock.test}\nrequests: [{path: /nohead}]\nload: {model: constant, rate: 10, duration: 1s}\n"),
		"-url", url, "-token", ingressToken)
	if code != 0 || rep.Reasons["PROTOCOL"].Count != 10 || rep.Reasons["PROTOCOL"].HTTPStatus != 502 || rep.Status["502"] != 10 {
		t.Errorf("%d %+v\n%s", code, rep.Reasons, out)
	}
}

// A consumer that reads slowly must slow the producer down, not make the
// client buffer the response: bounded memory, whichever HTTP version.
func TestSlowReaderBackpressure(t *testing.T) {
	for _, mode := range []string{"1", "2"} {
		t.Run("http"+mode, func(t *testing.T) {
			f := newFake()
			url, _ := startFake(t, f)
			ing := edgerun.NewIngress(url, ingressToken, edgerun.WithHTTPClient(newHTTPClient(mode, 0)))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			res, err := ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: mockHost, Path: "/bytes/500000000"}})
			if err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 64<<10)
			var read int64
			var ms runtime.MemStats
			stop := time.Now().Add(1500 * time.Millisecond)
			for time.Now().Before(stop) {
				n, err := res.Body.Read(buf)
				read += int64(n)
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(20 * time.Millisecond)
			}
			runtime.ReadMemStats(&ms)
			sent := f.Sent.Load()
			res.Body.Close()
			t.Logf("http/%s: the consumer read %.1f MiB, the producer had sent %.1f MiB (of 477), client heap %.1f MiB", mode, float64(read)/(1<<20), float64(sent)/(1<<20), float64(ms.HeapAlloc)/(1<<20))
			if sent-read > 64<<20 {
				t.Errorf("%d MiB were in flight between a slow reader and the server", (sent-read)>>20)
			}
			if ms.HeapAlloc > 128<<20 {
				t.Errorf("client heap %d MiB", ms.HeapAlloc>>20)
			}
		})
	}
}
