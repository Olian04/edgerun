package e2e

// Part 2: components behind proxies. Streaming responses must arrive as they are
// produced, a 6 MiB request must cross, a proxy's idle/header timeout must be
// understood, and a client that goes away must free the worker, behind
//
//   - a Go reverse proxy (httputil.ReverseProxy), with FlushInterval -1 and with
//     the default, and
//   - Caddy (reverse_proxy, with flush_interval -1 and with the default), HTTP/1.1
//     and h2c upstreams. Caddy is the only real proxy that could be obtained here
//     (see the report): it is found as $EDGERUN_E2E_CADDY or `caddy` on the PATH,
//     and its tests are skipped when there is none. nginx, envoy, haproxy and
//     traefik are not installed and are not tested.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	orchapp "github.com/Olian04/edgerun/internal/orchestrator/app"
)

type proxyCfg struct {
	kind              string // "go" or "caddy"
	flush             bool   // FlushInterval -1 / flush_interval -1
	upH2C             bool   // the upstream is spoken to in h2c
	respHeaderTimeout time.Duration
	// buffer makes a Go proxy read the whole upstream response before it sends any
	// of it (what nginx does with proxy_buffering on and an unbounded buffer): the
	// control that shows the streaming test can see buffering.
	buffer bool
}

func (c proxyCfg) String() string {
	s := c.kind
	if c.flush {
		s += "/flush=-1"
	} else {
		s += "/flush=default"
	}
	if c.upH2C {
		s += "/upstream=h2c"
	} else {
		s += "/upstream=h1"
	}
	if c.respHeaderTimeout > 0 {
		s += "/header-timeout=" + c.respHeaderTimeout.String()
	}
	if c.buffer {
		s += "/buffering-control"
	}
	return s
}

func caddyPath(t *testing.T) string {
	if p := os.Getenv("EDGERUN_E2E_CADDY"); p != "" {
		return p
	}
	if p, err := exec.LookPath("caddy"); err == nil {
		return p
	}
	return ""
}

// startProxy starts a proxy in front of target (a base URL) and returns its
// base URL.
func startProxy(t *testing.T, target string, c proxyCfg) string {
	t.Helper()
	u, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	switch c.kind {
	case "go":
		var up http.Protocols
		if c.upH2C {
			up.SetUnencryptedHTTP2(true)
		} else {
			up.SetHTTP1(true)
		}
		var tr http.RoundTripper = &http.Transport{Protocols: &up, DisableCompression: true, MaxIdleConnsPerHost: 100,
			ResponseHeaderTimeout: c.respHeaderTimeout}
		if c.buffer {
			tr = bufferingTransport{tr}
		}
		rp := &httputil.ReverseProxy{
			Rewrite:   func(pr *httputil.ProxyRequest) { pr.SetURL(u); pr.Out.Host = pr.In.Host },
			Transport: tr,
		}
		if c.flush {
			rp.FlushInterval = -1
		}
		var down http.Protocols
		down.SetHTTP1(true)
		down.SetUnencryptedHTTP2(true)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := &http.Server{Handler: rp, Protocols: &down}
		go srv.Serve(ln)
		t.Cleanup(func() { srv.Close() })
		return "http://" + ln.Addr().String()
	case "caddy":
		bin := caddyPath(t)
		if bin == "" {
			t.Skip("caddy is not installed (set EDGERUN_E2E_CADDY or put it on the PATH)")
		}
		dir := t.TempDir()
		listen := freePort(t)
		_, port, _ := net.SplitHostPort(listen)
		var tr strings.Builder
		if c.upH2C {
			tr.WriteString("\t\t\tversions h2c\n")
		}
		if c.respHeaderTimeout > 0 {
			fmt.Fprintf(&tr, "\t\t\tresponse_header_timeout %dms\n", c.respHeaderTimeout.Milliseconds())
		}
		flush := ""
		if c.flush {
			flush = "\t\tflush_interval -1\n"
		}
		cfg := fmt.Sprintf(`{
	admin off
	auto_https off
	servers {
		protocols h1 h2c
	}
}
http://:%s {
	bind 127.0.0.1
	reverse_proxy http://%s {
%s		transport http {
%s		}
	}
}
`, port, u.Host, flush, tr.String())
		file := filepath.Join(dir, "Caddyfile")
		if err := os.WriteFile(file, []byte(cfg), 0o644); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(bin, "run", "--config", file, "--adapter", "caddyfile")
		cmd.Env = append(os.Environ(), "XDG_DATA_HOME="+dir, "XDG_CONFIG_HOME="+dir, "HOME="+dir)
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
		eventually(t, "caddy to listen", 30*time.Second, func() bool {
			c, err := net.DialTimeout("tcp", listen, time.Second)
			if err != nil {
				return false
			}
			c.Close()
			return true
		})
		return "http://" + listen
	}
	t.Fatalf("unknown proxy %q", c.kind)
	return ""
}

type bufferingTransport struct{ http.RoundTripper }

func (b bufferingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	res, err := b.RoundTripper.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(res.Body)
	res.Body.Close()
	res.Body = io.NopCloser(bytes.NewReader(data))
	res.ContentLength = int64(len(data))
	res.Header.Set("Content-Length", fmt.Sprint(len(data)))
	res.Trailer = nil
	return res, err
}

type proxyCase struct {
	name       string
	cfg        proxyCfg
	mustStream bool // a configuration an operator is told to use: it must not buffer
}

func proxyCases() []proxyCase {
	return []proxyCase{
		{"go, FlushInterval -1", proxyCfg{kind: "go", flush: true}, true},
		{"go, default FlushInterval", proxyCfg{kind: "go"}, false},
		{"caddy, flush_interval -1", proxyCfg{kind: "caddy", flush: true}, true},
		{"caddy, default flush_interval", proxyCfg{kind: "caddy"}, false},
	}
}

// clients that reach the proxy in front of the orchestrator
type frontClient struct {
	name string
	h2   bool
	opts []connect.ClientOption
}

func frontClients() []frontClient {
	return []frontClient{
		{"connect/h1", false, nil},
		{"connect/h2c", true, nil},
		{"grpc/h2c", true, []connect.ClientOption{connect.WithGRPC()}},
		{"grpc-web/h1", false, []connect.ClientOption{connect.WithGRPCWeb()}},
	}
}

func (f frontClient) ingress(base string) *edgerun.Ingress {
	hc := http1Client()
	if f.h2 {
		hc = h2cClient()
	}
	return edgerun.NewIngress(base, ingressToken, edgerun.WithHTTPClient(hc), edgerun.WithClientOptions(f.opts...))
}

// proxyRig is one orchestrator with a real worker (a wasm echo with long sleeps,
// 6 MiB request bodies) and scripted workers that stream slowly and notice when
// they are cancelled.
type proxyRig struct {
	e *cenv

	mu        sync.Mutex
	started   map[string]chan struct{}
	cancelled map[string]time.Time
}

const streamGap = 800 * time.Millisecond

func (r *proxyRig) scripted(t *testing.T) *rawServer {
	srv := newRawServer(t)
	enc := func(m proto.Message) []byte { return mustMarshal(t, m) }
	srv.stream(workerv1connect.ExecutionServiceExecuteProcedure, func(ctx context.Context, _ http.Header, req []byte, send func([]byte) error) error {
		var er workerv1.ExecuteRequest
		if err := proto.Unmarshal(req, &er); err != nil {
			return err
		}
		path := er.GetRequest().GetPath()
		switch {
		case path == "/stream":
			send(enc(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Head{Head: &commonv1.HttpResponseHead{Status: 200}}}))
			send(enc(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Chunk{Chunk: []byte("one")}}))
			select {
			case <-time.After(streamGap):
			case <-ctx.Done():
				return ctx.Err()
			}
			send(enc(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Chunk{Chunk: []byte("two")}}))
			return send(enc(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Summary{Summary: &workerv1.ExecutionSummary{DurationMicros: 1}}}))
		case strings.HasPrefix(path, "/hang/"):
			id := strings.TrimPrefix(path, "/hang/")
			r.mu.Lock()
			ch := r.started[id]
			r.mu.Unlock()
			if ch != nil {
				close(ch)
			}
			<-ctx.Done()
			r.mu.Lock()
			r.cancelled[id] = time.Now()
			r.mu.Unlock()
			return ctx.Err()
		}
		return connect.NewError(connect.CodeNotFound, nil)
	})
	return srv
}

// scriptedFunction makes host served by a scripted worker whose address (the
// endpoint the orchestrator dials) is endpointOf(srv.URL()).
func (r *proxyRig) scriptedFunction(t *testing.T, name string, endpointOf func(string) string) string {
	srv := r.scripted(t)
	abi := "script-" + name
	startFakeWorkerAt(t, r.e.s, srv, endpointOf(srv.URL()), abi, 8)
	fid := r.e.s.createFunction("f-" + name)
	if _, err := r.e.s.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: []byte("not run " + name), Abi: abi, Activate: true}); err != nil {
		t.Fatal(err)
	}
	host := name + ".test"
	r.e.s.route(host, "/", fid)
	eventually(t, "the scripted worker to serve", 30*time.Second, func() bool {
		ing := edgerun.NewIngress(r.e.orch.base, ingressToken)
		res, err := ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: host, Path: "/stream"}})
		if err != nil {
			return false
		}
		_, err = res.ReadAll()
		return err == nil
	})
	return host
}

type chunkTimes struct {
	first, second time.Duration
	body          string
}

func readChunks(t *testing.T, ing *edgerun.Ingress, host string) (chunkTimes, error) {
	t.Helper()
	cctx, cancel := ctxWithTimeout(30 * time.Second)
	defer cancel()
	start := time.Now()
	res, err := ing.Invoke(cctx, &ingressv1.InvokeRequest{RequestId: "proxy", Request: &commonv1.HttpRequest{Method: "GET", Host: host, Path: "/stream"}})
	if err != nil {
		return chunkTimes{}, err
	}
	defer res.Body.Close()
	var ct chunkTimes
	buf := make([]byte, 100)
	for {
		n, err := res.Body.Read(buf)
		if n > 0 {
			ct.body += string(buf[:n])
			if ct.first == 0 && strings.Contains(ct.body, "one") {
				ct.first = time.Since(start)
			}
			if ct.second == 0 && strings.Contains(ct.body, "two") {
				ct.second = time.Since(start)
			}
		}
		if err != nil {
			if err.Error() == "EOF" {
				err = nil
			}
			return ct, err
		}
	}
}

func TestWireProxies(t *testing.T) {
	e := newCEnvWith(t, func(c *orchapp.Config) { c.MaxRequestBodyBytes = 6 << 20 })
	rig := &proxyRig{e: e, started: map[string]chan struct{}{}, cancelled: map[string]time.Time{}}
	direct := rig.scriptedFunction(t, "direct", func(u string) string { return u })
	// the control: without a proxy the second chunk comes streamGap after the first
	if ct, err := readChunks(t, edgerun.NewIngress(e.orch.base, ingressToken), direct); err != nil || ct.second-ct.first < streamGap-150*time.Millisecond || ct.body != "onetwo" {
		t.Fatalf("without a proxy: %+v %v", ct, err)
	} else {
		t.Logf("no proxy: first chunk after %v, second after %v", ct.first.Round(time.Millisecond), ct.second.Round(time.Millisecond))
	}

	// The control: a proxy that reads a response whole before it forwards it (what
	// nginx does with proxy_buffering on) makes both chunks arrive together, so the
	// measurement below can tell buffering from streaming.
	t.Run("a buffering proxy is seen to buffer", func(t *testing.T) {
		front := startProxy(t, e.orch.base, proxyCfg{kind: "go", buffer: true})
		ct, err := readChunks(t, edgerun.NewIngress(front, ingressToken), direct)
		if err != nil || ct.body != "onetwo" {
			t.Fatalf("%+v %v", ct, err)
		}
		t.Logf("buffering proxy: first chunk after %v, second after %v", ct.first.Round(time.Millisecond), ct.second.Round(time.Millisecond))
		if ct.first < streamGap-200*time.Millisecond {
			t.Errorf("the control proxy buffers, yet the first chunk came after %v: the streaming test cannot see buffering", ct.first)
		}
	})

	for _, pc := range proxyCases() {
		for _, up := range []bool{false, true} {
			cfg := pc.cfg
			cfg.upH2C = up
			name := cfg.String()
			t.Run("streaming/"+name, func(t *testing.T) {
				if cfg.kind == "caddy" && caddyPath(t) == "" {
					t.Skip("caddy is not installed")
				}
				// 1. a proxy between the client (every protocol) and the orchestrator, and
				// 2. one between the orchestrator and the worker; all at once, they do not
				// depend on each other
				front := startProxy(t, e.orch.base, cfg)
				backHost := rig.scriptedFunction(t, fmt.Sprintf("back%x", time.Now().UnixNano()%1e9), func(u string) string { return startProxy(t, u, cfg) })
				type result struct {
					what string
					ct   chunkTimes
					err  error
				}
				var wg sync.WaitGroup
				var results []result
				var rmu sync.Mutex
				run := func(what string, ing *edgerun.Ingress, host string) {
					wg.Add(1)
					go func() {
						defer wg.Done()
						ct, err := readChunks(t, ing, host)
						rmu.Lock()
						results = append(results, result{what, ct, err})
						rmu.Unlock()
					}()
				}
				for _, fc := range frontClients() {
					run("client "+fc.name+" -> proxy -> orchestrator", fc.ingress(front), direct)
				}
				run("orchestrator -> proxy -> worker", edgerun.NewIngress(e.orch.base, ingressToken), backHost)
				wg.Wait()
				sort.Slice(results, func(i, j int) bool { return results[i].what < results[j].what })
				for _, r := range results {
					if r.err != nil {
						t.Errorf("%s: %v", r.what, r.err)
						continue
					}
					gap := r.ct.second - r.ct.first
					t.Logf("%-42s first chunk after %v, second after %v", r.what, r.ct.first.Round(time.Millisecond), r.ct.second.Round(time.Millisecond))
					if r.ct.body != "onetwo" {
						t.Errorf("%s: body %q", r.what, r.ct.body)
					}
					buffered := r.ct.first > 500*time.Millisecond || gap < streamGap-200*time.Millisecond
					if pc.mustStream && buffered {
						t.Errorf("%s: the proxy buffered the stream: first chunk after %v, second %v later (want the first at once and the second %v later)", r.what, r.ct.first, gap, streamGap)
					}
					if !pc.mustStream && buffered {
						t.Logf("OBSERVATION: %s with %s buffers the response", r.what, name)
					}
				}
			})
		}
	}

	for _, pc := range proxyCases() {
		t.Run("large bodies/"+pc.cfg.String(), func(t *testing.T) {
			if pc.cfg.kind == "caddy" && caddyPath(t) == "" {
				t.Skip("caddy is not installed")
			}
			front := startProxy(t, e.orch.base, pc.cfg)
			viaProxy := edgerun.NewIngress(front, ingressToken)
			via := func(ing *edgerun.Ingress, n int) string {
				body := bytes.Repeat([]byte("0123456789abcdef"), n/16)
				body = append(body, make([]byte, n-len(body))...)
				cctx, cancel := ctxWithTimeout(60 * time.Second)
				defer cancel()
				res, err := ing.Invoke(cctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "POST", Host: "compat.test", Path: "/echo", Body: body}})
				if err != nil {
					return fmt.Sprintf("%s/%s", connect.CodeOf(err), edgerun.Reason(err))
				}
				got, err := res.ReadAll()
				if err != nil {
					return "body error: " + err.Error()
				}
				if !bytes.Equal(got, body) {
					return "the echo differs from the body"
				}
				return "echoed"
			}
			for _, tc := range []struct {
				name string
				n    int
				want string
			}{
				{"exactly 6 MiB", 6 << 20, "echoed"},
				{"6 MiB + 1 byte", 6<<20 + 1, "invalid_argument/ERROR_REASON_REQUEST_TOO_LARGE"},
				{"6 MiB + 300 KiB, past the transport's cap", 6<<20 + 300<<10, "resource_exhausted/ERROR_REASON_UNSPECIFIED"},
			} {
				direct := via(edgerun.NewIngress(e.orch.base, ingressToken), tc.n)
				got := via(viaProxy, tc.n)
				if direct != tc.want || got != direct {
					t.Errorf("%s: directly %q, through the proxy %q, want %q", tc.name, direct, got, tc.want)
				}
			}
			// the whole path an HTTP client uses: proxy -> frontend -> orchestrator -> worker
			feBase, _ := startFrontendAgainst(t, e.orch.base, 60*time.Second)
			cfg := pc.cfg
			cfg.upH2C = false // a frontend serves HTTP/1.1
			fp := startProxy(t, feBase, cfg)
			body := strings.Repeat("0123456789abcdef", (6<<20)/16)
			resp, got, err := httpGet(t, fp, "compat.test", "/echo", body)
			if err != nil || resp.StatusCode != 200 || string(got) != body {
				t.Errorf("a 6 MiB POST through the proxy and the frontend: %v %v (%d bytes back)", resp, err, len(got))
			}
			resp, _, err = httpGet(t, fp, "compat.test", "/echo", body+"x")
			if err != nil || resp.StatusCode != 413 {
				t.Errorf("a body one byte over the limit through the proxy and the frontend: %v %v, want 413", resp, err)
			}
		})
	}

	// A header (or idle) timeout shorter than an invocation. The worker buffers the
	// response, so nothing at all crosses the proxy while the function runs.
	for _, pc := range proxyCases() {
		if !pc.mustStream {
			continue
		}
		cfg := pc.cfg
		cfg.respHeaderTimeout = 700 * time.Millisecond
		t.Run("idle timeout/"+cfg.String(), func(t *testing.T) {
			if cfg.kind == "caddy" && caddyPath(t) == "" {
				t.Skip("caddy is not installed")
			}
			front := startProxy(t, e.orch.base, cfg)
			ing := edgerun.NewIngress(front, ingressToken)
			sleep := func(ms int) (time.Duration, error) {
				cctx, cancel := ctxWithTimeout(30 * time.Second)
				defer cancel()
				start := time.Now()
				res, err := ing.Invoke(cctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: "compat.test", Path: "/sleep", RawQuery: fmt.Sprintf("ms=%d", ms)}})
				if err == nil {
					_, err = res.ReadAll()
				}
				return time.Since(start), err
			}
			if d, err := sleep(100); err != nil {
				t.Fatalf("a short invocation behind the same proxy: %v after %v", err, d)
			}
			d, err := sleep(30000)
			if err == nil {
				t.Fatalf("an invocation of 30 s succeeded through a proxy that gives up after %v without a byte", cfg.respHeaderTimeout)
			}
			t.Logf("behind a %v header timeout, an invocation of 30 s fails after %v with %v (%v)", cfg.respHeaderTimeout, d.Round(time.Millisecond), connect.CodeOf(err), edgerun.Reason(err))
			if d > 5*time.Second {
				t.Errorf("the failure took %v", d)
			}
			// the proxy hung up, so the orchestrator must have stopped the guest
			eventually(t, "the guest to be stopped and its slot freed", 20*time.Second, func() bool {
				ws, err := e.s.ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
				if err != nil {
					return false
				}
				for _, w := range ws.Workers {
					if w.Info.GetImplementation().GetName() == "edgerun-worker-wazero" {
						return w.InFlight == 0
					}
				}
				return false
			})
		})
	}

	// A client that goes away: the cancellation must cross the proxy, and the
	// orchestrator, to the worker.
	for _, pc := range proxyCases() {
		t.Run("cancellation/"+pc.cfg.String(), func(t *testing.T) {
			if pc.cfg.kind == "caddy" && caddyPath(t) == "" {
				t.Skip("caddy is not installed")
			}
			cancelAndWait := func(what string, ing *edgerun.Ingress, host string) {
				id := fmt.Sprintf("%s-%d", strings.NewReplacer(" ", "", "/", "").Replace(what), time.Now().UnixNano())
				started := make(chan struct{})
				rig.mu.Lock()
				rig.started[id] = started
				rig.mu.Unlock()
				cctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				errc := make(chan error, 1)
				go func() {
					_, err := ing.Invoke(cctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: host, Path: "/hang/" + id}})
					errc <- err
				}()
				select {
				case <-started:
				case <-time.After(20 * time.Second):
					t.Fatalf("%s: the worker never saw the request", what)
				}
				at := time.Now()
				cancel()
				select {
				case <-errc:
				case <-time.After(10 * time.Second):
					t.Errorf("%s: the client's call did not return after cancel", what)
				}
				eventually(t, what+": the worker to see the cancellation", 15*time.Second, func() bool {
					rig.mu.Lock()
					defer rig.mu.Unlock()
					_, ok := rig.cancelled[id]
					return ok
				})
				rig.mu.Lock()
				t.Logf("%s: the worker saw the cancellation %v after the client cancelled", what, rig.cancelled[id].Sub(at).Round(time.Millisecond))
				rig.mu.Unlock()
			}
			front := startProxy(t, e.orch.base, pc.cfg)
			for _, fc := range frontClients() {
				cancelAndWait("client "+fc.name+" -> proxy -> orchestrator", fc.ingress(front), direct)
			}
			host := rig.scriptedFunction(t, fmt.Sprintf("cb%x", time.Now().UnixNano()%1e9), func(u string) string { return startProxy(t, u, pc.cfg) })
			cancelAndWait("orchestrator -> proxy -> worker", edgerun.NewIngress(e.orch.base, ingressToken), host)
		})
	}
}
