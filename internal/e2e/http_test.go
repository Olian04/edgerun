package e2e

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	feapp "github.com/Olian04/edgerun/internal/frontend/app"
)

type web struct {
	s    *stack
	base string
	fe   *feapp.Server
}

// startFrontend puts a real frontend in front of the stack.
func (s *stack) startFrontend(requestTimeout time.Duration) *web {
	s.t.Helper()
	fe, err := feapp.Start(context.Background(), feapp.Config{
		Listen: "127.0.0.1:0", OrchestratorURL: "http://" + s.addr, IngressToken: ingressToken,
		RequestTimeout: requestTimeout, LimitRefresh: 100 * time.Millisecond, Log: quiet,
	})
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { fe.Stop(context.Background()) })
	return &web{s: s, base: "http://" + fe.Addr(), fe: fe}
}

type reply struct {
	status int
	header http.Header
	body   string
}

func (w *web) do(method, host, path, body string, hdr ...string) reply {
	w.s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	return w.doCtx(ctx, method, host, path, body, hdr...)
}

func (w *web) doCtx(ctx context.Context, method, host, path, body string, hdr ...string) reply {
	w.s.t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, w.base+path, strings.NewReader(body))
	if err != nil {
		w.s.t.Fatal(err)
	}
	req.Host = host
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Add(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return reply{status: -1, body: err.Error()}
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return reply{status: resp.StatusCode, header: resp.Header, body: string(b)}
}

func TestHTTPThroughTheWholeStack(t *testing.T) {
	s := newStack(t)
	wk := s.startWorker(1) // one slot: leaks and refusals are immediately visible
	fid := s.createFunction("web")
	s.uploadVersion(fid, s.wasm, nil, true)
	s.route("web.test", "/", fid) // deliberately no catch-all
	w := s.startFrontend(2 * time.Second)
	eventually(t, "the stack to serve HTTP", 120*time.Second, func() bool {
		return w.do("GET", "web.test", "/echo", "").status == 200
	})

	t.Run("a request and its response", func(t *testing.T) {
		r := w.do("POST", "web.test", "/echo", "hello over http", "Content-Type", "text/x-web")
		if r.status != 200 || r.body != "hello over http" || r.header.Get("Content-Type") != "text/x-web" {
			t.Errorf("%+v", r)
		}
		if r.header.Get("X-Request-Id") == "" || r.header.Get("X-Edgerun-Invocation-Id") == "" {
			t.Errorf("ids missing: %v", r.header)
		}
	})

	t.Run("a function's status code reaches the client", func(t *testing.T) {
		if r := w.do("GET", "web.test", "/status?code=418", ""); r.status != 418 {
			t.Errorf("status %d", r.status)
		}
	})

	t.Run("a large streamed response arrives intact", func(t *testing.T) {
		r := w.do("GET", "web.test", "/big?bytes=3000000", "")
		if r.status != 200 || len(r.body) < 3_000_000 {
			t.Errorf("status %d, %d bytes", r.status, len(r.body))
		}
	})

	t.Run("client-supplied forwarding headers are not believed", func(t *testing.T) {
		r := w.do("GET", "web.test", "/env", "", "X-Forwarded-For", "6.6.6.6")
		if !strings.Contains(r.body, "HTTP_X_FORWARDED_FOR=127.0.0.1\n") || strings.Contains(r.body, "6.6.6.6") {
			t.Errorf("the guest must see the real peer, not the spoofed one:\n%s", r.body)
		}
	})

	t.Run("platform failures become the right HTTP status without leaking detail", func(t *testing.T) {
		if r := w.do("GET", "other.test", "/", ""); r.status != 404 {
			t.Errorf("unrouted host: %d", r.status)
		}
		r := w.do("GET", "web.test", "/exit?code=3", "")
		if r.status != 502 || strings.Contains(r.body, "exiting on request") {
			t.Errorf("a crashing function is a 502 whose stderr stays in the logs: %d %q", r.status, r.body)
		}
		if r := w.do("POST", "web.test", "/echo", strings.Repeat("x", 5<<20)); r.status != 413 {
			t.Errorf("oversized body: %d", r.status)
		}
	})

	t.Run("a runaway function is a 504 and costs nothing afterwards", func(t *testing.T) {
		start := time.Now()
		if r := w.do("GET", "web.test", "/spin", ""); r.status != 504 {
			t.Errorf("status %d", r.status)
		}
		if d := time.Since(start); d > 15*time.Second {
			t.Errorf("a 2s budget took %v", d)
		}
		eventually(t, "the slot to be free again", 10*time.Second, func() bool {
			return w.do("GET", "web.test", "/echo", "").status == 200
		})
	})

	t.Run("a client hanging up frees the worker's slot", func(t *testing.T) {
		// Cancellation must travel client -> frontend -> orchestrator -> worker
		// -> guest. With one slot, a leak makes the next request a 503.
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan reply, 1)
		go func() { done <- w.doCtx(ctx, "GET", "web.test", "/sleep?ms=60000", "") }()
		time.Sleep(500 * time.Millisecond) // the guest is now sleeping in the only slot
		if r := w.do("GET", "web.test", "/echo", ""); r.status != 503 || r.header.Get("Retry-After") == "" {
			t.Fatalf("with the slot busy: %d, Retry-After %q", r.status, r.header.Get("Retry-After"))
		}
		cancel()
		<-done
		// The 2s request timeout would free the slot eventually, so the window
		// here must be well inside it: only real cancellation is this fast.
		eventually(t, "the abandoned invocation to release its slot promptly", 1*time.Second, func() bool {
			return w.do("GET", "web.test", "/echo", "").status == 200
		})
	})

	t.Run("no workers is a 503 with Retry-After", func(t *testing.T) {
		wk.stop()
		eventually(t, "the orchestrator to notice", 10*time.Second, func() bool {
			r := w.do("GET", "web.test", "/echo", "")
			return r.status == 503 && r.header.Get("Retry-After") != ""
		})
	})
}
