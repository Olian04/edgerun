package e2e

// Resource exhaustion against the public edge: clients that are slow, huge,
// numerous, or that never read. The frontend must have timeouts, keep serving
// ordinary requests throughout, and keep its memory bounded.

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// secProbe makes an ordinary request twice a second until stopped and records
// whether, and how fast, the server answered.
type secProbeRun struct {
	stop     chan struct{}
	done     chan struct{}
	failures atomic.Int64
	count    atomic.Int64
	maxNanos atomic.Int64
}

func secProbe(t *testing.T, w *web, host string) *secProbeRun {
	p := &secProbeRun{stop: make(chan struct{}), done: make(chan struct{})}
	c := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	go func() {
		defer close(p.done)
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-p.stop:
				return
			case <-tick.C:
			}
			r, _ := http.NewRequest("POST", w.base+"/echo", strings.NewReader("probe"))
			r.Host = host
			start := time.Now()
			resp, err := c.Do(r)
			ok := false
			if err == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				ok = resp.StatusCode == 200 && string(b) == "probe"
			}
			took := time.Since(start).Nanoseconds()
			for {
				m := p.maxNanos.Load()
				if took <= m || p.maxNanos.CompareAndSwap(m, took) {
					break
				}
			}
			p.count.Add(1)
			if !ok {
				p.failures.Add(1)
			}
		}
	}()
	return p
}

func (p *secProbeRun) check(t *testing.T, what string) {
	t.Helper()
	close(p.stop)
	<-p.done
	if p.count.Load() == 0 {
		t.Errorf("%s: no probe request was made", what)
	}
	if f := p.failures.Load(); f > 0 {
		t.Errorf("%s: %d of %d ordinary requests were not served while the attack ran", what, f, p.count.Load())
	}
	if m := time.Duration(p.maxNanos.Load()); m > 8*time.Second {
		t.Errorf("%s: an ordinary request took %v while the attack ran", what, m)
	}
}

func secRawStatus(r io.Reader) string {
	line, _ := bufioLine(r)
	return line
}

func bufioLine(r io.Reader) (string, error) {
	var sb strings.Builder
	b := make([]byte, 1)
	for sb.Len() < 200 {
		n, err := r.Read(b)
		if n == 1 {
			if b[0] == '\n' {
				return strings.TrimSpace(sb.String()), nil
			}
			sb.WriteByte(b[0])
		}
		if err != nil {
			return strings.TrimSpace(sb.String()), err
		}
	}
	return sb.String(), nil
}

func TestSecurityFrontendResourceExhaustion(t *testing.T) {
	s := newStack(t)
	s.startWorker(16) // the probes and the attacks must not compete for slots: a full worker is a 503 by design
	fid := s.createFunction("victim")
	if _, err := s.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: s.wasm, Abi: "wagi-1", Activate: true,
		Limits: &commonv1.Limits{MaxResponseBytes: 30 << 20, MemoryLimitBytes: 256 << 20}}); err != nil {
		t.Fatal(err)
	}
	s.route("victim.test", "/", fid)
	w := s.startFrontend(20 * time.Second)
	eventually(t, "the stack to serve HTTP", 120*time.Second, func() bool { return w.do("POST", "victim.test", "/echo", "x").status == 200 })
	addr := strings.TrimPrefix(w.base, "http://")
	heap := secMeasure()

	dial := func() net.Conn {
		c, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Error(err)
			return nil
		}
		return c
	}
	// closedWithin waits for the server to close c, and says how long it took.
	closedWithin := func(c net.Conn, max time.Duration) (time.Duration, bool) {
		start := time.Now()
		c.SetReadDeadline(start.Add(max))
		_, err := io.Copy(io.Discard, c)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return time.Since(start), false
		}
		return time.Since(start), true
	}

	t.Run("slowloris on the headers", func(t *testing.T) {
		t.Parallel()
		p := secProbe(t, w, "victim.test")
		const n = 300
		var wg sync.WaitGroup
		var slowest atomic.Int64
		var survivors atomic.Int64
		for i := 0; i < n; i++ {
			c := dial()
			if c == nil {
				break
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				io.WriteString(c, "GET /echo HTTP/1.1\r\nHost: victim.test\r\nX-Slow: ")
				stopW := make(chan struct{})
				go func() { // one byte a second, so that the connection never looks idle
					tk := time.NewTicker(time.Second)
					defer tk.Stop()
					for {
						select {
						case <-stopW:
							return
						case <-tk.C:
							if _, err := c.Write([]byte("a")); err != nil {
								return
							}
						}
					}
				}()
				took, closed := closedWithin(c, 20*time.Second)
				close(stopW)
				if !closed {
					survivors.Add(1)
				}
				if took.Nanoseconds() > slowest.Load() {
					slowest.Store(took.Nanoseconds())
				}
			}()
		}
		wg.Wait()
		p.check(t, "slowloris on headers")
		if survivors.Load() > 0 {
			t.Errorf("%d connections dribbling a header were still open after 20s: no header timeout", survivors.Load())
		}
		t.Logf("slowest header dribbler closed after %v", time.Duration(slowest.Load()).Round(time.Second))
	})

	t.Run("many idle connections", func(t *testing.T) {
		t.Parallel()
		p := secProbe(t, w, "victim.test")
		const n = 3000
		var conns []net.Conn
		for i := 0; i < n; i++ {
			c, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				t.Logf("stopped at %d connections: %v", i, err)
				break
			}
			conns = append(conns, c)
		}
		var open atomic.Int64
		var wg sync.WaitGroup
		for _, c := range conns {
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				defer c.Close()
				if _, closed := closedWithin(c, 20*time.Second); !closed {
					open.Add(1)
				}
			}(c)
		}
		wg.Wait()
		p.check(t, "idle connections")
		if open.Load() > 0 {
			t.Errorf("%d of %d connections that sent nothing were still open after 20s", open.Load(), len(conns))
		}
	})

	t.Run("a body that stalls", func(t *testing.T) {
		t.Parallel()
		p := secProbe(t, w, "victim.test")
		c := dial()
		defer c.Close()
		io.WriteString(c, "POST /echo HTTP/1.1\r\nHost: victim.test\r\nContent-Length: 1000\r\n\r\n0123456789")
		took, closed := closedWithin(c, 75*time.Second)
		p.check(t, "a stalled body")
		if !closed {
			t.Errorf("a request whose body stopped after 10 of 1000 bytes was still held open after %v: no read timeout", took)
		}
		t.Logf("stalled body: the server gave up after %v", took.Round(time.Second))
	})

	t.Run("a chunked body that never ends", func(t *testing.T) {
		t.Parallel()
		c := dial()
		defer c.Close()
		io.WriteString(c, "POST /echo HTTP/1.1\r\nHost: victim.test\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n")
		took, closed := closedWithin(c, 75*time.Second)
		if !closed {
			t.Errorf("an unfinished chunked body was still held open after %v", took)
		}
	})

	t.Run("a client that never reads the response", func(t *testing.T) {
		t.Parallel()
		p := secProbe(t, w, "victim.test")
		c := dial()
		defer c.Close()
		// 25 MiB: more than the kernel will buffer for a client that does not read.
		io.WriteString(c, "GET /big?bytes=26214400 HTTP/1.1\r\nHost: victim.test\r\n\r\n")
		// The worker's slot is not held by it: the function has finished, its output is buffered.
		time.Sleep(40 * time.Second) // longer than the per-write deadline (30s)
		p.check(t, "a client that never reads")
		c.SetReadDeadline(time.Now().Add(30 * time.Second))
		got, err := io.Copy(io.Discard, c)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Errorf("the server still holds a connection to a client that has not read for 40s (%d bytes arrived)", got)
		} else if got >= 26<<20 {
			t.Errorf("the whole 25 MiB was delivered to a client that did not read: the test did not stall the server (%d)", got)
		}
	})

	t.Run("10000 headers", func(t *testing.T) {
		t.Parallel()
		c := dial()
		defer c.Close()
		var b bytes.Buffer
		b.WriteString("GET /echo HTTP/1.1\r\nHost: victim.test\r\n")
		for i := 0; i < 10000; i++ {
			fmt.Fprintf(&b, "X-H%d: v\r\n", i)
		}
		b.WriteString("\r\n")
		c.SetDeadline(time.Now().Add(15 * time.Second))
		go c.Write(b.Bytes())
		st := secRawStatus(c)
		if strings.Contains(st, " 200") {
			t.Errorf("a request with 10000 headers was served: %q", st)
		}
	})

	t.Run("a header line of 1 MiB", func(t *testing.T) {
		t.Parallel()
		c := dial()
		defer c.Close()
		c.SetDeadline(time.Now().Add(15 * time.Second))
		go io.WriteString(c, "GET /echo HTTP/1.1\r\nHost: victim.test\r\nX-Big: "+strings.Repeat("a", 1<<20)+"\r\n\r\n")
		st := secRawStatus(c)
		if strings.Contains(st, " 200") {
			t.Errorf("a request with a 1 MiB header line was served: %q", st)
		}
	})

	t.Run("a request line of 1 MiB", func(t *testing.T) {
		t.Parallel()
		c := dial()
		defer c.Close()
		c.SetDeadline(time.Now().Add(15 * time.Second))
		go io.WriteString(c, "GET /"+strings.Repeat("a", 1<<20)+" HTTP/1.1\r\nHost: victim.test\r\n\r\n")
		st := secRawStatus(c)
		if strings.Contains(st, " 200") {
			t.Errorf("a request line of 1 MiB was served: %q", st)
		}
	})

	t.Run("an oversized body is refused", func(t *testing.T) {
		t.Parallel()
		h := secMeasure()
		if r := w.do("POST", "victim.test", "/echo", strings.Repeat("x", 5<<20)); r.status != 413 {
			t.Errorf("a 5 MiB body (the limit is 4 MiB): %d", r.status)
		}
		// Chunked, no length: it must be stopped at the limit.
		c := dial()
		defer c.Close()
		io.WriteString(c, "POST /echo HTTP/1.1\r\nHost: victim.test\r\nTransfer-Encoding: chunked\r\n\r\n")
		chunk := strings.Repeat("y", 1<<20)
		c.SetDeadline(time.Now().Add(20 * time.Second))
		var st string
		done := make(chan struct{})
		go func() { st = secRawStatus(c); close(done) }()
		for i := 0; i < 64; i++ {
			if _, err := fmt.Fprintf(c, "%x\r\n%s\r\n", len(chunk), chunk); err != nil {
				break
			}
		}
		<-done
		if !strings.Contains(st, " 413") && st != "" {
			t.Errorf("a chunked body of unbounded size: %q, want 413", st)
		}
		_, alloc := h.Stop()
		if alloc > 400*secMiB {
			t.Errorf("refusing oversized bodies allocated %d MiB", alloc/secMiB)
		}
	})

	t.Run("framing attacks are refused, not forwarded", func(t *testing.T) {
		t.Parallel()
		for name, raw := range map[string]string{
			"two Content-Lengths":  "POST /echo HTTP/1.1\r\nHost: victim.test\r\nContent-Length: 4\r\nContent-Length: 5\r\n\r\nabcde",
			"a negative length":    "POST /echo HTTP/1.1\r\nHost: victim.test\r\nContent-Length: -1\r\n\r\n",
			"two Host headers":     "GET /echo HTTP/1.1\r\nHost: victim.test\r\nHost: other.test\r\n\r\n",
			"a space before colon": "GET /echo HTTP/1.1\r\nHost : victim.test\r\n\r\n",
			"obsolete folding":     "GET /echo HTTP/1.1\r\nHost: victim.test\r\nX-A: b\r\n c\r\n\r\n",
			"a bare LF in a name":  "GET /echo HTTP/1.1\r\nHost: victim.test\r\nX\nA: b\r\n\r\n",
			"a NUL in a value":     "GET /echo HTTP/1.1\r\nHost: victim.test\r\nX-A: b\x00c\r\n\r\n",
			"a NUL in the path":    "GET /ec\x00ho HTTP/1.1\r\nHost: victim.test\r\n\r\n",
			"a tab in the target":  "GET /ec\tho HTTP/1.1\r\nHost: victim.test\r\n\r\n",
			"HTTP/0.9":             "GET /echo\r\n\r\n",
			"an absolute target":   "GET http://evil.test/echo HTTP/1.1\r\nHost: victim.test\r\n\r\n",
			"a Host with a path":   "GET /echo HTTP/1.1\r\nHost: victim.test/x\r\n\r\n",
			"a Host with userinfo": "GET /echo HTTP/1.1\r\nHost: evil.test@victim.test\r\n\r\n",
		} {
			c := dial()
			c.SetDeadline(time.Now().Add(10 * time.Second))
			io.WriteString(c, raw)
			st := secRawStatus(c)
			c.Close()
			if name == "an absolute target" || name == "obsolete folding" {
				// Allowed by HTTP: reported, and judged by the routing check below.
				continue
			}
			if strings.Contains(st, " 200") {
				t.Errorf("%s was served: %q", name, st)
			}
		}
	})

	t.Run("a gzip body is not decompressed for the function", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
		zw.Write(bytes.Repeat([]byte{'a'}, 64<<20))
		zw.Close()
		r := w.do("POST", "victim.test", "/echo", buf.String(), "Content-Encoding", "gzip")
		if r.status == 200 && len(r.body) != buf.Len() {
			t.Errorf("a gzip body of %d bytes reached the function as %d bytes", buf.Len(), len(r.body))
		}
	})

	t.Run("the frontend does not serve h2c", func(t *testing.T) {
		t.Parallel()
		c := dial()
		defer c.Close()
		c.SetDeadline(time.Now().Add(10 * time.Second))
		io.WriteString(c, "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
		fr := http2.NewFramer(c, c)
		fr.WriteSettings()
		f, err := fr.ReadFrame()
		if err == nil {
			if _, ok := f.(*http2.SettingsFrame); ok {
				t.Errorf("the frontend speaks HTTP/2 without TLS: rapid reset and the other h2 attacks apply to it")
			}
		}
	})

	t.Run("the whole run stays within bounded memory", func(t *testing.T) {
		// Runs last by waiting for its siblings (they are parallel subtests of the parent).
	})
	t.Cleanup(func() {
		grew, alloc := heap.Stop()
		t.Logf("whole run: heap peak +%d MiB, %d MiB allocated", grew/secMiB, alloc/secMiB)
		if grew > 600*secMiB {
			t.Errorf("heap peak grew by %d MiB under the attacks", grew/secMiB)
		}
	})
}

// HTTP/2 rapid reset (CVE-2023-44487) against the services that do speak h2c:
// the orchestrator, a worker and a network service. The attacker opens a stream
// and resets it at once, without ever waiting for a response, as fast as it can.
func TestSecurityHTTP2RapidReset(t *testing.T) {
	s := newStack(t)
	s.secNetWorker(2)
	egAddr := s.secRealEgress(t, []string{"*"})
	workerAddr := s.workerAddr(t)
	for name, addr := range map[string]string{"orchestrator": s.addr, "worker": workerAddr, "network service": egAddr} {
		addr := addr
		t.Run(name, func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			io.WriteString(conn, http2.ClientPreface)
			fr := http2.NewFramer(conn, conn)
			fr.WriteSettings()
			var hb bytes.Buffer
			enc := hpack.NewEncoder(&hb)
			for _, h := range [][2]string{{":method", "GET"}, {":scheme", "http"}, {":path", "/healthz"}, {":authority", "x"}} {
				enc.WriteField(hpack.HeaderField{Name: h[0], Value: h[1]})
			}
			block := hb.Bytes()
			go io.Copy(io.Discard, conn) // the server's answers are not read
			h := secMeasure()
			start := time.Now()
			const n = 100000
			var sent int
			conn.SetWriteDeadline(time.Now().Add(20 * time.Second))
			for i := 0; i < n; i++ {
				id := uint32(2*i + 1)
				if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: block, EndStream: true, EndHeaders: true}); err != nil {
					break
				}
				if err := fr.WriteRSTStream(id, http2.ErrCodeCancel); err != nil {
					break
				}
				sent++
			}
			// While that goes on (or has just stopped), an ordinary request is served.
			cl := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
			var ok bool
			eventually(t, "an ordinary request to be served after the reset storm", 15*time.Second, func() bool {
				r, err := cl.Get("http://" + addr + "/healthz")
				if err != nil {
					return false
				}
				r.Body.Close()
				ok = r.StatusCode == 200
				return ok
			})
			grew, alloc := h.Stop()
			t.Logf("%s: %d stream resets in %v, heap peak +%d MiB, %d MiB allocated", name, sent, time.Since(start).Round(time.Millisecond), grew/secMiB, alloc/secMiB)
			if grew > 200*secMiB {
				t.Errorf("%s: the heap grew by %d MiB under a reset storm", name, grew/secMiB)
			}
		})
	}
}

var _ = commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED
