package egressconf

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Olian04/edgerun/conformance/internal/check"
)

// origin is the web server the service is asked to reach. It records what it
// was sent, so tests can say what did and did not arrive.
type origin struct {
	net check.Net
	srv *check.Server
	tls *check.Server

	mu   sync.Mutex
	hits map[string]int
	last map[string]seenRequest
	gone chan struct{} // receives when a /hang request is abandoned by its client
}

// seenRequest is one request as the origin received it.
type seenRequest struct {
	Method, URI, Host string
	Header            http.Header
	Body              []byte
}

func newOrigin(t *testing.T, n check.Net) *origin {
	o := &origin{net: n, hits: map[string]int{}, last: map[string]seenRequest{}, gone: make(chan struct{}, 16)}
	o.srv = n.Serve(t, http.HandlerFunc(o.serve), false)
	o.tls = n.ServeTLS(t, http.HandlerFunc(o.serve))
	return o
}

// host is the host the service reaches the origin at.
func (o *origin) host() string { return o.net.Host() }

func (o *origin) addr() string  { return o.srv.Addr }
func (o *origin) entry() string { return o.addr() }
func (o *origin) port() int     { return o.srv.Port }

// hostEntry is an allow-list entry for the origin's host, with no port.
func (o *origin) hostEntry() string { return bracket(o.host()) }

// url returns an http URL for path on the origin.
func (o *origin) url(path string) string { return "http://" + o.addr() + path }

// bracket writes an IPv6 literal the way URLs and allow-list entries do.
func bracket(host string) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return "[" + host + "]"
	}
	return host
}

// neighbour returns another IPv4 address next to host, or "" if host is not
// one: for testing that an entry for one address does not allow another.
func (o *origin) neighbour() string {
	a, err := netip.ParseAddr(o.host())
	if err != nil || !a.Is4() {
		return ""
	}
	b := a.As4()
	b[3] ^= 1
	return netip.AddrFrom4(b).String()
}

// loopback reports whether the origin is on the machine of the service, which
// is when names like "localhost" lead to it.
func (o *origin) loopback() bool {
	a, err := netip.ParseAddr(o.host())
	return (err == nil && a.IsLoopback()) || o.host() == "localhost"
}

func (o *origin) count(path string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.hits[path]
}

func (o *origin) lastRequest(path string) seenRequest {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.last[path]
}

func (o *origin) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	o.mu.Lock()
	o.hits[r.URL.Path]++
	o.last[r.URL.Path] = seenRequest{Method: r.Method, URI: r.RequestURI, Host: r.Host, Header: r.Header.Clone(), Body: body}
	o.mu.Unlock()

	q := r.URL.Query()
	n, _ := strconv.Atoi(q.Get("n"))
	switch r.URL.Path {
	case "/echo":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "%s %s", r.Method, r.RequestURI)
	case "/status":
		code, _ := strconv.Atoi(q.Get("code"))
		w.WriteHeader(code)
		io.WriteString(w, q.Get("body"))
	case "/redirect":
		http.Redirect(w, r, "/echo", http.StatusFound)
	case "/multi":
		w.Header().Add("Set-Cookie", "a=1")
		w.Header().Add("Set-Cookie", "b=2")
		w.Header().Add("X-Multi", "x")
		w.Header().Add("X-Multi", "y")
		w.Header().Set("Keep-Alive", "timeout=5")
		w.Header().Set("Proxy-Authenticate", "Basic")
		io.WriteString(w, "multi")
	case "/big": // length not declared: chunked
		stream(w, n, false)
	case "/bigcl": // length declared
		stream(w, n, true)
	case "/gzip":
		var b bytes.Buffer
		zw := gzip.NewWriter(&b)
		zw.Write([]byte("a compressed body"))
		zw.Close()
		w.Header().Set("Content-Encoding", "gzip")
		w.Write(b.Bytes())
	case "/empty":
		w.WriteHeader(http.StatusNoContent)
	case "/hang":
		select {
		case <-r.Context().Done():
			o.gone <- struct{}{}
		case <-make(chan struct{}):
		}
	case "/cut": // promises a body, delivers part of it, and hangs up
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, buf, _ := hj.Hijack()
		buf.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\npartial")
		buf.Flush()
		conn.Close()
	default:
		http.NotFound(w, r)
	}
}

// stream writes n bytes of a repeating pattern, in pieces.
func stream(w http.ResponseWriter, n int, declare bool) {
	if declare {
		w.Header().Set("Content-Length", strconv.Itoa(n))
	}
	piece := make([]byte, 32<<10)
	for i := 0; i < n; {
		m := min(len(piece), n-i)
		for j := 0; j < m; j++ {
			piece[j] = byte((i + j) % 251)
		}
		if _, err := w.Write(piece[:m]); err != nil {
			return
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		i += m
	}
}

// pattern returns what stream wrote.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}
