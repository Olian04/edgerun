package egressconf

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
)

func headerValues(head *commonv1.HttpResponseHead, name string) []string {
	var out []string
	for _, h := range head.GetHeaders() {
		if h.GetName() == name {
			out = append(out, h.GetValue())
		}
	}
	return out
}

func (h *harness) requests(t *testing.T) {
	allow := []string{h.origin.entry()}

	t.Run("methods", func(t *testing.T) {
		for _, m := range []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"} {
			r := h.fetch(t, &egressv1.FetchRequest{Method: m, Url: h.origin.url("/echo"), Allow: allow}, 0)
			if r.err != nil || r.head.GetStatus() != 200 {
				t.Errorf("%s: %v %v", m, r.head, r.err)
				continue
			}
			if got := h.origin.lastRequest("/echo").Method; got != m {
				t.Errorf("the origin saw %q, want %q", got, m)
			}
		}
	})

	t.Run("the origin sees the guest's request and nothing a service adds", func(t *testing.T) {
		r := h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: h.origin.url("/echo"), Allow: allow}, 0)
		if r.err != nil {
			t.Fatal(r.err)
		}
		seen := h.origin.lastRequest("/echo")
		for _, name := range []string{"User-Agent", "Accept-Encoding", "Via", "X-Forwarded-For", "Forwarded"} {
			if v := seen.Header.Values(name); len(v) != 0 {
				t.Errorf("the guest sent no %s, the origin saw %q: a service adds nothing to the request", name, v)
			}
		}
	})

	t.Run("path and query as given", func(t *testing.T) {
		r := h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: h.origin.url("/echo?a=1&b=%20x&a=2#fragment"), Allow: allow}, 0)
		if r.err != nil {
			t.Fatal(r.err)
		}
		if got := h.origin.lastRequest("/echo").URI; got != "/echo?a=1&b=%20x&a=2" {
			t.Errorf("the origin saw %q: the query must arrive as written, and a fragment is not sent", got)
		}
	})

	t.Run("headers and body", func(t *testing.T) {
		body := []byte("binary \x00\x01\xff body")
		r := h.fetch(t, &egressv1.FetchRequest{
			Method: "POST", Url: h.origin.url("/echo"), Allow: allow, Body: body,
			Headers: []*commonv1.Header{
				hdr("X-One", "a"), hdr("X-One", "b"), hdr("Content-Type", "text/x-conformance"),
				hdr("Authorization", "Bearer guest-secret"), hdr("User-Agent", "a-guest/1"),
				// The guest does not control the connection...
				hdr("Connection", "keep-alive"), hdr("Keep-Alive", "timeout=5"), hdr("Proxy-Authorization", "Basic c2VjcmV0"),
				hdr("Proxy-Connection", "keep-alive"), hdr("TE", "trailers"), hdr("Upgrade", "x-conformance"),
				// ...nor the destination, nor the framing.
				hdr("Host", "evil.test"), hdr("Content-Length", "3"),
			},
		}, 0)
		if r.err != nil || r.head.GetStatus() != 200 {
			t.Fatalf("%v %v", r.head, r.err)
		}
		got := h.origin.lastRequest("/echo")
		if string(got.Body) != string(body) {
			t.Errorf("body = %q, want %q", got.Body, body)
		}
		if fmt.Sprint(got.Header["X-One"]) != "[a b]" || got.Header.Get("Content-Type") != "text/x-conformance" ||
			got.Header.Get("Authorization") != "Bearer guest-secret" || got.Header.Get("User-Agent") != "a-guest/1" {
			t.Errorf("headers the guest set did not arrive: %v", got.Header)
		}
		for _, name := range []string{"Keep-Alive", "Proxy-Authorization", "Proxy-Connection", "Te", "Upgrade"} {
			if v := got.Header.Values(name); len(v) > 0 {
				t.Errorf("connection-level header %s reached the origin: %v", name, v)
			}
		}
		if got.Host != h.origin.addr() {
			t.Errorf("the origin saw Host %q, want %q: the URL decides the destination, not a header", got.Host, h.origin.addr())
		}
	})

	t.Run("encodings are the guest's business", func(t *testing.T) {
		h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: h.origin.url("/echo"), Allow: allow}, 0)
		if v := h.origin.lastRequest("/echo").Header.Get("Accept-Encoding"); v != "" {
			t.Errorf("the service added Accept-Encoding %q on the guest's behalf", v)
		}
		h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: h.origin.url("/echo"), Allow: allow, Headers: []*commonv1.Header{hdr("Accept-Encoding", "gzip")}}, 0)
		if v := h.origin.lastRequest("/echo").Header.Get("Accept-Encoding"); v != "gzip" {
			t.Errorf("Accept-Encoding = %q, want the guest's gzip", v)
		}
	})
}

func (h *harness) responses(t *testing.T) {
	t.Run("status, headers and body", func(t *testing.T) {
		r := h.get(t, "/multi")
		if r.err != nil || r.head.GetStatus() != 200 || string(r.body) != "multi" {
			t.Fatalf("%v %q %v", r.head, r.body, r.err)
		}
		if fmt.Sprint(headerValues(r.head, "set-cookie")) != "[a=1 b=2]" || fmt.Sprint(headerValues(r.head, "x-multi")) != "[x y]" {
			t.Errorf("repeated headers must all arrive, in order: %v", r.head.GetHeaders())
		}
		for _, hd := range r.head.GetHeaders() {
			if hd.GetName() != strings.ToLower(hd.GetName()) {
				t.Errorf("header name %q is not lower-case", hd.GetName())
			}
		}
		for _, name := range []string{"keep-alive", "proxy-authenticate", "connection", "transfer-encoding"} {
			if v := headerValues(r.head, name); len(v) > 0 {
				t.Errorf("connection-level header %q was forwarded: %v", name, v)
			}
		}
	})

	t.Run("an upstream error is a response", func(t *testing.T) {
		for _, code := range []int{201, 400, 404, 418, 500, 503} {
			r := h.get(t, "/status?code="+strconv.Itoa(code)+"&body=text")
			if r.err != nil || int(r.head.GetStatus()) != code || string(r.body) != "text" {
				t.Errorf("%d: head %v body %q err %v", code, r.head, r.body, r.err)
			}
		}
	})

	t.Run("redirects are not followed", func(t *testing.T) {
		before := h.origin.count("/echo")
		r := h.get(t, "/redirect")
		if r.err != nil || r.head.GetStatus() != 302 || fmt.Sprint(headerValues(r.head, "location")) != "[/echo]" {
			t.Fatalf("%v %v", r.head, r.err)
		}
		if h.origin.count("/echo") != before {
			t.Error("the redirect was followed: every hop is a destination of its own, and the caller decides")
		}
	})

	t.Run("a large body is streamed in chunks", func(t *testing.T) {
		const n = 3 << 20
		r := h.get(t, "/big?n="+strconv.Itoa(n))
		if r.err != nil || !bytes.Equal(r.body, pattern(n)) {
			t.Fatalf("%d bytes, %v", len(r.body), r.err)
		}
		if r.chunks < 3 {
			t.Errorf("%d bytes arrived in %d chunks: each is at most 1 MiB", n, r.chunks)
		}
	})

	t.Run("encodings are left as the upstream sent them", func(t *testing.T) {
		r := h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: h.origin.url("/gzip"), Allow: []string{h.origin.entry()},
			Headers: []*commonv1.Header{hdr("Accept-Encoding", "gzip")}}, 0)
		if r.err != nil || fmt.Sprint(headerValues(r.head, "content-encoding")) != "[gzip]" || len(r.body) < 2 || r.body[0] != 0x1f || r.body[1] != 0x8b {
			t.Errorf("the compressed body must arrive compressed, with its Content-Encoding: %v %v %v", r.head, r.body, r.err)
		}
	})

	t.Run("no body", func(t *testing.T) {
		r := h.get(t, "/empty")
		if r.err != nil || r.head.GetStatus() != 204 || len(r.body) != 0 {
			t.Errorf("%v %q %v", r.head, r.body, r.err)
		}
	})
}

// validation: a request that is not well formed is the caller's bug.
func (h *harness) validation(t *testing.T) {
	allow := []string{h.origin.entry()}
	mk := func(method, url string, allow []string, headers ...*commonv1.Header) *egressv1.FetchRequest {
		return &egressv1.FetchRequest{Method: method, Url: url, Allow: allow, Headers: headers}
	}
	bad := map[string]*egressv1.FetchRequest{
		"no method":                      mk("", h.origin.url("/echo"), allow),
		"method with a space":            mk("GE T", h.origin.url("/echo"), allow),
		"method with a newline":          mk("GET\n", h.origin.url("/echo"), allow),
		"no URL":                         mk("GET", "", allow),
		"relative URL":                   mk("GET", "echo", allow),
		"a path":                         mk("GET", "/echo", allow),
		"ftp":                            mk("GET", "ftp://"+h.origin.addr()+"/", allow),
		"file":                           mk("GET", "file:///etc/passwd", allow),
		"no host":                        mk("GET", "http:///echo", allow),
		"credentials":                    mk("GET", "http://user:pw@"+h.origin.addr()+"/echo", allow),
		"port zero":                      mk("GET", "http://127.0.0.1:0/", []string{"127.0.0.1:1"}),
		"port too large":                 mk("GET", "http://127.0.0.1:99999/", []string{"127.0.0.1:1"}),
		"non-ASCII host":                 mk("GET", "http://bücher.example/", []string{"example.com"}),
		"space in host":                  mk("GET", "http://exa mple.com/", []string{"example.com"}),
		"a newline in a header value":    mk("GET", h.origin.url("/echo"), allow, hdr("X-A", "b\r\nX-Injected: 1")),
		"a bad header name":              mk("GET", h.origin.url("/echo"), allow, hdr("X A", "b")),
		"CONNECT":                        mk("CONNECT", h.origin.url("/echo"), allow),
		"a space in the query":           mk("GET", h.origin.url("/echo?a=b c"), allow),
		"a space in the path":            mk("GET", h.origin.url("/ec ho"), allow),
		"a control byte in the path":     mk("GET", h.origin.url("/ec\x01ho"), allow),
		"a non-ASCII byte in the path":   mk("GET", h.origin.url("/caf\u00e9"), allow),
		"a host spelled 127.1":           mk("GET", "http://127.1/", []string{"example.com"}),
		"a host spelled as an integer":   mk("GET", "http://2130706433/", []string{"example.com"}),
		"a host spelled in hex":          mk("GET", "http://0x7f.1/", []string{"example.com"}),
		"an entry spelled 127.1":         mk("GET", h.origin.url("/echo"), []string{"127.1"}),
		"an entry spelled as an integer": mk("GET", h.origin.url("/echo"), []string{"2130706433"}),
		"a bare IPv6 entry":              mk("GET", h.origin.url("/echo"), []string{"::1"}),
		"a bare wildcard":                mk("GET", h.origin.url("/echo"), []string{"*"}),
		"an empty entry":                 mk("GET", h.origin.url("/echo"), []string{""}),
		"an entry that is a URL":         mk("GET", h.origin.url("/echo"), []string{"http://" + h.origin.addr()}),
		"an entry with a path":           mk("GET", h.origin.url("/echo"), []string{h.origin.addr() + "/echo"}),
		"a bad entry beside a good one":  mk("GET", h.origin.url("/echo"), []string{h.origin.entry(), "not an entry"}),
	}
	var many []string
	for i := 0; i < 65; i++ {
		many = append(many, fmt.Sprintf("h%d.example.com", i))
	}
	bad["too many entries"] = mk("GET", h.origin.url("/echo"), many)

	before := h.origin.count("/echo")
	for name, req := range bad {
		r := h.fetch(t, req, 0)
		if connect.CodeOf(r.err) != connect.CodeInvalidArgument {
			t.Errorf("%s: %v, want invalid_argument", name, r.err)
			continue
		}
		if reason, _ := check.ReasonOf(r.err); reason != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
			t.Errorf("%s: carries reason %v; a malformed request has none", name, reason)
		}
	}
	if h.origin.count("/echo") != before {
		t.Error("a malformed request reached the origin")
	}
}

// allowList: the one decision this seam exists for.
func (h *harness) allowList(t *testing.T) {
	denied := func(t *testing.T, r fetched, what string) {
		t.Helper()
		if reason, _ := check.ReasonOf(r.err); reason != commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED {
			t.Errorf("%s: %v, want EGRESS_DENIED", what, r.err)
		} else {
			check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED)
		}
	}
	// notDenied: the allow list let the request through; whether it then
	// worked is another matter (these names do not resolve).
	notDenied := func(t *testing.T, r fetched, what string) {
		t.Helper()
		if reason, _ := check.ReasonOf(r.err); reason == commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED {
			t.Errorf("%s: refused, but the allow list names it: %v", what, r.err)
		}
	}
	do := func(url string, allow ...string) fetched {
		return h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: url, Allow: allow}, 15*time.Second)
	}

	t.Run("closed by default", func(t *testing.T) {
		before := h.origin.count("/echo")
		denied(t, do(h.origin.url("/echo")), "an empty list")
		if h.origin.count("/echo") != before {
			t.Error("a refused request reached the origin")
		}
	})

	t.Run("ports", func(t *testing.T) {
		p, host := h.origin.port(), h.origin.hostEntry()
		if r := do(h.origin.url("/echo"), fmt.Sprintf("%s:%d", host, p)); r.err != nil {
			t.Errorf("an entry naming the port: %v", r.err)
		}
		denied(t, do(h.origin.url("/echo"), host), "an entry without a port, for a port that is not the scheme's default")
		denied(t, do(h.origin.url("/echo"), fmt.Sprintf("%s:%d", host, p+1)), "an entry naming another port")
		denied(t, do(h.origin.url("/echo"), host+":80"), "an entry naming the default port, for another port")
		if r := do(h.origin.url("/echo"), "other.covered.test", "nothing.covered.test:1", fmt.Sprintf("%s:%d", host, p)); r.err != nil {
			t.Errorf("any matching entry is enough: %v", r.err)
		}
		// The port is what is compared, whatever the scheme.
		notDenied(t, do(fmt.Sprintf("https://%s:%d/echo", host, p), fmt.Sprintf("%s:%d", host, p)), "https to a named port")
	})

	t.Run("a name that resolves to an address the service may reach", func(t *testing.T) {
		if !h.origin.loopback() {
			t.Skip("the suite's web servers are not on the service's own machine, so localhost does not lead to them")
		}
		if !check.Covered(h.registration.Info.GetCovers(), "localhost") {
			t.Skip("the service does not carry localhost")
		}
		p := h.origin.port()
		r := do(fmt.Sprintf("http://localhost:%d/echo", p), fmt.Sprintf("localhost:%d", p))
		if r.err != nil || r.head.GetStatus() != 200 {
			t.Errorf("localhost resolves to the loopback address the service was configured to reach: %v %v", r.head, r.err)
		}
	})

	t.Run("hosts", func(t *testing.T) {
		p := h.origin.port()
		if other := h.origin.neighbour(); other != "" {
			denied(t, do(fmt.Sprintf("http://%s:%d/echo", other, p), h.origin.entry()), "another address")
		}
		denied(t, do("http://sub.api.covered.test/", "api.covered.test"), "an exact entry does not cover subdomains")
		denied(t, do("http://api.covered.test.evil.test/", "api.covered.test"), "an exact entry does not cover a longer name")
		notDenied(t, do("http://api.covered.test/", "api.covered.test"), "an exact name")
		notDenied(t, do("http://API.Covered.Test/", "api.covered.test"), "an exact name, in another case")
		notDenied(t, do("http://api.covered.test./", "api.covered.test"), "an exact name with a trailing dot")
		notDenied(t, do("http://api.covered.test/", "API.COVERED.TEST"), "an entry in upper case")
	})

	t.Run("wildcards", func(t *testing.T) {
		w := "*.wild.covered.test"
		notDenied(t, do("http://a.wild.covered.test/", w), "a name under the suffix")
		notDenied(t, do("http://a.b.wild.covered.test/", w), "a name two labels under the suffix")
		notDenied(t, do("http://A.WILD.Covered.Test/", w), "a name in another case")
		notDenied(t, do("http://a.wild.covered.test./", w), "a name with a trailing dot")
		notDenied(t, do("https://a.wild.covered.test/", w), "https, the other default port")
		denied(t, do("http://wild.covered.test/", w), "a wildcard does not match its apex")
		denied(t, do("http://xwild.covered.test/", w), "a wildcard matches on a label boundary")
		denied(t, do("http://a.wild.covered.test.evil.test/", w), "a wildcard matches a suffix, not a prefix")
		denied(t, do("http://a.wild.covered.test:8080/", w), "an entry without a port does not match another port")
		denied(t, do("http://a.wild.covered.test:443/", w), "http on the https default port")
		notDenied(t, do("http://a.wild.covered.test:8080/", "*.wild.covered.test:8080"), "a wildcard with a port")
		denied(t, do("http://a.wild.covered.test/", "*.wild.covered.test:8080"), "a wildcard with a port, on another")
	})

	t.Run("a refusal reveals nothing about the network", func(t *testing.T) {
		start := time.Now()
		r := do("http://never-resolved-"+strconv.FormatInt(time.Now().UnixNano(), 36)+".covered.test/", "api.covered.test")
		denied(t, r, "a name that is not allowed")
		if took := time.Since(start); took > 2*time.Second {
			t.Errorf("took %v: a request the allow list refuses must be refused before anything is looked up", took)
		}
	})
}

func (h *harness) limits(t *testing.T) {
	allow := []string{h.origin.entry()}
	d, err := h.client.Describe(context.Background(), connect.NewRequest(&egressv1.DescribeRequest{}))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("the caller's response limit", func(t *testing.T) {
		r := h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: h.origin.url("/big?n=100000"), Allow: allow, MaxResponseBytes: 1000}, 0)
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE)
		if len(r.body) > 1000 {
			t.Errorf("%d bytes were delivered against a limit of 1000", len(r.body))
		}
		r = h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: h.origin.url("/bigcl?n=100000"), Allow: allow, MaxResponseBytes: 1000}, 0)
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE)
		if r.head != nil {
			t.Error("a body whose length is declared too large must be refused before anything is delivered")
		}
		for _, path := range []string{"/big?n=1000", "/bigcl?n=1000"} {
			if r := h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: h.origin.url(path), Allow: allow, MaxResponseBytes: 1000}, 0); r.err != nil || len(r.body) != 1000 {
				t.Errorf("%s: a body of exactly the limit is allowed: %d bytes, %v", path, len(r.body), r.err)
			}
		}
	})

	t.Run("the service's own limit", func(t *testing.T) {
		max := int(d.Msg.GetMaxResponseBodyBytes())
		over := h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: h.origin.url("/big?n=" + strconv.Itoa(max+1)), Allow: allow, MaxResponseBytes: 1 << 40}, 0)
		check.WantReason(t, over.err, commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE)
		if len(over.body) > max {
			t.Errorf("%d bytes delivered against the service's limit of %d", len(over.body), max)
		}
	})

	t.Run("the request limit", func(t *testing.T) {
		max := int(d.Msg.GetMaxRequestBodyBytes())
		r := h.fetch(t, &egressv1.FetchRequest{Method: "POST", Url: h.origin.url("/echo"), Allow: allow, Body: make([]byte, max+1)}, 0)
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE)
		if r := h.fetch(t, &egressv1.FetchRequest{Method: "POST", Url: h.origin.url("/echo"), Allow: allow, Body: make([]byte, max)}, 0); r.err != nil {
			t.Errorf("a body of exactly max_request_body_bytes (%d) is accepted: %v", max, r.err)
		}
	})
}

func (h *harness) failures(t *testing.T) {
	t.Run("connection refused", func(t *testing.T) {
		addr := h.origin.net.ClosedAddr(t)
		r := h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: "http://" + addr + "/", Allow: []string{addr}}, 0)
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED)
	})
	t.Run("a name that does not resolve", func(t *testing.T) {
		r := h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: "http://nothing-here.covered.test/", Allow: []string{"*.covered.test"}}, 0)
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED)
	})
	t.Run("a certificate that does not verify", func(t *testing.T) {
		addr := h.origin.tls.Addr
		r := h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: "https://" + addr + "/echo", Allow: []string{addr}}, 0)
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED)
		if r.head != nil {
			t.Error("an unverified server's response must never be delivered")
		}
	})
	t.Run("the upstream hangs up mid-body", func(t *testing.T) {
		r := h.get(t, "/cut")
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED)
		if r.head != nil && !bytes.HasPrefix([]byte("partial"), r.body) {
			t.Errorf("what was delivered before the failure must be what the upstream sent: %q", r.body)
		}
	})
}

func (h *harness) deadline(t *testing.T) {
	for len(h.origin.gone) > 0 {
		<-h.origin.gone
	}
	start := time.Now()
	r := h.fetchBudget(t, &egressv1.FetchRequest{Method: "GET", Url: h.origin.url("/hang"), Allow: []string{h.origin.entry()}}, time.Second)
	check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED)
	if took := time.Since(start); took > 8*time.Second {
		t.Errorf("answered after %v with a budget of 1s", took)
	}
	select {
	case <-h.origin.gone:
	case <-time.After(10 * time.Second):
		t.Error("the upstream request was still running 10s after the deadline: it must be abandoned")
	}
}

func (h *harness) cancellation(t *testing.T) {
	for len(h.origin.gone) > 0 {
		<-h.origin.gone
	}
	before := h.origin.count("/hang")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan fetched, 1)
	go func() {
		done <- h.fetchCtx(t, ctx, &egressv1.FetchRequest{Method: "GET", Url: h.origin.url("/hang"), Allow: []string{h.origin.entry()}})
	}()
	check.Eventually(t, "the request to reach the origin", settle, func() bool { return h.origin.count("/hang") > before })
	cancel()
	<-done
	select {
	case <-h.origin.gone:
	case <-time.After(10 * time.Second):
		t.Error("cancelling the call did not abandon the upstream request")
	}
}

// forbidden: what a service never reaches, however the caller's list is
// written. Each destination is listed explicitly, so the allow list is not
// what stops it.
func (h *harness) forbidden(t *testing.T) {
	port := strconv.Itoa(h.origin.port())
	cases := map[string]struct{ url, entry string }{
		"loopback by name":            {"http://localhost:" + port + "/echo", "localhost:" + port},
		"loopback, another address":   {"http://127.1.2.3:" + port + "/", "127.1.2.3:" + port},
		"IPv6 loopback":               {"http://[::1]:" + port + "/", "[::1]:" + port},
		"IPv4-mapped loopback":        {"http://[::ffff:127.0.0.1]:" + port + "/echo", "[::ffff:127.0.0.1]:" + port},
		"unspecified":                 {"http://0.0.0.0:" + port + "/", "0.0.0.0:" + port},
		"IPv6 unspecified":            {"http://[::]:" + port + "/", "[::]:" + port},
		"private 10/8":                {"http://10.0.0.1/", "10.0.0.1"},
		"private 172.16/12":           {"http://172.16.0.1/", "172.16.0.1"},
		"private 192.168/16":          {"http://192.168.1.1/", "192.168.1.1"},
		"carrier-grade NAT":           {"http://100.64.0.1/", "100.64.0.1"},
		"link-local (cloud metadata)": {"http://169.254.169.254/latest/meta-data/", "169.254.169.254"},
		"IPv6 unique-local":           {"http://[fd00::1]/", "[fd00::1]"},
		"IPv6 link-local":             {"http://[fe80::1]/", "[fe80::1]"},
		"multicast":                   {"http://224.0.0.1/", "224.0.0.1"},
		"broadcast":                   {"http://255.255.255.255/", "255.255.255.255"},
		"documentation":               {"http://192.0.2.1/", "192.0.2.1"},
		"benchmarking":                {"http://198.18.0.1/", "198.18.0.1"},
		"IPv6 documentation":          {"http://[2001:db8::1]/", "[2001:db8::1]"},
		"6to4 embedding loopback":     {"http://[2002:7f00:1::]/", "[2002:7f00:1::]"},
		"NAT64 embedding loopback":    {"http://[64:ff9b::7f00:1]/", "[64:ff9b::7f00:1]"},
	}
	// Every forbidden range, at both of its ends: an implementation that has
	// the range a little too small refuses the middle and lets the edge through.
	// Only addresses inside the ranges are used, so nothing is ever sent to an
	// address that is not forbidden.
	for _, cidr := range forbiddenRanges {
		p := netip.MustParsePrefix(cidr)
		for which, a := range map[string]netip.Addr{"first": p.Masked().Addr(), "last": lastAddr(p)} {
			cases["range "+cidr+", "+which+" address "+a.String()] = struct{ url, entry string }{
				"http://" + bracket(a.String()) + "/", bracket(a.String())}
		}
	}
	for name, addr := range map[string]string{
		"IPv4-mapped private":         "::ffff:10.1.2.3",
		"IPv4-mapped link-local":      "::ffff:169.254.169.254",
		"IPv6 outside 2000::/3":       "4000::1",
		"IPv6 multicast":              "ff02::1",
		"NAT64 embedding a public v4": "64:ff9b::808:808",
	} {
		cases[name] = struct{ url, entry string }{"http://" + bracket(addr) + "/", bracket(addr)}
	}
	// The suite's own web server is a forbidden destination only if it is on
	// a forbidden address (loopback, a private network...).
	if a, err := netip.ParseAddr(h.origin.host()); err == nil && (a.IsLoopback() || a.IsPrivate() || a.IsLinkLocalUnicast()) {
		cases["the suite's own web server"] = struct{ url, entry string }{h.origin.url("/echo"), h.origin.entry()}
	}
	covers := h.registration.Info.GetCovers()
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			u, _ := url.Parse(c.url)
			if !check.Covered(covers, u.Hostname()) {
				t.Skipf("the service declared it carries %v, not this destination: it would be refused for that, which is not what is tested", covers)
			}
			before := h.origin.count("/echo")
			start := time.Now()
			r := h.fetch(t, &egressv1.FetchRequest{Method: "GET", Url: c.url, Allow: []string{c.entry}}, 8*time.Second)
			check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED)
			if took := time.Since(start); took > 4*time.Second {
				t.Errorf("took %v: a forbidden address must be refused, not connected to", took)
			}
			if h.origin.count("/echo") != before {
				t.Error("the request reached the origin")
			}
		})
	}
}

// forbiddenRanges is the table in docs/protocol.md ("What a network service
// does with a Fetch", step 3), kept here independently of any implementation.
var forbiddenRanges = []string{
	// IPv4
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12",
	"192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "192.168.0.0/16", "198.18.0.0/15",
	"198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
	// IPv6, inside 2000::/3 (everything outside it is refused too)
	"2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20",
	// IPv6, outside 2000::/3
	"::/128", "::1/128", "fc00::/7", "fe80::/10", "ff00::/8", "64:ff9b::/96",
}

// lastAddr is the highest address of a prefix.
func lastAddr(p netip.Prefix) netip.Addr {
	a := p.Masked().Addr().AsSlice()
	for bit := p.Bits(); bit < len(a)*8; bit++ {
		a[bit/8] |= 0x80 >> (bit % 8)
	}
	out, _ := netip.AddrFromSlice(a)
	return out
}
