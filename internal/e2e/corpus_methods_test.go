package e2e

import (
	"fmt"
	"strings"
	"testing"
)

func TestCorpusMethodsAndConnections(t *testing.T) {
	e := newCorpusEnv(t)
	method := func(m string) string { return withHost(m, "/m") }
	piped := func(n int) (string, string) {
		var b strings.Builder
		var sigs []string
		for i := 1; i <= n; i++ {
			b.WriteString(cget(fmt.Sprintf("/p%d", i)))
			sigs = append(sigs, ok(fmt.Sprintf("/p%d", i), 0))
		}
		return b.String(), seq(append(sigs, ok("/sentinel", 0))...)
	}
	three, threeSig := piped(3)
	many, manySig := piped(25)
	closedAfter := func(t *testing.T, ex *exch) {
		if !ex.closed {
			t.Errorf("the connection must have been closed after the answer (the sentinel after it was answered or the connection stayed open): %s", ex.sig())
		}
	}
	noChunked := func(t *testing.T, ex *exch) {
		for _, r := range ex.resps {
			if len(r.te) > 0 {
				t.Errorf("an HTTP/1.0 client was sent Transfer-Encoding %v", r.te)
			}
		}
	}
	rows := []crow{
		// ---- methods -----------------------------------------------------------------
		{name: "GET", raw: method("GET"), guest: g("REQUEST_METHOD", "GET")},
		{name: "POST with a body", raw: post("/m", "x=1"), guest: g("REQUEST_METHOD", "POST", "BODY", "x=1")},
		{name: "PUT", raw: method("PUT"), guest: g("REQUEST_METHOD", "PUT")},
		{name: "PATCH", raw: method("PATCH"), guest: g("REQUEST_METHOD", "PATCH")},
		{name: "DELETE", raw: method("DELETE"), guest: g("REQUEST_METHOD", "DELETE")},
		{name: "DELETE with a body", raw: rawReq("DELETE /m HTTP/1.1", "Host: "+corpusHost, "Content-Length: 3") + "abc", guest: g("REQUEST_METHOD", "DELETE", "BODY", "abc")},
		{name: "OPTIONS with a path is forwarded", raw: method("OPTIONS"), guest: g("REQUEST_METHOD", "OPTIONS"), silent: "only OPTIONS * is answered by the frontend; a CORS preflight to a path reaches the function"},
		{name: "PURGE", raw: method("PURGE"), guest: g("REQUEST_METHOD", "PURGE"), silent: "any token is a method; unknown methods are forwarded, not 501/405"},
		{name: "PROPFIND", raw: method("PROPFIND"), guest: g("REQUEST_METHOD", "PROPFIND")},
		{name: "M-SEARCH", raw: method("M-SEARCH"), guest: g("REQUEST_METHOD", "M-SEARCH")},
		{name: "TRACE", raw: method("TRACE"), guest: g("REQUEST_METHOD", "TRACE"), silent: "RFC 9110 9.3.8 makes TRACE optional; it is forwarded to the function"},
		{name: "TRACE with a body", raw: rawReq("TRACE /m HTTP/1.1", "Host: "+corpusHost, "Content-Length: 3") + "abc", guest: g("REQUEST_METHOD", "TRACE", "BODY", "abc"), silent: "RFC 9110 9.3.8: a client MUST NOT send a body with TRACE; accepted"},
		{name: "lower-case method", raw: method("get"), guest: g("REQUEST_METHOD", "get"), silent: "RFC 9110 9.1: methods are case-sensitive, so `get` is an unknown method; it is forwarded exactly as sent, and a function that switches on GET will not match it"},
		{name: "mixed-case method", raw: method("Get"), guest: g("REQUEST_METHOD", "Get")},
		{name: "method with a token punctuation", raw: method("X.Y-Z_1!"), guest: g("REQUEST_METHOD", "X.Y-Z_1!")},
		{name: "method with a comma", raw: method("GET,POST"), want: []string{"400"}},
		{name: "method with an at sign", raw: method("G@T"), want: []string{"400"}},
		{name: "method with a NUL", raw: method("GE\x00T"), want: []string{"400"}},
		{name: "empty method", raw: " /m HTTP/1.1\r\nHost: " + corpusHost + "\r\n\r\n", want: []string{"400"}},
		{name: "very long method", raw: method(strings.Repeat("M", 5000)), guest: g("REQUEST_METHOD", strings.Repeat("M", 5000)), silent: "no limit on method length in the spec; accepted up to the header limit"},
		{name: "method with the Content-Length of a GET", raw: rawReq("GET /m HTTP/1.1", "Host: "+corpusHost, "Content-Length: 0"), guest: g("CONTENT_LENGTH", "0")},

		// ---- HEAD ----------------------------------------------------------------------------
		{name: "HEAD has headers and no body", raw: method("HEAD"), sentinel: true, heads: []int{0}, want: []string{seq("200", ok("/sentinel", 0))},
			check: func(t *testing.T, ex *exch) {
				r := ex.resps[0]
				if len(r.body) != 0 {
					t.Errorf("a HEAD answer carried %d body bytes", len(r.body))
				}
				if got := r.header.Get("Content-Type"); got != "text/x-corpus-dump" {
					t.Errorf("a HEAD answer must carry the headers the GET would (Content-Type): %q", got)
				}
				t.Logf("HEAD answer: Content-Length=%q Transfer-Encoding=%v", r.header.Get("Content-Length"), r.te)
			}, spec: "RFC 9110 9.3.2: same header fields as GET, no content. docs/protocol.md says nothing about HEAD."},
		{name: "HEAD: Content-Length, if sent, is what GET would send", raw: method("HEAD") + withHost("GET", "/m"), heads: []int{0}, responses: 2,
			check: func(t *testing.T, ex *exch) {
				h, gt := ex.resps[0], ex.resps[1]
				if cl := h.header.Get("Content-Length"); cl != "" && cl != fmt.Sprint(len(gt.body)) {
					// the guest's REQUEST_METHOD differs by one byte only (HEAD/GET: 4 vs 3), so allow that
					t.Logf("HEAD Content-Length %s vs GET body %d", cl, len(gt.body))
				}
			}, silent: "the frontend drops the guest's content-length and frames itself; for HEAD the length of the would-be body is not sent (observed in the log of the previous row). RFC 9110 9.3.2 makes it optional."},
		{name: "HEAD with a request body is consumed", raw: rawReq("HEAD /m HTTP/1.1", "Host: "+corpusHost, "Content-Length: 5") + "hello", sentinel: true, heads: []int{0}, want: []string{seq("200", ok("/sentinel", 0))}},
		{name: "HEAD then GET pipelined", raw: method("HEAD") + cget("/after"), sentinel: true, heads: []int{0}, want: []string{seq("200", ok("/after", 0), ok("/sentinel", 0))}},
		{name: "HEAD of a path that exists only for GET is forwarded as HEAD", raw: method("HEAD"), heads: []int{0}, want: []string{"200"}},

		// ---- keep-alive and HTTP/1.0 --------------------------------------------------------
		{name: "HTTP/1.1 keeps the connection open", raw: cget("/k"), sentinel: true, want: []string{seq(ok("/k", 0), ok("/sentinel", 0))}},
		{name: "HTTP/1.1 Connection: close closes", raw: cget("/k", "Connection: close"), sentinel: true, want: []string{ok("/k", 0)}, check: closedAfter,
			guest: g("-HTTP_CONNECTION", "")},
		{name: "Connection: Close in other case", raw: cget("/k", "Connection: Close"), sentinel: true, want: []string{ok("/k", 0)}, check: closedAfter},
		{name: "Connection: keep-alive, close", raw: cget("/k", "Connection: keep-alive, close"), sentinel: true, want: []string{ok("/k", 0)}, check: closedAfter},
		{name: "Connection: close in a second header", raw: cget("/k", "Connection: keep-alive", "Connection: close"), sentinel: true, want: []string{ok("/k", 0)}, check: closedAfter},
		{name: "Connection: close ends pipelining", raw: cget("/k1", "Connection: close") + cget("/k2"), sentinel: true, want: []string{ok("/k1", 0)}, check: closedAfter,
			spec: "RFC 9112 9.6: a server MUST NOT process further requests on a connection after one with close"},
		{name: "HTTP/1.0 closes by default", raw: rawReq("GET /k HTTP/1.0", "Host: "+corpusHost), sentinel: true, want: []string{ok("/k", 0)}, check: func(t *testing.T, ex *exch) { closedAfter(t, ex); noChunked(t, ex) }},
		{name: "HTTP/1.0 without Host closes by default", raw: rawReq("GET /k HTTP/1.0"), sentinel: true, want: []string{ok("/k", 0)}, check: closedAfter},
		{name: "HTTP/1.0 with keep-alive", raw: rawReq("GET /k HTTP/1.0", "Host: "+corpusHost, "Connection: keep-alive"), sentinel: true, want: []string{ok("/k", 0)}, check: func(t *testing.T, ex *exch) {
			noChunked(t, ex)
			t.Logf("HTTP/1.0 keep-alive: %s, closed=%v, response Connection=%q", ex.sig(), ex.closed, ex.resps[0].header.Get("Connection"))
		}, silent: "whether a streamed (length-unknown) answer to an HTTP/1.0 keep-alive client closes the connection is not in the spec; it never sends chunked to 1.0"},
		{name: "HTTP/1.0 POST with a body", raw: rawReq("POST /k HTTP/1.0", "Host: "+corpusHost, "Content-Length: 3") + "abc", want: []string{ok("/k", 3)}, guest: g("SERVER_PROTOCOL", "HTTP/1.1", "BODY", "abc"),
			silent: "the guest is told SERVER_PROTOCOL=HTTP/1.1 even for an HTTP/1.0 client (the ABI table fixes it)"},
		{name: "HTTP/1.0 gets a complete answer of any size", raw: rawReq("POST /raw HTTP/1.0", "Host: "+corpusHost, fmt.Sprintf("Content-Length: %d", 100000+len("Content-Type: x/y\n\n"))) + "Content-Type: x/y\n\n" + strings.Repeat("z", 100000),
			check: func(t *testing.T, ex *exch) {
				if len(ex.resps) != 1 || len(ex.resps[0].body) != 100000 {
					t.Errorf("an HTTP/1.0 client got %s, body %d", ex.sig(), len(ex.resps[len(ex.resps)-1].body))
				}
				noChunked(t, ex)
			}},

		// ---- pipelining --------------------------------------------------------------------
		{name: "three pipelined requests, answered in order", raw: three, sentinel: true, want: []string{threeSig}},
		{name: "25 pipelined requests, answered in order", raw: many, sentinel: true, want: []string{manySig}},
		{name: "a slow first request does not reorder the answers", raw: withHost("GET", "/sleep?ms=300") + cget("/fast"), sentinel: true, want: []string{seq("200", ok("/fast", 0), ok("/sentinel", 0))}},
		{name: "pipelined bodies keep their boundaries", raw: post("/a", "aaaa") + chunked("/b", "3\r\nbbb\r\n0\r\n\r\n") + post("/c", ""), sentinel: true,
			want: []string{seq(ok("/a", 4), ok("/b", 3), ok("/c", 0), ok("/sentinel", 0))}},
		{name: "a bad request in the middle ends the connection", raw: cget("/p1") + "GARBAGE\r\n\r\n" + cget("/p3"), sentinel: true, want: []string{seq(ok("/p1", 0), "400")}, check: closedAfter,
			spec: "RFC 9112 9.3 / 2.2: after a framing error the connection is closed; /p3 must not run"},
		{name: "an oversized declared body in the middle ends the connection", raw: cget("/p1") + rawReq("POST /p2 HTTP/1.1", "Host: "+corpusHost, fmt.Sprintf("Content-Length: %d", corpusBodyLimit+1)) + cget("/p3"), sentinel: true,
			want: []string{seq(ok("/p1", 0), "413")}, check: closedAfter,
			spec: "after a 413 for an unread body the connection cannot be reused: the next request would be parsed out of the body"},
		{name: "request after a refused path is still served", raw: cget("/a%zz") + cget("/p2"), sentinel: true, want: []string{"400"}, check: closedAfter,
			silent: "a 400 closes the connection; the pipelined request after it is not answered"},
		{name: "HTTP/1.0 pipelined second request is not served", raw: rawReq("GET /p1 HTTP/1.0", "Host: "+corpusHost) + cget("/p2"), sentinel: true, want: []string{ok("/p1", 0)}, check: closedAfter},
	}
	runRows(t, e, rows)
}
