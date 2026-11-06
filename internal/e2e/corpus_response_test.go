package e2e

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	specStatus   = "docs/protocol.md \"Frontend behaviour\" / \"Guest ABI wagi-1\": a status outside 200-599 is a platform fault (502); a bad Status line is ABI_MISMATCH (502)"
	specRespHdr  = "docs/protocol.md \"HTTP messages\": a guest's response header name is a token and its value has no control character other than tab, else ABI_MISMATCH"
	specFrame    = "docs/protocol.md \"Frontend behaviour\": headers are forwarded as given except the connection-level ones and content-length: the frontend frames the response itself"
	specNoDetail = "docs/protocol.md \"Frontend mapping of platform errors\": the body is generic, the message is never forwarded"
)

// raw is a request whose response is exactly out (the guest copies its body to stdout).
func raw(out string) string { return post("/raw", out) }

// headerValues returns the values of the header name (case-insensitive), as the
// bytes the client received.
func headerValues(r *rresp, name string) []string {
	var out []string
	for k, vs := range r.header {
		if strings.EqualFold(k, name) {
			out = append(out, vs...)
		}
	}
	return out
}

func bodyIs(want string) func(*testing.T, *exch) {
	return func(t *testing.T, ex *exch) {
		t.Helper()
		if len(ex.resps) == 0 {
			t.Fatalf("no answer")
		}
		if got := string(ex.resps[0].body); got != want {
			t.Errorf("body %q, want %q", trunc(got), trunc(want))
		}
	}
}

func trunc(s string) string {
	if len(s) > 120 {
		return s[:120] + fmt.Sprintf("...(%d bytes)", len(s))
	}
	return s
}

func hdrIs(name string, want ...string) func(*testing.T, *exch) {
	return func(t *testing.T, ex *exch) {
		t.Helper()
		got := headerValues(ex.resps[0], name)
		if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", want) {
			t.Errorf("header %s = %q, want %q", name, got, want)
		}
	}
}

func all(checks ...func(*testing.T, *exch)) func(*testing.T, *exch) {
	return func(t *testing.T, ex *exch) {
		t.Helper()
		for _, c := range checks {
			c(t, ex)
		}
	}
}

// platformFault asserts the 502 is generic, names its invocation and does not
// echo what the guest wrote.
func platformFault(t *testing.T, ex *exch) {
	t.Helper()
	r := ex.resps[0]
	if r.status != 502 {
		t.Fatalf("status %d", r.status)
	}
	if got := strings.TrimSpace(string(r.body)); got != "Bad Gateway" {
		t.Errorf("a platform error has a generic body, got %q", trunc(got))
	}
	if !hasHeader(t, r, "X-Edgerun-Invocation-Id") {
		t.Errorf("a failure after routing names its invocation (X-Edgerun-Invocation-Id)")
	}
	if !hasHeader(t, r, "X-Request-Id") {
		t.Errorf("X-Request-Id missing")
	}
}

// headPost is a HEAD of /raw with the guest's output as the request body.
func headPost(out string) string {
	return rawReq("HEAD /raw HTTP/1.1", "Host: "+corpusHost, fmt.Sprintf("Content-Length: %d", len(out))) + out
}

func abi(name, out, spec string) crow {
	return crow{name: name, raw: raw(out), want: []string{"502"}, check: platformFault, spec: spec}
}

func TestCorpusResponse(t *testing.T) {
	e := newCorpusEnv(t)
	bigHdr := strings.Repeat("a", 1<<20)
	binHdr := strings.Repeat("\x80\xff\xfe\x90", 1<<18)
	many := &strings.Builder{}
	for i := 0; i < 2000; i++ {
		fmt.Fprintf(many, "X-H%04d: %d\n", i, i)
	}
	pageSize := 1900 << 10
	bigBody := strings.Repeat("0123456789abcdef", pageSize/16)
	text := func(s string) string { return "Content-Type: text/plain\n" + s }
	rows := []crow{
		// ---- the baseline -------------------------------------------------------------------
		{name: "baseline", raw: raw(text("\nhi")), want: []string{"200"}, check: all(bodyIs("hi"), hdrIs("Content-Type", "text/plain")), spec: specFrame},
		{name: "headers end with CRLF line endings", raw: raw("Content-Type: text/plain\r\nX-A: 1\r\n\r\nbody\r\nmore"), check: all(bodyIs("body\r\nmore"), hdrIs("X-A", "1"))},
		{name: "empty body", raw: raw(text("\n")), want: []string{"200"}, check: bodyIs("")},
		{name: "only the blank line", raw: raw("\n"), want: []string{"200"}, check: bodyIs(""), silent: "no header at all is a valid response (status defaults to 200)"},
		{name: "a leading blank line then body", raw: raw("\n\nbody"), want: []string{"200"}, check: bodyIs("\nbody")},
		{name: "1.9 MiB body arrives intact", raw: raw(text("\n") + bigBody), want: []string{"200"}, check: func(t *testing.T, ex *exch) {
			if sha(string(ex.resps[0].body)) != sha(bigBody) {
				t.Errorf("the body changed on the way (%d bytes)", len(ex.resps[0].body))
			}
		}},
		{name: "no Content-Type: none is invented", raw: raw("\n<html><body>x</body></html>"), want: []string{"200"}, check: hdrIs("Content-Type"),
			spec: "HTTP messages: a response is forwarded as the function wrote it: no Content-Type is invented"},
		{name: "no Content-Type on a binary body", raw: raw("\n\x00\x01\x02\xff\xfe"), want: []string{"200"}, check: hdrIs("Content-Type")},
		{name: "a Date header is added", raw: raw(text("\nx")), check: func(t *testing.T, ex *exch) {
			d := ex.resps[0].header.Get("Date")
			if _, err := http.ParseTime(d); err != nil {
				t.Errorf("Date %q: %v", d, err)
			}
		}, spec: "RFC 9110 6.6.1: an origin server with a clock MUST send Date"},
		{name: "the guest's Date wins", raw: raw(text("Date: Mon, 01 Jan 2001 00:00:00 GMT\n\nx")), check: hdrIs("Date", "Mon, 01 Jan 2001 00:00:00 GMT"), silent: "forwarded as given"},

		// ---- status -----------------------------------------------------------------------------
		{name: "201 with a reason", raw: raw("Status: 201 Created\n\nx"), want: []string{"201"}, check: bodyIs("x")},
		{name: "status without a reason", raw: raw("Status: 404\n\nx"), want: []string{"404"}},
		{name: "status with a long odd reason", raw: raw("Status: 599 this reason is ignored \xff\n\nx"), want: []string{"599"}},
		{name: "Status header name in other case", raw: raw("sTaTuS: 202\n\nx"), want: []string{"202"}},
		{name: "two Status lines: the last wins", raw: raw("Status: 201\nStatus: 202\n\nx"), want: []string{"202"}, silent: "the ABI does not say what repeated Status lines mean; the last wins"},
		{name: "redirect keeps its Location", raw: raw("Status: 302 Found\nLocation: /elsewhere?a=%zz\n\n"), want: []string{"302"}, check: hdrIs("Location", "/elsewhere?a=%zz")},
		{name: "204 with a body drops the body", raw: raw("Status: 204 No Content\nX-A: 1\n\nthis body must not be sent"), want: []string{seq("204", ok("/sentinel", 0))},
			check: all(bodyIs(""), func(t *testing.T, ex *exch) {
				if len(ex.resps[0].te) > 0 || ex.resps[0].header.Get("Content-Length") != "" {
					t.Errorf("a 204 has no framing headers: te=%v CL=%q", ex.resps[0].te, ex.resps[0].header.Get("Content-Length"))
				}
			}), sentinel: true, silent: "docs/protocol.md does not say what happens to a body the guest wrote with a status that forbids one (204, 304): it is dropped silently, and the invocation counts as a success"},
		{name: "304 with a body and a lying Content-Length", raw: raw("Status: 304 Not Modified\nContent-Length: 99\nETag: \"x\"\n\nbody"), want: []string{seq("304", ok("/sentinel", 0))}, check: all(bodyIs(""), hdrIs("Etag", `"x"`)), sentinel: true},
		{name: "205 with a body", raw: raw("Status: 205 Reset Content\n\nbody that RFC 9110 15.3.6 forbids"), want: []string{seq("205", ok("/sentinel", 0))}, sentinel: true,
			check: bodyIs(""), spec: "RFC 9110 15.3.6: a server MUST NOT generate content in a 205"},
		abi("status 100", "Status: 100 Continue\n\nx", specStatus),
		abi("status 101", "Status: 101 Switching Protocols\nUpgrade: websocket\nConnection: Upgrade\n\n", specStatus),
		abi("status 103", "Status: 103 Early Hints\nLink: </a.css>; rel=preload\n\nx", specStatus),
		abi("status 199", "Status: 199\n\nx", specStatus),
		abi("status 600", "Status: 600\n\nx", specStatus),
		abi("status 999", "Status: 999 Nope\n\nx", specStatus),
		abi("status 1000", "Status: 1000\n\nx", specStatus),
		abi("status 0", "Status: 0\n\nx", specStatus),
		abi("status 99", "Status: 99\n\nx", specStatus),
		abi("status negative", "Status: -200\n\nx", specStatus),
		abi("status not a number", "Status: OK\n\nx", specStatus),
		abi("status with trailing letters", "Status: 200abc\n\nx", specStatus),
		abi("status empty", "Status:\n\nx", specStatus),
		abi("status with a tab", "Status: 200\tOK\n\nx", specStatus),
		abi("status huge", "Status: 99999999999999999999\n\nx", specStatus),
		abi("status in Arabic-Indic digits", "Status: \u0662\u0660\u0660\n\nx", specStatus),
		abi("status with an underscore", "Status: 2_00\n\nx", specStatus),
		abi("an HTTP status line instead of a Status header", "HTTP/1.1 200 OK\nContent-Type: text/plain\n\nx", specStatus),
		abi("status 200.5", "Status: 200.5\n\nx", specStatus),
		{name: "status with a plus sign", raw: raw("Status: +200\n\nx"), want: []string{"502"}, check: platformFault, spec: specStatus},
		{name: "status with leading zeros", raw: raw("Status: 0200\n\nx"), want: []string{"502"}, check: platformFault, spec: specStatus},
		{name: "status two numbers is valid (the rest is the reason)", raw: raw("Status: 200 300\n\nx"), want: []string{"200"}},

		// ---- header syntax ----------------------------------------------------------------------
		abi("header name with a space", "X Bad: 1\n\nx", specRespHdr),
		abi("header name with parentheses", "X(Bad): 1\n\nx", specRespHdr),
		abi("header name with non-ASCII", "X-\xc3\xa9: 1\n\nx", specRespHdr),
		abi("header name with a NUL", "X-\x00: 1\n\nx", specRespHdr),
		abi("header name with a slash and quote", "X/\"A: 1\n\nx", specRespHdr),
		abi("empty header name", ": v\n\nx", specRespHdr),
		abi("space before the colon", "X-A : 1\n\nx", specRespHdr),
		abi("leading space on a header line", " X-A: 1\n\nx", specRespHdr),
		abi("a header line without a colon", "X-A\n\nx", specRespHdr),
		abi("obs-fold continuation in a response", "X-A: 1\n 2\n\nx", specRespHdr),
		{name: "a header name made of token punctuation is forwarded", raw: raw("X.Y_Z!#$%&'*+^|~: 1\n\nx"), want: []string{"200"},
			check: func(t *testing.T, ex *exch) {
				if got := headerValues(ex.resps[0], "X.Y_Z!#$%&'*+^|~"); len(got) != 1 || got[0] != "1" {
					t.Errorf("headers %v", ex.resps[0].header)
				}
			}},
		{name: "a name in any case, a value kept", raw: raw("x-MiXeD-cAsE:   padded value  \t\n\nx"), check: hdrIs("X-Mixed-Case", "padded value"), spec: "the guest's value is every byte after the colon, spaces and tabs trimmed"},
		{name: "an empty value", raw: raw("X-E:\n\nx"), check: hdrIs("X-E", "")},
		{name: "a tab inside a value", raw: raw("X-T: a\tb\n\nx"), check: hdrIs("X-T", "a\tb")},
		abi("control 0x01 in a value", "X-A: a\x01b\n\nx", specRespHdr),
		abi("DEL in a value", "X-A: a\x7fb\n\nx", specRespHdr),
		abi("NUL in a value", "X-A: a\x00b\n\nx", specRespHdr),
		abi("vertical tab in a value", "X-A: a\x0bb\n\nx", specRespHdr),
		abi("bare CR inside a value", "X-A: a\rb\n\nx", specRespHdr),
		abi("CR CR LF line ending", "X-A: a\r\r\n\nx", specRespHdr),
		abi("ESC in a value", "X-A: \x1b[0m\n\nx", specRespHdr),
		abi("control character in Content-Length", "Content-Length: 5\x01\n\nx", specRespHdr),
		abi("control character in Set-Cookie", "Set-Cookie: a=b\x03\n\nx", specRespHdr),
		{name: "binary bytes in a value are forwarded verbatim", raw: raw("X-Bin: \xff\xfe\x80\n\nx"), check: hdrIs("X-Bin", "\xff\xfe\x80"), spec: specHdrBytes},
		{name: "latin-1 and NBSP bytes", raw: raw("X-L: caf\xe9\nX-N: \xc2\xa0v\xc2\xa0\n\nx"), check: all(hdrIs("X-L", "caf\xe9"), hdrIs("X-N", "\xc2\xa0v\xc2\xa0")), spec: specHdrBytes},
		{name: "repeated Set-Cookie stay separate, in order", raw: raw("Set-Cookie: a=1; Path=/\nSet-Cookie: b=2; Expires=Wed, 21 Oct 2015 07:28:00 GMT\nSet-Cookie: c=3; HttpOnly\n\nx"),
			check: hdrIs("Set-Cookie", "a=1; Path=/", "b=2; Expires=Wed, 21 Oct 2015 07:28:00 GMT", "c=3; HttpOnly"), spec: "Frontend behaviour: repeated headers are forwarded as given"},
		{name: "repeated headers of any name stay separate and ordered", raw: raw("X-Dup: a\nx-dup: b\nX-DUP: c\n\nx"), check: hdrIs("X-Dup", "a", "b", "c")},
		{name: "a Set-Cookie with a 100 KB value", raw: raw("Set-Cookie: a=" + strings.Repeat("v", 100000) + "\n\nx"), want: []string{"200"},
			check: func(t *testing.T, ex *exch) {
				if v := headerValues(ex.resps[0], "Set-Cookie"); len(v) != 1 || len(v[0]) != 100002 {
					t.Errorf("Set-Cookie came back as %d values", len(v))
				}
			}, silent: "the spec states no limit on a response header's size"},
		{name: "a header value of exactly 1 MiB is forwarded", raw: raw("X-Big: " + bigHdr + "\n\nx"), want: []string{"200"},
			check: func(t *testing.T, ex *exch) {
				if v := headerValues(ex.resps[0], "X-Big"); len(v) != 1 || v[0] != bigHdr {
					t.Errorf("X-Big came back as %d values of %d bytes", len(v), func() int {
						if len(v) > 0 {
							return len(v[0])
						}
						return 0
					}())
				}
			}, silent: "the frontend writes a 1 MiB header value without complaint, which Go's own HTTP client (1 MiB header limit) and most proxies (8-64 KiB) cannot read; the spec bounds neither the size of one response header nor the whole head, so a guest can make an answer that clients cannot consume"},
		{name: "a header value of 1 MiB of non-ASCII bytes", raw: raw("X-Bin: " + binHdr + "\n\nx"), want: []string{"200"},
			check: func(t *testing.T, ex *exch) {
				if v := headerValues(ex.resps[0], "X-Bin"); len(v) != 1 || v[0] != binHdr {
					t.Errorf("X-Bin did not survive")
				}
			}},
		{name: "2000 response headers", raw: raw(many.String() + "\nx"), want: []string{"200"},
			check: func(t *testing.T, ex *exch) {
				if v := headerValues(ex.resps[0], "X-H1999"); len(v) != 1 || v[0] != "1999" {
					t.Errorf("last header: %q", v)
				}
			}, silent: "no limit on header count in the spec"},

		// ---- framing headers are the frontend's own -------------------------------------------------
		{name: "a Content-Length that is too small does not truncate", raw: raw("Content-Length: 5\n\n0123456789"), want: []string{"200"}, check: bodyIs("0123456789"), spec: specFrame},
		{name: "a Content-Length that is too large does not hang", raw: raw("Content-Length: 100\n\nabc"), want: []string{"200"}, check: bodyIs("abc"), spec: specFrame},
		{name: "a Content-Length that is not a number", raw: raw("Content-Length: abc\n\nabc"), want: []string{"200"}, check: bodyIs("abc"), spec: specFrame},
		{name: "a negative Content-Length", raw: raw("Content-Length: -1\n\nabc"), want: []string{"200"}, check: bodyIs("abc"), spec: specFrame},
		{name: "two Content-Length headers", raw: raw("Content-Length: 1\nContent-Length: 2\n\nabc"), want: []string{"200"}, check: bodyIs("abc"), spec: specFrame},
		{name: "a Content-Length of zero on a body", raw: raw("content-length: 0\n\nabc"), want: []string{"200"}, check: bodyIs("abc"), spec: specFrame},
		{name: "Transfer-Encoding: chunked from the guest is dropped", raw: raw("Transfer-Encoding: chunked\n\nplain, not chunked"), sentinel: true, want: []string{seq("200", ok("/sentinel", 0))},
			check: bodyIs("plain, not chunked"), spec: specFrame},
		{name: "Transfer-Encoding: gzip from the guest is dropped", raw: raw("Transfer-Encoding: gzip, chunked\n\nplain"), sentinel: true, want: []string{seq("200", ok("/sentinel", 0))}, check: bodyIs("plain")},
		{name: "Transfer-Encoding: identity from the guest is dropped", raw: raw("Transfer-Encoding: identity\n\nplain"), sentinel: true, want: []string{seq("200", ok("/sentinel", 0))}, check: bodyIs("plain")},
		{name: "Content-Encoding from the guest is forwarded as given", raw: raw("Content-Encoding: gzip\n\nnot gzip"), want: []string{"200"}, check: all(hdrIs("Content-Encoding", "gzip"), bodyIs("not gzip")),
			silent: "the frontend does not check that Content-Encoding describes the body"},
		{name: "Connection: close from the guest cannot close the client's connection", raw: raw("Connection: close\n\nx"), sentinel: true, want: []string{seq("200", ok("/sentinel", 0))},
			check: hdrIs("Connection"), spec: specFrame},
		{name: "Connection: keep-alive and Keep-Alive are dropped", raw: raw("Connection: keep-alive\nKeep-Alive: timeout=0, max=0\n\nx"), sentinel: true, want: []string{seq("200", ok("/sentinel", 0))},
			check: all(hdrIs("Connection"), hdrIs("Keep-Alive"))},
		{name: "Upgrade, Trailer, TE and proxy headers are dropped", raw: raw("Upgrade: websocket\nTrailer: X-T\nTE: trailers\nProxy-Authenticate: Basic\nProxy-Connection: close\nX-Kept: 1\n\nx"), want: []string{"200"},
			check: all(hdrIs("Upgrade"), hdrIs("Trailer"), hdrIs("Te"), hdrIs("Proxy-Authenticate"), hdrIs("Proxy-Connection"), hdrIs("X-Kept", "1")), spec: specFrame},
		{name: "Connection naming another header does not hide it", raw: raw("Connection: X-Hide\nX-Hide: v\n\nx"), check: hdrIs("X-Hide", "v"), silent: "the Connection header is dropped without acting on its token list (RFC 9110 7.6.1 asks an intermediary to remove the named headers)"},
		{name: "the guest cannot spoof X-Request-Id", raw: rawReq("POST /raw HTTP/1.1", "Host: "+corpusHost, "X-Request-Id: real-id", "Content-Length: 31") + "X-Request-Id: spoof\n\nbody-goes-here-and-then-some",
			check: hdrIs("X-Request-Id", "real-id"), spec: "Frontend behaviour: X-Request-Id echoed"},
		{name: "the guest cannot spoof X-Edgerun-Invocation-Id", raw: raw("X-Edgerun-Invocation-Id: spoof\n\nx"), check: func(t *testing.T, ex *exch) {
			if v := headerValues(ex.resps[0], "X-Edgerun-Invocation-Id"); len(v) != 1 || v[0] == "spoof" {
				t.Errorf("X-Edgerun-Invocation-Id = %q", v)
			}
		}, spec: "Frontend behaviour: the invocation is named by the platform"},
		{name: "the guest's own x-forwarded headers are not special on the way out", raw: raw("X-Forwarded-For: 1.2.3.4\n\nx"), check: hdrIs("X-Forwarded-For", "1.2.3.4")},

		// ---- HEAD and a guest that writes a body -----------------------------------------------------
		{name: "HEAD: the guest's body is not sent", raw: headPost("Content-Type: text/plain\n\nbody body body"), sentinel: true, heads: []int{0},
			want: []string{seq("200", ok("/sentinel", 0))}, check: hdrIs("Content-Type", "text/plain")},
		{name: "HEAD of a 204 with a body", raw: headPost("Status: 204\n\nthis is not sent..."), sentinel: true, heads: []int{0}, want: []string{seq("204", ok("/sentinel", 0))}},

		// ---- not a response at all ------------------------------------------------------------------------
		abi("no output", "", "Guest ABI: no output is ABI_MISMATCH"),
		abi("headers never terminated", "Content-Type: text/plain", "Guest ABI: headers not ended by a blank line are ABI_MISMATCH"),
		abi("headers terminated by EOF after a newline", "Content-Type: text/plain\n", "Guest ABI"),
		abi("a body without a blank line", "hello world", "Guest ABI"),
		abi("only whitespace", "   ", "Guest ABI"),
	}
	runRows(t, e, rows)
	_ = bytes.MinRead
	_ = time.Second
}
