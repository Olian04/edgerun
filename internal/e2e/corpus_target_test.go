package e2e

import (
	"strings"
	"testing"
)

const (
	specPath   = "docs/protocol.md \"HTTP messages\": a path is percent-decoded once, is valid UTF-8 without control characters, else 400"
	specTarget = "docs/protocol.md \"HTTP messages\": a request target is printable ASCII, else 400 and not forwarded"
	specKept   = "docs/protocol.md \"Frontend behaviour\": dot segments and repeated slashes are kept, nothing is cleaned"
)

// selfAnswered asserts the frontend answered by itself (2xx or 4xx, no invocation
// behind it): docs/protocol.md "Frontend behaviour", for a target that is not a path.
func selfAnswered(t *testing.T, ex *exch) {
	t.Helper()
	if len(ex.resps) != 1 {
		t.Fatalf("want one answer, got %s", ex.sig())
	}
	r := ex.resps[0]
	if !(r.status/100 == 2 || r.status/100 == 4) || r.dump != nil || hasHeader(t, r, "X-Edgerun-Invocation-Id") {
		t.Errorf("the frontend must answer such a request itself with a 2xx or 4xx, never forward it: %s", ex.sig())
	}
}

func TestCorpusTarget(t *testing.T) {
	e := newCorpusEnv(t)
	long := func(n int) string { return strings.Repeat("a", n) }
	rows := []crow{
		// ---- origin form -----------------------------------------------------------
		{name: "origin form with a query", raw: cget("/a/b?x=1&y=%20"), want: []string{ok("/a/b", 0)},
			guest: g("PATH_INFO", "/a/b", "QUERY_STRING", "x=1&y=%20", "REQUEST_METHOD", "GET", "WHO", "exact"), spec: "Frontend behaviour: raw_query as received, still encoded"},
		{name: "root", raw: cget("/"), want: []string{ok("/", 0)}, guest: g("PATH_INFO", "/", "QUERY_STRING", "")},
		{name: "query only", raw: cget("/?a=1"), guest: g("PATH_INFO", "/", "QUERY_STRING", "a=1")},
		{name: "empty query", raw: cget("/a?"), guest: g("PATH_INFO", "/a", "QUERY_STRING", "")},
		{name: "question mark inside the query", raw: cget("/a?b?c"), guest: g("PATH_INFO", "/a", "QUERY_STRING", "b?c")},
		{name: "query keeps what is not text-safe, undecoded", raw: cget("/a?q=%00%ff%zz%"), want: []string{ok("/a", 0)},
			guest: g("PATH_INFO", "/a", "QUERY_STRING", "q=%00%ff%zz%"), spec: "a query is raw: not decoded, so %00 and a lone % in it are just text"},
		{name: "query with a plus and an ampersand soup", raw: cget("/a?a+b&&=;c=%2B"), guest: g("QUERY_STRING", "a+b&&=;c=%2B")},
		{name: "literal fragment in the target", raw: cget("/a#frag"), guest: g("PATH_INFO", "/a#frag"),
			silent: "an unencoded # is not legal in a request-target (RFC 9112 3.2.1) and the spec does not say refuse it; Go keeps it in the path"},
		{name: "literal fragment after a query", raw: cget("/a?b#c"), guest: g("PATH_INFO", "/a", "QUERY_STRING", "b#c"),
			silent: "# stays in raw_query"},
		{name: "backslash and other printable specials in the path", raw: cget(`/a\b|c"d<e>f{g}h^i`), guest: g("PATH_INFO", `/a\b|c"d<e>f{g}h^i`),
			silent: "RFC 3986 forbids these unescaped; the spec only asks for printable ASCII, which they are"},
		{name: "semicolon parameters are not special", raw: cget("/a;b=c/d;e?f;g"), guest: g("PATH_INFO", "/a;b=c/d;e", "QUERY_STRING", "f;g")},
		{name: "8000 byte path", raw: cget("/" + long(8000)), guest: g("PATH_INFO", "/"+long(8000))},
		{name: "8000 byte query", raw: cget("/?q=" + long(8000)), guest: g("QUERY_STRING", "q="+long(8000))},
		{name: "target of 70000 bytes exceeds the header limit", raw: cget("/" + long(70000)), want: []string{"431"},
			silent: "the spec states no maximum for a request target or the header block; Go's MaxHeaderBytes (64 KiB) answers 431"},

		// ---- percent-encoding: decoded once -------------------------------------------
		{name: "%2F is a slash after decoding", raw: cget("/a%2Fb"), guest: g("PATH_INFO", "/a/b"), spec: specPath + " (\"/a%2Fb arrives as /a/b\")"},
		{name: "%25 is decoded once and not again", raw: cget("/a%252Fb"), guest: g("PATH_INFO", "/a%2Fb"), spec: "a % in a path is a %"},
		{name: "%2525 is %25", raw: cget("/%2525"), guest: g("PATH_INFO", "/%25")},
		{name: "%2520 is %20 and not a space", raw: cget("/a%2520b"), guest: g("PATH_INFO", "/a%20b")},
		{name: "%20 is a space (text) and is allowed", raw: cget("/a%20b"), guest: g("PATH_INFO", "/a b")},
		{name: "plus is not a space in a path", raw: cget("/a+b"), guest: g("PATH_INFO", "/a+b")},
		{name: "unreserved characters encoded", raw: cget("/%41%42%7e"), guest: g("PATH_INFO", "/AB~")},
		{name: "encoded ? stays in the path", raw: cget("/a%3Fb%3Dc?d=e"), guest: g("PATH_INFO", "/a?b=c", "QUERY_STRING", "d=e"),
			spec: "decoded once: an encoded ? is not distinguishable from a real one only if it is in the query; here it must be path"},
		{name: "encoded # stays in the path", raw: cget("/a%23b"), guest: g("PATH_INFO", "/a#b")},
		{name: "valid two-byte UTF-8", raw: cget("/caf%c3%a9"), guest: g("PATH_INFO", "/café")},
		{name: "valid four-byte UTF-8", raw: cget("/%f0%9f%98%80"), guest: g("PATH_INFO", "/\U0001F600")},
		{name: "C1 control U+0085 is not a control character for the spec", raw: cget("/a%c2%85b"), guest: g("PATH_INFO", "/a\u0085b"),
			spec:   specPath + ": controls are U+0000-U+001F and U+007F only",
			silent: "U+0085, U+2028, bidi overrides and BOM are text by the letter of the spec"},
		{name: "U+2028 line separator", raw: cget("/a%e2%80%a8b"), guest: g("PATH_INFO", "/a b")},
		{name: "U+202E right-to-left override", raw: cget("/a%e2%80%aeb"), guest: g("PATH_INFO", "/a‮b")},
		{name: "noncharacter U+FFFE", raw: cget("/a%ef%bf%beb"), guest: g("PATH_INFO", "/a￾b")},
		{name: "lone percent", raw: cget("/a%"), want: []string{"400"}, spec: specPath},
		{name: "percent with one digit", raw: cget("/a%2"), want: []string{"400"}, spec: specPath},
		{name: "percent zz", raw: cget("/a%zz"), want: []string{"400"}, spec: specPath},
		{name: "IIS-style %u0041", raw: cget("/%u0041"), want: []string{"400"}, spec: specPath},
		{name: "percent percent", raw: cget("/%%41"), want: []string{"400"}, spec: specPath},
		{name: "encoded NUL", raw: cget("/%00"), want: []string{"400"}, spec: specPath},
		{name: "encoded NUL inside", raw: cget("/a%00b"), want: []string{"400"}, spec: specPath},
		{name: "encoded LF", raw: cget("/a%0Ab"), want: []string{"400"}, spec: specPath},
		{name: "encoded CR LF", raw: cget("/a%0d%0ab"), want: []string{"400"}, spec: specPath},
		{name: "encoded tab", raw: cget("/a%09b"), want: []string{"400"}, spec: specPath},
		{name: "encoded US 0x1f", raw: cget("/a%1fb"), want: []string{"400"}, spec: specPath},
		{name: "encoded DEL", raw: cget("/a%7fb"), want: []string{"400"}, spec: specPath},
		{name: "encoded 0xff", raw: cget("/%ff"), want: []string{"400"}, spec: specPath},
		{name: "overlong slash %c0%af", raw: cget("/%c0%af"), want: []string{"400"}, spec: specPath},
		{name: "overlong NUL %c0%80", raw: cget("/%c0%80"), want: []string{"400"}, spec: specPath},
		{name: "overlong three-byte slash", raw: cget("/%e0%80%af"), want: []string{"400"}, spec: specPath},
		{name: "overlong dot %c0%ae", raw: cget("/%c0%ae%c0%ae/"), want: []string{"400"}, spec: specPath},
		{name: "encoded UTF-16 surrogate", raw: cget("/%ed%a0%80"), want: []string{"400"}, spec: specPath},
		{name: "above U+10FFFF", raw: cget("/%f4%90%80%80"), want: []string{"400"}, spec: specPath},
		{name: "truncated multibyte", raw: cget("/%c3"), want: []string{"400"}, spec: specPath},
		{name: "lone continuation byte", raw: cget("/%80"), want: []string{"400"}, spec: specPath},

		// ---- raw bytes in the target ---------------------------------------------------
		{name: "raw UTF-8 in the path", raw: cget("/café"), want: []string{"400"}, spec: specTarget},
		{name: "raw 0xff in the path", raw: cget("/a\xffb"), want: []string{"400"}, spec: specTarget},
		{name: "raw NUL in the path", raw: cget("/a\x00b"), want: []string{"400"}, spec: specTarget},
		{name: "raw DEL in the path", raw: cget("/a\x7fb"), want: []string{"400"}, spec: specTarget},
		{name: "raw tab in the path", raw: cget("/a\tb"), want: []string{"400"}, spec: specTarget},
		{name: "raw space in the path", raw: cget("/a b"), want: []string{"400"}, spec: specTarget},
		{name: "raw UTF-8 in the query", raw: cget("/a?q=é"), want: []string{"400"}, spec: specTarget},
		{name: "raw NUL in the query", raw: cget("/a?q=\x00"), want: []string{"400"}, spec: specTarget},
		{name: "raw control in the query", raw: cget("/a?q=\x01"), want: []string{"400"}, spec: specTarget},
		{name: "raw space in the query", raw: cget("/a?q=a b"), want: []string{"400"}, spec: specTarget},

		// ---- dot segments and slashes are kept -------------------------------------------
		{name: "single dot segment", raw: cget("/a/./b"), guest: g("PATH_INFO", "/a/./b"), spec: specKept},
		{name: "double dot segment", raw: cget("/a/../b"), guest: g("PATH_INFO", "/a/../b"), spec: specKept},
		{name: "dot segments above the root", raw: cget("/../.."), guest: g("PATH_INFO", "/../.."), spec: specKept},
		{name: "trailing dot segment", raw: cget("/a/b/.."), guest: g("PATH_INFO", "/a/b/.."), spec: specKept},
		{name: "encoded dots", raw: cget("/a/%2e%2e/b"), guest: g("PATH_INFO", "/a/../b"), spec: specKept + "; decoded once, then not cleaned"},
		{name: "encoded dot and slash", raw: cget("/a/..%2fb"), guest: g("PATH_INFO", "/a/../b"), spec: specKept},
		{name: "mixed-case encoded dots", raw: cget("/%2E%2e/%2E"), guest: g("PATH_INFO", "/../.")},
		{name: "repeated slashes", raw: cget("//a//b"), guest: g("PATH_INFO", "//a//b"), spec: specKept},
		{name: "path of only slashes", raw: cget("///"), guest: g("PATH_INFO", "///"), spec: specKept},
		{name: "double slash then a host-looking segment", raw: cget("//evil.test/x"), guest: g("PATH_INFO", "//evil.test/x", "SERVER_NAME", corpusHost, "WHO", "exact"),
			spec: "a request target starting // is a path in origin-form, never an authority"},
		{name: "trailing slash", raw: cget("/a/"), guest: g("PATH_INFO", "/a/")},
		{name: "encoded slashes only", raw: cget("/%2f%2f"), guest: g("PATH_INFO", "///")},

		// ---- absolute form ---------------------------------------------------------------
		{name: "absolute form", raw: rawReq("GET http://corpus.test/a?b HTTP/1.1", "Host: corpus.test"),
			guest: g("PATH_INFO", "/a", "QUERY_STRING", "b", "SERVER_NAME", "corpus.test", "WHO", "exact"), spec: "RFC 9112 3.2.2"},
		{name: "absolute form wins over a disagreeing Host header", raw: rawReq("GET http://corpus.test/a HTTP/1.1", "Host: evil.test"),
			guest:  g("SERVER_NAME", "corpus.test", "WHO", "exact", "HTTP_X_FORWARDED_HOST", "corpus.test"),
			silent: "RFC 9112 3.2.2 says the authority of an absolute-form target replaces Host; docs/protocol.md says `host` is \"the Host header as it arrived\" and does not mention absolute form. Go follows the RFC, so the host in HttpRequest is NOT the Host header."},
		{name: "absolute form to another host, Host header names ours", raw: rawReq("GET http://evil.test/a HTTP/1.1", "Host: corpus.test"),
			guest:  g("SERVER_NAME", "evil.test", "WHO", "any", "HTTP_X_FORWARDED_HOST", "evil.test"),
			silent: "routes by the absolute-form authority, ignoring the Host header; same gap as the row above"},
		{name: "absolute form scheme in upper case", raw: rawReq("GET HTTP://CORPUS.TEST/a HTTP/1.1", "Host: corpus.test"),
			guest: g("PATH_INFO", "/a", "SERVER_NAME", "CORPUS.TEST", "WHO", "exact")},
		{name: "absolute form https does not change x-forwarded-proto", raw: rawReq("GET https://corpus.test/a HTTP/1.1", "Host: corpus.test"),
			guest:  g("HTTP_X_FORWARDED_PROTO", "http", "PATH_INFO", "/a"),
			silent: "the scheme of an absolute-form target is ignored; x-forwarded-proto reflects the connection (spec: \"Who the client is comes from the connection\")"},
		{name: "absolute form with userinfo", raw: rawReq("GET http://user:pw@corpus.test/a HTTP/1.1", "Host: corpus.test"),
			guest:  g("SERVER_NAME", "corpus.test", "PATH_INFO", "/a", "-HTTP_AUTHORIZATION"),
			silent: "userinfo of the absolute form is dropped silently (not turned into Authorization)"},
		{name: "absolute form with a port", raw: rawReq("GET http://corpus.test:80/a HTTP/1.1", "Host: corpus.test"), guest: g("SERVER_NAME", "corpus.test:80", "WHO", "exact")},
		{name: "absolute form with an unknown scheme", raw: rawReq("GET ftp://corpus.test/a HTTP/1.1", "Host: corpus.test"), guest: g("PATH_INFO", "/a"),
			silent: "any scheme is accepted"},
		{name: "absolute form without a Host header", raw: rawReq("GET http://corpus.test/a HTTP/1.1"), want: []string{"400"},
			silent: "RFC 9112 3.2 requires Host in HTTP/1.1 even with an absolute form; the spec is silent"},
		{name: "absolute form with empty authority", raw: rawReq("GET http:///a HTTP/1.1", "Host: corpus.test"), want: []string{ok("/a", 0)},
			guest: g("PATH_INFO", "/a"), silent: "empty authority is accepted (Go): see which host the guest sees in SERVER_NAME"},
		{name: "absolute form without a path", raw: rawReq("GET http://corpus.test HTTP/1.1", "Host: corpus.test"), check: selfAnswered,
			silent: "RFC 9110 4.2.3 would read an empty absolute path as /; the spec lets the frontend answer 2xx/4xx itself, and it answers 400"},
		{name: "absolute form without a path but with a query", raw: rawReq("GET http://corpus.test?x=1 HTTP/1.1", "Host: corpus.test"), check: selfAnswered},

		// ---- asterisk and authority form ---------------------------------------------------
		{name: "OPTIONS *", raw: rawReq("OPTIONS * HTTP/1.1", "Host: corpus.test"), check: selfAnswered, spec: "Frontend behaviour: a target that is not a path starting with / is answered by the frontend, 2xx or 4xx"},
		{name: "GET *", raw: rawReq("GET * HTTP/1.1", "Host: corpus.test"), check: selfAnswered, spec: "same"},
		{name: "OPTIONS * HTTP/1.0 without Host", raw: rawReq("OPTIONS * HTTP/1.0"), check: selfAnswered, spec: "same"},
		{name: "CONNECT authority form", raw: rawReq("CONNECT corpus.test:443 HTTP/1.1", "Host: corpus.test:443"), check: selfAnswered, spec: "same"},
		{name: "CONNECT authority form without Host", raw: rawReq("CONNECT corpus.test:443 HTTP/1.1"), check: selfAnswered, spec: "same"},
		{name: "GET with an authority as target", raw: rawReq("GET corpus.test:443 HTTP/1.1", "Host: corpus.test"), check: selfAnswered, spec: "same"},
		{name: "CONNECT with an origin-form target is forwarded", raw: withHost("CONNECT", "/dump"),
			check: func(t *testing.T, ex *exch) {
				if len(ex.resps) != 1 {
					t.Fatalf("%s", ex.sig())
				}
				r := ex.resps[0]
				if r.status == 200 && (len(r.te) > 0 || r.header.Get("Content-Length") != "") {
					t.Errorf("RFC 9112 6.3 / 9110 8.6: a 2xx answer to CONNECT turns the connection into a tunnel and carries neither Content-Length nor Transfer-Encoding; got te=%v CL=%q", r.te, r.header.Get("Content-Length"))
				}
			},
			defect: "CORPUS-CONNECT: a function can answer CONNECT /x with 200; the frontend frames it with chunked (RFC 9110 9.3.6 turns the connection into a tunnel after the head, and a client that honours that reads chunk framing as tunnel data)",
			silent: "the spec never says what to do with CONNECT/TRACE and other methods; an origin-form CONNECT is malformed per RFC 9112 3.2.3 but is forwarded"},

		// ---- request line --------------------------------------------------------------------
		{name: "HTTP/1.2 is accepted as 1.x", raw: rawReq("GET /a HTTP/1.2", "Host: corpus.test"), guest: g("SERVER_PROTOCOL", "HTTP/1.1", "PATH_INFO", "/a"),
			silent: "RFC 9112 2.3: a 1.x recipient treats 1.2 as 1.1; the ABI always says SERVER_PROTOCOL=HTTP/1.1"},
		{name: "HTTP/2.0 on the HTTP/1 port", raw: rawReq("GET /a HTTP/2.0", "Host: corpus.test"), want: []string{"505"}, silent: "RFC 9112 2.3: 505"},
		{name: "HTTP/0.9 style request line", raw: "GET /a\r\n\r\n", want: []string{"400"}, silent: "no HTTP/0.9"},
		{name: "lower-case protocol name", raw: rawReq("GET /a http/1.1", "Host: corpus.test"), want: []string{"400"}},
		{name: "double space in the request line", raw: rawReq("GET  /a HTTP/1.1", "Host: corpus.test"), want: []string{"400"}},
		{name: "trailing space in the request line", raw: rawReq("GET /a HTTP/1.1 ", "Host: corpus.test"), want: []string{"400"}},
		{name: "request line with a tab separator", raw: rawReq("GET\t/a\tHTTP/1.1", "Host: corpus.test"), want: []string{"400"}},
		{name: "leading empty line before the request line", raw: "\r\n" + cget("/a"), want: []string{"400"},
			silent: "RFC 9112 2.2: a server SHOULD ignore at least one empty line before the request line; Go answers 400 (a request that arrives after a stray CRLF, as some old clients send after a POST body, is refused)"},
		{name: "NUL in the request line", raw: "GET /a\x00 HTTP/1.1\r\nHost: corpus.test\r\n\r\n", want: []string{"400"}},
	}
	runRows(t, e, rows)
}
