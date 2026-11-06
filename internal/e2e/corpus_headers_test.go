package e2e

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

const (
	specHdrBytes = "docs/protocol.md \"HTTP messages\": a header value is bytes, forwarded exactly as it arrived, optional whitespace (SP, HTAB) removed; no control character other than tab"
	specHdrName  = "docs/protocol.md \"HTTP messages\": a header name is an RFC 9110 token"
	specJoin     = "docs/protocol.md \"Guest ABI wagi-1\": HTTP_<NAME> repeated headers joined with \", \""
	specFwd      = "docs/protocol.md \"Frontend behaviour\": client x-forwarded-*, forwarded, x-real-ip are discarded; the frontend sets x-forwarded-for/proto/host itself"
	specHop      = "docs/protocol.md \"Frontend behaviour\": connection-level headers are dropped"
	specReqID    = "docs/protocol.md \"Frontend behaviour\": request_id is the client's X-Request-Id if printable ASCII of at most 128 characters, otherwise fresh; echoed in every response"
)

func hdrReq(hdr ...string) string { return cget("/h", hdr...) }

var traceparentRe = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

// guestTraceparent is the traceparent the guest was given, "" if none.
func guestTraceparent(t *testing.T, ex *exch) string {
	t.Helper()
	f := ex.first()
	if f == nil {
		t.Fatalf("not forwarded: %s", ex.sig())
	}
	v, _ := f.dump.get("HTTP_TRACEPARENT")
	return v
}

func continuesTrace(trace string) func(*testing.T, *exch) {
	return func(t *testing.T, ex *exch) {
		t.Helper()
		v := guestTraceparent(t, ex)
		parts := strings.Split(v, "-")
		if len(parts) != 4 || parts[1] != trace {
			t.Errorf("the guest's traceparent %q must carry the client's trace id %s", v, trace)
		}
		if vs := ex.first().dump.all("HTTP_BAGGAGE"); len(vs) > 0 {
			t.Errorf("the guest must not see the client's baggage: %q", vs)
		}
	}
}

func ignoresTraceparent(bad string) func(*testing.T, *exch) {
	return func(t *testing.T, ex *exch) {
		t.Helper()
		v := guestTraceparent(t, ex)
		if v != "" && (v == bad || !traceparentRe.MatchString(v)) {
			t.Errorf("a malformed client traceparent %q must not reach the guest; it saw %q", bad, v)
		}
	}
}

func requestIDEchoed(want string) func(*testing.T, *exch) {
	return func(t *testing.T, ex *exch) {
		t.Helper()
		if len(ex.resps) < 1 {
			t.Fatalf("no answer")
		}
		if got := ex.resps[0].header.Get("X-Request-Id"); got != want {
			t.Errorf("X-Request-Id = %q, want %q", got, want)
		}
	}
}

func requestIDPresent(t *testing.T, ex *exch) {
	t.Helper()
	if len(ex.resps) < 1 || ex.resps[0].header.Get("X-Request-Id") == "" {
		t.Errorf("no X-Request-Id on the answer (docs/protocol.md: every response carries one; a fresh one is fine for a refusal made before application code runs)")
	}
}

func requestIDReplaced(sent string) func(*testing.T, *exch) {
	return func(t *testing.T, ex *exch) {
		t.Helper()
		if len(ex.resps) < 1 {
			t.Fatalf("no answer")
		}
		got := ex.resps[0].header.Get("X-Request-Id")
		if got == "" || got == sent {
			t.Errorf("X-Request-Id = %q: the client's %q is not acceptable and a fresh id was expected", got, sent)
		}
	}
}

// noCredentials: no platform token may reach a guest.
func noCredentials(t *testing.T, ex *exch) {
	t.Helper()
	f := ex.first()
	if f == nil {
		t.Fatalf("not forwarded: %s", ex.sig())
	}
	for _, e := range f.dump.env {
		for _, tok := range []string{ingressToken, workerToken, adminToken, executionToken, networkToken} {
			if strings.Contains(e, tok) {
				t.Errorf("a platform credential reached the guest: %q", e)
			}
		}
	}
}

func TestCorpusHeaders(t *testing.T) {
	e := newCorpusEnv(t)
	manyHeaders := func(n int) []string {
		var h []string
		for i := 0; i < n; i++ {
			h = append(h, fmt.Sprintf("h%05d: %d", i, i))
		}
		return h
	}
	const tid, sid = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	_ = sid
	validTP := "00-" + tid + "-" + sid + "-01"
	rows := []crow{
		// ---- value syntax --------------------------------------------------------------
		{name: "plain value", raw: hdrReq("X-A: 1"), guest: g("HTTP_X_A", "1")},
		{name: "optional whitespace is trimmed", raw: hdrReq("X-A:   v \t "), guest: g("HTTP_X_A", "v"), spec: specHdrBytes},
		{name: "leading tab is trimmed", raw: hdrReq("X-A:\tv"), guest: g("HTTP_X_A", "v"), spec: specHdrBytes},
		{name: "inner tab and spaces are kept", raw: hdrReq("X-A: a\tb  c"), guest: g("HTTP_X_A", "a\tb  c"), spec: specHdrBytes},
		{name: "empty value", raw: hdrReq("X-A:"), guest: g("HTTP_X_A", ""), spec: "an empty value is a value"},
		{name: "repeated header is joined with comma space", raw: hdrReq("X-A: 1", "X-A: 2"), guest: g("HTTP_X_A", "1, 2"), spec: specJoin},
		{name: "comma list and repeats", raw: hdrReq("X-A: 1, 2", "X-B: b", "X-A: 3"), guest: g("HTTP_X_A", "1, 2, 3", "HTTP_X_B", "b"), spec: specJoin},
		{name: "names are case-insensitive for joining", raw: hdrReq("x-a: 1", "X-A: 2", "X-a: 3"), guest: g("HTTP_X_A", "1, 2, 3"), spec: specJoin},
		{name: "empty values are joined too", raw: hdrReq("X-A:", "X-A: 2"), guest: g("HTTP_X_A", ", 2"), spec: specJoin,
			silent: "an empty repeat leaves a leading \", \" in the joined variable"},
		{name: "latin-1 byte in a value", raw: hdrReq("X-A: caf\xe9"), guest: g("HTTP_X_A", "caf\xe9"), spec: specHdrBytes},
		{name: "invalid UTF-8 value", raw: hdrReq("X-A: \xff\xfe\x80"), guest: g("HTTP_X_A", "\xff\xfe\x80"), spec: specHdrBytes},
		{name: "UTF-8 value", raw: hdrReq("X-A: caf\u00e9 \U0001F600"), guest: g("HTTP_X_A", "caf\u00e9 \U0001F600"), spec: specHdrBytes},
		{name: "NBSP is not optional whitespace", raw: hdrReq("X-A: \xc2\xa0v\xc2\xa0"), guest: g("HTTP_X_A", "\xc2\xa0v\xc2\xa0"), spec: specHdrBytes},
		{name: "NUL in a value", raw: hdrReq("X-A: a\x00b"), want: []string{"400"}, spec: specHdrBytes},
		{name: "control 0x01 in a value", raw: hdrReq("X-A: a\x01b"), want: []string{"400"}, spec: specHdrBytes},
		{name: "vertical tab in a value", raw: hdrReq("X-A: a\x0bb"), want: []string{"400"}, spec: specHdrBytes},
		{name: "form feed in a value", raw: hdrReq("X-A: a\x0cb"), want: []string{"400"}, spec: specHdrBytes},
		{name: "DEL in a value", raw: hdrReq("X-A: a\x7fb"), want: []string{"400"}, spec: specHdrBytes},
		{name: "bare CR in a value", raw: hdrReq("X-A: a\rb"), want: []string{"400"}, spec: specHdrBytes},
		{name: "ESC in a value", raw: hdrReq("X-A: \x1b[31mred"), want: []string{"400"}, spec: specHdrBytes},

		// ---- line syntax ---------------------------------------------------------------
		{name: "obs-fold is rejected or unfolded", raw: hdrReq("X-A: 1", " 2"),
			check: func(t *testing.T, ex *exch) {
				if ex.resps[0].status == 400 {
					return
				}
				v, _ := ex.first().dump.get("HTTP_X_A")
				if strings.ContainsAny(v, "\r\n") || !strings.HasPrefix(v, "1") {
					t.Errorf("RFC 9112 5.2: reject, or replace the fold with SP; the guest saw %q", v)
				}
			}, silent: "docs/protocol.md does not mention obs-fold; RFC 9112 5.2 lets the server reject it or replace it with SP"},
		{name: "obs-fold with a tab", raw: hdrReq("X-A: 1", "\t2"), want: []string{ok("/h", 0), "400"}},
		{name: "obs-fold that tries to hide a header", raw: hdrReq("X-A: 1", " X-Injected: 2"),
			check: func(t *testing.T, ex *exch) {
				if ex.resps[0].status == 200 && len(ex.first().dump.all("HTTP_X_INJECTED")) > 0 {
					t.Errorf("a folded line must not become a header of its own")
				}
			}},
		{name: "whitespace before the first header", raw: "GET /h HTTP/1.1\r\n X: y\r\nHost: corpus.test\r\n\r\n",
			check: func(t *testing.T, ex *exch) {
				if ex.resps[0].status == 200 && len(ex.first().dump.all("HTTP_X")) > 0 {
					t.Errorf("RFC 9112 2.2: whitespace between the start-line and the first header must be rejected or consumed without processing")
				}
			}, silent: "RFC 9112 2.2: reject, or consume without processing; the spec is silent"},
		{name: "bare LF line endings", raw: "GET /h HTTP/1.1\nHost: corpus.test\nX-A: 1\n\n", want: []string{ok("/h", 0)}, guest: g("HTTP_X_A", "1"),
			silent: "RFC 9112 2.2 permits a recipient to accept bare LF; the spec is silent; accepted"},
		{name: "bare LF inside the header block alone", raw: "GET /h HTTP/1.1\r\nHost: corpus.test\nX-A: 1\r\n\r\n", guest: g("HTTP_X_A", "1")},
		{name: "bare CR as the line ending", raw: "GET /h HTTP/1.1\rHost: corpus.test\r\r", closeWrite: true, want: []string{"400"}},
		{name: "space before the colon", raw: hdrReq("X-A : 1"), want: []string{"400"}, spec: "RFC 9112 5.1: MUST reject"},
		{name: "tab before the colon", raw: hdrReq("X-A\t: 1"), want: []string{"400"}, spec: "RFC 9112 5.1: MUST reject"},
		{name: "empty header name", raw: hdrReq(": v"), want: []string{"400"}, spec: specHdrName},
		{name: "no colon", raw: hdrReq("X-A"), want: []string{"400"}},
		{name: "space inside a name", raw: hdrReq("X A: 1"), want: []string{"400"}, spec: specHdrName},
		{name: "parenthesis in a name", raw: hdrReq("X(A): 1"), want: []string{"400"}, spec: specHdrName},
		{name: "slash and at in a name", raw: hdrReq("X/A@B: 1"), want: []string{"400"}, spec: specHdrName},
		{name: "non-ASCII in a name", raw: hdrReq("X-\u00e9: 1"), want: []string{"400"}, spec: specHdrName},
		{name: "NUL in a name", raw: hdrReq("X-\x00A: 1"), want: []string{"400"}, spec: specHdrName},
		{name: "name that is a digit", raw: hdrReq("1: x"), guest: g("HTTP_1", "x")},
		{name: "name with token punctuation", raw: hdrReq("X.A!#$%&'*+^`|~: 1"), guest: g("HTTP_X.A!#$%&'*+^`|~", "1"), spec: specHdrName,
			silent: "any token is a legal name, so the guest's environment can hold variable names with punctuation, because the ABI uppercases and maps only -"},
		{name: "4000 byte header name", raw: hdrReq(strings.Repeat("a", 4000) + ": 1"), guest: g("HTTP_"+strings.ToUpper(strings.Repeat("a", 4000)), "1")},

		// ---- sizes ---------------------------------------------------------------------
		{name: "8 KiB value", raw: hdrReq("X-A: " + strings.Repeat("v", 8<<10)), guest: g("HTTP_X_A", strings.Repeat("v", 8<<10))},
		{name: "60000 byte value", raw: hdrReq("X-A: " + strings.Repeat("v", 60000)), want: []string{ok("/h", 0)},
			silent: "the spec states no limit on the header block; the frontend allows up to 64 KiB"},
		{name: "70000 byte value", raw: hdrReq("X-A: " + strings.Repeat("v", 70000)), want: []string{"431"},
			silent: "431 comes from the HTTP library; the spec names no status or limit"},
		{name: "200 small headers", raw: hdrReq(manyHeaders(200)...), guest: g("HTTP_H00000", "0", "HTTP_H00199", "199")},
		{name: "3000 small headers", raw: hdrReq(manyHeaders(3000)...), want: []string{"431"},
			silent: "3000 headers of 11 bytes (33 KB) are refused with 431: the HTTP library charges each header 200 bytes of overhead against its 64 KiB budget, so the real limit is about 300 headers; the spec states no limit on count or size"},
		{name: "10000 headers", raw: hdrReq(manyHeaders(10000)...), want: []string{"431"}},
		{name: "200 repeats of one header", raw: hdrReq(strings.Split(strings.TrimSuffix(strings.Repeat("X-A: 1\n", 200), "\n"), "\n")...),
			guest: g("HTTP_X_A", strings.TrimSuffix(strings.Repeat("1, ", 200), ", ")), spec: specJoin},
		{name: "5000 repeats of one header", raw: hdrReq(strings.Split(strings.TrimSuffix(strings.Repeat("X-A: 1\n", 5000), "\n"), "\n")...), want: []string{"431"}},

		// ---- hop-by-hop ----------------------------------------------------------------
		{name: "connection-level headers are dropped", raw: hdrReq("Connection: keep-alive", "Keep-Alive: timeout=5", "Proxy-Authorization: Basic eDp5", "Proxy-Connection: keep-alive",
			"TE: trailers", "Trailer: X-T", "Upgrade: foo", "Proxy-Authenticate: Basic", "Transfer-Encoding: identity"),
			want: []string{"400", "501", ok("/h", 0)},
			check: func(t *testing.T, ex *exch) {
				if ex.resps[0].status != 200 {
					return
				}
				for _, n := range []string{"CONNECTION", "KEEP_ALIVE", "PROXY_AUTHORIZATION", "PROXY_CONNECTION", "TE", "TRAILER", "UPGRADE", "PROXY_AUTHENTICATE", "TRANSFER_ENCODING"} {
					if vs := ex.first().dump.all("HTTP_" + n); len(vs) > 0 {
						t.Errorf("HTTP_%s reached the guest: %q", n, vs)
					}
				}
			}, spec: specHop, silent: "Transfer-Encoding: identity is accepted or refused, depending on the HTTP library"},
		{name: "connection-level headers (without TE identity)", raw: hdrReq("Connection: keep-alive", "Keep-Alive: timeout=5", "Proxy-Authorization: Basic eDp5", "Proxy-Connection: keep-alive",
			"TE: trailers", "Trailer: X-T", "Upgrade: foo", "Proxy-Authenticate: Basic"),
			want:  []string{ok("/h", 0)},
			guest: g("-HTTP_CONNECTION", "", "-HTTP_KEEP_ALIVE", "", "-HTTP_PROXY_AUTHORIZATION", "", "-HTTP_PROXY_CONNECTION", "", "-HTTP_TE", "", "-HTTP_TRAILER", "", "-HTTP_UPGRADE", "", "-HTTP_PROXY_AUTHENTICATE", ""), spec: specHop},
		{name: "a header named by Connection is still forwarded", raw: hdrReq("Connection: X-Secret", "X-Secret: v"), guest: g("HTTP_X_SECRET", "v"),
			silent: "RFC 9110 7.6.1 says an intermediary removes every header named in Connection; the spec's list of dropped headers is fixed and does not say so"},
		{name: "websocket handshake is just a request", raw: hdrReq("Connection: Upgrade", "Upgrade: websocket", "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==", "Sec-WebSocket-Version: 13"),
			want: []string{ok("/h", 0)}, guest: g("-HTTP_UPGRADE", "", "HTTP_SEC_WEBSOCKET_KEY", "dGhlIHNhbXBsZSBub25jZQ==", "-HTTP_CONNECTION", ""),
			silent: "Upgrade is not supported and not forwarded; the function answers a plain 200 and the handshake simply fails at the client"},
		{name: "h2c upgrade offer is ignored", raw: hdrReq("Connection: Upgrade, HTTP2-Settings", "Upgrade: h2c", "HTTP2-Settings: AAMAAABkAAQAoAAAAAIAAAAA"), want: []string{ok("/h", 0)}, guest: g("-HTTP_UPGRADE", "")},

		// ---- who the client is -----------------------------------------------------------
		{name: "forwarding headers are replaced", raw: hdrReq("X-Forwarded-For: 6.6.6.6", "x-forwarded-for: 7.7.7.7", "X-Forwarded-Proto: https", "X-Forwarded-Host: evil.test",
			"Forwarded: for=6.6.6.6;host=evil.test", "X-Real-IP: 6.6.6.6"),
			guest: g("HTTP_X_FORWARDED_FOR", "127.0.0.1", "HTTP_X_FORWARDED_PROTO", "http", "HTTP_X_FORWARDED_HOST", corpusHost, "-HTTP_FORWARDED", "", "-HTTP_X_REAL_IP", ""), spec: specFwd},
		{name: "remote address is the peer", raw: hdrReq("X-Forwarded-For: 6.6.6.6"), check: func(t *testing.T, ex *exch) {
			v, _ := ex.first().dump.get("REMOTE_ADDR")
			if !strings.HasPrefix(v, "127.0.0.1:") {
				t.Errorf("REMOTE_ADDR %q", v)
			}
		}},
		{name: "underscore spelling of x-real-ip", raw: hdrReq("X_Real_IP: 6.6.6.6"), guest: g("-HTTP_X_REAL_IP", ""),
			spec: specFwd},
		{name: "underscore spelling of x-forwarded-for", raw: hdrReq("X_Forwarded_For: 6.6.6.6"), guest: g("HTTP_X_FORWARDED_FOR", "127.0.0.1"),
			spec: specFwd},
		{name: "underscore spelling of x-forwarded-proto", raw: hdrReq("X_Forwarded_Proto: https"), guest: g("HTTP_X_FORWARDED_PROTO", "http"), spec: specFwd},
		{name: "underscore spelling of content-type", raw: hdrReq("Content_Type: evil/type"), guest: g("-CONTENT_TYPE", "", "-HTTP_CONTENT_TYPE", ""), spec: "Guest ABI: CONTENT_TYPE is not also passed as HTTP_CONTENT_TYPE"},
		{name: "underscore spelling of content-length", raw: hdrReq("Content_Length: 99"), guest: g("CONTENT_LENGTH", "0", "-HTTP_CONTENT_LENGTH", ""), spec: "Guest ABI: a content-length header is not also passed"},
		{name: "two spellings of one name collide", raw: hdrReq("X-A: dash", "X_A: underscore"), guest: g("HTTP_X_A", "dash"),
			spec: "Guest ABI: a header whose name contains an underscore is not given to the guest (it would collide with the dash spelling)"},
		{name: "no platform credential reaches the guest", raw: hdrReq(), check: noCredentials, spec: "Authentication: tokens are for the seams"},
		{name: "a client Authorization header is forwarded like any other", raw: hdrReq("Authorization: Bearer " + ingressToken), guest: g("HTTP_AUTHORIZATION", "Bearer "+ingressToken),
			silent: "the client's Authorization is data for the function, not an edgerun credential; it is forwarded untouched (and does not authenticate anything)"},

		// ---- trace context -----------------------------------------------------------------
		{name: "a client traceparent's trace id reaches the guest", raw: hdrReq("Traceparent: "+validTP, "Baggage: k=v"), check: continuesTrace(tid),
			spec:   "Observability: the guest's traceparent has the trace id of the call; baggage is never given to the guest",
			silent: "in a stack with no tracing SDK the guest's traceparent is the client's byte for byte, span id included, while the spec says the worker gives the guest \"a span id of its own\" (and in the same paragraph allows a component that makes no span to forward what it received): the two sentences disagree for the no-SDK case"},
		{name: "a client tracestate reaches the guest", raw: hdrReq("Traceparent: "+validTP, "Tracestate: a=b"), guest: g("HTTP_TRACESTATE", "a=b"),
			spec: "Observability: the trace context (tracestate with it) travels with the trace to the guest; only the header copies in HttpRequest.headers are dropped"},
		{name: "malformed traceparent", raw: hdrReq("Traceparent: garbage"), check: ignoresTraceparent("garbage"), spec: "W3C Trace Context: invalid headers are ignored"},
		{name: "upper-case traceparent is invalid", raw: hdrReq("Traceparent: " + strings.ToUpper(validTP)), check: ignoresTraceparent(strings.ToUpper(validTP))},
		{name: "all-zero trace id", raw: hdrReq("Traceparent: 00-00000000000000000000000000000000-" + sid + "-01"), check: ignoresTraceparent("00-00000000000000000000000000000000-" + sid + "-01")},
		{name: "two traceparent headers", raw: hdrReq("Traceparent: "+validTP, "Traceparent: "+validTP), check: func(t *testing.T, ex *exch) {
			v := guestTraceparent(t, ex)
			if v != "" && !traceparentRe.MatchString(v) {
				t.Errorf("the guest's traceparent %q is not well formed", v)
			}
		}, silent: "W3C says two traceparent headers are invalid; whichever it picks, the guest must get one valid value"},
		{name: "traceparent with underscores is not the platform's", raw: hdrReq("Trace_Parent: " + validTP), guest: g("-HTTP_TRACEPARENT", ""),
			silent: "trace_parent maps to HTTP_TRACE_PARENT, which is not HTTP_TRACEPARENT: no collision"},

		// ---- request id ---------------------------------------------------------------------
		{name: "valid request id is echoed", raw: hdrReq("X-Request-Id: abc-123_XYZ"), check: requestIDEchoed("abc-123_XYZ"), guest: g("HTTP_X_REQUEST_ID", "abc-123_XYZ"), spec: specReqID},
		{name: "128 characters is accepted", raw: hdrReq("X-Request-Id: " + strings.Repeat("a", 128)), check: requestIDEchoed(strings.Repeat("a", 128)), spec: specReqID},
		{name: "129 characters is replaced", raw: hdrReq("X-Request-Id: " + strings.Repeat("a", 129)), check: requestIDReplaced(strings.Repeat("a", 129)), spec: specReqID},
		{name: "a space inside is replaced", raw: hdrReq("X-Request-Id: a b"), check: requestIDReplaced("a b"), spec: specReqID},
		{name: "a tab inside is replaced", raw: hdrReq("X-Request-Id: a\tb"), check: requestIDReplaced("a\tb"), spec: specReqID},
		{name: "non-ASCII is replaced", raw: hdrReq("X-Request-Id: caf\xc3\xa9"), check: requestIDReplaced("caf\xc3\xa9"), spec: specReqID},
		{name: "empty is replaced", raw: hdrReq("X-Request-Id:"), check: requestIDReplaced(""), spec: specReqID},
		{name: "two ids: the first is used", raw: hdrReq("X-Request-Id: first", "X-Request-Id: second"), check: requestIDEchoed("first"),
			silent: "the spec says \"the client's X-Request-Id\"; it does not say what a repeated header means. The first is used (the guest sees both, joined)"},
		{name: "request id is echoed on an answer the frontend makes", raw: cget("/a%00b", "X-Request-Id: req-400"), want: []string{"400"}, check: requestIDEchoed("req-400"), spec: specReqID},
		{name: "request id on a path the HTTP library cannot decode", raw: cget("/a%zz", "X-Request-Id: req-badescape"), want: []string{"400"}, check: requestIDPresent,
			defect: "CORPUS-RID: the spec's own 400 for a path that does not decode (/a%zz, /a%) is made by the HTTP library before the handler runs and carries NO X-Request-Id, not even a fresh one (the spec now allows a fresh id for such refusals, but every response must still carry one)", spec: specReqID},
		{name: "request id is echoed on a 413", raw: rawReq("POST /x HTTP/1.1", "Host: "+corpusHost, "Content-Length: 99999999999", "X-Request-Id: req-413"),
			want: []string{"413"}, check: requestIDEchoed("req-413"), spec: specReqID},
		{name: "request id on an error of the HTTP library itself", raw: hdrReq("X-Request-Id: req-parse", "X-A : 1"), want: []string{"400"}, check: requestIDPresent,
			defect: "CORPUS-RID: a request the HTTP library refuses before the handler runs (here a space before a colon) is answered 400 with no X-Request-Id at all",
			spec:   specReqID},
		{name: "request id on a 431", raw: hdrReq("X-Request-Id: req-431", "X-A: "+strings.Repeat("v", 70000)), want: []string{"431"}, check: requestIDPresent,
			defect: "CORPUS-RID: same, for 431 (header block too large)", spec: specReqID},

		// ---- content type, cookies -------------------------------------------------------------
		{name: "content-type becomes CONTENT_TYPE only", raw: hdrReq("Content-Type: text/x-a; charset=x"), guest: g("CONTENT_TYPE", "text/x-a; charset=x", "-HTTP_CONTENT_TYPE", ""), spec: "Guest ABI: CONTENT_TYPE is not also passed as HTTP_CONTENT_TYPE"},
		{name: "empty content-type means none", raw: hdrReq("Content-Type:"), guest: g("-CONTENT_TYPE", "", "-HTTP_CONTENT_TYPE", ""), spec: "Guest ABI: CONTENT_TYPE if any"},
		{name: "two content-types: the first wins", raw: hdrReq("Content-Type: a/a", "Content-Type: b/b"), guest: g("CONTENT_TYPE", "a/a"),
			spec: "Guest ABI: CONTENT_TYPE is the first Content-Type"},
		{name: "cookie", raw: hdrReq("Cookie: a=1; b=2; c=\"x y\""), guest: g("HTTP_COOKIE", `a=1; b=2; c="x y"`)},
		{name: "two Cookie headers are joined with a semicolon", raw: hdrReq("Cookie: a=1", "Cookie: b=2"), guest: g("HTTP_COOKIE", "a=1; b=2"), spec: "Guest ABI: cookie lines are joined with \"; \" (RFC 6265)"},
		{name: "cookie with binary bytes and 4000 bytes", raw: hdrReq("Cookie: a=\xff\xfe; b=" + strings.Repeat("x", 4000)), guest: g("HTTP_COOKIE", "a=\xff\xfe; b="+strings.Repeat("x", 4000))},
		{name: "empty cookie", raw: hdrReq("Cookie:"), guest: g("HTTP_COOKIE", "")},
		{name: "accept-encoding is passed and nothing is compressed", raw: hdrReq("Accept-Encoding: gzip, br"), guest: g("HTTP_ACCEPT_ENCODING", "gzip, br"),
			check: func(t *testing.T, ex *exch) {
				if ce := ex.resps[0].header.Get("Content-Encoding"); ce != "" {
					t.Errorf("the frontend added Content-Encoding %q", ce)
				}
			}},
	}
	runRows(t, e, rows)
}
