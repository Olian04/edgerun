package e2e

import (
	"fmt"
	"strings"
	"testing"
)

const (
	specNoSmuggle = "RFC 9112 6.1 and 11.2: a message whose framing is ambiguous is rejected or framed by Transfer-Encoding, never parsed two ways; docs/protocol.md is silent on request framing and defers to HTTP"
	specCL        = "RFC 9110 8.6 / RFC 9112 6.3"
)

// framed returns a POST to /b with the given header lines (Host is added).
func framed(body string, hdr ...string) string {
	return rawReq("POST /b HTTP/1.1", append([]string{"Host: " + corpusHost}, hdr...)...) + body
}

func TestCorpusFraming(t *testing.T) {
	e := newCorpusEnv(t)
	inner := "GET /smuggled HTTP/1.1\r\nHost: " + corpusHost + "\r\n\r\n"
	smuggled := fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(inner), inner) // a chunked body that carries a whole request
	good := "5\r\nhello\r\n0\r\n\r\n"
	teWins := seq(ok("/b", len(inner)), ok("/sentinel", 0))
	hello := seq(ok("/b", 5), ok("/sentinel", 0))
	rejects := []string{"400", "501"}
	rejectOrTE := []string{"400", "501", teWins}

	rows := []crow{
		// ---- Content-Length --------------------------------------------------------------
		{name: "CL: plain body", raw: framed("hello", "Content-Length: 5"), sentinel: true, want: []string{hello},
			guest: g("BODY", "hello", "CONTENT_LENGTH", "5", "-HTTP_CONTENT_LENGTH", ""), spec: "Guest ABI: CONTENT_LENGTH is measured"},
		{name: "CL: 0", raw: framed("", "Content-Length: 0"), sentinel: true, want: []string{seq(ok("/b", 0), ok("/sentinel", 0))}, guest: g("CONTENT_LENGTH", "0")},
		{name: "CL: POST without length or encoding has no body", raw: framed(""), sentinel: true, want: []string{seq(ok("/b", 0), ok("/sentinel", 0))}, spec: "RFC 9112 6.3 rule 7"},
		{name: "CL: GET with a body", raw: rawReq("GET /b HTTP/1.1", "Host: "+corpusHost, "Content-Length: 5") + "hello", sentinel: true, want: []string{hello},
			guest: g("BODY", "hello", "REQUEST_METHOD", "GET"), silent: "the spec says nothing about a body on GET (or HEAD, DELETE, TRACE); it is read and handed to the guest"},
		{name: "CL: duplicate header with equal values", raw: framed("hello", "Content-Length: 5", "Content-Length: 5"), sentinel: true, want: []string{hello, "400"}, spec: specCL + ": equal duplicates may be merged, or refused"},
		{name: "CL: list of equal values", raw: framed("hello", "Content-Length: 5, 5"), sentinel: true, want: []string{hello, "400"}, spec: specCL},
		{name: "CL: conflicting duplicates", raw: framed("hello", "Content-Length: 5", "Content-Length: 6"), sentinel: true, want: []string{"400"}, spec: specCL + ": MUST be refused"},
		{name: "CL: conflicting list", raw: framed("hello", "Content-Length: 5, 6"), sentinel: true, want: []string{"400"}, spec: specCL},
		{name: "CL: negative", raw: framed("hello", "Content-Length: -1"), sentinel: true, want: []string{"400"}, spec: specCL},
		{name: "CL: plus sign", raw: framed("hello", "Content-Length: +5"), sentinel: true, want: []string{"400"}, spec: specCL},
		{name: "CL: hex", raw: framed("hello", "Content-Length: 0x5"), sentinel: true, want: []string{"400"}, spec: specCL},
		{name: "CL: trailing garbage", raw: framed("hello", "Content-Length: 5abc"), sentinel: true, want: []string{"400"}, spec: specCL},
		{name: "CL: non-numeric", raw: framed("hello", "Content-Length: five"), sentinel: true, want: []string{"400"}, spec: specCL},
		{name: "CL: decimal point", raw: framed("hello", "Content-Length: 5.0"), sentinel: true, want: []string{"400"}, spec: specCL},
		{name: "CL: exponent", raw: framed("hello", "Content-Length: 1e1"), sentinel: true, want: []string{"400"}, spec: specCL},
		{name: "CL: empty value", raw: framed("hello", "Content-Length:"), sentinel: true, want: []string{"400"}, spec: specCL},
		{name: "CL: surrounding whitespace is trimmed", raw: framed("hello", "Content-Length:  5 \t"), sentinel: true, want: []string{hello}, spec: specCL},
		{name: "CL: leading zeros", raw: framed("hello", "Content-Length: 0005"), sentinel: true, want: []string{hello, "400"}, spec: specCL + ": digits"},
		{name: "CL: space between digits", raw: framed("hello", "Content-Length: 5 5"), sentinel: true, want: []string{"400"}, spec: specCL},
		{name: "CL: Arabic-Indic digits", raw: framed("hello", "Content-Length: ٥"), sentinel: true, want: []string{"400"}, spec: specCL},
		{name: "CL: overflows 64 bits", raw: framed("hello", "Content-Length: 99999999999999999999999"), sentinel: true, want: []string{"400"}, spec: specCL},
		{name: "CL: 2^63-1 declared, nothing sent", raw: framed("", "Content-Length: 9223372036854775807"), want: []string{"413"}, spec: "Limits: the frontend enforces max_request_body_bytes itself and answers 413"},
		{name: "CL: 2^32 declared, nothing sent", raw: framed("", "Content-Length: 4294967296"), want: []string{"413"}, spec: "Limits"},
		{name: "CL: truncated body then EOF", raw: framed("hel", "Content-Length: 5"), closeWrite: true, want: []string{"400"}},
		{name: "CL: bytes after the body are the next request", raw: framed("hello"+cget("/b2"), "Content-Length: 5"), sentinel: true, want: []string{seq(ok("/b", 5), ok("/b2", 0), ok("/sentinel", 0))}},
		{name: "CL: junk after the body is a bad next request", raw: framed("helloXXXX\r\n\r\n", "Content-Length: 5"), sentinel: true, want: []string{seq(ok("/b", 5), "400")}},
		{name: "CL: header name in other case", raw: framed("hello", "cOnTeNt-LeNgTh: 5"), sentinel: true, want: []string{hello}},
		{name: "CL: with a space before the colon", raw: framed("hello", "Content-Length : 5"), sentinel: true, want: []string{"400"}},

		// ---- Transfer-Encoding ------------------------------------------------------------
		{name: "TE: chunked", raw: chunked("/b", good), sentinel: true, want: []string{hello}, guest: g("BODY", "hello", "CONTENT_LENGTH", "5", "-HTTP_TRANSFER_ENCODING", "")},
		{name: "TE: Chunked in other case", raw: framed(good, "Transfer-Encoding: Chunked"), sentinel: true, want: []string{hello}, spec: "RFC 9112 7: transfer-coding names are case-insensitive"},
		{name: "TE: trailing whitespace", raw: framed(good, "Transfer-Encoding: chunked \t"), sentinel: true, want: []string{hello}},
		{name: "TE: tab after the colon", raw: framed(good, "Transfer-Encoding:\tchunked"), sentinel: true, want: []string{hello}},
		{name: "TE: xchunked", raw: framed(good, "Transfer-Encoding: xchunked"), sentinel: true, want: rejects, spec: "RFC 9112 6.1: an unknown transfer-coding in a request is 501"},
		{name: "TE: gzip, chunked", raw: framed(good, "Transfer-Encoding: gzip, chunked"), sentinel: true, want: rejects, spec: "RFC 9112 6.1: 501 for a coding the server does not understand"},
		{name: "TE: chunked, gzip (chunked not last)", raw: framed(good, "Transfer-Encoding: chunked, gzip"), sentinel: true, want: rejects, spec: "RFC 9112 6.1: chunked must be last; 400"},
		{name: "TE: chunked twice", raw: framed(good, "Transfer-Encoding: chunked, chunked"), sentinel: true, want: rejects, spec: "RFC 9112 6.1: chunked more than once is an error"},
		{name: "TE: chunked in two headers", raw: framed(good, "Transfer-Encoding: chunked", "Transfer-Encoding: chunked"), sentinel: true, want: append([]string{hello}, rejects...)},
		{name: "TE: chunked then identity", raw: framed(good, "Transfer-Encoding: chunked", "Transfer-Encoding: identity"), sentinel: true, want: rejects},
		{name: "TE: unknown then chunked in two headers", raw: framed(good, "Transfer-Encoding: x", "Transfer-Encoding: chunked"), sentinel: true, want: rejects},
		{name: "TE: identity alone", raw: framed("hello", "Transfer-Encoding: identity", "Content-Length: 5"), sentinel: true, want: append([]string{hello}, rejects...),
			silent: "identity was removed from HTTP; whether it is accepted or refused is up to the HTTP library"},
		{name: "TE: empty", raw: framed(good, "Transfer-Encoding:"), sentinel: true, want: rejects, spec: specNoSmuggle},
		{name: "TE: quoted chunked", raw: framed(good, `Transfer-Encoding: "chunked"`), sentinel: true, want: rejects},
		{name: "TE: space before the colon", raw: framed(good, "Transfer-Encoding : chunked"), sentinel: true, want: []string{"400"}, spec: "RFC 9112 5.1"},
		{name: "TE: obs-fold", raw: framed(good, "Transfer-Encoding:", " chunked"), sentinel: true, want: append([]string{hello}, rejects...)},
		{name: "TE: control character after chunked", raw: framed(good, "Transfer-Encoding: chunked\x0b"), sentinel: true, want: []string{"400"}},
		{name: "TE: underscore spelling is just another header", raw: framed("hello", "Transfer_Encoding: chunked", "Content-Length: 5"), sentinel: true, want: []string{hello}},
		{name: "TE: on HTTP/1.0", raw: rawReq("POST /b HTTP/1.0", "Host: "+corpusHost, "Transfer-Encoding: chunked") + good, want: []string{ok("/b", 0)},
			silent: "RFC 9112 6.1: an HTTP/1.0 message with Transfer-Encoding has faulty framing and the connection must be closed after it; the HTTP library ignores Transfer-Encoding on 1.0, frames by (absent) Content-Length, so the body is empty, and the 1.0 connection closes anyway"},

		// ---- request smuggling: both Content-Length and Transfer-Encoding ------------------------
		{name: "CL.TE: Content-Length first, chunked wins or 400", raw: framed(good, "Content-Length: 3", "Transfer-Encoding: chunked"), sentinel: true, want: []string{hello, "400"}, spec: specNoSmuggle},
		{name: "CL.TE: Transfer-Encoding first", raw: framed(good, "Transfer-Encoding: chunked", "Content-Length: 3"), sentinel: true, want: []string{hello, "400"}, spec: specNoSmuggle},
		{name: "TE.CL: a request hidden in a chunk, CL=4", raw: framed(smuggled, "Transfer-Encoding: chunked", "Content-Length: 4"), sentinel: true, want: rejectOrTE, spec: specNoSmuggle},
		{name: "TE.CL: Content-Length first", raw: framed(smuggled, "Content-Length: 4", "Transfer-Encoding: chunked"), sentinel: true, want: rejectOrTE, spec: specNoSmuggle},
		{name: "TE.CL: Content-Length larger than the chunk framing", raw: framed(smuggled, "Content-Length: 400", "Transfer-Encoding: chunked"), sentinel: true, want: rejectOrTE, spec: specNoSmuggle},
		{name: "TE.CL: zero Content-Length", raw: framed(smuggled, "Content-Length: 0", "Transfer-Encoding: chunked"), sentinel: true, want: rejectOrTE, spec: specNoSmuggle},
		{name: "TE.CL: duplicate Content-Length", raw: framed(smuggled, "Content-Length: 4", "Content-Length: 4", "Transfer-Encoding: chunked"), sentinel: true, want: rejectOrTE, spec: specNoSmuggle},
		{name: "TE.CL: xchunked", raw: framed(smuggled, "Content-Length: 4", "Transfer-Encoding: xchunked"), sentinel: true, want: rejects, spec: specNoSmuggle},
		{name: "TE.CL: gzip, chunked", raw: framed(smuggled, "Content-Length: 4", "Transfer-Encoding: gzip, chunked"), sentinel: true, want: rejects, spec: specNoSmuggle},
		{name: "TE.CL: chunked, identity", raw: framed(smuggled, "Content-Length: 4", "Transfer-Encoding: chunked, identity"), sentinel: true, want: rejects, spec: specNoSmuggle},
		{name: "TE.CL: tab-prefixed value", raw: framed(smuggled, "Content-Length: 4", "Transfer-Encoding:\tchunked"), sentinel: true, want: rejectOrTE, spec: specNoSmuggle},
		{name: "TE.CL: Transfer-Encoding hidden behind an obs-fold of the previous header", raw: framed(smuggled, "Content-Length: 4", "X-Pad: 1", " Transfer-Encoding: chunked"), sentinel: true,
			want: []string{seq(ok("/b", 4), ok("/smuggled", 0), "400")}, spec: specNoSmuggle,
			silent: "here Transfer-Encoding is not a header at all (it is part of X-Pad's value after unfolding), so Content-Length frames the body and the next request is legitimately the one in the body; the row only shows the behaviour is consistent"},
		{name: "TE.CL: space before the colon", raw: framed(smuggled, "Content-Length: 4", "Transfer-Encoding : chunked"), sentinel: true, want: []string{"400"}, spec: specNoSmuggle},
		{name: "TE.CL: two Transfer-Encoding headers, second hidden", raw: framed(smuggled, "Content-Length: 4", "Transfer-Encoding: x", "Transfer-Encoding: chunked"), sentinel: true, want: rejects, spec: specNoSmuggle},
		{name: "TE.CL: on HTTP/1.0 (closes)", raw: rawReq("POST /b HTTP/1.0", "Host: "+corpusHost, "Content-Length: 4", "Transfer-Encoding: chunked") + smuggled, want: []string{ok("/b", 4)},
			spec: specNoSmuggle, silent: "Content-Length frames the body because Transfer-Encoding is ignored on 1.0; the connection closes (1.0 default), so the hidden request is not run"},
		{name: "TE.CL: on HTTP/1.0 with keep-alive", raw: rawReq("POST /b HTTP/1.0", "Host: "+corpusHost, "Connection: keep-alive", "Content-Length: 4", "Transfer-Encoding: chunked") + smuggled, sentinel: true,
			want: []string{ok("/b", 4)}, spec: "RFC 9112 6.1: a server that receives HTTP/1.0 with Transfer-Encoding MUST treat the framing as faulty, even if Content-Length is present, and close the connection after processing the message",
		},
		{name: "CL.TE: the connection is closed after both headers were seen", raw: framed(good, "Content-Length: 3", "Transfer-Encoding: chunked"), sentinel: true, want: []string{ok("/b", 5), "400"},
			spec:   "RFC 9112 6.1: a server that processes a request carrying both Transfer-Encoding and Content-Length MUST close the connection after responding",
			defect: "CORPUS-FRAMING-CLOSE: a request with both Content-Length and Transfer-Encoding is framed by chunking and answered, and the connection is kept open for the next request; RFC 9112 6.1 requires closing it. Only exploitable with another HTTP parser in front"},
		{name: "TE.CL: the connection is closed after both headers were seen", raw: framed(smuggled, "Transfer-Encoding: chunked", "Content-Length: 4"), sentinel: true, want: []string{ok("/b", len(inner)), "400", "501"},
			spec: "RFC 9112 6.1 (same MUST)", defect: "CORPUS-FRAMING-CLOSE: same, TE first"},
		{name: "TE.CL: GET carrying both", raw: rawReq("GET /b HTTP/1.1", "Host: "+corpusHost, "Content-Length: 4", "Transfer-Encoding: chunked") + smuggled, sentinel: true, want: rejectOrTE, spec: specNoSmuggle},
		{name: "TE.CL: Expect with both", raw: framed(smuggled, "Expect: 100-continue", "Content-Length: 4", "Transfer-Encoding: chunked"), sentinel: true, want: rejectOrTE, spec: specNoSmuggle},

		// ---- chunked bodies ----------------------------------------------------------------
		{name: "chunk: several chunks", raw: chunked("/b", "3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n"), sentinel: true, want: []string{seq(ok("/b", 5), ok("/sentinel", 0))}, guest: g("BODY", "abcde")},
		{name: "chunk: zero chunks only", raw: chunked("/b", "0\r\n\r\n"), sentinel: true, want: []string{seq(ok("/b", 0), ok("/sentinel", 0))}},
		{name: "chunk: hex digits in either case", raw: chunked("/b", "A\r\n0123456789\r\na\r\nabcdefghij\r\n0\r\n\r\n"), sentinel: true, want: []string{seq(ok("/b", 20), ok("/sentinel", 0))}},
		{name: "chunk: leading zeros", raw: chunked("/b", "0000000000000005\r\nhello\r\n0\r\n\r\n"), sentinel: true, want: []string{hello}},
		{name: "chunk: extension", raw: chunked("/b", "5;ext=1;flag\r\nhello\r\n0;done\r\n\r\n"), sentinel: true, want: []string{hello}, guest: g("BODY", "hello")},
		{name: "chunk: quoted extension with a semicolon", raw: chunked("/b", "5;a=\"b;c\"\r\nhello\r\n0\r\n\r\n"), sentinel: true, want: []string{hello}},
		{name: "chunk: space before the extension", raw: chunked("/b", "5 ;x\r\nhello\r\n0\r\n\r\n"), sentinel: true, want: []string{hello, "400"}},
		{name: "chunk: trailers are dropped", raw: chunked("/b", "5\r\nhello\r\n0\r\nX-Trailer: v\r\nX-Other: w\r\n\r\n"), sentinel: true, want: []string{hello},
			guest: g("-HTTP_X_TRAILER", "", "-HTTP_X_OTHER", ""), spec: "Frontend behaviour: trailer is dropped; trailers are not part of HttpRequest"},
		{name: "chunk: a trailer cannot change Host, length or routing", raw: chunked("/b", "5\r\nhello\r\n0\r\nHost: evil.test\r\nContent-Length: 99\r\nX-Forwarded-For: 6.6.6.6\r\n\r\n"), sentinel: true,
			want: []string{hello, "400"}, guest: g("SERVER_NAME", corpusHost, "WHO", "exact", "CONTENT_LENGTH", "5", "HTTP_X_FORWARDED_FOR", "127.0.0.1")},
		{name: "chunk: declared Trailer header", raw: chunked("/b", "5\r\nhello\r\n0\r\nX-T: v\r\n\r\n", "Trailer: X-T"), sentinel: true, want: []string{hello}, guest: g("-HTTP_TRAILER", "", "-HTTP_X_T", "")},
		{name: "chunk: malformed trailer line", raw: chunked("/b", "5\r\nhello\r\n0\r\nnot a header\r\n\r\n"), sentinel: true, want: []string{"400"}},
		{name: "chunk: 70000 byte trailer", raw: chunked("/b", "5\r\nhello\r\n0\r\nX-T: "+strings.Repeat("v", 70000)+"\r\n\r\n"), sentinel: true, want: []string{hello, "400"},
			silent: "the spec does not bound trailers"},
		{name: "chunk: 1000 chunks of one byte", raw: chunked("/b", strings.Repeat("1\r\nx\r\n", 1000)+"0\r\n\r\n"), sentinel: true, want: []string{seq(ok("/b", 1000), ok("/sentinel", 0))}},
		{name: "chunk: empty size line", raw: chunked("/b", "\r\nhello\r\n0\r\n\r\n"), sentinel: true, want: []string{"400"}},
		{name: "chunk: plus sign in size", raw: chunked("/b", "+5\r\nhello\r\n0\r\n\r\n"), sentinel: true, want: []string{"400"}},
		{name: "chunk: minus sign in size", raw: chunked("/b", "-5\r\nhello\r\n0\r\n\r\n"), sentinel: true, want: []string{"400"}},
		{name: "chunk: 0x prefix in size", raw: chunked("/b", "0x5\r\nhello\r\n0\r\n\r\n"), sentinel: true, want: []string{"400"}},
		{name: "chunk: non-hex size", raw: chunked("/b", "g\r\nhello\r\n0\r\n\r\n"), sentinel: true, want: []string{"400"}},
		{name: "chunk: space inside the size", raw: chunked("/b", "5 5\r\nhello\r\n0\r\n\r\n"), sentinel: true, want: []string{"400"}},
		{name: "chunk: NUL after the size", raw: chunked("/b", "5\x00\r\nhello\r\n0\r\n\r\n"), sentinel: true, want: []string{"400"}},
		{name: "chunk: size overflows 64 bits", raw: chunked("/b", "10000000000000000\r\nhello\r\n0\r\n\r\n"), sentinel: true, want: []string{"400"}},
		{name: "chunk: size 2^63-1 and then EOF", raw: chunked("/b", "7fffffffffffffff\r\nhello"), closeWrite: true, want: []string{"400", "413"}},
		{name: "chunk: size bigger than the data, then EOF", raw: chunked("/b", "10\r\nhello"), closeWrite: true, want: []string{"400"}},
		{name: "chunk: data longer than the size", raw: chunked("/b", "3\r\nhello\r\n0\r\n\r\n"), sentinel: true, want: []string{"400"}},
		{name: "chunk: missing CRLF after the data", raw: chunked("/b", "5\r\nhello0\r\n\r\n"), sentinel: true, want: []string{"400"}},
		{name: "chunk: LF only after the data", raw: chunked("/b", "5\r\nhello\n0\r\n\r\n"), sentinel: true, want: []string{"400", hello}, spec: "RFC 9112 7.1: chunk data is followed by CRLF"},
		{name: "chunk: no terminating chunk, then EOF", raw: chunked("/b", "5\r\nhello\r\n"), closeWrite: true, want: []string{"400"}},
		{name: "chunk: last chunk without its final CRLF, then EOF", raw: chunked("/b", "5\r\nhello\r\n0\r\n"), closeWrite: true, want: []string{"400"}},
		{name: "chunk: bare LF framing", raw: chunked("/b", "5\nhello\n0\n\n"), sentinel: true, want: []string{hello, "400"}, spec: "RFC 9112 7.1 (CRLF) and 2.2 (bare LF tolerance applies to start-line and field lines only)"},
		{name: "chunk: 5000 byte chunk-extension line", raw: chunked("/b", "5;x="+strings.Repeat("a", 5000)+"\r\nhello\r\n0\r\n\r\n"), sentinel: true, want: []string{hello, "400"},
			silent: "the spec states no bound for chunk extensions"},

		// ---- Expect: 100-continue ----------------------------------------------------------
		{name: "expect: the body is sent after 100 Continue", parts: []string{rawReq("POST /b HTTP/1.1", "Host: "+corpusHost, "Content-Length: 5", "Expect: 100-continue"), "hello"}, hold100: true,
			want: []string{ok("/b", 5)}, guest: g("BODY", "hello"), check: func(t *testing.T, ex *exch) {
				if len(ex.interim) != 1 || ex.interim[0] != 100 {
					t.Errorf("interim responses %v, want [100]", ex.interim)
				}
			}, spec: "RFC 9110 10.1.1"},
		{name: "expect: header value in other case", parts: []string{rawReq("POST /b HTTP/1.1", "Host: "+corpusHost, "Content-Length: 5", "Expect: 100-Continue"), "hello"}, hold100: true, want: []string{ok("/b", 5)}},
		{name: "expect: chunked body after 100 Continue", parts: []string{rawReq("POST /b HTTP/1.1", "Host: "+corpusHost, "Transfer-Encoding: chunked", "Expect: 100-continue"), good}, hold100: true, want: []string{ok("/b", 5)}},
		{name: "expect: body sent at once", raw: framed("hello", "Content-Length: 5", "Expect: 100-continue"), sentinel: true, want: []string{hello},
			check: func(t *testing.T, ex *exch) { t.Logf("interim: %v", ex.interim) }},
		{name: "expect: refused without inviting the body (too large)", parts: []string{rawReq("POST /b HTTP/1.1", "Host: "+corpusHost, fmt.Sprintf("Content-Length: %d", corpusBodyLimit+1), "Expect: 100-continue"), "x"}, hold100: true,
			want: []string{"413"}, check: func(t *testing.T, ex *exch) {
				if len(ex.interim) != 0 {
					t.Errorf("a 100 Continue was sent for a body that was refused: %v", ex.interim)
				}
			}, spec: "Frontend behaviour: 413 without reading; RFC 9110 10.1.1: do not invite a body you will refuse"},
		{name: "expect: unknown expectation", raw: framed("hello", "Content-Length: 5", "Expect: 200-ok"), want: []string{"417"}, spec: "RFC 9110 10.1.1: 417"},
		{name: "expect: ignored on HTTP/1.0", raw: rawReq("POST /b HTTP/1.0", "Host: "+corpusHost, "Content-Length: 5", "Expect: 100-continue") + "hello", want: []string{ok("/b", 5)},
			check: func(t *testing.T, ex *exch) {
				if len(ex.interim) != 0 {
					t.Errorf("RFC 9110 10.1.1: a 100-continue expectation in an HTTP/1.0 request MUST be ignored; got interim %v", ex.interim)
				}
			}},
		{name: "expect: GET without a body", raw: rawReq("GET /b HTTP/1.1", "Host: "+corpusHost, "Expect: 100-continue"), want: []string{ok("/b", 0)},
			check: func(t *testing.T, ex *exch) { t.Logf("interim: %v", ex.interim) }},
	}
	runRows(t, e, rows)
}
