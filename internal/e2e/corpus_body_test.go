package e2e

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

const specLimit = "docs/protocol.md \"Limits fixed by the protocol\": the frontend enforces max_request_body_bytes itself, 413 without calling the orchestrator, also for a body of undeclared length which it must stop reading at the limit"

func sha(b string) string { s := sha256.Sum256([]byte(b)); return hex.EncodeToString(s[:]) }

// chunks frames body as chunked, in pieces of size n.
func chunks(body string, n int) string {
	var b strings.Builder
	for len(body) > 0 {
		k := min(n, len(body))
		fmt.Fprintf(&b, "%x\r\n%s\r\n", k, body[:k])
		body = body[k:]
	}
	b.WriteString("0\r\n\r\n")
	return b.String()
}

func noInvocation(t *testing.T, ex *exch) {
	t.Helper()
	for _, r := range ex.resps {
		if hasHeader(t, r, "X-Edgerun-Invocation-Id") {
			t.Errorf("the request was refused by the frontend and must not have reached the orchestrator, yet the answer names an invocation")
		}
	}
}

func TestCorpusBodies(t *testing.T) {
	e := newCorpusEnv(t)
	L := corpusBodyLimit
	rnd := rand.New(rand.NewSource(1))
	random := func(n int) string { b := make([]byte, n); rnd.Read(b); return string(b) }
	atLimit := random(L)
	over := atLimit + "x"
	small := random(1 << 20)

	var bomb bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&bomb, gzip.BestSpeed)
	zero := make([]byte, 1<<20)
	for i := 0; i < 128; i++ { // 128 MiB of zeros, a few hundred KB compressed
		zw.Write(zero)
	}
	zw.Close()
	var small4 bytes.Buffer
	zw = gzip.NewWriter(&small4)
	zw.Write([]byte("hello hello hello hello"))
	zw.Close()

	overLast := strings.TrimSuffix(chunks(atLimit, 1<<20), "0\r\n\r\n") + "1\r\nx\r\n0\r\n\r\n"
	rows := []crow{
		{name: "declared length exactly at the limit", raw: post("/b", atLimit, "Content-Type: application/octet-stream"), sentinel: true,
			want: []string{seq(ok("/b", L), ok("/sentinel", 0))}, guest: g("BODY_LEN", fmt.Sprint(L), "BODY_SHA", sha(atLimit), "CONTENT_LENGTH", fmt.Sprint(L)), spec: specLimit},
		{name: "declared length one over the limit, body sent", raw: post("/b", over), want: []string{"413"}, check: noInvocation, spec: specLimit},
		{name: "declared length one over the limit, body never sent", raw: rawReq("POST /b HTTP/1.1", "Host: "+corpusHost, fmt.Sprintf("Content-Length: %d", L+1)), closeWrite: true, want: []string{"413"}, check: noInvocation,
			spec: specLimit + " (answered before the body is read)"},
		{name: "chunked exactly at the limit", raw: chunked("/b", chunks(atLimit, 64<<10)), sentinel: true,
			want: []string{seq(ok("/b", L), ok("/sentinel", 0))}, guest: g("BODY_SHA", sha(atLimit), "CONTENT_LENGTH", fmt.Sprint(L)), spec: specLimit},
		{name: "chunked one over the limit", raw: chunked("/b", chunks(over, 64<<10)), want: []string{"413"}, check: noInvocation, spec: specLimit},
		{name: "chunked one over the limit in a single chunk", raw: chunked("/b", chunks(over, len(over))), want: []string{"413"}, check: noInvocation, spec: specLimit},
		{name: "chunked, last chunk is the one byte over", raw: chunked("/b", overLast), want: []string{"413"}, check: noInvocation, spec: specLimit},
		{name: "chunked, 4 limits long, sent without waiting", raw: chunked("/b", chunks(strings.Repeat("y", 4*L), 256<<10)), want: []string{"413"}, check: noInvocation,
			spec: specLimit + ": the frontend must stop reading at the limit (here the client keeps writing; the answer must still arrive)"},
		{name: "expect 100-continue with a body at the limit", parts: []string{rawReq("POST /b HTTP/1.1", "Host: "+corpusHost, fmt.Sprintf("Content-Length: %d", L), "Expect: 100-continue"), atLimit}, hold100: true,
			want: []string{ok("/b", L)}, spec: specLimit},
		{name: "expect 100-continue with chunked over the limit", parts: []string{rawReq("POST /b HTTP/1.1", "Host: "+corpusHost, "Transfer-Encoding: chunked", "Expect: 100-continue"), chunks(over, 64<<10)}, hold100: true,
			want: []string{"413"}, check: noInvocation,
			silent: "for an undeclared length the limit cannot be known in advance, so 100 Continue is sent and the body refused after the limit"},
		{name: "two bodies at the limit on one connection", raw: post("/b1", atLimit) + post("/b2", atLimit), sentinel: true, want: []string{seq(ok("/b1", L), ok("/b2", L), ok("/sentinel", 0))}},

		// ---- bodies are bytes -----------------------------------------------------------------
		{name: "1 MiB of random bytes arrives intact", raw: post("/b", small), want: []string{ok("/b", len(small))}, guest: g("BODY_SHA", sha(small))},
		{name: "NUL, CRLF and high bytes in a small body", raw: post("/b", "a\x00b\r\n\r\nc\xff\xfe\n"), guest: g("BODY", "a\x00b\r\n\r\nc\xff\xfe\n", "BODY_LEN", "11")},
		{name: "a body that looks like a request is only a body", raw: post("/b", cget("/inner")), want: []string{ok("/b", len(cget("/inner")))}, sentinel: false},
		{name: "a gzip request body is not decoded", raw: post("/b", small4.String(), "Content-Encoding: gzip", "Content-Type: text/plain"),
			guest: g("BODY_SHA", sha(small4.String()), "HTTP_CONTENT_ENCODING", "gzip", "BODY_LEN", fmt.Sprint(small4.Len())),
			spec:  "Guest ABI: the guest gets the body bytes as sent, and Content-Encoding as a header; nothing in the protocol decodes a request"},
		{name: "a gzip bomb is forwarded as the small thing it is", raw: post("/b", bomb.String(), "Content-Encoding: gzip"),
			guest: g("BODY_SHA", sha(bomb.String()), "BODY_LEN", fmt.Sprint(bomb.Len())), spec: "same: no decompression at the edge, so a 128 MiB expansion never happens there"},
		{name: "Content-Encoding: gzip on a body that is not gzip", raw: post("/b", "not gzip at all", "Content-Encoding: gzip"), guest: g("BODY", "not gzip at all", "HTTP_CONTENT_ENCODING", "gzip")},
		{name: "Content-Encoding stacked", raw: post("/b", small4.String(), "Content-Encoding: gzip, br"), guest: g("BODY_SHA", sha(small4.String()), "HTTP_CONTENT_ENCODING", "gzip, br")},
		{name: "Content-Encoding: identity", raw: post("/b", "plain", "Content-Encoding: identity"), guest: g("BODY", "plain")},
		{name: "empty POST with Content-Type", raw: post("/b", "", "Content-Type: application/json"), guest: g("BODY_LEN", "0", "CONTENT_LENGTH", "0", "CONTENT_TYPE", "application/json")},
		{name: "empty PUT without Content-Length", raw: withHost("PUT", "/b"), guest: g("BODY_LEN", "0", "CONTENT_LENGTH", "0"), spec: "RFC 9112 6.3: no framing, no body"},
		{name: "form body is not parsed", raw: post("/b?q=1", "a=1&b=%zz", "Content-Type: application/x-www-form-urlencoded"), guest: g("BODY", "a=1&b=%zz", "QUERY_STRING", "q=1")},
	}
	runRows(t, e, rows)
}
