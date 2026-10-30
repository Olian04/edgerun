package frontendconf

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"strings"
	"testing"
	"time"
)

// This file is the suite for what the frontend does with the bytes of an HTTP
// message (docs/protocol.md, "HTTP messages"): the path is carried decoded and
// as text, the request target is ASCII, and header values are opaque bytes.

// rawReply is the answer to a request sent byte for byte.
type rawReply struct {
	status int
	header textproto.MIMEHeader
	body   []byte
}

// raw sends request exactly as given (so with bytes no HTTP client would send)
// and reads the answer. It marks the request with an id header, unless the
// request already carries one.
func (h *harness) raw(t testing.TB, id, request string) rawReply {
	t.Helper()
	addr := strings.TrimPrefix(h.url, "http://")
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(20 * time.Second))
	request = strings.Replace(request, "\r\n\r\n", "\r\nX-Conformance-Test: "+id+"\r\nHost: "+addr+"\r\nConnection: close\r\n\r\n", 1)
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	tp := textproto.NewReader(br)
	line, err := tp.ReadLine()
	if err != nil {
		t.Fatalf("no answer to %q: %v", request, err)
	}
	var out rawReply
	fmt.Sscanf(line, "HTTP/1.1 %d", &out.status)
	out.header, _ = tp.ReadMIMEHeader()
	out.body, _ = io.ReadAll(br)
	return out
}

func (h *harness) forwarded(id string) bool { return len(h.orch.invocationsWith(id)) > 0 }

func (h *harness) httpMessages(t *testing.T) {
	t.Run("the path is carried decoded", func(t *testing.T) {
		for i, tc := range []struct{ target, path, query string }{
			{"/a%2Fb", "/a/b", ""},
			{"/a%25b", "/a%b", ""},
			{"/%41", "/A", ""},
			{"/a%20b", "/a b", ""},
			{"/caf%C3%A9", "/caf\u00e9", ""},
			{"/a/./b", "/a/./b", ""},
			{"//x", "//x", ""},
			{"/p?x=%2F&y=%ff&z", "/p", "x=%2F&y=%ff&z"}, // the query is raw: decoded only by whoever reads it
			{"/p%3Fq?a=b", "/p?q", "a=b"},
		} {
			h.orch.script(answer(200, "ok"))
			id := fmt.Sprintf("msg-path-%d", i)
			if r := h.raw(t, id, "GET "+tc.target+" HTTP/1.1\r\n\r\n"); r.status != 200 {
				t.Errorf("%s: status %d", tc.target, r.status)
				continue
			}
			got := h.only(t, id).req.GetRequest()
			if got.GetPath() != tc.path || got.GetRawQuery() != tc.query {
				t.Errorf("%s: path %q and raw_query %q, want %q and %q: the path is decoded before it is carried (so that an ingress that is not HTTP never has to encode one), the query is as received", tc.target, got.GetPath(), got.GetRawQuery(), tc.path, tc.query)
			}
		}
	})

	t.Run("a path that does not decode to text is refused, not forwarded", func(t *testing.T) {
		for i, target := range []string{"/%ff", "/%c3", "/a%00b", "/a%0Ab", "/a%0Db", "/%7f", "/%1b%5b", "/ok/%e2%28%a1"} {
			h.orch.script(answer(200, "ok"))
			id := fmt.Sprintf("msg-badpath-%d", i)
			r := h.raw(t, id, "GET "+target+" HTTP/1.1\r\n\r\n")
			if r.status != 400 {
				t.Errorf("%s: status %d, want 400", target, r.status)
			}
			if h.forwarded(id) {
				t.Errorf("%s: was forwarded to the orchestrator", target)
			}
		}
	})

	t.Run("a request target that is not ASCII is refused, not forwarded", func(t *testing.T) {
		for i, request := range []string{
			"GET /caf\xc3\xa9 HTTP/1.1\r\n\r\n",
			"GET /\xff HTTP/1.1\r\n\r\n",
			"GET /?q=\xff HTTP/1.1\r\n\r\n",
			"GET /?q=caf\xc3\xa9 HTTP/1.1\r\n\r\n",
			"GET /a\x01b HTTP/1.1\r\n\r\n",
			"GET /a\x7fb HTTP/1.1\r\n\r\n",
		} {
			h.orch.script(answer(200, "ok"))
			id := fmt.Sprintf("msg-nonascii-%d", i)
			r := h.raw(t, id, request)
			if r.status != 400 {
				t.Errorf("%q: status %d, want 400", strings.SplitN(request, " HTTP", 2)[0], r.status)
			}
			if h.forwarded(id) {
				t.Errorf("%q: was forwarded to the orchestrator", strings.SplitN(request, " HTTP", 2)[0])
			}
		}
	})

	t.Run("header values are bytes, forwarded as they arrived", func(t *testing.T) {
		h.orch.script(answer(200, "ok"))
		r := h.raw(t, "msg-hdr-bytes", "GET / HTTP/1.1\r\nX-Latin: caf\xe9\r\nX-Junk: \xff\xfe\x80\r\nX-Tab: a\tb\r\nX-Pad:   padded \t\r\nX-Dup: one\r\nX-Dup: \xe9two\r\n\r\n")
		if r.status != 200 {
			t.Fatalf("a request whose header values are not text answered %d, want 200: a header value is opaque", r.status)
		}
		got := h.only(t, "msg-hdr-bytes").req.GetRequest()
		want := map[string][]string{
			"x-latin": {"caf\xe9"}, "x-junk": {"\xff\xfe\x80"}, "x-tab": {"a\tb"},
			"x-pad": {"padded"}, // optional whitespace around a value is not part of it
			"x-dup": {"one", "\xe9two"},
		}
		for name, values := range want {
			if g := headerValues(got, name); fmt.Sprintf("%q", g) != fmt.Sprintf("%q", values) {
				t.Errorf("%s = %q, want %q (the bytes, unchanged, in order)", name, g, values)
			}
		}
	})

	t.Run("a response header value is bytes too", func(t *testing.T) {
		h.orch.script(answer(200, "ok", hdr("x-latin", "caf\xe9"), hdr("x-junk", "\xff\xfe\x80"), hdr("x-dup", "one"), hdr("x-dup", "\xe9two")))
		r := h.raw(t, "msg-resp-bytes", "GET / HTTP/1.1\r\n\r\n")
		if r.status != 200 {
			t.Fatalf("status %d", r.status)
		}
		for name, want := range map[string][]string{"X-Latin": {"caf\xe9"}, "X-Junk": {"\xff\xfe\x80"}, "X-Dup": {"one", "\xe9two"}} {
			if g := r.header[name]; fmt.Sprintf("%q", g) != fmt.Sprintf("%q", want) {
				t.Errorf("%s = %q, want %q: the client is given the bytes the function wrote", name, g, want)
			}
		}
	})

	t.Run("a response header cannot be used to split the response", func(t *testing.T) {
		// The orchestrator is not meant to send these (a value has no control character), but an edge that
		// believed it would let a function write headers the client takes for its own.
		h.orch.script(answer(200, "ok", hdr("x-evil", "a\r\nSet-Cookie: injected=1"), hdr("x-evil2", "a\x00b")))
		r := h.raw(t, "msg-resp-split", "GET / HTTP/1.1\r\n\r\n")
		if _, ok := r.header["Set-Cookie"]; ok {
			t.Errorf("a CRLF in a response header value split the response: %v", r.header)
		}
		if bytes.Contains(r.body, []byte("injected")) && r.status == 200 {
			t.Errorf("a CRLF in a response header value reached the body: %q", r.body)
		}
	})

	t.Run("a request header value with a control character is refused", func(t *testing.T) {
		h.orch.script(answer(200, "ok"))
		r := h.raw(t, "msg-hdr-ctl", "GET / HTTP/1.1\r\nX-Bad: a\x00b\r\n\r\n")
		if r.status != 400 || h.forwarded("msg-hdr-ctl") {
			t.Errorf("status %d (forwarded: %v), want 400 and not forwarded", r.status, h.forwarded("msg-hdr-ctl"))
		}
	})
}

var _ = http.StatusOK
