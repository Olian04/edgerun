package e2e

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http/httputil"
	"net/textproto"
	"strings"
	"testing"
	"time"
)

// These tests run the HTTP-message rules (docs/protocol.md, "HTTP messages")
// through the whole real stack: a real frontend, orchestrator and database, a
// real worker and a real guest. The conformance suites judge each role on its
// own; this is the proof that the roles, put together, carry the bytes.

type rawReply struct {
	status int
	header textproto.MIMEHeader
	body   []byte
}

// rawHTTP sends request byte for byte, with the connection closed after the
// answer, and reads the answer.
func rawHTTP(t *testing.T, addr, request string) rawReply {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))
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
	if strings.EqualFold(out.header.Get("Transfer-Encoding"), "chunked") {
		out.body, _ = io.ReadAll(httputil.NewChunkedReader(br))
	} else {
		out.body, _ = io.ReadAll(br)
	}
	return out
}

func (s *stack) fidelityStack(t *testing.T) (*web, string, string) {
	t.Helper()
	s.startWorker(4)
	fid := s.createFunction("fidelity")
	s.uploadVersion(fid, s.wasm, nil, true)
	s.route("*", "/", fid)
	s.waitServing("e2e.test", "/echo")
	w := s.startFrontend(20 * time.Second)
	return w, strings.TrimPrefix(w.base, "http://"), fid
}

// settledRecords counts the invocation records of fid once no more are being
// written (records appear shortly after a call ends).
func settledRecords(s *stack, fid string) int {
	last, since := -1, time.Now()
	for time.Since(since) < 600*time.Millisecond {
		if n := len(s.invocations(fid)); n != last {
			last, since = n, time.Now()
		}
		time.Sleep(50 * time.Millisecond)
	}
	return last
}

func get(addr, target string, headers ...string) string {
	return "GET " + target + " HTTP/1.1\r\nHost: e2e.test\r\n" + strings.Join(headers, "") + "Connection: close\r\n\r\n"
}

// The path is carried decoded, once: the router and the guest see the same
// text, and nothing decodes it again.
func TestThePathIsDecodedOnceFromTheClientToTheGuest(t *testing.T) {
	s := newStack(t)
	_, addr, _ := s.fidelityStack(t)

	other := s.createFunction("fidelity-other")
	s.uploadVersion(other, s.wasm, nil, true)
	s.route("*", "/pathinfo/a", other) // a more specific route, on whole segments of the decoded path
	s.waitServing("e2e.test", "/echo")

	for _, tc := range []struct{ target, path string }{
		{"/pathinfo/caf%C3%A9/%E6%97%A5%E6%9C%AC", "/pathinfo/café/日本"},
		{"/pathinfo/a%20b", "/pathinfo/a b"},
		{"/pathinfo/a%25b", "/pathinfo/a%b"}, // a % is a %: nothing decodes twice
		{"/pathinfo/%2561", "/pathinfo/%61"}, // %25 then 61: still not 'a'
		{"/pathinfo/%41", "/pathinfo/A"},
		{"/pathinfo/x/./y", "/pathinfo/x/./y"}, // nothing is cleaned
		{"/pathinfo//x", "/pathinfo//x"},
		{"/pathinfo/q%3Fr", "/pathinfo/q?r"},
	} {
		r := rawHTTP(t, addr, get(addr, tc.target))
		if r.status != 200 || string(r.body) != tc.path {
			t.Errorf("GET %s: status %d, the guest saw %q, want %q", tc.target, r.status, r.body, tc.path)
		}
	}

	t.Run("the router matches the decoded path", func(t *testing.T) {
		before := len(s.invocations(other))
		// /pathinfo%2Fa/x decodes to /pathinfo/a/x: it belongs to the more specific route.
		r := rawHTTP(t, addr, get(addr, "/pathinfo%2Fa/x"))
		if r.status != 200 || string(r.body) != "/pathinfo/a/x" {
			t.Fatalf("status %d, body %q", r.status, r.body)
		}
		eventually(t, "the record of the invocation", 10*time.Second, func() bool { return len(s.invocations(other)) == before+1 })
	})

	t.Run("the query stays raw", func(t *testing.T) {
		r := rawHTTP(t, addr, get(addr, "/env?p=%41&q=%2F&r=%ff"))
		if r.status != 200 || !strings.Contains(string(r.body), "QUERY_STRING=p=%41&q=%2F&r=%ff\n") || !strings.Contains(string(r.body), "PATH_INFO=/env\n") {
			t.Errorf("status %d, env:\n%s", r.status, r.body)
		}
	})
}

// What is not text, or not ASCII where it must be, is refused at the edge and
// never becomes an invocation.
func TestARequestThatIsNotWellFormedNeverBecomesAnInvocation(t *testing.T) {
	s := newStack(t)
	_, addr, fid := s.fidelityStack(t)
	before := settledRecords(s, fid)

	for _, target := range []string{
		"/%ff", "/%c3", "/a%00b", "/a%0Ab", "/%7f", "/ok/%e2%28%a1", // paths that do not decode to text
		"/caf\xc3\xa9", "/\xff", "/?q=\xff", "/?q=caf\xc3\xa9", "/a\x01b", // targets that are not ASCII
	} {
		r := rawHTTP(t, addr, get(addr, target))
		if r.status != 400 {
			t.Errorf("GET %q: status %d, want 400", target, r.status)
		}
	}
	r := rawHTTP(t, addr, get(addr, "/env", "X-Bad: a\x00b\r\n"))
	if r.status != 400 {
		t.Errorf("a NUL in a header value: status %d, want 400", r.status)
	}
	// Nothing was forwarded, so nothing was recorded; and the stack is as healthy as before.
	if got := settledRecords(s, fid); got != before {
		t.Errorf("%d invocation records came of requests that were refused", got-before)
	}
	if r := rawHTTP(t, addr, get(addr, "/pathinfo/ok")); r.status != 200 || string(r.body) != "/pathinfo/ok" {
		t.Errorf("the stack is not healthy afterwards: %d %q", r.status, r.body)
	}
}

// A request header value is bytes, and it reaches the guest as the bytes.
func TestRequestHeaderValuesReachTheGuestAsBytes(t *testing.T) {
	s := newStack(t)
	_, addr, _ := s.fidelityStack(t)
	r := rawHTTP(t, addr, get(addr, "/env",
		"X-Latin: caf\xe9\r\n", "X-Junk: \xff\xfe\x80\r\n", "X-Pad:   padded \t\r\n", "X-Dup: one\r\n", "X-Dup: \xe9two\r\n", "X-Tab: a\tb\r\n"))
	if r.status != 200 {
		t.Fatalf("status %d: a header value that is not text must not make a request fail", r.status)
	}
	for _, want := range []string{"HTTP_X_LATIN=caf\xe9\n", "HTTP_X_JUNK=\xff\xfe\x80\n", "HTTP_X_PAD=padded\n", "HTTP_X_DUP=one, \xe9two\n", "HTTP_X_TAB=a\tb\n"} {
		if !strings.Contains(string(r.body), want) {
			t.Errorf("the guest's environment lacks %q:\n%q", want, r.body)
		}
	}
}

// A response header value is bytes too, comes back unchanged, and a control
// character in one is the guest's violation of the ABI, never a split response.
func TestResponseHeaderValuesComeBackAsBytes(t *testing.T) {
	s := newStack(t)
	_, addr, _ := s.fidelityStack(t)

	r := rawHTTP(t, addr, get(addr, "/rawheaders"))
	if r.status != 200 {
		t.Fatalf("status %d", r.status)
	}
	for name, want := range map[string][]string{
		"X-Latin": {"caf\xe9"}, "X-Junk": {"\xff\xfe\x80"}, "X-Dup": {"one", "\xe9two"},
		"X-Nbsp": {"\u00a0v\u00a0"}, // only space and tab are trimmed, not the Unicode spaces
	} {
		if got := r.header[name]; fmt.Sprintf("%q", got) != fmt.Sprintf("%q", want) {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}

	bad := rawHTTP(t, addr, get(addr, "/ctlheader"))
	if bad.status != 502 {
		t.Errorf("a control character in a response header: status %d, want 502 (the function broke the ABI)", bad.status)
	}
	if _, ok := bad.header["X-Bad"]; ok {
		t.Errorf("the offending header reached the client: %q", bad.header["X-Bad"])
	}
}
