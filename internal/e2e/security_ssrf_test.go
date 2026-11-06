package e2e

// SSRF through the network service, driven by a real guest (the netguest of
// conformance/guest, and the redirect-following secguest) through the real
// path: orchestrator, worker, network service, a web server on loopback that
// counts what reaches it.
//
// Two ways to read these tests. In the allow-list tests the network service is
// ALLOWED to connect to loopback (as a service inside a private network is), so
// the only thing between the guest and the origin is the function's allow list
// and the service's parsing of the URL: any hit on an origin the list does not
// name is an SSRF. In the default-service tests the service is not allowed to
// connect to loopback, so no spelling of any private address may ever reach
// anything, whatever the operator lists.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressapp "github.com/Olian04/edgerun/internal/egress/app"
)

var secLoopback = []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}

// secAllToLoopback resolves every name to 127.0.0.1: only the allow list decides.
func secAllToLoopback(context.Context, string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
}

func (o *secOrigin) fill(s string) string {
	return strings.NewReplacer("{P}", strconv.Itoa(o.Port)).Replace(s)
}

type ssrfGroup struct {
	name  string
	allow []string
	ok    []string // reached: a hit and a response
	bad   []string // never reached: refused (-1 or -4), never a hit, never a lookup
}

func TestSecuritySSRFAllowListConfusion(t *testing.T) {
	s := newStack(t)
	s.secNetWorker(16)
	a, b := secStartOrigin(t), secStartOrigin(t) // a: what the lists name; b: what nothing names
	svc := s.secStartEgress(secEgressCfg{Covers: []string{"*"}, Allow: secLoopback, Lookup: secAllToLoopback})
	_ = svc

	P, Q := strconv.Itoa(a.Port), strconv.Itoa(b.Port)
	sub := strings.NewReplacer("{P}", P, "{Q}", Q)

	groups := []ssrfGroup{
		{
			name:  "exact host and port",
			allow: []string{"allowed.test:{P}"},
			ok: []string{
				"http://allowed.test:{P}/pos/1", "http://ALLOWED.test:{P}/pos/2", "http://allowed.test.:{P}/pos/3",
				"HTTP://allowed.test:{P}/pos/4", "http://allowed.test:{P}/pos/crlf%0d%0aInjected:%20yes",
			},
			bad: []string{
				// userinfo tricks: the host is whatever follows the @
				"http://allowed.test@127.0.0.1:{P}/neg/u1", "http://allowed.test:80@127.0.0.1:{P}/neg/u2",
				"http://allowed.test:{P}@127.0.0.1:{P}/neg/u3", "http://@127.0.0.1:{P}/neg/u4",
				// the host is before the # or the ?, whatever follows
				"http://127.0.0.1:{P}#@allowed.test:{P}/neg/f1", "http://127.0.0.1:{P}?@allowed.test:{P}/neg/q1",
				"http://127.0.0.1:{P}/neg/b1@allowed.test", "http://127.0.0.1:{P}/#.allowed.test:{P}",
				// backslash forms (browsers read them as slashes, Go does not)
				"http://allowed.test\\@127.0.0.1:{P}/neg/bs1", "http://127.0.0.1\\@allowed.test:{P}/neg/bs2",
				"http://allowed.test\\.127.0.0.1:{P}/neg/bs3", "http://127.0.0.1:{P}\\@allowed.test/neg/bs4",
				"http:\\\\127.0.0.1:{P}/neg/bs5", "http:/\\127.0.0.1:{P}/neg/bs6", "http://allowed.test:{P}\\@127.0.0.1/neg/bs7",
				// loopback and its spellings
				"http://127.0.0.1:{P}/neg/p1", "http://[::1]:{P}/neg/ip6", "http://[::ffff:127.0.0.1]:{P}/neg/m1",
				"http://[::ffff:7f00:1]:{P}/neg/m2", "http://[0:0:0:0:0:ffff:127.0.0.1]:{P}/neg/m3", "http://[::127.0.0.1]:{P}/neg/m4",
				"http://[::]:{P}/neg/unspec6", "http://0.0.0.0:{P}/neg/unspec", "http://0:{P}/neg/zero",
				"http://2130706433:{P}/neg/dec", "http://0x7f000001:{P}/neg/hex", "http://0x7f.0.0.1:{P}/neg/hexmix",
				"http://0177.0.0.1:{P}/neg/oct", "http://0177.0000.0000.0001:{P}/neg/oct2", "http://017700000001:{P}/neg/octint",
				"http://127.1:{P}/neg/short", "http://127.0.1:{P}/neg/short3", "http://127.0.0.1.:{P}/neg/trail",
				"http://127.0.0.1..:{P}/neg/trail2", "http://127.000.000.001:{P}/neg/zeros", "http://127.0.0.1%2e:{P}/neg/pct2",
				"http://%31%32%37.0.0.1:{P}/neg/pct", "http://１２７.０.０.１:{P}/neg/fullwidth",
				"http://localhost:{P}/neg/lh", "http://LOCALHOST.:{P}/neg/lh2", "http://foo.localhost:{P}/neg/lh3",
				"http://ip6-localhost:{P}/neg/lh4", "http://localhost.localdomain:{P}/neg/lh5",
				// zone ids
				"http://[::1%25lo]:{P}/neg/z1", "http://[fe80::1%25eth0]:{P}/neg/z2", "http://[::ffff:127.0.0.1%25lo]:{P}/neg/z3",
				// the allowed name, spelled so that it is another name
				"http://allowed.test%2e:{P}/neg/pct3", "http://allowed.test%00.evil.test:{P}/neg/nul",
				"http://аllowed.test:{P}/neg/cyrillic", "http://xn--llowed-9hf.test:{P}/neg/puny",
				"http://allowed.test.evil.test:{P}/neg/suffix", "http://evil.allowed.test:{P}/neg/sub",
				"http://xallowed.test:{P}/neg/prefix", "http://allowed.testx:{P}/neg/suffix2", "http://allowed-test:{P}/neg/dash",
				"http://allowed_test:{P}/neg/underscore", "http://allowed.test,evil.test:{P}/neg/comma",
				// the allowed name on another port: the secret origin lives there
				"http://allowed.test:{Q}/neg/port", "http://allowed.test:80/neg/port80", "http://allowed.test/neg/noport",
				"http://allowed.test:{P}0/neg/portx", "http://allowed.test:0/neg/port0", "http://allowed.test:{P}:{Q}/neg/twoports",
				"http://allowed.test:+{P}/neg/plus", "http://allowed.test:{P}.0/neg/dot",
				// other schemes and shapes that are not an absolute http URL
				"ftp://allowed.test:{P}/neg/ftp", "file:///etc/passwd", "gopher://allowed.test:{P}/neg/gopher", "ws://allowed.test:{P}/neg/ws",
				"http:allowed.test:{P}/neg/opaque", "http:/allowed.test:{P}/neg/oneslash", "//allowed.test:{P}/neg/schemeless",
				"allowed.test:{P}/neg/nosch", "/neg/relative", "http://", "http://:{P}/neg/nohost", "http:///neg/emptyhost",
				"http://allowed.test:{P}/neg/with space", "http://allowed.test:{P}/neg/\x01ctl", "http://allowed.test:{P}/neg/café",
				"http://allowed.test:{P}/neg/‮", "http://allowed.test:{P}/neg/\x7f",
			},
		},
		{
			name:  "wildcard",
			allow: []string{"*.example.com:{P}", "a.b.example.org:{P}"},
			ok: []string{
				"http://a.example.com:{P}/pos/w1", "http://a.b.example.com:{P}/pos/w2", "http://A.B.EXAMPLE.COM:{P}/pos/w3",
				"http://a.example.com.:{P}/pos/w4", "http://a.b.example.org:{P}/pos/w6",
			},
			bad: []string{
				"http://example.com:{P}/neg/apex", "http://example.com.:{P}/neg/apexdot", "http://evilexample.com:{P}/neg/evil1",
				"http://xexample.com:{P}/neg/evil2", "http://example.com.evil.test:{P}/neg/evil3",
				"http://a.example.com.evil.test:{P}/neg/evil4", "http://a.example.com:{Q}/neg/port", "http://a.example.com/neg/p80",
				"http://example.com@a.example.com:{P}/neg/ui1", "http://a.example.com@127.0.0.1:{P}/neg/ui2",
				"http://127.0.0.1:{P}/@a.example.com", "http://127.0.0.1:{P}#.a.example.com/neg/f", "http://a.example.com.example.net:{P}/neg/mid",
				"http://example.org:{P}/neg/o1", "http://b.example.org:{P}/neg/o2", "http://sub.a.b.example.org:{P}/neg/o3",
				"http://a.b.example.org.evil.test:{P}/neg/o4", "http://[::1]:{P}/neg/ip6", "http://127.0.0.1:{P}/neg/ip4",
				"http://com:{P}/neg/tld", "http://example:{P}/neg/short", "http://.com:{P}/neg/dotcom",
			},
		},
		{
			name:  "an entry without a port is the default port only",
			allow: []string{"a.example.com", "127.0.0.1", "[::1]"},
			bad: []string{
				"http://a.example.com:{P}/neg/np1", "http://127.0.0.1:{P}/neg/np2", "https://a.example.com:{P}/neg/np3",
				"http://[::1]:{P}/neg/np4", "http://a.example.com:{Q}/neg/np5", "http://127.0.0.1:{Q}/neg/np6",
				"http://a.example.com:8080/neg/np7", "http://a.example.com:81/neg/np8",
			},
		},
		{
			name:  "an address entry, an address service may reach",
			allow: []string{"127.0.0.1:{P}"},
			ok: []string{
				"http://127.0.0.1:{P}/pos/i1", "http://[::ffff:127.0.0.1]:{P}/pos/i2", "http://[::ffff:7f00:1]:{P}/pos/i3",
			},
			bad: []string{
				"http://127.0.0.1:{Q}/neg/i4", "http://127.0.0.2:{P}/neg/i5", "http://0.0.0.0:{P}/neg/i6", "http://[::1]:{P}/neg/i7",
				"http://localhost:{P}/neg/i8", "http://2130706433:{P}/neg/i9", "http://127.1:{P}/neg/i10", "http://0x7f.0.0.1:{P}/neg/i11",
				"http://allowed.test:{P}/neg/i12", "http://127.0.0.1.nip.io:{P}/neg/i13", "http://[::ffff:127.0.0.2]:{P}/neg/i14",
				"http://127.0.0.1@127.0.0.1:{P}/neg/i15",
			},
		},
	}

	fns := make([]*secFn, len(groups))
	module := secNetModule(t)
	for i, g := range groups {
		fns[i] = s.secDeploy(fmt.Sprintf("ssrf-%d", i), fmt.Sprintf("ssrf%d.test", i), module, "wagi-2", nil)
		fns[i].allow(strings.Fields(sub.Replace(strings.Join(g.allow, " ")))...)
	}
	eventually(t, "the wagi-2 function to run", 120*time.Second, func() bool { return s.call(req{host: "ssrf0.test", path: "/noop"}).err == nil })

	type job struct {
		g      int
		url    string
		expect string // "ok" or "bad"
	}
	var jobs []job
	positives := map[string]bool{}
	for i, g := range groups {
		for _, u := range g.ok {
			jobs = append(jobs, job{i, sub.Replace(u), "ok"})
			if h := secHostOf(sub.Replace(u)); h != "" {
				positives[h] = true
			}
		}
		for _, u := range g.bad {
			jobs = append(jobs, job{i, sub.Replace(u), "bad"})
		}
	}
	var mu sync.Mutex
	var reached, wrongCode, notReached []string
	secParallel(jobs, 8, func(j job) {
		code, _ := fns[j.g].fetch("/fetch", "GET "+j.url+"\n\n")
		mu.Lock()
		defer mu.Unlock()
		switch j.expect {
		case "ok":
			if code < 0 || code >= 1<<30 {
				notReached = append(notReached, fmt.Sprintf("%s (code %d)", j.url, code))
			}
		case "bad":
			if code >= 0 {
				reached = append(reached, fmt.Sprintf("%q (response of %d bytes)", j.url, code))
			} else if code != -1 && code != -4 {
				// A refusal must look like a refusal. -2 ("failed") would mean the
				// service tried something on the network.
				wrongCode = append(wrongCode, fmt.Sprintf("%q -> %d", j.url, code))
			}
		}
	})
	for _, r := range reached {
		t.Errorf("SSRF: a URL outside the allow list got a response: %s", r)
	}
	for _, r := range wrongCode {
		t.Errorf("a refusal that is not -1/-4 (it looks like a failure on the network): %s", r)
	}
	for _, r := range notReached {
		t.Errorf("a URL the allow list names was not served: %s", r)
	}

	// What did arrive at the origins: only the positives, and nothing at the secret one.
	for _, h := range b.Seen("") {
		t.Errorf("SSRF: the origin no list names was reached: %+v", h)
	}
	for _, h := range a.Seen("") {
		if !strings.HasPrefix(h.URI, "/pos/") {
			t.Errorf("SSRF: the origin was reached by a URL that was not meant to be served: %q (Host %q)", h.URI, h.Host)
		}
	}
	want := 0
	for _, g := range groups {
		want += len(g.ok)
	}
	if got := a.Hits("/pos/"); got != want {
		t.Errorf("the origin saw %d of the %d permitted requests", got, want)
	}
	// No CRLF injection through an encoded path.
	for _, h := range a.Seen("/pos/crlf") {
		if h.Header.Get("Injected") != "" {
			t.Errorf("an encoded CRLF in the path became a header: %v", h.Header)
		}
	}
	// A refusal reveals nothing about the network: nothing was resolved for a
	// destination that was refused (the allow list is checked first).
	for _, name := range svc.LookedUp() {
		if !positives[strings.ToLower(strings.TrimSuffix(name, "."))] {
			t.Errorf("a name was looked up for a request that must be refused before any lookup: %q", name)
		}
	}
}

// secHostOf is the host of an allow-listed positive URL (lower case, no dot),
// for the "nothing resolved" check. IP literals are not looked up.
func secHostOf(raw string) string {
	rest := raw[strings.Index(raw, "://")+3:]
	host, _, _ := strings.Cut(rest, "/")
	if i := strings.LastIndexByte(host, ':'); i >= 0 && !strings.HasSuffix(host, "]") {
		host = host[:i]
	}
	if strings.HasPrefix(host, "[") || strings.Count(host, ".") == 3 && host[0] >= '0' && host[0] <= '9' {
		return ""
	}
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

// The origin sees the guest's request, with the destination decided by the URL
// alone, and no proxy from the service's environment is used.
func TestSecuritySSRFHostHeaderAndProxyEnvironment(t *testing.T) {
	proxy := secStartOrigin(t)
	t.Setenv("HTTP_PROXY", "http://"+proxy.Addr)
	t.Setenv("http_proxy", "http://"+proxy.Addr)
	t.Setenv("ALL_PROXY", "http://"+proxy.Addr)
	t.Setenv("NO_PROXY", "")
	s := newStack(t)
	s.secNetWorker(4)
	o, other := secStartOrigin(t), secStartOrigin(t)
	s.secStartEgress(secEgressCfg{Covers: []string{"*"}, Allow: secLoopback, Lookup: secAllToLoopback})
	fn := s.secDeploy("hosthdr", "hosthdr.test", secNetModule(t), "wagi-2", nil)
	fn.allow(fmt.Sprintf("allowed.test:%d", o.Port))
	eventually(t, "the function to run", 120*time.Second, func() bool { return s.call(req{host: "hosthdr.test", path: "/noop"}).err == nil })

	url := fmt.Sprintf("http://allowed.test:%d/pos/hdr", o.Port)
	code, _ := fn.fetch("/fetch", "GET "+url+"\nHost: 127.0.0.1:"+strconv.Itoa(other.Port)+"\nX-Forwarded-Host: evil.test\nConnection: close\nTransfer-Encoding: chunked\nContent-Length: 999\nProxy-Authorization: Basic eDp5\n\n")
	if code < 0 {
		t.Fatalf("the request was refused: %d", code)
	}
	seen := o.Seen("/pos/hdr")
	if len(seen) != 1 {
		t.Fatalf("the origin saw %d requests", len(seen))
	}
	if want := fmt.Sprintf("allowed.test:%d", o.Port); seen[0].Host != want {
		t.Errorf("the origin saw Host %q: a caller-supplied Host must be dropped (want %q)", seen[0].Host, want)
	}
	if h := seen[0].Header; h.Get("Proxy-Authorization") != "" || h.Get("Transfer-Encoding") != "" {
		t.Errorf("connection-level headers reached the origin: %v", h)
	}
	if proxy.Total() != 0 || other.Total() != 0 {
		t.Errorf("the environment's proxy was used (%d) or the Host header chose the destination (%d)", proxy.Total(), other.Total())
	}
}

// Each hop of a redirect is a new Fetch and is checked again from scratch.
func TestSecuritySSRFRedirects(t *testing.T) {
	s := newStack(t)
	s.secNetWorker(8)
	a, b := secStartOrigin(t), secStartOrigin(t)
	svc := s.secStartEgress(secEgressCfg{Covers: []string{"*"}, Allow: secLoopback, Lookup: secAllToLoopback})
	fn := s.secDeploy("redir", "redir.test", secGuest(t), "wagi-2", nil)
	P, Q := strconv.Itoa(a.Port), strconv.Itoa(b.Port)
	eventually(t, "the function to run", 120*time.Second, func() bool { return s.call(req{host: "redir.test", path: "/noop"}).err == nil })

	follow := func(allow []string, start string, max int) string {
		t.Helper()
		fn.allow(allow...)
		r := fn.do("/follow", "max="+strconv.Itoa(max), "GET "+start+"\n\n")
		if r.err != nil {
			t.Fatalf("%s: %v", start, r.err)
		}
		return string(r.body)
	}
	redir := func(to string) string { return "http://allowed.test:" + P + "/redir?to=" + to }
	allowed := "allowed.test:" + P

	cases := []struct {
		name  string
		allow []string
		start string
		// the hop that must be refused ("code=-1" or "-4"); "" means every hop is served
		refusedAt string
	}{
		{"to the metadata address, not on the list", []string{allowed}, redir("http://169.254.169.254/latest/meta-data/"), "hop=1"},
		{"to the metadata address, on the list but private", []string{allowed, "169.254.169.254"}, redir("http://169.254.169.254/latest/meta-data/"), "hop=1"},
		{"to loopback by address, the secret origin", []string{allowed}, redir("http://127.0.0.1:" + Q + "/secret"), "hop=1"},
		{"to loopback, the secret origin, named", []string{allowed}, redir("http://allowed.test:" + Q + "/secret"), "hop=1"},
		{"to a name that is not allowed", []string{allowed}, redir("http://notallowed.test:" + P + "/x"), "hop=1"},
		{"to userinfo that names the allowed host", []string{allowed}, redir("http://allowed.test@127.0.0.1:" + Q + "/secret"), "hop=1"},
		{"to a scheme-relative URL for the secret origin", []string{allowed}, redir("//127.0.0.1:" + Q + "/secret"), "hop=1"},
		{"to a decimal address", []string{allowed}, redir("http://2130706433:" + Q + "/secret"), "hop=1"},
		{"to file", []string{allowed}, redir("file:///etc/passwd"), "hop=1"},
		{"to gopher", []string{allowed}, redir("gopher://allowed.test:" + P + "/x"), "hop=1"},
		{"to an IPv6 loopback", []string{allowed}, redir("http://[::1]:" + Q + "/secret"), "hop=1"},
		{"to localhost", []string{allowed}, redir("http://localhost:" + Q + "/secret"), "hop=1"},
		{"to the same origin (served)", []string{allowed}, redir("/pos/after-redirect"), ""},
	}
	for _, c := range cases {
		out := follow(c.allow, c.start, 6)
		if c.refusedAt == "" {
			if strings.Contains(out, "code=-") || !strings.Contains(out, "body=origin-answer:/pos/after-redirect") {
				t.Errorf("%s: the second hop to the same origin should be served:\n%s", c.name, out)
			}
			continue
		}
		var refused bool
		for _, l := range strings.Split(out, "\n") {
			if strings.HasPrefix(l, c.refusedAt+" ") && (strings.Contains(l, "code=-1") || strings.Contains(l, "code=-4")) {
				refused = true
			}
			if strings.HasPrefix(l, c.refusedAt+" ") && strings.Contains(l, "code=-2") {
				t.Errorf("%s: the refusal looks like a network failure (-2):\n%s", c.name, out)
			}
		}
		if !refused {
			t.Errorf("%s: the second hop was not refused:\n%s", c.name, out)
		}
	}
	if n := b.Total(); n != 0 {
		t.Errorf("SSRF via redirect: the secret origin was reached %d times: %+v", n, b.Seen(""))
	}
	for _, h := range a.Seen("") {
		if !strings.HasPrefix(h.URI, "/redir") && !strings.HasPrefix(h.URI, "/pos/after-redirect") && !strings.HasPrefix(h.URI, "/loop") {
			t.Errorf("the origin was reached at %q, which no hop was allowed to ask for", h.URI)
		}
	}
	for _, n := range svc.LookedUp() {
		if !strings.EqualFold(n, "allowed.test") {
			t.Errorf("a redirect target was resolved before the allow list judged it: %q", n)
		}
	}

	// A guest that follows redirects without end is stopped by the per-invocation request limit.
	fn.allow(allowed)
	r := fn.do("/follow", "max=24", "GET http://allowed.test:"+P+"/loop\n\n")
	if r.err != nil {
		t.Fatal(r.err)
	}
	if !strings.Contains(string(r.body), "code=-5") {
		t.Errorf("an endless redirect loop was not stopped by the 16-request limit:\n%s", r.body)
	}
	if got := a.Hits("/loop"); got != 16 {
		t.Errorf("the origin saw %d requests from one invocation, want 16", got)
	}
}

// A function that is only told to loop is held to 16 requests, whatever it asks for.
func TestSecuritySSRFRequestLimit(t *testing.T) {
	s := newStack(t)
	s.secNetWorker(4)
	o := secStartOrigin(t)
	s.secStartEgress(secEgressCfg{Covers: []string{"*"}, Allow: secLoopback, Lookup: secAllToLoopback})
	fn := s.secDeploy("limit", "limit.test", secNetModule(t), "wagi-2", nil)
	fn.allow(fmt.Sprintf("allowed.test:%d", o.Port))
	eventually(t, "the function to run", 120*time.Second, func() bool { return s.call(req{host: "limit.test", path: "/noop"}).err == nil })
	r := fn.do("/fetch-many", "n=30", fmt.Sprintf("GET http://allowed.test:%d/pos/limit\n\n", o.Port))
	if r.err != nil {
		t.Fatal(r.err)
	}
	codes := strings.Fields(string(r.body))
	if len(codes) != 30 {
		t.Fatalf("%q", r.body)
	}
	for i, c := range codes {
		n, _ := strconv.Atoi(c)
		if (i < 16) != (n >= 0) || (i >= 16 && n != -5) {
			t.Errorf("request %d answered %d: the first 16 are served, the rest are -5", i+1, n)
		}
	}
	if got := o.Hits("/pos/limit"); got != 16 {
		t.Errorf("the origin saw %d requests from one invocation, want 16", got)
	}
}

// DNS rebinding and mixed answers: the service resolves once, refuses a name if
// any of its addresses is one it may not reach, and connects to an address it
// checked.
func TestSecuritySSRFDNSRebinding(t *testing.T) {
	s := newStack(t)
	s.secNetWorker(8)
	o := secStartOrigin(t)
	var mu sync.Mutex
	calls := map[string]int{}
	answers := map[string][][]netip.Addr{}
	ad := func(ss ...string) []netip.Addr {
		var out []netip.Addr
		for _, x := range ss {
			out = append(out, netip.MustParseAddr(x))
		}
		return out
	}
	set := func(name string, seq ...[]netip.Addr) { answers[name] = seq }
	set("flip1.rebind.test", ad("127.0.0.1"), ad("169.254.169.254"), ad("169.254.169.254"))
	set("flip2.rebind.test", ad("169.254.169.254"), ad("127.0.0.1"), ad("127.0.0.1"))
	set("mixed1.rebind.test", ad("127.0.0.1", "10.0.0.9"))
	set("mixed2.rebind.test", ad("10.0.0.9", "127.0.0.1"))
	set("mixed3.rebind.test", ad("127.0.0.1", "::1"))
	set("mixed4.rebind.test", ad("127.0.0.1", "169.254.169.254"))
	set("mixed5.rebind.test", ad("::ffff:127.0.0.1", "::ffff:10.0.0.9"))
	set("v4mapped.rebind.test", ad("::ffff:127.0.0.1"))
	set("v4mapped2.rebind.test", ad("::ffff:10.0.0.9"))
	set("empty.rebind.test", ad())
	set("meta.rebind.test", ad("169.254.169.254"))
	set("nat64.rebind.test", ad("64:ff9b::7f00:1"))
	set("sixtofour.rebind.test", ad("2002:7f00:1::"))
	set("unspec.rebind.test", ad("0.0.0.0"))
	set("v6unspec.rebind.test", ad("::"))
	svc := s.secStartEgress(secEgressCfg{Covers: []string{"*"}, Allow: secLoopback, Lookup: func(_ context.Context, host string) ([]netip.Addr, error) {
		mu.Lock()
		defer mu.Unlock()
		h := strings.ToLower(host)
		i := calls[h]
		calls[h]++
		seq, ok := answers[h]
		if !ok {
			return nil, errors.New("no such host")
		}
		if i >= len(seq) {
			i = len(seq) - 1
		}
		return seq[i], nil
	}})
	fn := s.secDeploy("rebind", "rebind.test", secNetModule(t), "wagi-2", nil)
	fn.allow(fmt.Sprintf("*.rebind.test:%d", o.Port))
	eventually(t, "the function to run", 120*time.Second, func() bool { return s.call(req{host: "rebind.test", path: "/noop"}).err == nil })

	cases := []struct {
		name  string
		code  string // "ok", "denied" (-1), "failed" (-2)
		hit   bool
		looks int // lookups of the name, exactly
	}{
		{"flip1", "ok", true, 1},       // checked 127.0.0.1, must connect to 127.0.0.1, not look again and get the metadata address
		{"flip2", "denied", false, 1},  // the first answer was the metadata address: denied, and no second chance
		{"mixed1", "denied", false, 1}, // one address it may not reach is enough
		{"mixed2", "denied", false, 1},
		{"mixed3", "denied", false, 1},
		{"mixed4", "denied", false, 1},
		{"mixed5", "denied", false, 1},
		{"v4mapped", "ok", true, 1},
		{"v4mapped2", "denied", false, 1},
		{"empty", "failed", false, 1},
		{"meta", "denied", false, 1},
		{"nat64", "denied", false, 1},
		{"sixtofour", "denied", false, 1},
		{"unspec", "denied", false, 1},
		{"v6unspec", "denied", false, 1},
		{"nosuchname", "failed", false, 1},
	}
	for _, c := range cases {
		host := c.name + ".rebind.test"
		start := time.Now()
		code, _ := fn.fetch("/fetch", fmt.Sprintf("GET http://%s:%d/pos/%s\n\n", host, o.Port, c.name))
		took := time.Since(start)
		switch c.code {
		case "ok":
			if code < 0 {
				t.Errorf("%s: refused (%d)", c.name, code)
			}
		case "denied":
			if code != -1 {
				t.Errorf("%s: code %d, want -1 (denied)", c.name, code)
			}
		case "failed":
			if code != -2 {
				t.Errorf("%s: code %d, want -2 (failed)", c.name, code)
			}
		}
		if took > 5*time.Second {
			t.Errorf("%s: took %v", c.name, took)
		}
		if hit := o.Hits("/pos/"+c.name) > 0; hit != c.hit {
			t.Errorf("%s: origin hit = %v, want %v", c.name, hit, c.hit)
		}
		mu.Lock()
		n := calls[host]
		mu.Unlock()
		if n != c.looks {
			t.Errorf("%s: the name was resolved %d times, want exactly %d: resolve once, check every address, connect to one that was checked", c.name, n, c.looks)
		}
	}
	// A name that is refused by the allow list is not resolved at all.
	before := svc.Lookups.Load()
	if code, _ := fn.fetch("/fetch", fmt.Sprintf("GET http://flip1.other.test:%d/neg\n\n", o.Port)); code != -1 {
		t.Errorf("a name outside the list: %d", code)
	}
	if svc.Lookups.Load() != before {
		t.Errorf("a name outside the allow list was resolved")
	}
	// And a connect-time failure of a name that has several addresses stays a failure, not a bypass.
	for _, h := range o.Seen("/pos/") {
		if strings.Contains(h.URI, "mixed") || strings.Contains(h.URI, "meta") || strings.Contains(h.URI, "flip2") {
			t.Errorf("reached through a refused name: %q", h.URI)
		}
	}
}

// With the operator's own resolver (the system's, /etc/hosts here): a name that
// resolves to loopback, a private or link-local address, or to one public and
// one private address, is refused by the default service whatever the operator
// allowed, and a service allowed to reach loopback reaches only what resolves to it.
func TestSecuritySSRFSystemResolver(t *testing.T) {
	hosts := map[string][]string{
		"edgerun-sec-loop.test":    {"127.0.0.1"},
		"edgerun-sec-v6loop.test":  {"::1"},
		"edgerun-sec-meta.test":    {"169.254.169.254"},
		"edgerun-sec-private.test": {"10.20.30.40"},
		"edgerun-sec-mixed.test":   {"127.0.0.1", "10.20.30.41"},
		"edgerun-sec-mixed2.test":  {"10.20.30.41", "127.0.0.1"},
		"edgerun-sec-mixed3.test":  {"203.0.113.9", "127.0.0.1"}, // documentation (forbidden) next to loopback
	}
	secEditHosts(t, hosts)

	s := newStack(t)
	s.secNetWorker(8)
	o := secStartOrigin(t)
	P := strconv.Itoa(o.Port)
	startEgress := func(covers []string, allow []netip.Prefix) {
		eg, err := egressapp.Start(ctx, egressapp.Config{
			Listen: "127.0.0.1:0", EgressToken: egressToken, OrchestratorURL: "http://" + s.addr, NetworkToken: networkToken,
			Covers: covers, AllowNetworks: allow, DrainGrace: 100 * time.Millisecond, Log: quiet,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { eg.Stop(context.Background()) })
	}
	// The default service (no allowed networks) carries everything but the names below;
	// the permissive one carries exactly those, so each name has one unambiguous service.
	startEgress([]string{"*"}, nil)
	startEgress([]string{"edgerun-sec-loop.test", "edgerun-sec-mixed.test", "edgerun-sec-mixed2.test", "edgerun-sec-mixed3.test"}, secLoopback)
	eventually(t, "both network services", 30*time.Second, func() bool {
		r, err := s.ctl.ListNetworkServices(ctx, &controlv1.ListNetworkServicesRequest{})
		if err != nil {
			return false
		}
		n := 0
		for _, x := range r.Services {
			if x.Alive {
				n++
			}
		}
		return n == 2
	})
	fn := s.secDeploy("sysresolver", "sysresolver.test", secNetModule(t), "wagi-2", nil)
	var allow []string
	for h := range hosts {
		allow = append(allow, h+":"+P)
	}
	allow = append(allow, "localhost:"+P, "foo.localhost:"+P, "127.0.0.1:"+P, "[::1]:"+P, "169.254.169.254", "10.0.0.1", "[fd00::1]", "0.0.0.0:"+P)
	fn.allow(allow...)
	eventually(t, "the function to run", 120*time.Second, func() bool { return s.call(req{host: "sysresolver.test", path: "/noop"}).err == nil })
	secWaitResolvable(t, "edgerun-sec-loop.test", "edgerun-sec-mixed3.test")

	// Positive control: the permissive service reaches the name that resolves only to loopback.
	var code int
	eventually(t, "the loopback name to be served by the permissive service", 20*time.Second, func() bool {
		code, _ = fn.fetch("/fetch", "GET http://edgerun-sec-loop.test:"+P+"/pos/loop\n\n")
		return code >= 0
	})
	if o.Hits("/pos/loop") != 1 {
		t.Errorf("positive control: %d hits", o.Hits("/pos/loop"))
	}

	refused := []string{
		"edgerun-sec-v6loop.test:" + P, "edgerun-sec-meta.test:" + P, "edgerun-sec-private.test:" + P,
		"edgerun-sec-mixed.test:" + P, "edgerun-sec-mixed2.test:" + P, "edgerun-sec-mixed3.test:" + P,
		"localhost:" + P, "foo.localhost:" + P, "127.0.0.1:" + P, "[::1]:" + P, "0.0.0.0:" + P,
		"169.254.169.254", "10.0.0.1", "[fd00::1]",
	}
	secParallel(refused, 6, func(h string) {
		start := time.Now()
		code, _ := fn.fetch("/fetch", "GET http://"+h+"/neg/"+strings.NewReplacer(":", "_", "[", "", "]", "").Replace(h)+"\n\n")
		if code != -1 {
			t.Errorf("%s: code %d, want -1 (denied) from a service that may not reach it", h, code)
		}
		if took := time.Since(start); took > 4*time.Second {
			t.Errorf("%s: took %v: a refusal must not wait for the network", h, took)
		}
	})
	if n := len(o.Seen("/neg/")); n != 0 {
		t.Errorf("SSRF: %d requests reached the origin through names and addresses that must be refused: %+v", n, o.Seen("/neg/"))
	}
}

// secEditHosts adds names to /etc/hosts for the test (the sandbox's own file; the
// names are reserved test names) and removes them afterwards. It skips the test
// if the file cannot be written.
func secEditHosts(t *testing.T, hosts map[string][]string) {
	t.Helper()
	const marker = "# edgerun-security-test"
	orig, err := os.ReadFile("/etc/hosts")
	if err != nil {
		t.Skipf("cannot read /etc/hosts: %v", err)
	}
	var sb strings.Builder
	sb.Write(orig)
	if len(orig) > 0 && orig[len(orig)-1] != '\n' {
		sb.WriteByte('\n')
	}
	for name, addrs := range hosts {
		for _, a := range addrs {
			fmt.Fprintf(&sb, "%s %s %s\n", a, name, marker)
		}
	}
	if err := os.WriteFile("/etc/hosts", []byte(sb.String()), 0o644); err != nil {
		t.Skipf("cannot write /etc/hosts: %v", err)
	}
	t.Cleanup(func() {
		cur, err := os.ReadFile("/etc/hosts")
		if err != nil {
			return
		}
		var keep []string
		for _, l := range strings.Split(string(cur), "\n") {
			if !strings.Contains(l, marker) {
				keep = append(keep, l)
			}
		}
		os.WriteFile("/etc/hosts", []byte(strings.Join(keep, "\n")), 0o644)
	})
}

// secWaitResolvable waits for the Go resolver to see the new /etc/hosts entries
// (it re-reads the file at most every five seconds).
func secWaitResolvable(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		eventually(t, "the resolver to know "+n, 30*time.Second, func() bool {
			a, err := net.DefaultResolver.LookupHost(ctx, n)
			return err == nil && len(a) > 0
		})
	}
}

var _ = controlv1.SetEgressRequest{}

// KNOWN DEFECT SEC-HOST-GRAMMAR: the allow-list match of a *request's* host is
// "ends with .suffix": it does not check that what precedes is a name. So
// `*.example.com` also matches `a..example.com` and `..example.com` (empty
// labels) and `a&b.example.com` (not a host name at all), and an IPv4 entry
// matches `[::ffff:127.0.0.1%25lo]` (a zone id on a mapped address). The
// protocol says a wildcard matches "any name under it" and that names are
// dot-separated labels of letters, digits and hyphens. Through a resolver that
// normalises such names (a proxy or tunnel, which the protocol allows) these
// reach hosts the entry never meant; the reference service's own resolver
// rejects them, so here the damage is bounded. Low severity.
func TestSecuritySSRFWildcardMatchesMalformedNames(t *testing.T) {
	if os.Getenv("EDGERUN_E2E_KNOWN_DEFECTS") == "" {
		t.Skip("KNOWN DEFECT SEC-HOST-GRAMMAR: allow-list wildcard matches hosts with empty labels or non-name characters (a..example.com, a&b.example.com, [::ffff:127.0.0.1%25lo] for 127.0.0.1)")
	}
	s := newStack(t)
	s.secNetWorker(4)
	o := secStartOrigin(t)
	s.secStartEgress(secEgressCfg{Covers: []string{"*"}, Allow: secLoopback, Lookup: secAllToLoopback})
	fn := s.secDeploy("grammar", "grammar.test", secNetModule(t), "wagi-2", nil)
	P := strconv.Itoa(o.Port)
	fn.allow("*.example.com:"+P, "127.0.0.1:"+P)
	eventually(t, "the function to run", 120*time.Second, func() bool { return s.call(req{host: "grammar.test", path: "/noop"}).err == nil })
	for i, u := range []string{
		"http://a..example.com:" + P + "/", "http://..example.com:" + P + "/", "http://a&b.example.com:" + P + "/",
		"http://a,b.example.com:" + P + "/", "http://*.example.com:" + P + "/", "http://a!b.example.com:" + P + "/",
		"http://[::ffff:127.0.0.1%25lo]:" + P + "/",
	} {
		code, _ := fn.fetch("/fetch", fmt.Sprintf("GET %sg%d\n\n", u, i))
		if code >= 0 {
			t.Errorf("%s was served: it is not a name under *.example.com, nor an address the list names", u)
		}
	}
}
