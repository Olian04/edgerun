package e2e

// The admin UI (internal/admin): sign-in, sessions, CSRF, redirects, upload and
// what it renders of text that outsiders can write.

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/html"

	"github.com/Olian04/edgerun"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/internal/admin"
	"github.com/Olian04/edgerun/internal/admin/auth"
)

type secAdminSite struct {
	s    *stack
	srv  *httptest.Server
	base string
}

// secAdmin serves the admin site in front of the stack's orchestrator.
// secure: serve it over TLS and mark cookies Secure, as a deployment behind TLS does.
func (s *stack) secAdmin(t *testing.T, secure bool) *secAdminSite {
	t.Helper()
	var users []auth.User
	for _, u := range []struct{ name, role, pw string }{{"alice", auth.RoleAdmin, "alice-pw"}, {"bob", auth.RoleViewer, "bob-pw"}} {
		h, err := auth.HashPassword(u.pw)
		if err != nil {
			t.Fatal(err)
		}
		us, err := auth.ParseUsers(u.name + ":" + u.role + ":" + h)
		if err != nil {
			t.Fatal(err)
		}
		users = append(users, us...)
	}
	h, err := admin.Handler(admin.HandlerConfig{Control: admin.NewControl(http.DefaultClient, "http://"+s.addr, adminToken),
		Users: users, SessionKey: auth.NewSessionKey(), SecureCookies: secure, Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	var srv *httptest.Server
	if secure {
		srv = httptest.NewTLSServer(h)
	} else {
		srv = httptest.NewServer(h)
	}
	t.Cleanup(srv.Close)
	return &secAdminSite{s: s, srv: srv, base: srv.URL}
}

type secBrowser struct {
	t    *testing.T
	site *secAdminSite
	c    *http.Client
}

func (a *secAdminSite) browser(t *testing.T, follow bool) *secBrowser {
	jar, _ := cookiejar.New(nil)
	tr := a.srv.Client().Transport
	if tr == nil {
		tr = &http.Transport{}
	}
	if t2, ok := tr.(*http.Transport); ok {
		t2 = t2.Clone()
		t2.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // the test server's own certificate
		tr = t2
	}
	c := &http.Client{Jar: jar, Transport: tr, Timeout: 60 * time.Second}
	if !follow {
		c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	}
	return &secBrowser{t: t, site: a, c: c}
}

type secPage struct {
	status int
	header http.Header
	body   string
	cookie []*http.Cookie
	url    string
}

func (b *secBrowser) do(r *http.Request) secPage {
	b.t.Helper()
	resp, err := b.c.Do(r)
	if err != nil {
		b.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return secPage{status: resp.StatusCode, header: resp.Header, body: string(body), cookie: resp.Cookies(), url: resp.Request.URL.String()}
}

func (b *secBrowser) get(path string, hdr ...string) secPage {
	r, _ := http.NewRequest("GET", b.site.base+path, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		r.Header.Set(hdr[i], hdr[i+1])
	}
	return b.do(r)
}

func (b *secBrowser) post(path string, form url.Values, hdr ...string) secPage {
	r, _ := http.NewRequest("POST", b.site.base+path, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for i := 0; i+1 < len(hdr); i += 2 {
		r.Header.Set(hdr[i], hdr[i+1])
	}
	return b.do(r)
}

func (b *secBrowser) signIn(user, pw string) secPage {
	return b.post("/login", url.Values{"username": {user}, "password": {pw}, "next": {"/admin/"}})
}

// --- the html a page holds ------------------------------------------------------

func secParse(t *testing.T, body string) *html.Node {
	t.Helper()
	doc, err := html.Parse(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func secWalk(n *html.Node, f func(*html.Node)) {
	f(n)
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		secWalk(c, f)
	}
}

func secAttr(n *html.Node, k string) string {
	for _, a := range n.Attr {
		if a.Key == k {
			return a.Val
		}
	}
	return ""
}

// secInjected reports where the page lets text become markup: an event-handler
// attribute anywhere, or an element that executes or embeds whose attributes or
// content carry one of the markers.
func secInjected(t *testing.T, body string, markers ...string) []string {
	t.Helper()
	var found []string
	secWalk(secParse(t, body), func(n *html.Node) {
		if n.Type != html.ElementNode {
			return
		}
		for _, a := range n.Attr {
			if strings.HasPrefix(a.Key, "on") {
				found = append(found, fmt.Sprintf("<%s %s=%q>", n.Data, a.Key, a.Val))
			}
			for _, m := range markers {
				if strings.Contains(a.Val, m) && (a.Key == "href" || a.Key == "src" || a.Key == "action" || a.Key == "formaction") &&
					strings.HasPrefix(strings.ToLower(strings.TrimSpace(a.Val)), "javascript:") {
					found = append(found, fmt.Sprintf("<%s %s=%q>", n.Data, a.Key, a.Val))
				}
			}
		}
		switch n.Data {
		case "script", "iframe", "object", "embed", "svg", "img", "style", "base", "meta", "link":
			var sb strings.Builder
			secWalk(n, func(c *html.Node) {
				if c.Type == html.TextNode {
					sb.WriteString(c.Data)
				}
			})
			all := sb.String()
			for _, a := range n.Attr {
				all += " " + a.Key + "=" + a.Val
			}
			for _, m := range markers {
				if strings.Contains(all, m) {
					found = append(found, fmt.Sprintf("<%s> carries %q", n.Data, m))
				}
			}
		}
	})
	return found
}

// ---------------------------------------------------------------------------------

func TestSecurityAdminSessionCookie(t *testing.T) {
	s := newStack(t)
	for _, secure := range []bool{false, true} {
		site := s.secAdmin(t, secure)
		b := site.browser(t, false)
		p := b.signIn("alice", "alice-pw")
		if p.status != http.StatusSeeOther {
			t.Fatalf("secure=%v: sign-in: %d", secure, p.status)
		}
		var c *http.Cookie
		for _, x := range p.cookie {
			if x.Name == "edgerun_session" {
				c = x
			}
		}
		if c == nil {
			t.Fatalf("secure=%v: no session cookie", secure)
		}
		if !c.HttpOnly {
			t.Errorf("secure=%v: the session cookie is readable by script (no HttpOnly)", secure)
		}
		if c.SameSite != http.SameSiteLaxMode && c.SameSite != http.SameSiteStrictMode {
			t.Errorf("secure=%v: SameSite = %v, want Lax or Strict", secure, c.SameSite)
		}
		if c.Secure != secure {
			t.Errorf("secure=%v: Secure = %v: behind TLS the cookie must be Secure", secure, c.Secure)
		}
		if c.Domain != "" || c.Path != "/" {
			t.Errorf("secure=%v: Domain %q Path %q: the cookie should be host-only", secure, c.Domain, c.Path)
		}
		if c.MaxAge <= 0 || c.MaxAge > 24*3600 {
			t.Errorf("secure=%v: MaxAge %d", secure, c.MaxAge)
		}
		if strings.Contains(c.Value, "alice-pw") {
			t.Errorf("the password is in the cookie")
		}
	}
}

// A session cookie that someone made up is not a session.
func TestSecurityAdminForgedSessions(t *testing.T) {
	s := newStack(t)
	site := s.secAdmin(t, false)
	b := site.browser(t, false)
	signed := b.signIn("bob", "bob-pw") // a viewer's genuine cookie
	var real string
	for _, c := range signed.cookie {
		real = c.Value
	}
	enc, sig, _ := strings.Cut(real, ".")
	raw, _ := secDecodeB64(enc)
	forge := []struct{ name, val string }{
		{"empty", ""},
		{"no signature", enc},
		{"empty signature", enc + "."},
		{"role raised, signature kept", secEncB64(strings.Replace(raw, "|viewer|", "|admin|", 1)) + "." + sig},
		{"user changed, signature kept", secEncB64(strings.Replace(raw, "|bob|", "|alice|", 1)) + "." + sig},
		{"expiry extended, signature kept", secEncB64(strings.Replace(raw, raw[strings.LastIndex(raw, "|")+1:], "99999999999", 1)) + "." + sig},
		{"signature of the empty string", enc + ".47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU"},
		{"signature stripped to one byte", enc + "." + sig[:1]},
		{"payload twice", enc + "." + enc + "." + sig},
		{"a different payload format", secEncB64("v1|alice|admin|99999999999") + "." + sig},
		{"json", `{"user":"alice","role":"admin"}`},
	}
	for _, f := range forge {
		r, _ := http.NewRequest("GET", site.base+"/admin/", nil)
		r.AddCookie(&http.Cookie{Name: "edgerun_session", Value: f.val})
		p := b.do(r)
		if p.status == 200 {
			t.Errorf("FORGED SESSION accepted: %s", f.name)
		}
		// state change
		r, _ = http.NewRequest("POST", site.base+"/admin/function/new", strings.NewReader(url.Values{"_leaf": {"p"}, "f0": {"forged"}}.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.AddCookie(&http.Cookie{Name: "edgerun_session", Value: f.val})
		if p := b.do(r); p.status != http.StatusUnauthorized {
			t.Errorf("a state change with a forged session (%s): %d", f.name, p.status)
		}
	}
	if fns, _ := s.ctl.ListFunctions(ctx, &controlv1.ListFunctionsRequest{}); fns != nil {
		for _, f := range fns.Functions {
			if f.Name == "forged" {
				t.Error("a forged session created a function")
			}
		}
	}
	// And the genuine viewer cookie works, as a viewer.
	r, _ := http.NewRequest("GET", site.base+"/admin/", nil)
	r.AddCookie(&http.Cookie{Name: "edgerun_session", Value: real})
	if p := b.do(r); p.status != 200 {
		t.Errorf("the genuine cookie: %d", p.status)
	}
}

// KNOWN DEFECT SEC-ADMIN-OPEN-REDIRECT: SafeNext refuses "//", "/\" and CR/LF
// but not a tab: after sign-in the server answers "Location: /<TAB>/evil.example/"
// and browsers remove tabs and line feeds from a URL before parsing it, which
// makes it "//evil.example/": a phishing link on the real sign-in page sends the
// operator, now signed in and trusting the page, to another site.
func TestSecurityAdminOpenRedirect(t *testing.T) {

	s := newStack(t)
	site := s.secAdmin(t, false)
	targets := []string{
		"//evil.example/", "///evil.example/", "/\\evil.example/", "/\\/evil.example/", "\\\\evil.example/", "http://evil.example/", "https://evil.example/",
		"%2f%2fevil.example/", "/%2fevil.example/", "/%5cevil.example/", "javascript:alert(1)", "data:text/html,x", " //evil.example/",
		"/\t/evil.example/", "/\n/evil.example/", "/\r/evil.example/", "/%09/evil.example/", "/%0a/evil.example/", "/%0d%0aX-Injected: 1",
		"/../..//evil.example/", "/.//evil.example/", "/@evil.example/", "@evil.example",
		"/ /evil.example/", "/ /evil.example/", "/　/evil.example/", "/\t\\evil.example/", "/\\\t/evil.example/",
	}
	raw := func(r *http.Request) (*http.Response, error) {
		tr := &http.Transport{DisableKeepAlives: true}
		defer tr.CloseIdleConnections()
		return tr.RoundTrip(r)
	}
	for _, next := range targets {
		r, _ := http.NewRequest("POST", site.base+"/login", strings.NewReader(url.Values{"username": {"alice"}, "password": {"alice-pw"}, "next": {next}}.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := raw(r)
		if err != nil {
			t.Errorf("next=%q: %v", next, err)
			continue
		}
		resp.Body.Close()
		loc := resp.Header.Get("Location")
		if resp.StatusCode != http.StatusSeeOther {
			t.Errorf("next=%q: %d", next, resp.StatusCode)
			continue
		}
		if secLeavesSite(loc) {
			t.Errorf("OPEN REDIRECT: after sign-in, next=%q sends the browser to Location: %q (browsers drop tabs and line feeds from a URL, and read \\ as /)", next, loc)
		}
		// Already signed in: the sign-in form's GET redirects as well.
		var cookie *http.Cookie
		for _, c := range resp.Cookies() {
			cookie = c
		}
		g, _ := http.NewRequest("GET", site.base+"/login?next="+url.QueryEscape(next), nil)
		g.AddCookie(cookie)
		gr, err := raw(g)
		if err != nil {
			continue
		}
		gr.Body.Close()
		if gl := gr.Header.Get("Location"); gr.StatusCode == http.StatusSeeOther && secLeavesSite(gl) {
			t.Errorf("OPEN REDIRECT: GET /login?next=%q while signed in -> Location: %q", next, gl)
		}
	}
	// The form reflects next only as an escaped attribute.
	p := site.browser(t, true).get("/login?next=" + url.QueryEscape(`"><script>alert(1)</script>`))
	if found := secInjected(t, p.body, "alert(1)"); len(found) > 0 || strings.Contains(p.body, "<script>alert") {
		t.Errorf("next is reflected as markup: %v", found)
	}
}

// secLeavesSite reports whether a browser given this Location header would go to another site.
func secLeavesSite(loc string) bool {
	// What the URL standard does to the string before it looks at it: it removes
	// tab, CR and LF anywhere, trims C0 controls and space at the ends, and reads \ as /.
	clean := strings.Map(func(r rune) rune {
		if r == '\t' || r == '\n' || r == '\r' {
			return -1
		}
		return r
	}, loc)
	clean = strings.Trim(clean, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f ")
	clean = strings.ReplaceAll(clean, "\\", "/")
	if strings.HasPrefix(clean, "//") {
		return true
	}
	if strings.ContainsAny(loc, "\r\n") {
		return true // header injection
	}
	u, err := url.Parse(clean)
	return err == nil && (u.Host != "" || u.Scheme != "")
}

func TestSecurityAdminLoginBruteForce(t *testing.T) {
	s := newStack(t)
	site := s.secAdmin(t, false)
	// Sequential guesses stop after a few.
	b := site.browser(t, false)
	limited := -1
	for i := 0; i < 20; i++ {
		p := b.post("/login", url.Values{"username": {"alice"}, "password": {fmt.Sprintf("guess-%d", i)}, "next": {"/admin/"}})
		if p.status == http.StatusTooManyRequests {
			limited = i
			break
		}
		if p.status != http.StatusUnauthorized {
			t.Fatalf("guess %d: %d", i, p.status)
		}
		for _, c := range p.cookie {
			if c.Name == "edgerun_session" && c.Value != "" {
				t.Fatal("a session was issued for a wrong password")
			}
		}
	}
	if limited < 0 || limited > 12 {
		t.Errorf("no rate limit within 20 sequential wrong passwords (limited at %d)", limited)
	}
	// While limited, even the right password is refused: the limit holds against a lucky guess.
	if p := b.signIn("alice", "alice-pw"); p.status == http.StatusSeeOther {
		t.Errorf("the right password was accepted from an address that is over its limit")
	}
	// The error does not say whether the user exists.
	site2 := s.secAdmin(t, false)
	b2 := site2.browser(t, false)
	known := b2.post("/login", url.Values{"username": {"alice"}, "password": {"x"}})
	unknown := b2.post("/login", url.Values{"username": {"nobody"}, "password": {"x"}})
	if known.status != unknown.status || known.body != unknown.body {
		t.Errorf("a wrong password for a known and an unknown user are answered differently")
	}
	// Forwarding headers do not give an attacker a fresh address.
	site3 := s.secAdmin(t, false)
	b3 := site3.browser(t, false)
	var got429 bool
	for i := 0; i < 15; i++ {
		p := b3.post("/login", url.Values{"username": {"alice"}, "password": {"nope"}}, "X-Forwarded-For", fmt.Sprintf("10.9.8.%d", i), "X-Real-Ip", fmt.Sprintf("10.9.7.%d", i))
		got429 = got429 || p.status == http.StatusTooManyRequests
	}
	if !got429 {
		t.Errorf("the rate limit is keyed on a header the client chooses")
	}
}

// KNOWN DEFECT SEC-ADMIN-LIMITER-RACE: allow() is checked, then bcrypt runs for
// ~60 ms, then fail() is recorded. A burst of concurrent guesses all pass the
// check before any is recorded, so the limit of 10 per window is a limit on
// sequential guesses only.
func TestSecurityAdminLoginBruteForceConcurrent(t *testing.T) {

	s := newStack(t)
	site := s.secAdmin(t, false)
	var wg sync.WaitGroup
	var mu sync.Mutex
	processed := 0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := site.browser(t, false)
			p := b.post("/login", url.Values{"username": {"alice"}, "password": {fmt.Sprintf("burst-%d", i)}})
			if p.status == http.StatusUnauthorized {
				mu.Lock()
				processed++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if processed > 12 {
		t.Errorf("%d of 100 concurrent wrong passwords were checked against the hash; the limit is 10 per window", processed)
	}
}

func secEncB64(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
func secDecodeB64(s string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	return string(b), err
}

// CSRF: a state-changing request that comes from another origin must not act,
// whatever cookies the browser attaches. SameSite=Lax is a second line, not the
// first: a page served from a sibling name (a function the platform hosts under
// the same registrable domain as the admin UI) is same-site, and Lax cookies go
// with its form posts.
// The app's forms refuse cross-origin and same-site-but-other-origin posts.
func TestSecurityAdminCSRF(t *testing.T) {
	s := newStack(t)
	site := s.secAdmin(t, false)
	b := site.browser(t, true)
	if p := b.signIn("alice", "alice-pw"); p.status != 200 {
		t.Fatalf("sign-in: %d", p.status)
	}
	form := secFormWith(t, b.get("/admin/function/new").body, "Create", "")
	exists := func(name string) bool {
		r, _ := s.ctl.ListFunctions(ctx, &controlv1.ListFunctionsRequest{})
		for _, f := range r.Functions {
			if f.Name == name {
				return true
			}
		}
		return false
	}
	create := func(name string, hdr ...string) secPage {
		return b.post(form.action, form.secValues(t, map[string]string{"Name": name}), hdr...)
	}
	create("csrf-control") // from the page itself
	if !exists("csrf-control") {
		t.Fatalf("the control request did not create a function: the test cannot judge the others")
	}
	for i, h := range [][]string{
		{"Origin", "https://evil.example", "Sec-Fetch-Site", "cross-site"},
		{"Origin", "https://evil.example", "Sec-Fetch-Site", "same-site"},
		{"Origin", "null"},
		{"Sec-Fetch-Site", "cross-site"},
		{"Sec-Fetch-Site", "same-site", "Referer", "https://fn.tenant.example/page"},
	} {
		name := fmt.Sprintf("csrf-%d", i)
		p := create(name, h...)
		if exists(name) {
			t.Errorf("CSRF: a form post with %v was executed with the operator's cookie (status %d)", h, p.status)
		}
	}
}

// The login form is protected against cross-site posts (login CSRF).
func TestSecurityAdminLoginIsCrossOriginProtected(t *testing.T) {
	s := newStack(t)
	site := s.secAdmin(t, false)
	b := site.browser(t, false)
	for _, h := range [][]string{{"Origin", "https://evil.example", "Sec-Fetch-Site", "cross-site"}, {"Sec-Fetch-Site", "cross-site"}, {"Origin", "https://evil.example"}} {
		p := b.post("/login", url.Values{"username": {"bob"}, "password": {"bob-pw"}}, h...)
		if p.status == http.StatusOK || p.status == http.StatusSeeOther {
			t.Errorf("a cross-site login with %v was processed: %d", h, p.status)
		}
	}
}

// Module upload through the UI: a file name is only a name, a module over the limit is
// refused without being kept, and a viewer cannot deploy.
func TestSecurityAdminUpload(t *testing.T) {
	s := newStack(t)
	site := s.secAdmin(t, false)
	fid := s.createFunction("uploads")
	admin := site.browser(t, true)
	admin.signIn("alice", "alice-pw")
	viewer := site.browser(t, true)
	viewer.signIn("bob", "bob-pw")

	deploy := secFormWith(t, admin.get("/admin/function/"+fid+"/deploy").body, "Upload", "")
	opts := map[string]string{"ABI": "wagi-1"}
	versions := func() int {
		r, err := s.ctl.ListVersions(ctx, &controlv1.ListVersionsRequest{FunctionId: fid})
		if err != nil {
			t.Fatal(err)
		}
		return len(r.Versions)
	}
	tmpBefore := secTempCount()
	// File names that try to be paths: only a name, and no effect outside the store.
	canary := filepath.Join(t.TempDir(), "canary")
	for _, name := range []string{"../../../../etc/cron.d/x", "..\\..\\x.wasm", canary, "/proc/self/mem", "a\x00b.wasm", strings.Repeat("a", 10000) + ".wasm", "<script>alert(1)</script>.wasm"} {
		p := admin.submitFile(t, deploy, opts, "Module", name, s.wasm)
		if p.status >= 500 {
			t.Errorf("file name %.40q: status %d", name, p.status)
		}
	}
	if _, err := os.Stat(canary); err == nil {
		t.Errorf("an uploaded file name was used as a path")
	}
	// A module over the limit (the orchestrator's default is 32 MiB) is refused, and memory stays bounded.
	n := versions()
	over := bytes.Repeat([]byte{0}, 40<<20)
	h := secMeasure()
	p := admin.submitFile(t, deploy, opts, "Module", "big.wasm", over)
	grew, alloc := h.Stop()
	t.Logf("a 40 MiB upload over the limit: status %d, heap peak +%d MiB, %d MiB allocated", p.status, grew/secMiB, alloc/secMiB)
	if versions() != n {
		t.Errorf("a module over the limit was stored")
	}
	if strings.Contains(p.body, "Active version") {
		t.Errorf("a module over the limit was accepted: status %d", p.status)
	}
	t.Logf("the page says: %.200s", secVisible(t, p.body))
	if alloc > 300*secMiB {
		t.Errorf("a refused upload of 40 MiB allocated %d MiB", alloc/secMiB)
	}
	// A viewer's upload is refused.
	n = versions()
	vp := viewer.submitFile(t, deploy, opts, "Module", "v.wasm", s.wasm) // the admin's form, posted with a viewer's session
	if versions() != n {
		t.Errorf("a viewer deployed a version (status %d)", vp.status)
	}
	eventually(t, "multipart temporary files to be removed", 10*time.Second, func() bool { return secTempCount() <= tmpBefore })
}

func secTempCount() int {
	es, _ := os.ReadDir(os.TempDir())
	n := 0
	for _, e := range es {
		if strings.HasPrefix(e.Name(), "multipart-") {
			n++
		}
	}
	return n
}

// Text that outsiders can write is rendered as text. The operator's pages show
// things the public controls without any account: the path, query and method of
// every request that was routed (invocation records), a guest's stderr, an
// X-Request-Id; and things the operators of other tools control: labels, route
// path prefixes, environment values.
func TestSecurityAdminRendersOutsiderTextAsText(t *testing.T) {
	s := newStack(t)
	s.startWorker(2)
	site := s.secAdmin(t, false)
	web := s.startFrontend(10 * time.Second)

	payloads := []string{
		`<script>window.__xss_%d=1</script>`,
		`"><img src=x onerror=window.__xss_%d=1>`,
		`'><svg onload=window.__xss_%d=1>`,
		`</td></tr></table><iframe srcdoc="<script>parent.__xss_%d=1</script>">`,
		`" autofocus onfocus="window.__xss_%d=1`,
		`javascript:window.__xss_%d=1`,
		`{{.}}${7*7}<%%=7*7%%>`,
	}
	fid := s.createFunction("xss-target")
	s.uploadVersion(fid, s.wasm, nil, true)
	var markers []string
	// Labels and the route prefix: set by operators of other tools.
	labels := map[string]string{}
	for i, pl := range payloads {
		markers = append(markers, fmt.Sprintf("__xss_%d", i))
		v := strings.NewReplacer("%d", fmt.Sprint(i), "%%", "%").Replace(pl)
		if len(v) > 250 {
			v = v[:250]
		}
		labels[fmt.Sprintf("probe-%d", i)] = v
		s.setEnv(fid, fmt.Sprintf("NOTE_%d", i), v, false)
	}
	if _, err := s.ctl.SetLabels(ctx, &controlv1.SetLabelsRequest{FunctionId: fid, Labels: labels}); err != nil {
		t.Fatal(err)
	}
	prefix := "/<svg/onload=window.__xss_p=1>"
	markers = append(markers, "__xss_p")
	if _, err := s.ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "xss.test", PathPrefix: prefix, FunctionId: fid, Labels: labels}); err != nil {
		t.Fatalf("a route prefix with markup was refused (fine, but then it cannot be rendered): %v", err)
	}
	s.route("xss.test", "/", fid)
	eventually(t, "the stack to serve", 120*time.Second, func() bool { return web.do("GET", "xss.test", "/echo", "").status == 200 })

	// What the public sends: a path, a query, a method, a request id, a Host, a header.
	for i, pl := range payloads {
		v := strings.NewReplacer("%d", fmt.Sprint(100+i), "%%", "%").Replace(pl)
		markers = append(markers, fmt.Sprintf("__xss_%d", 100+i))
		esc := url.PathEscape(v)
		web.do("GET", "xss.test", "/echo/"+esc+"?q="+url.QueryEscape(v), "", "X-Request-Id", strings.ReplaceAll(v, " ", ""))
		web.do("POST", "xss.test", "/stderr", v)
	}
	// A function that writes the payload to stderr.
	web.do("GET", "xss.test", "/exit?code=3", "")

	b := site.browser(t, true)
	b.signIn("alice", "alice-pw")
	pages := []string{"/admin/", "/admin/function", "/admin/function/" + fid, "/admin/route", "/admin/worker", "/admin/network", "/admin/invocation"}
	for _, tab := range []string{"environment", "labels", "routes", "versions", "network", "invocations", "overview"} {
		pages = append(pages, "/admin/function/"+fid+"?tabs.tab="+tab)
	}
	eventually(t, "invocation records", 20*time.Second, func() bool { return len(s.invocations(fid)) > 5 })
	var seenPayload bool
	for _, pg := range pages {
		p := b.get(pg)
		if p.status != 200 {
			t.Logf("%s: status %d", pg, p.status)
			continue
		}
		for _, f := range secInjected(t, p.body, markers...) {
			t.Errorf("XSS: %s renders text as markup: %s", pg, f)
		}
		if strings.Contains(p.body, "__xss_") {
			seenPayload = true
		}
	}
	if !seenPayload {
		t.Logf("no page showed any of the payloads: the test may not be reaching what it means to judge")
	}
	// The invocation detail page, for each record.
	for _, inv := range s.invocations(fid) {
		for _, pg := range []string{"/admin/invocation/" + inv.Id, "/admin/invocation?id=" + inv.Id} {
			p := b.get(pg)
			for _, f := range secInjected(t, p.body, markers...) {
				t.Errorf("XSS: %s renders text as markup: %s", pg, f)
			}
		}
	}
}

// Responses of the app are not framed or sniffed.
func TestSecurityAdminResponseHeaders(t *testing.T) {
	s := newStack(t)
	site := s.secAdmin(t, false)
	b := site.browser(t, true)
	b.signIn("alice", "alice-pw")
	var missing []string
	for _, pg := range []string{"/login", "/admin/", "/admin/function"} {
		p := b.get(pg)
		framing := p.header.Get("X-Frame-Options") != "" || strings.Contains(p.header.Get("Content-Security-Policy"), "frame-ancestors")
		if !framing {
			missing = append(missing, pg+": no X-Frame-Options or frame-ancestors")
		}
		if p.header.Get("X-Content-Type-Options") != "nosniff" {
			missing = append(missing, pg+": no X-Content-Type-Options: nosniff")
		}
		if cc := p.header.Get("Cache-Control"); pg != "/login" && !strings.Contains(cc, "no-store") {
			missing = append(missing, pg+": Cache-Control "+cc)
		}
	}
	if len(missing) > 0 {
		t.Logf("hardening headers missing on the admin app (reported): %v", missing)
	}
	// Only the page the operator asked for is cacheable by a shared cache: nothing of the app is.
	if p := b.get("/admin/function"); !strings.Contains(p.header.Get("Cache-Control"), "no-store") {
		t.Errorf("an authenticated page is cacheable: %q", p.header.Get("Cache-Control"))
	}
}

// KNOWN DEFECT SEC-ADMIN-LOGOUT: signing out only clears the browser's cookie.
// The session is a stateless signed token, so a copy of the cookie (from a log,
// a proxy, a stolen laptop) keeps working until it expires, 12 hours by default.
func TestSecurityAdminLogoutEndsTheSession(t *testing.T) {

	s := newStack(t)
	site := s.secAdmin(t, false)
	b := site.browser(t, false)
	p := b.signIn("alice", "alice-pw")
	var c *http.Cookie
	for _, x := range p.cookie {
		c = x
	}
	b.get("/logout")
	r, _ := http.NewRequest("GET", site.base+"/admin/", nil)
	r.AddCookie(c)
	if got := b.do(r); got.status == 200 {
		t.Errorf("the session cookie still works after sign-out")
	}
}

var _ = edgerun.Digest

func secVisible(t *testing.T, body string) string {
	var sb strings.Builder
	secWalk(secParse(t, body), func(n *html.Node) {
		if n.Type == html.TextNode && n.Parent != nil && n.Parent.Data != "script" && n.Parent.Data != "style" {
			sb.WriteString(strings.TrimSpace(n.Data) + " ")
		}
	})
	return strings.Join(strings.Fields(sb.String()), " ")
}
