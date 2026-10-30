package auth

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func hash(t *testing.T, pw string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(h)
}

type fixture struct {
	t    *testing.T
	a    *Authenticator
	now  time.Time
	spec string
}

func newFixture(t *testing.T, tweak func(*Config)) *fixture {
	t.Helper()
	f := &fixture{t: t, now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	f.spec = "alice:admin:" + hash(t, "alice-pw") + ";bob:viewer:" + hash(t, "bob-pw")
	users, err := ParseUsers(f.spec)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Users: users, SessionKey: []byte(strings.Repeat("k", 32)), Now: func() time.Time { return f.now }}
	if tweak != nil {
		tweak(&cfg)
	}
	f.a, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) login(user, pw, next string, hdr ...string) *httptest.ResponseRecorder {
	f.t.Helper()
	form := url.Values{"username": {user}, "password": {pw}, "next": {next}}
	req := httptest.NewRequest("POST", "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.RemoteAddr = "203.0.113.7:5555"
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rr := httptest.NewRecorder()
	f.a.Login("/admin/").ServeHTTP(rr, req)
	return rr
}

func session(rr *httptest.ResponseRecorder) *http.Cookie {
	for _, c := range rr.Result().Cookies() {
		if c.Name == cookieName {
			return c
		}
	}
	return nil
}

func TestParseUsers(t *testing.T) {
	good := hash(t, "x")
	for name, tc := range map[string]struct{ spec, want string }{
		"empty":        {"", "at least one"},
		"two parts":    {"a:admin", "want name:role:bcrypt-hash"},
		"bad role":     {"a:root:" + good, "role must be"},
		"not a hash":   {"a:admin:plaintext", "not a valid bcrypt hash"},
		"empty name":   {":admin:" + good, "not allowed"},
		"duplicate":    {"a:admin:" + good + ";a:viewer:" + good, "listed twice"},
		"pipe in name": {"a|b:admin:" + good, "not allowed"},
	} {
		if _, err := ParseUsers(tc.spec); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want containing %q", name, err, tc.want)
		}
	}
	us, err := ParseUsers("a:admin:" + good + "\nb:viewer:" + good + " c:viewer:" + good)
	if err != nil || len(us) != 3 {
		t.Errorf("separators: %v %v", us, err)
	}
	// An error must never echo the hash (a credential).
	if _, err := ParseUsers("a:admin"); err != nil && strings.Contains(err.Error(), good) {
		t.Error("error leaked a hash")
	}
}

func TestConfigValidation(t *testing.T) {
	users, _ := ParseUsers("a:admin:" + hash(t, "x"))
	if _, err := New(Config{Users: users, SessionKey: []byte("short")}); err == nil {
		t.Error("a short session key must be refused")
	}
	if _, err := New(Config{SessionKey: []byte(strings.Repeat("k", 32))}); err == nil {
		t.Error("no users must be refused")
	}
}

func TestSessionRoundTripAndAttacks(t *testing.T) {
	f := newFixture(t, nil)
	alice := f.a.users["alice"]
	token := f.a.issue(alice)
	if id, ok := f.a.verify(token); !ok || id.Name != "alice" || id.Role != RoleAdmin {
		t.Fatalf("round trip: %+v %v", id, ok)
	}

	enc, sig, _ := strings.Cut(token, ".")
	forge := func(payload string) string {
		return strings.TrimRight(base64Std(payload), "=") + "." + sig
	}
	for name, tok := range map[string]string{
		"empty":              "",
		"garbage":            "not-a-token",
		"no signature":       enc,
		"bad signature":      enc + ".AAAA",
		"payload swapped":    forge("v1|bob|viewer|9999999999"),
		"role escalated":     forge("v1|bob|admin|9999999999"),
		"signature stripped": enc + ".",
		"extra field":        forge("v1|alice|admin|9999999999|x"),
	} {
		if _, ok := f.a.verify(tok); ok {
			t.Errorf("%s: forged token accepted", name)
		}
	}

	other := newFixture(t, func(c *Config) { c.SessionKey = []byte(strings.Repeat("z", 32)) })
	if _, ok := other.a.verify(token); ok {
		t.Error("a token signed under another key must not verify")
	}
}

func base64Std(s string) string {
	const tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	var out []byte
	b := []byte(s)
	for i := 0; i < len(b); i += 3 {
		var v uint32
		n := 0
		for j := 0; j < 3; j++ {
			v <<= 8
			if i+j < len(b) {
				v |= uint32(b[i+j])
				n++
			}
		}
		for j := 0; j < n+1; j++ {
			out = append(out, tbl[(v>>(18-6*j))&63])
		}
	}
	return string(out)
}

func TestSessionExpiresAndTracksTheConfiguration(t *testing.T) {
	f := newFixture(t, func(c *Config) { c.SessionTTL = time.Hour })
	token := f.a.issue(f.a.users["alice"])

	f.now = f.now.Add(59 * time.Minute)
	if _, ok := f.a.verify(token); !ok {
		t.Error("valid until the TTL")
	}
	f.now = f.now.Add(2 * time.Minute)
	if _, ok := f.a.verify(token); ok {
		t.Error("expired session accepted")
	}

	// Removing a user, or changing their role, takes effect at once.
	f.now = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	token = f.a.issue(f.a.users["alice"])
	delete(f.a.users, "alice")
	if _, ok := f.a.verify(token); ok {
		t.Error("a removed user's session must stop working")
	}
	f.a.users["alice"] = User{Name: "alice", Role: RoleViewer}
	if _, ok := f.a.verify(token); ok {
		t.Error("a session issued for another role must stop working")
	}
}

func TestLoginSuccessSetsAHardenedCookie(t *testing.T) {
	f := newFixture(t, func(c *Config) { c.SecureCookies = true })
	rr := f.login("alice", "alice-pw", "/admin/function")
	if rr.Code != http.StatusSeeOther || rr.Header().Get("Location") != "/admin/function" {
		t.Fatalf("status %d -> %q", rr.Code, rr.Header().Get("Location"))
	}
	c := session(rr)
	if c == nil || !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteLaxMode || c.Path != "/" || c.MaxAge != 12*3600 {
		t.Errorf("cookie = %+v", c)
	}
	if id, ok := f.a.verify(c.Value); !ok || id.Name != "alice" {
		t.Error("the cookie does not verify")
	}
}

func TestLoginFailureDoesNotRevealWhichPartWasWrong(t *testing.T) {
	f := newFixture(t, nil)
	wrongPW := f.login("alice", "nope", "")
	noUser := f.login("mallory", "nope", "")
	if wrongPW.Code != 401 || noUser.Code != 401 {
		t.Fatalf("codes %d %d", wrongPW.Code, noUser.Code)
	}
	if wrongPW.Body.String() != noUser.Body.String() {
		t.Error("an unknown user and a wrong password must look identical")
	}
	if session(wrongPW) != nil || session(noUser) != nil {
		t.Error("no cookie on failure")
	}
	// A known user with the right password for someone else's account is still wrong.
	if rr := f.login("alice", "bob-pw", ""); rr.Code != 401 {
		t.Errorf("another user's password: %d", rr.Code)
	}
}

func TestNextCannotBeAnOpenRedirect(t *testing.T) {
	f := newFixture(t, nil)
	for _, evil := range []string{"//evil.example/x", "https://evil.example/", "/\\evil.example", "javascript:alert(1)", "evil", "/ok\r\nSet-Cookie: x=1"} {
		rr := f.login("alice", "alice-pw", evil)
		if loc := rr.Header().Get("Location"); loc != "/admin/" {
			t.Errorf("next=%q redirected to %q, want the home page", evil, loc)
		}
	}
	if rr := f.login("alice", "alice-pw", "/admin/route?x=1"); rr.Header().Get("Location") != "/admin/route?x=1" {
		t.Error("a local path with a query is fine")
	}
}

func TestBruteForceIsLimitedPerAddressAndSuccessResets(t *testing.T) {
	f := newFixture(t, nil)
	for i := range 10 {
		if rr := f.login("alice", "wrong", ""); rr.Code != 401 {
			t.Fatalf("attempt %d: %d", i, rr.Code)
		}
	}
	rr := f.login("alice", "alice-pw", "") // the right password, but the address is locked out
	if rr.Code != http.StatusTooManyRequests || rr.Header().Get("Retry-After") == "" || session(rr) != nil {
		t.Fatalf("after 10 failures: %d retry-after=%q cookie=%v", rr.Code, rr.Header().Get("Retry-After"), session(rr))
	}

	// Another address is unaffected, so one attacker cannot lock everyone out.
	other := httptest.NewRequest("POST", "/login", strings.NewReader(url.Values{"username": {"alice"}, "password": {"alice-pw"}}.Encode()))
	other.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	other.RemoteAddr = "198.51.100.9:1"
	orr := httptest.NewRecorder()
	f.a.Login("/admin/").ServeHTTP(orr, other)
	if orr.Code != http.StatusSeeOther {
		t.Errorf("a different address: %d", orr.Code)
	}

	// The window passes.
	f.now = f.now.Add(6 * time.Minute)
	if rr := f.login("alice", "alice-pw", ""); rr.Code != http.StatusSeeOther {
		t.Errorf("after the window: %d", rr.Code)
	}
}

func TestForwardedHeadersDoNotChooseTheRateLimitBucket(t *testing.T) {
	f := newFixture(t, nil)
	for i := range 10 {
		f.login("alice", "wrong", "", "X-Forwarded-For", "10.0.0."+string(rune('1'+i)))
	}
	if rr := f.login("alice", "alice-pw", "", "X-Forwarded-For", "10.9.9.9"); rr.Code != http.StatusTooManyRequests {
		t.Errorf("rotating X-Forwarded-For must not evade the limit: %d", rr.Code)
	}
}

func TestSuccessfulLoginResetsTheCounter(t *testing.T) {
	f := newFixture(t, nil)
	for range 9 {
		f.login("alice", "wrong", "")
	}
	f.login("alice", "alice-pw", "") // success on the 10th try
	for i := range 9 {
		if rr := f.login("alice", "wrong", ""); rr.Code != 401 {
			t.Fatalf("attempt %d after reset: %d (the counter should have restarted)", i, rr.Code)
		}
	}
}

func TestCrossSitePostsAreRefused(t *testing.T) {
	f := newFixture(t, nil)
	rr := f.login("alice", "alice-pw", "", "Sec-Fetch-Site", "cross-site")
	if rr.Code != http.StatusForbidden || session(rr) != nil {
		t.Errorf("a cross-site sign-in must be refused: %d", rr.Code)
	}
}

func TestLoginPageIsStaticAndLocked(t *testing.T) {
	f := newFixture(t, nil)
	req := httptest.NewRequest("GET", `/login?next=/admin/"><script>alert(1)</script>`, nil)
	rr := httptest.NewRecorder()
	f.a.Login("/admin/").ServeHTTP(rr, req)
	body := rr.Body.String()
	if rr.Code != 200 || !strings.Contains(body, `name="password"`) {
		t.Fatalf("%d", rr.Code)
	}
	if strings.Contains(body, "<script>") {
		t.Error("next was reflected unescaped")
	}
	csp := rr.Header().Get("Content-Security-Policy")
	for _, want := range []string{"default-src 'none'", "form-action 'self'", "frame-ancestors 'none'"} {
		if !strings.Contains(csp, want) {
			t.Errorf("CSP %q lacks %q", csp, want)
		}
	}
	if rr.Header().Get("Cache-Control") != "no-store" || rr.Header().Get("X-Frame-Options") != "DENY" {
		t.Error("headers")
	}
	if strings.Contains(csp, "unsafe-inline") {
		t.Error("the page uses a stylesheet file precisely so inline styles can stay forbidden")
	}
}

func TestAlreadySignedInSkipsTheForm(t *testing.T) {
	f := newFixture(t, nil)
	c := session(f.login("alice", "alice-pw", ""))
	req := httptest.NewRequest("GET", "/login", nil)
	req.AddCookie(c)
	rr := httptest.NewRecorder()
	f.a.Login("/admin/").ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther || rr.Header().Get("Location") != "/admin/" {
		t.Errorf("%d -> %q", rr.Code, rr.Header().Get("Location"))
	}
}

func TestRequire(t *testing.T) {
	f := newFixture(t, nil)
	var seen Identity
	var calls int
	h := f.a.Require(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		seen, _ = FromContext(r.Context())
		w.Write([]byte("secret page"))
	}))

	// Page loads without a session go to the form, remembering where they were going.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/admin/function?q=1", nil))
	loc, _ := url.Parse(rr.Header().Get("Location"))
	if rr.Code != http.StatusSeeOther || loc.Path != "/login" || loc.Query().Get("next") != "/admin/function?q=1" {
		t.Errorf("GET: %d -> %q", rr.Code, rr.Header().Get("Location"))
	}
	// Anything that changes state is refused outright, not redirected.
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("POST", "/admin/function/new", nil))
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("POST: %d", rr.Code)
	}
	if calls != 0 {
		t.Fatal("the protected handler ran without a session")
	}

	// With a session: through, with the identity in the context and no caching.
	c := session(f.login("bob", "bob-pw", ""))
	req := httptest.NewRequest("GET", "/admin/", nil)
	req.AddCookie(c)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != 200 || seen != (Identity{"bob", RoleViewer}) || rr.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("%d %+v cache=%q", rr.Code, seen, rr.Header().Get("Cache-Control"))
	}

	// A garbage cookie is no session.
	req = httptest.NewRequest("GET", "/admin/", nil)
	req.AddCookie(&http.Cookie{Name: cookieName, Value: "garbage"})
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Errorf("garbage cookie: %d", rr.Code)
	}
}

func TestLogout(t *testing.T) {
	f := newFixture(t, nil)
	rr := httptest.NewRecorder()
	f.a.Logout().ServeHTTP(rr, httptest.NewRequest("GET", "/logout", nil))
	c := session(rr)
	if rr.Code != http.StatusSeeOther || c == nil || c.MaxAge >= 0 || c.Value != "" {
		t.Errorf("%d %+v", rr.Code, c)
	}
}

func TestSafeNext(t *testing.T) {
	for in, want := range map[string]string{
		"": "/h", "/a": "/a", "/a?b=c#d": "/a?b=c#d", "//x": "/h", "/\\x": "/h", "http://x": "/h", "a": "/h", "/a\nb": "/h",
	} {
		if got := SafeNext(in, "/h"); got != want {
			t.Errorf("SafeNext(%q) = %q, want %q", in, got, want)
		}
	}
}
