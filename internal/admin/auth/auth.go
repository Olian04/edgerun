// Package auth is the admin UI's sign-in. webui does no authentication of its
// own, so everything an operator can reach is mounted behind this.
//
// Operators are configured with a name, a role and a bcrypt hash. A session is
// a signed, expiring cookie that holds no secret: it names the user and role,
// and is checked against the configured users on every request, so removing a
// user (or changing their role) takes effect immediately without a session
// store.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Roles. An admin may change things; a viewer may only look.
const (
	RoleAdmin  = "admin"
	RoleViewer = "viewer"
)

const cookieName = "edgerun_session"

// User is a configured operator.
type User struct {
	Name string
	Role string
	hash []byte
}

// ParseUsers reads operators from "name:role:bcrypt-hash" entries separated by
// whitespace or semicolons. A bcrypt hash contains neither, nor a colon.
func ParseUsers(spec string) ([]User, error) {
	var users []User
	seen := map[string]bool{}
	for _, entry := range strings.FieldsFunc(spec, func(r rune) bool { return r == ';' || r == '\n' || r == ' ' || r == '\t' }) {
		parts := strings.SplitN(entry, ":", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("user %q: want name:role:bcrypt-hash", abbreviate(entry))
		}
		name, role, hash := parts[0], parts[1], parts[2]
		if name == "" || strings.ContainsAny(name, "|\x00") {
			return nil, fmt.Errorf("user name %q is not allowed", name)
		}
		if role != RoleAdmin && role != RoleViewer {
			return nil, fmt.Errorf("user %q: role must be %q or %q, got %q", name, RoleAdmin, RoleViewer, role)
		}
		if _, err := bcrypt.Cost([]byte(hash)); err != nil {
			return nil, fmt.Errorf("user %q: the password is not a valid bcrypt hash", name)
		}
		if seen[name] {
			return nil, fmt.Errorf("user %q is listed twice", name)
		}
		seen[name] = true
		users = append(users, User{Name: name, Role: role, hash: []byte(hash)})
	}
	if len(users) == 0 {
		return nil, errors.New("at least one user is required")
	}
	return users, nil
}

func abbreviate(s string) string {
	if len(s) > 24 {
		return s[:24] + "..."
	}
	return s
}

// HashPassword returns the bcrypt hash to put in the configuration.
func HashPassword(password string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(h), err
}

// Identity is who is signed in.
type Identity struct{ Name, Role string }

type ctxKey struct{}

// FromContext returns the signed-in operator, if any.
func FromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}

// WithIdentity returns a context that carries id. The middleware does this for
// real requests; tests use it to drive guards directly.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// Config configures an Authenticator.
type Config struct {
	Users []User
	// SessionKey signs session cookies (at least 32 bytes). Restarting with a
	// different key signs everyone out.
	SessionKey []byte
	// SessionTTL is how long a sign-in lasts. Default 12 hours.
	SessionTTL time.Duration
	// SecureCookies marks the cookie Secure: set it when served over HTTPS.
	SecureCookies bool
	// LoginPath is where the sign-in form lives. Default "/login".
	LoginPath string
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// Authenticator signs operators in and guards handlers.
type Authenticator struct {
	cfg     Config
	users   map[string]User
	dummy   []byte // compared against when the user is unknown, to hide that fact by timing
	limiter *limiter
	now     func() time.Time
}

// New returns an Authenticator.
func New(cfg Config) (*Authenticator, error) {
	if len(cfg.SessionKey) < 32 {
		return nil, errors.New("the session key must be at least 32 bytes")
	}
	if len(cfg.Users) == 0 {
		return nil, errors.New("at least one user is required")
	}
	if cfg.SessionTTL == 0 {
		cfg.SessionTTL = 12 * time.Hour
	}
	if cfg.LoginPath == "" {
		cfg.LoginPath = "/login"
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	a := &Authenticator{cfg: cfg, users: map[string]User{}, now: now, limiter: newLimiter(10, 5*time.Minute, now)}
	for _, u := range cfg.Users {
		a.users[u.Name] = u
	}
	a.dummy, _ = bcrypt.GenerateFromPassword([]byte("not-a-real-password"), bcrypt.DefaultCost)
	return a, nil
}

// --- sessions ----------------------------------------------------------------

func (a *Authenticator) sign(payload string) string {
	m := hmac.New(sha256.New, a.cfg.SessionKey)
	m.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

func (a *Authenticator) issue(u User) string {
	payload := fmt.Sprintf("v1|%s|%s|%d", u.Name, u.Role, a.now().Add(a.cfg.SessionTTL).Unix())
	return base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." + a.sign(payload)
}

// verify returns the identity a session cookie vouches for. The user must
// still be configured with the same role.
func (a *Authenticator) verify(token string) (Identity, bool) {
	enc, sig, ok := strings.Cut(token, ".")
	if !ok {
		return Identity{}, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(enc)
	if err != nil {
		return Identity{}, false
	}
	payload := string(raw)
	if subtle.ConstantTimeCompare([]byte(sig), []byte(a.sign(payload))) != 1 {
		return Identity{}, false
	}
	parts := strings.Split(payload, "|")
	if len(parts) != 4 || parts[0] != "v1" {
		return Identity{}, false
	}
	exp, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || a.now().Unix() >= exp {
		return Identity{}, false
	}
	u, ok := a.users[parts[1]]
	if !ok || u.Role != parts[2] {
		return Identity{}, false
	}
	return Identity{Name: u.Name, Role: u.Role}, true
}

func (a *Authenticator) setCookie(w http.ResponseWriter, token string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name: cookieName, Value: token, Path: "/", HttpOnly: true, Secure: a.cfg.SecureCookies,
		SameSite: http.SameSiteLaxMode, MaxAge: maxAge,
	})
}

// --- middleware ----------------------------------------------------------------

// Require runs next only for a signed-in operator, carrying their identity in
// the request context for webui's guards. Others are sent to the sign-in form
// (for page loads) or refused with 401 (for anything else).
func (a *Authenticator) Require(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c, err := r.Cookie(cookieName); err == nil {
			if id, ok := a.verify(c.Value); ok {
				w.Header().Set("Cache-Control", "no-store")
				next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), id)))
				return
			}
		}
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			http.Redirect(w, r, a.cfg.LoginPath+"?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
			return
		}
		http.Error(w, "Sign in required", http.StatusUnauthorized)
	})
}

// SafeNext returns next if it is a path on this site, else fallback. A sign-in
// that redirected anywhere would be an open redirect.
func SafeNext(next, fallback string) string {
	if next == "" || next[0] != '/' || strings.HasPrefix(next, "//") || strings.HasPrefix(next, "/\\") || strings.ContainsAny(next, "\r\n") {
		return fallback
	}
	return next
}

// --- handlers ------------------------------------------------------------------

// Login serves the sign-in form (GET) and processes it (POST).
func (a *Authenticator) Login(home string) http.Handler {
	crossOrigin := http.NewCrossOriginProtection()
	return crossOrigin.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next := SafeNext(r.FormValue("next"), home)
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			if c, err := r.Cookie(cookieName); err == nil {
				if _, ok := a.verify(c.Value); ok {
					http.Redirect(w, r, next, http.StatusSeeOther)
					return
				}
			}
			renderLogin(w, http.StatusOK, next, "")
		case http.MethodPost:
			a.attempt(w, r, next)
		default:
			w.Header().Set("Allow", "GET, HEAD, POST")
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	}))
}

func (a *Authenticator) attempt(w http.ResponseWriter, r *http.Request, next string) {
	ip := clientIP(r)
	if !a.limiter.allow(ip) {
		w.Header().Set("Retry-After", "300")
		renderLogin(w, http.StatusTooManyRequests, next, "Too many attempts. Try again in a few minutes.")
		return
	}
	name, password := r.FormValue("username"), r.FormValue("password")
	u, known := a.users[name]
	hash := u.hash
	if !known {
		hash = a.dummy // do the same work, so timing does not reveal which names exist
	}
	ok := bcrypt.CompareHashAndPassword(hash, []byte(password)) == nil && known
	if !ok {
		a.limiter.fail(ip)
		renderLogin(w, http.StatusUnauthorized, next, "Wrong username or password.")
		return
	}
	a.limiter.reset(ip)
	a.setCookie(w, a.issue(u), int(a.cfg.SessionTTL.Seconds()))
	http.Redirect(w, r, next, http.StatusSeeOther)
}

// Logout clears the session and returns to the sign-in form. It accepts GET so
// that a plain link can sign out; signing someone out is harmless to forge.
func (a *Authenticator) Logout() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.setCookie(w, "", -1)
		http.Redirect(w, r, a.cfg.LoginPath, http.StatusSeeOther)
	})
}

// NewSessionKey returns a random key suitable for Config.SessionKey.
func NewSessionKey() []byte {
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		panic("auth: no entropy: " + err.Error())
	}
	return k
}
