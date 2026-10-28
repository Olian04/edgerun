package service

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"

	"example.com/edgerun-wgservice/internal/allow"
)

// fakeNet stands in for the tunnel: names map to addresses, every connection
// goes to one local server, and what was asked is recorded.
type fakeNet struct {
	mu      sync.Mutex
	names   map[string][]netip.Addr
	lookups []string
	dials   []netip.AddrPort
	target  string
}

func (f *fakeNet) LookupHost(_ context.Context, h string) ([]netip.Addr, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups = append(f.lookups, h)
	a, ok := f.names[h]
	if !ok {
		return nil, fmt.Errorf("no such host %s", h)
	}
	return a, nil
}

func (f *fakeNet) DialIP(ctx context.Context, ap netip.AddrPort) (net.Conn, error) {
	f.mu.Lock()
	f.dials = append(f.dials, ap)
	f.mu.Unlock()
	return (&net.Dialer{}).DialContext(ctx, "tcp", f.target)
}

type rig struct {
	fake   *fakeNet
	origin *httptest.Server
	client *edgerun.Egress
	svc    *Server
}

const tok = "egress-secret"

func newRig(t *testing.T, mutate func(*Config)) *rig {
	t.Helper()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/echo", http.StatusFound)
		case "/big":
			w.Write(make([]byte, 5000))
		case "/bigcl":
			w.Header().Set("Content-Length", "5000")
			w.Write(make([]byte, 5000))
		case "/head":
			w.Header().Set("Content-Length", "99999999")
		case "/hang":
			<-r.Context().Done()
		default:
			b, _ := io.ReadAll(r.Body)
			fmt.Fprintf(w, "%s %s host=%s ua=%q ae=%q body=%q", r.Method, r.RequestURI, r.Host, r.Header.Get("User-Agent"), r.Header.Get("Accept-Encoding"), b)
		}
	}))
	t.Cleanup(origin.Close)
	fake := &fakeNet{target: strings.TrimPrefix(origin.URL, "http://"), names: map[string][]netip.Addr{
		"good.test":    {netip.MustParseAddr("93.184.216.34")},
		"rebind.test":  {netip.MustParseAddr("93.184.216.34"), netip.MustParseAddr("127.0.0.1")},
		"rebind2.test": {netip.MustParseAddr("169.254.169.254"), netip.MustParseAddr("93.184.216.34")},
		"mapped.test":  {netip.MustParseAddr("::ffff:10.0.0.1")},
		"inside.test":  {netip.MustParseAddr("10.20.0.5")},
		"lo.test":      {netip.MustParseAddr("127.0.0.1")},
	}}
	cov, text, err := allow.ParseCoverage([]string{"*.test", "93.184.216.0/24", "169.254.169.254", "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Dialer: fake, Coverage: cov, CoverageText: text, MaxRequestBody: 1000, MaxResponseBody: 2000,
		AllowNetworks: []netip.Prefix{netip.MustParsePrefix("10.20.0.0/16")}}
	if mutate != nil {
		mutate(&cfg)
	}
	svc := New(cfg)
	mux := http.NewServeMux()
	mux.Handle(svc.Handler(edgerun.Verifier{tok: edgerun.ScopeEgress}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &rig{fake: fake, origin: origin, svc: svc, client: edgerun.NewEgress(srv.URL, tok)}
}

func (r *rig) fetch(req *egressv1.FetchRequest) (int, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := r.client.Fetch(ctx, req)
	if err != nil {
		return 0, "", err
	}
	b, err := res.ReadAll()
	return res.Status(), string(b), err
}

func want(t *testing.T, err error, reason commonv1.ErrorReason) {
	t.Helper()
	if edgerun.Reason(err) != reason {
		t.Errorf("error %v: reason %v, want %v", err, edgerun.Reason(err), reason)
	}
}

func TestDNSRebindingAndPinning(t *testing.T) {
	r := newRig(t, nil)
	// A name with ANY forbidden address gets nowhere, in either order, and nothing is connected to.
	for _, n := range []string{"rebind.test", "rebind2.test", "mapped.test", "lo.test"} {
		_, _, err := r.fetch(&egressv1.FetchRequest{Method: "GET", Url: "http://" + n + "/", Allow: []string{n}})
		want(t, err, commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED)
	}
	if len(r.fake.dials) != 0 {
		t.Fatalf("connected to %v though the name resolved to a forbidden address", r.fake.dials)
	}
	// An allowed name: resolved once, connected by address (never by name).
	st, body, err := r.fetch(&egressv1.FetchRequest{Method: "GET", Url: "http://good.test/x?a=1", Allow: []string{"good.test"}})
	if err != nil || st != 200 || !strings.Contains(body, "GET /x?a=1 host=good.test") {
		t.Fatalf("%d %q %v", st, body, err)
	}
	if len(r.fake.dials) != 1 || r.fake.dials[0].Addr().String() != "93.184.216.34" || r.fake.dials[0].Port() != 80 {
		t.Errorf("dialed %v: the checked address, by address", r.fake.dials)
	}
	if got := fmt.Sprint(r.fake.lookups); strings.Count(got, "good.test") != 1 {
		t.Errorf("good.test was resolved more than once: %v", r.fake.lookups)
	}
	// The operator's range opens a private address.
	if st, _, err := r.fetch(&egressv1.FetchRequest{Method: "GET", Url: "http://inside.test/", Allow: []string{"inside.test"}}); err != nil || st != 200 {
		t.Errorf("inside.test (10.20.0.5, an opened range): %d %v", st, err)
	}
}

func TestOrderAndSecrecy(t *testing.T) {
	r := newRig(t, nil)
	// Refused by the allow list or the coverage: nothing is resolved.
	for name, req := range map[string]*egressv1.FetchRequest{
		"not allowed":  {Method: "GET", Url: "http://good.test/", Allow: []string{"other.test"}},
		"empty list":   {Method: "GET", Url: "http://good.test/"},
		"not covered":  {Method: "GET", Url: "http://good.example/", Allow: []string{"good.example"}},
		"wrong port":   {Method: "GET", Url: "http://good.test:8080/", Allow: []string{"good.test"}},
		"localhost":    {Method: "GET", Url: "http://localhost.test/", Allow: []string{"*.test:80", "localhost.test:80"}}, // allowed by name... see below
		"literal loop": {Method: "GET", Url: "http://127.0.0.1:1/", Allow: []string{"127.0.0.1:1"}},
	} {
		if name == "localhost" {
			continue
		}
		_, _, err := r.fetch(req)
		if name == "literal loop" {
			want(t, err, commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED) // covered, but forbidden: denied at connect
			continue
		}
		want(t, err, commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED)
	}
	if len(r.fake.lookups) != 0 || len(r.fake.dials) != 0 {
		t.Errorf("a refused request looked up %v and dialed %v", r.fake.lookups, r.fake.dials)
	}
	// localhost names are refused by name, resolver or not.
	_, _, err := r.fetch(&egressv1.FetchRequest{Method: "GET", Url: "http://a.localhost/", Allow: []string{"*.localhost"}})
	want(t, err, commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED)
	// A name that does not resolve is a failure, not a refusal.
	_, _, err = r.fetch(&egressv1.FetchRequest{Method: "GET", Url: "http://nowhere.test/", Allow: []string{"nowhere.test"}})
	want(t, err, commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED)
}

func TestRequestRules(t *testing.T) {
	r := newRig(t, nil)
	allowGood := []string{"good.test"}
	// The request as the origin sees it.
	_, body, err := r.fetch(&egressv1.FetchRequest{Method: "POST", Url: "http://good.test/echo?q=%20x#frag", Allow: allowGood, Body: []byte("hi"),
		Headers: []*commonv1.Header{{Name: "Host", Value: "evil.test"}, {Name: "Connection", Value: "close"}, {Name: "Content-Length", Value: "1"}}})
	if err != nil || !strings.Contains(body, `POST /echo?q=%20x host=good.test ua="" ae="" body="hi"`) {
		t.Errorf("%q %v", body, err)
	}
	// Redirects are the caller's.
	st, _, err := r.fetch(&egressv1.FetchRequest{Method: "GET", Url: "http://good.test/redirect", Allow: allowGood})
	if err != nil || st != 302 {
		t.Errorf("redirect: %d %v", st, err)
	}
	// Malformed: invalid_argument, no reason.
	for _, req := range []*egressv1.FetchRequest{
		{Method: "", Url: "http://good.test/", Allow: allowGood},
		{Method: "GET", Url: "http://good.test/", Allow: []string{"*"}},
		{Method: "CONNECT", Url: "http://good.test/", Allow: allowGood},
	} {
		_, _, err := r.fetch(req)
		if err == nil || edgerun.Reason(err) != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
			t.Errorf("%v: %v", req, err)
		}
	}
	// Request bound.
	_, _, err = r.fetch(&egressv1.FetchRequest{Method: "POST", Url: "http://good.test/", Allow: allowGood, Body: make([]byte, 1001)})
	want(t, err, commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE)
	if _, _, err = r.fetch(&egressv1.FetchRequest{Method: "POST", Url: "http://good.test/", Allow: allowGood, Body: []byte(strings.Repeat("a", 1000))}); err != nil {
		t.Errorf("a body of exactly the limit: %v", err)
	}
}

func TestResponseBounds(t *testing.T) {
	r := newRig(t, nil)
	a := []string{"good.test"}
	for _, c := range []struct {
		path string
		max  uint64
		ok   bool
	}{
		{"/big", 0, false},     // service limit 2000 < 5000
		{"/big", 6000, false},  // the service's limit applies in any case
		{"/big", 100, false},   // the caller's
		{"/bigcl", 100, false}, // declared: refused before the head
		{"/head", 0, true},     // HEAD declares a length and has no body
	} {
		method := "GET"
		if c.path == "/head" {
			method = "HEAD"
		}
		st, body, err := r.fetch(&egressv1.FetchRequest{Method: method, Url: "http://good.test" + c.path, Allow: a, MaxResponseBytes: c.max})
		if c.ok {
			if err != nil || st != 200 {
				t.Errorf("%s: %d %v", c.path, st, err)
			}
			continue
		}
		want(t, err, commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE)
		limit := 2000
		if c.max != 0 && c.max < 2000 {
			limit = int(c.max)
		}
		if len(body) > limit {
			t.Errorf("%s: %d bytes delivered against a limit of %d", c.path, len(body), limit)
		}
	}
}

func TestDeadlineAndCancel(t *testing.T) {
	r := newRig(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	res, err := r.client.Fetch(ctx, &egressv1.FetchRequest{Method: "GET", Url: "http://good.test/hang", Allow: []string{"good.test"}})
	if err == nil {
		_, err = res.ReadAll()
	}
	// The caller's own timer and the service's budget are one value here, so
	// either side's error may arrive first; the service's typed error is
	// asserted by the conformance suite, which separates them.
	if err == nil {
		t.Fatal("no error")
	}
}

func TestAuthentication(t *testing.T) {
	r := newRig(t, nil)
	srv := httptest.NewServer(func() http.Handler {
		mux := http.NewServeMux()
		mux.Handle(r.svc.Handler(edgerun.Verifier{tok: edgerun.ScopeEgress, "admin-secret": edgerun.ScopeAdmin}))
		return mux
	}())
	defer srv.Close()
	for name, c := range map[string]string{"none": "", "wrong": "nope", "wrong scope": "admin-secret"} {
		_, err := edgerun.NewEgress(srv.URL, c).Describe(context.Background(), &egressv1.DescribeRequest{})
		if err == nil {
			t.Errorf("%s: Describe accepted", name)
		}
		res, err := edgerun.NewEgress(srv.URL, c).Fetch(context.Background(), &egressv1.FetchRequest{Method: "GET", Url: "http://good.test/", Allow: []string{"good.test"}})
		if err == nil {
			res.Body.Close()
			t.Errorf("%s: Fetch accepted", name)
		}
	}
	if len(r.fake.dials) != 0 {
		t.Error("an unauthenticated call reached the network")
	}
}
