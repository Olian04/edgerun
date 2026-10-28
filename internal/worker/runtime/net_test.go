package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Olian04/edgerun/conformance/guest"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

func TestParseFetchRequest(t *testing.T) {
	for name, c := range map[string]struct {
		in      string
		method  string
		url     string
		headers string
		body    string
	}{
		"minimal, no blank line":    {"GET http://a.test/", "GET", "http://a.test/", "[]", ""},
		"blank line, no body":       {"GET http://a.test/\n\n", "GET", "http://a.test/", "[]", ""},
		"crlf":                      {"GET http://a.test/\r\nX-A: 1\r\n\r\nbody", "GET", "http://a.test/", "[X-A=1]", "body"},
		"headers kept in order":     {"POST http://a.test/x?y=1\nX-A: 1\nX-A: 2\nContent-Type:  text/plain \n\nthe body", "POST", "http://a.test/x?y=1", "[X-A=1 X-A=2 Content-Type=text/plain]", "the body"},
		"body may hold blank lines": {"PUT http://a.test/\n\nline\n\nline", "PUT", "http://a.test/", "[]", "line\n\nline"},
		"headers without a body":    {"GET http://a.test/\nX-A: 1", "GET", "http://a.test/", "[X-A=1]", ""},
		"a colon inside a value":    {"GET http://a.test/\nReferer: http://b.test/p\n\n", "GET", "http://a.test/", "[Referer=http://b.test/p]", ""},
		"binary body":               {"POST http://a.test/\n\n\x00\xff\x01", "POST", "http://a.test/", "[]", "\x00\xff\x01"},
	} {
		r, err := ParseFetchRequest([]byte(c.in))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		var hs []string
		for _, h := range r.Headers {
			hs = append(hs, h.Name+"="+h.Value)
		}
		if r.Method != c.method || r.URL != c.url || fmt.Sprint(hs) != c.headers || string(r.Body) != c.body {
			t.Errorf("%s: got %s %s %v %q", name, r.Method, r.URL, hs, r.Body)
		}
	}
	for name, in := range map[string]string{
		"empty":                  "",
		"no URL":                 "GET",
		"no URL after space":     "GET \n\n",
		"no method":              " http://a.test/\n\n",
		"a header with no colon": "GET http://a.test/\nnot a header\n\n",
		"an unnamed header":      "GET http://a.test/\n: value\n\n",
	} {
		if r, err := ParseFetchRequest([]byte(in)); err == nil {
			t.Errorf("%s: parsed as %+v, want an error", name, r)
		}
	}
}

func TestFormatFetchResponse(t *testing.T) {
	got := string(FormatFetchResponse(&NetResponse{Status: 201,
		Headers: []*commonv1.Header{{Name: "x-a", Value: "1"}, {Name: "x-a", Value: "2"}}, Body: []byte("body\n\nmore")}))
	if want := "Status: 201\nx-a: 1\nx-a: 2\n\nbody\n\nmore"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got := string(FormatFetchResponse(&NetResponse{Status: 204})); got != "Status: 204\n\n" {
		t.Errorf("an empty response: %q", got)
	}
}

// fakeNet is a Network the test scripts.
type fakeNet struct {
	calls []NetRequest
	reply func(NetRequest) (*NetResponse, error)
}

func (f *fakeNet) Fetch(_ context.Context, r NetRequest) (*NetResponse, error) {
	f.calls = append(f.calls, r)
	return f.reply(r)
}

// netRun runs the network guest on an engine that has net, returning what the
// guest printed after the headers.
func netRun(t *testing.T, e *Engine, wasm []byte, abi, path, body string, grants []Grant, limits *commonv1.Limits) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := e.Run(ctx, Invocation{
		Digest: "sha256:netguest", Wasm: wasm, ABI: abi, Egress: grants, Limits: limits, InvocationID: "inv-1",
		Request: &commonv1.HttpRequest{Method: "POST", Path: path, Body: []byte(body)},
	})
	if err != nil {
		return "", err
	}
	return string(res.Body), nil
}

var a1 = []Grant{{Allow: "a.test", Service: "http://svc-a"}}

// The limits live in two places, here and in the network client, because a
// Network is not trusted to keep to the bound it was given.
func TestTheRuntimeBoundsWhatANetworkReturns(t *testing.T) {
	net := &fakeNet{reply: func(NetRequest) (*NetResponse, error) {
		return &NetResponse{Status: 200, Body: bytes.Repeat([]byte("x"), 2000)}, nil
	}}
	e, err := New(Config{Network: net})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	wasm := guest.BuildNet(t)

	out, err := netRun(t, e, wasm, ABI2, "/fetch", "GET http://a.test/\n\n", a1, &commonv1.Limits{MaxResponseBytes: 1000})
	if err != nil || !strings.HasPrefix(out, "code=-3") {
		t.Errorf("a response over the function's limit: %q, %v, want code=-3", out, err)
	}
	if len(net.calls) != 1 || net.calls[0].MaxResponseBytes != 1000 || strings.Join(net.calls[0].Allow, ",") != "a.test" || net.calls[0].Service != "http://svc-a" || net.calls[0].InvocationID != "inv-1" {
		t.Errorf("what the Network was given: %+v", net.calls)
	}

	net.reply = func(NetRequest) (*NetResponse, error) {
		return &NetResponse{Status: 200, Body: bytes.Repeat([]byte("x"), 5<<20)}, nil
	}
	out, err = netRun(t, e, wasm, ABI2, "/fetch", "GET http://a.test/\n\n", a1, &commonv1.Limits{MaxResponseBytes: 32 << 20})
	if err != nil || !strings.HasPrefix(out, "code=-3") {
		t.Errorf("a response over 4 MiB: %q, %v, want code=-3", out, err)
	}
}

func TestAFailedFetchLeavesNothingToRead(t *testing.T) {
	net := &fakeNet{reply: func(r NetRequest) (*NetResponse, error) {
		if strings.Contains(r.URL, "deny.test") {
			return nil, &NetError{Kind: NetDenied, Msg: "no"}
		}
		return &NetResponse{Status: 200, Body: bytes.Repeat([]byte("r"), 300)}, nil
	}}
	e, err := New(Config{Network: net})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	out, err := netRun(t, e, guest.BuildNet(t), ABI2, "/fetch-fail-discards", "GET http://a.test/\n\n", a1, nil)
	want := "first=" + strconv.Itoa(len("Status: 200\n\n")+300) + " second=-1 read=0"
	if err != nil || !strings.Contains(out, want) {
		t.Errorf("%q, %v, want it to contain %q", out, err, want)
	}
}

func TestWagi2NeedsANetwork(t *testing.T) {
	e, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	if got := fmt.Sprint(e.ABIs()); got != "["+ABI+"]" {
		t.Errorf("an engine without a network offers %s", got)
	}
	_, err = netRun(t, e, guest.BuildNet(t), ABI2, "/noop", "", nil, nil)
	var re *Error
	if !errors.As(err, &re) || re.Reason != commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH {
		t.Errorf("wagi-2 without a network: %v, want ABI_MISMATCH", err)
	}
	e2, err := New(Config{Network: &fakeNet{}})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close(context.Background())
	if got := fmt.Sprint(e2.ABIs()); got != "["+ABI+" "+ABI2+"]" {
		t.Errorf("an engine with a network offers %s", got)
	}
}

// A request goes through the service that carries the first grant matching its
// URL, is told only that service's entries, and none is made if nothing matches.
func TestARequestGoesThroughTheServiceThatCarriesItsGrant(t *testing.T) {
	net := &fakeNet{reply: func(NetRequest) (*NetResponse, error) { return &NetResponse{Status: 200, Body: []byte("ok")}, nil }}
	e, err := New(Config{Network: net})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	wasm := guest.BuildNet(t)
	grants := []Grant{
		{Allow: "*.corp.internal", Service: "http://private"},
		{Allow: "10.1.2.3:8080", Service: "http://private"},
		{Allow: "api.example.com", Service: "http://public"},
		{Allow: "db.corp.internal", Service: "http://elsewhere"}, // allowed by an earlier grant too: that one wins
		{Allow: "not a valid entry", Service: "http://public"},   // unreadable: lets nothing through
		{Allow: "bad.example.com", Service: ""},                  // no service: nowhere to send it
	}
	for _, c := range []struct {
		url     string
		want    int
		service string
		allow   string
	}{
		{"https://api.example.com/x", 0, "http://public", "api.example.com"},
		{"http://db.corp.internal/", 0, "http://private", "*.corp.internal,10.1.2.3:8080"},
		{"http://10.1.2.3:8080/", 0, "http://private", "*.corp.internal,10.1.2.3:8080"},
		{"http://10.1.2.3/", -1, "", ""}, // a port other than the one granted
		{"http://api.example.com:8443/", -1, "", ""},
		{"http://elsewhere.test/", -1, "", ""},
		{"http://bad.example.com/", -1, "", ""},
		{"ftp://api.example.com/", -4, "", ""},
	} {
		net.calls = nil
		out, err := netRun(t, e, wasm, ABI2, "/fetch", "GET "+c.url+"\n\n", grants, nil)
		if err != nil {
			t.Fatalf("%s: %v", c.url, err)
		}
		first, _, _ := strings.Cut(out, "\n")
		code, _ := strconv.Atoi(strings.TrimPrefix(first, "code="))
		if (c.want == 0) != (code >= 0) || (c.want < 0 && code != c.want) {
			t.Errorf("%s: code %d, want %d", c.url, code, c.want)
		}
		if c.service == "" {
			if len(net.calls) != 0 {
				t.Errorf("%s: a request nothing allows reached a service: %+v", c.url, net.calls)
			}
			continue
		}
		if len(net.calls) != 1 || net.calls[0].Service != c.service || strings.Join(net.calls[0].Allow, ",") != c.allow {
			t.Errorf("%s: calls %+v, want one through %s with allow %s", c.url, net.calls, c.service, c.allow)
		}
	}
}
