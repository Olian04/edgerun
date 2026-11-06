package server

import (
	"context"
	"net/netip"
	"testing"

	"connectrpc.com/connect"

	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
)

// What makes a request malformed: the caller's bug, invalid_argument.
func TestMalformedRequestsAreInvalidArgument(t *testing.T) {
	s := New(Config{Covers: []string{"*"}})
	ok := &egressv1.FetchRequest{Method: "GET", Url: "http://example.com/a?b=c", Allow: []string{"example.com"}}
	if _, _, err := s.check(ok); err != nil {
		t.Fatalf("a good request: %v", err)
	}
	for name, mutate := range map[string]func(*egressv1.FetchRequest){
		"CONNECT":                  func(r *egressv1.FetchRequest) { r.Method = "CONNECT" },
		"a space in the query":     func(r *egressv1.FetchRequest) { r.Url = "http://example.com/a?b=c d" },
		"a space in the path":      func(r *egressv1.FetchRequest) { r.Url = "http://example.com/a b" },
		"a control byte":           func(r *egressv1.FetchRequest) { r.Url = "http://example.com/a\x01" },
		"a non-ASCII byte":         func(r *egressv1.FetchRequest) { r.Url = "http://example.com/caf\u00e9" },
		"a legacy numeric host":    func(r *egressv1.FetchRequest) { r.Url = "http://127.1/"; r.Allow = []string{"example.com"} },
		"a decimal-integer host":   func(r *egressv1.FetchRequest) { r.Url = "http://2130706433/" },
		"a hex-spelled host":       func(r *egressv1.FetchRequest) { r.Url = "http://0x7f.1/" },
		"credentials in the URL":   func(r *egressv1.FetchRequest) { r.Url = "http://u:p@example.com/" },
		"a scheme that is not web": func(r *egressv1.FetchRequest) { r.Url = "ftp://example.com/" },
	} {
		r := &egressv1.FetchRequest{Method: ok.Method, Url: ok.Url, Allow: ok.Allow}
		mutate(r)
		_, _, err := s.check(r)
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: %v, want invalid_argument", name, err)
		}
	}
}

func TestLocalhostIsRefusedByNameWhateverTheResolverSays(t *testing.T) {
	// A resolver that has never heard of localhost (one inside a tunnel).
	g := &guardedDialer{lookup: func(context.Context, string) ([]netip.Addr, error) { return nil, context.DeadlineExceeded }}
	for _, host := range []string{"localhost:80", "LOCALHOST:80", "app.localhost:80"} {
		if _, err := g.DialContext(context.Background(), "tcp", host); !isDenied(err) {
			t.Errorf("%s: %v, want a refusal", host, err)
		}
	}
}
