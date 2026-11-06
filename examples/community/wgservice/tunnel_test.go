package wgservice_test

import (
	"context"
	"crypto/x509"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"net/http/httptest"

	"example.com/edgerun-wgservice/internal/allow"
	"example.com/edgerun-wgservice/internal/service"
	"example.com/edgerun-wgservice/internal/wgnet"
	"example.com/edgerun-wgservice/internal/wgtest"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// Two userspace wireguard-go peers in this process, over a loopback UDP relay:
// the "site" hosts an origin on 10.20.0.2:80, which exists nowhere but inside
// the tunnel; the "gateway" is what the service dials through.
func TestTunnel(t *testing.T) {
	site, err := wgtest.NewSite(portTunnelSite, map[string][]netip.Addr{
		"origin.corp.internal": {wgtest.OriginAddr},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer site.Close()
	if err := site.ServeOrigin(); err != nil {
		t.Fatal(err)
	}
	const secret = "PLAINTEXT-MARKER-4f9a1c"
	relay, err := wgtest.NewRelay(portTunnelRelay, portTunnelBack, portTunnelSite, secret)
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()

	cfg, err := wgnet.ParseConfig(strings.NewReader(site.GatewayConfig(relay.Addr, portTunnelGW)))
	if err != nil {
		t.Fatal(err)
	}
	gw, err := wgnet.Up(context.Background(), cfg, quiet())
	if err != nil {
		t.Fatal(err)
	}
	defer gw.Close()

	t.Run("the origin is not reachable except through the tunnel", func(t *testing.T) {
		// From the host's network stack: the address has no route to our site.
		// (This sandbox has a transparent proxy that completes TCP handshakes to
		// any 10.x address on port 80, so "can connect" is not the test: what
		// answers must not be the origin, and the origin must hear nothing.)
		for _, addr := range []string{"10.20.0.2:80", "10.20.0.2:8081", "127.0.0.1:80", "[::1]:80"} {
			c, err := net.DialTimeout("tcp", addr, 1500*time.Millisecond)
			if err != nil {
				continue
			}
			c.SetDeadline(time.Now().Add(2 * time.Second))
			io.WriteString(c, "GET /hello HTTP/1.0\r\nHost: x\r\n\r\n")
			b, _ := io.ReadAll(c)
			c.Close()
			if strings.Contains(string(b), "inside the tunnel") {
				t.Errorf("the host reached the origin at %s: %q", addr, b)
			}
		}
		// From a peer holding the wrong key: the site drops it silently.
		otherKey := must(wgnet.GenerateKey())
		sitePub := cfg.Peers[0].PublicKey
		bad, err := wgnet.Up(context.Background(), wgnet.Config{
			PrivateKey: otherKey, Address: []netip.Addr{wgtest.GatewayAddr}, ListenPort: 23014,
			Peers: []wgnet.Peer{{PublicKey: sitePub, Endpoint: relay.Addr, AllowedIPs: []netip.Prefix{netip.MustParsePrefix("10.20.0.0/16")}, Keepalive: time.Second}},
		}, quiet())
		if err != nil {
			t.Fatal(err)
		}
		defer bad.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if c, err := bad.DialIP(ctx, netip.MustParseAddrPort("10.20.0.2:80")); err == nil {
			c.Close()
			t.Error("a peer with the wrong key reached the origin")
		}
		if n := len(site.Seen()); n != 0 {
			t.Fatalf("the origin saw %d requests before anything legitimate was sent", n)
		}
	})

	t.Run("through the tunnel it is, and only ciphertext crosses", func(t *testing.T) {
		client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			return gw.DialIP(ctx, netip.MustParseAddrPort(addr))
		}}}
		req, _ := http.NewRequest("GET", "http://10.20.0.2:80/hello?"+secret, nil)
		req.Header.Set("X-Secret", secret)
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 200 || !strings.Contains(string(body), "hello from inside the tunnel; you are 10.20.0.1:") {
			t.Fatalf("%d %q", res.StatusCode, body)
		}
		seen := site.Seen()
		if len(seen) != 1 || !strings.HasPrefix(seen[0].Remote, "10.20.0.1:") || seen[0].Header.Get("X-Secret") != secret {
			t.Errorf("the origin saw %+v", seen)
		}
		if relay.Packets.Load() < 4 || relay.MarkerSeen.Load() {
			t.Errorf("relay carried %d packets, plaintext seen = %v", relay.Packets.Load(), relay.MarkerSeen.Load())
		}
	})

	t.Run("names are resolved inside the tunnel, and only there", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		before := site.DNSLookup.Load()
		ips, err := gw.LookupHost(ctx, "origin.corp.internal")
		if err != nil || len(ips) != 1 || ips[0] != wgtest.OriginAddr || site.DNSLookup.Load() == before {
			t.Errorf("origin.corp.internal: %v %v (queries seen by the site's DNS: %d)", ips, err, site.DNSLookup.Load()-before)
		}
		// Not the machine's /etc/hosts: "localhost" is a name like any other here.
		if ips, err := gw.LookupHost(ctx, "localhost"); err == nil {
			t.Errorf("localhost resolved to %v: the system resolver leaked in", ips)
		}
	})

	t.Run("netstack's DialContext resolves again: a checked name must not be dialed by name", func(t *testing.T) {
		// This is why wgnet.Tunnel splits LookupHost from DialIP. The obvious
		// implementation (check the resolved addresses, then hand the NAME to
		// tnet.DialContext) resolves twice, and the second answer is the one used.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		q := site.DNSLookup.Load()
		if _, err := gw.LookupHost(ctx, "origin.corp.internal"); err != nil { // the check
			t.Fatal(err)
		}
		c, err := gw.Net.DialContext(ctx, "tcp", "origin.corp.internal:80") // the naive connection
		if err != nil {
			t.Fatal(err)
		}
		c.Close()
		if n := site.DNSLookup.Load() - q; n != 2 {
			t.Errorf("%d DNS queries; expected the check and netstack's own lookup: 2", n)
		}
	})

	t.Run("https through the tunnel: a private CA is trusted only when configured", func(t *testing.T) {
		cert, pool, err := wgtest.SelfSigned(wgtest.OriginAddr)
		if err != nil {
			t.Fatal(err)
		}
		if err := site.ServeTLSOrigin(cert); err != nil {
			t.Fatal(err)
		}
		cov, text, _ := allow.ParseCoverage([]string{"10.20.0.0/16", "*.corp.internal"})
		for _, tc := range []struct {
			name  string
			roots *x509.CertPool
			ok    bool
		}{{"system roots", nil, false}, {"configured CA", pool, true}} {
			svc := service.New(service.Config{Dialer: gw, Coverage: cov, CoverageText: text, RootCAs: tc.roots,
				AllowNetworks:  []netip.Prefix{netip.MustParsePrefix("10.20.0.0/16")},
				MaxRequestBody: 1 << 20, MaxResponseBody: 4 << 20})
			mux := http.NewServeMux()
			mux.Handle(svc.Handler(edgerun.Verifier{"tok": edgerun.ScopeEgress}))
			srv := httptest.NewServer(mux)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			res, err := edgerun.NewEgress(srv.URL, "tok").Fetch(ctx, &egressv1.FetchRequest{Method: "GET", Url: "https://origin.corp.internal/", Allow: []string{"*.corp.internal"}})
			if tc.ok {
				if err != nil {
					t.Errorf("%s: %v", tc.name, err)
				} else if b, _ := res.ReadAll(); !strings.Contains(string(b), "secure hello to origin.corp.internal") {
					t.Errorf("%s: %q", tc.name, b)
				}
			} else if edgerun.Reason(err) != commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED {
				t.Errorf("%s: %v, want EGRESS_FAILED (a certificate that does not verify is a failure)", tc.name, err)
			}
			cancel()
			srv.Close()
		}
	})

	t.Run("the service reaches the origin through the tunnel, with the protocol's rules", func(t *testing.T) {
		cov, text, _ := allow.ParseCoverage([]string{"10.20.0.0/16", "*.corp.internal"})
		svc := service.New(service.Config{
			Dialer: gw, Coverage: cov, CoverageText: text, AllowNetworks: []netip.Prefix{netip.MustParsePrefix("10.20.0.0/16")},
			MaxRequestBody: 1 << 20, MaxResponseBody: 4 << 20,
		})
		mux := http.NewServeMux()
		mux.Handle(svc.Handler(edgerun.Verifier{"tok": edgerun.ScopeEgress}))
		srv := httptest.NewServer(mux)
		defer srv.Close()
		cl := edgerun.NewEgress(srv.URL, "tok")
		fetch := func(url string, allow ...string) (int, string, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := cl.Fetch(ctx, &egressv1.FetchRequest{Method: "GET", Url: url, Allow: allow})
			if err != nil {
				return 0, "", err
			}
			b, err := res.ReadAll()
			return res.Status(), string(b), err
		}
		before := len(site.Seen())

		if st, b, err := fetch("http://10.20.0.2/hello", "10.20.0.2"); err != nil || st != 200 || !strings.Contains(b, "inside the tunnel") {
			t.Errorf("by address: %d %q %v", st, b, err)
		}
		q := site.DNSLookup.Load()
		if st, b, err := fetch("http://origin.corp.internal/hello", "*.corp.internal"); err != nil || st != 200 || !strings.Contains(b, "inside the tunnel") {
			t.Errorf("by name: %d %q %v", st, b, err)
		}
		// Resolved once, then connected by the checked address.
		if n := site.DNSLookup.Load() - q; n != 1 {
			t.Errorf("one fetch made %d DNS queries; the check and the connection must share one answer", n)
		}
		if seen := site.Seen(); len(seen) != before+2 || seen[before+1].Host != "origin.corp.internal" {
			t.Errorf("the origin saw %+v", seen[before:])
		}
		// No redirects.
		if st, _, err := fetch("http://10.20.0.2/redirect", "10.20.0.2"); err != nil || st != 302 {
			t.Errorf("redirect: %d %v", st, err)
		}

		// Refusals: nothing reaches the origin.
		n := len(site.Seen())
		site.SetName("rebind.corp.internal", wgtest.OriginAddr, netip.MustParseAddr("127.0.0.1"))
		site.SetName("meta.corp.internal", netip.MustParseAddr("169.254.169.254"))
		site.SetName("loop.corp.internal", netip.MustParseAddr("127.0.0.2"))
		for name, u := range map[string][2]string{
			"not on the allow list":              {"http://10.20.0.2/hello", "10.20.0.3"},
			"another port":                       {"http://10.20.0.2:8080/hello", "10.20.0.2"},
			"not carried":                        {"http://203.0.114.1/", "203.0.114.1"},
			"a name with one forbidden address":  {"http://rebind.corp.internal/", "*.corp.internal"},
			"a name resolving to cloud metadata": {"http://meta.corp.internal/", "*.corp.internal"},
			"a name resolving to loopback":       {"http://loop.corp.internal/", "*.corp.internal"},
			"localhost":                          {"http://x.localhost/", "*.localhost"},
		} {
			_, _, err := fetch(u[0], u[1])
			if edgerun.Reason(err) != commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED {
				t.Errorf("%s: %v, want EGRESS_DENIED", name, err)
			}
		}
		if len(site.Seen()) != n {
			t.Errorf("a refused request reached the origin")
		}
		// Many at once.
		var wg sync.WaitGroup
		var bad atomic.Int64
		for i := 0; i < 40; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if st, _, err := fetch("http://10.20.0.2/hello", "10.20.0.2"); err != nil || st != 200 {
					bad.Add(1)
				}
			}()
		}
		wg.Wait()
		if bad.Load() != 0 {
			t.Errorf("%d of 40 concurrent fetches failed", bad.Load())
		}
		// A name the DNS does not know, and a closed port: failures.
		_, _, err := fetch("http://nope.corp.internal/", "*.corp.internal")
		if edgerun.Reason(err) != commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED {
			t.Errorf("unknown name: %v", err)
		}
		_, _, err = fetch("http://10.20.0.2:81/", "10.20.0.2:81")
		if edgerun.Reason(err) != commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED {
			t.Errorf("closed port: %v", err)
		}
	})
}
