// Package wgtest builds the far side of a WireGuard tunnel for tests: a
// "site", a second userspace wireguard-go peer whose addresses exist only
// inside the tunnel. It hosts an HTTP origin and a DNS server there, and, for
// the conformance judge, can forward connections to destinations in the tunnel
// to the real local machine.
package wgtest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/net/dns/dnsmessage"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/waiter"

	"example.com/edgerun-wgservice/internal/wgnet"
)

// Addresses inside the tunnel.
var (
	GatewayAddr = netip.MustParseAddr("10.20.0.1")
	OriginAddr  = netip.MustParseAddr("10.20.0.2")
	DNSAddr     = netip.MustParseAddr("10.20.0.53")
)

// Site is the far side.
type Site struct {
	Tunnel *wgnet.Tunnel
	// GatewayPriv is the private key of the other end (the service), and
	// SitePub the public key of this one: what a gateway config needs.
	gatewayKey wgnet.Key
	sitePub    wgnet.Key
	listenPort int

	mu        sync.Mutex
	Requests  []Request
	DNSNames  map[string][]netip.Addr
	DNSLookup atomic.Int64
	http      *http.Server
	closers   []func()
}

// Request is what the origin saw.
type Request struct {
	Method, URI, Host, Remote string
	Header                    http.Header
}

// NewSite brings the site up on UDP port listenPort (loopback), peered with a
// gateway whose key it generates.
func NewSite(listenPort int, names map[string][]netip.Addr) (*Site, error) {
	siteKey, err := wgnet.GenerateKey()
	if err != nil {
		return nil, err
	}
	gwKey, err := wgnet.GenerateKey()
	if err != nil {
		return nil, err
	}
	gwPub, _ := gwKey.Public()
	sitePub, _ := siteKey.Public()
	tun, err := wgnet.Up(context.Background(), wgnet.Config{
		PrivateKey: siteKey,
		Address:    []netip.Addr{OriginAddr, DNSAddr},
		ListenPort: listenPort,
		Peers: []wgnet.Peer{{
			PublicKey:  gwPub,
			AllowedIPs: []netip.Prefix{netip.PrefixFrom(GatewayAddr, 32)},
		}},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		return nil, err
	}
	s := &Site{Tunnel: tun, gatewayKey: gwKey, sitePub: sitePub, listenPort: listenPort, DNSNames: names}
	if err := s.serveDNS(); err != nil {
		tun.Close()
		return nil, err
	}
	return s, nil
}

// GatewayConfig is a wg-quick file for the service (the gateway), reaching the
// site's UDP socket at endpoint (host:port) and listening on listenPort itself.
func (s *Site) GatewayConfig(endpoint string, listenPort int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[Interface]\nPrivateKey = %s\nAddress = %s/32\nDNS = %s\nListenPort = %d\n\n", s.gatewayKey, GatewayAddr, DNSAddr, listenPort)
	fmt.Fprintf(&b, "[Peer]\nPublicKey = %s\nEndpoint = %s\nAllowedIPs = 10.20.0.0/16\nPersistentKeepalive = 5\n", s.sitePub, endpoint)
	return b.String()
}

// ServeOrigin hosts an HTTP origin on 10.20.0.2:80, inside the tunnel, which
// records every request.
func (s *Site) ServeOrigin() error {
	ln, err := s.Tunnel.Net.ListenTCP(&net.TCPAddr{IP: OriginAddr.AsSlice(), Port: 80})
	if err != nil {
		return err
	}
	s.http = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.Requests = append(s.Requests, Request{r.Method, r.RequestURI, r.Host, r.RemoteAddr, r.Header.Clone()})
		s.mu.Unlock()
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/hello", http.StatusFound)
		case "/status":
			w.WriteHeader(418)
		default:
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprintf(w, "hello from inside the tunnel; you are %s", r.RemoteAddr)
		}
	})}
	go s.http.Serve(ln)
	return nil
}

// Seen returns the requests the origin has received.
func (s *Site) Seen() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.Requests...)
}

// Close takes the site down.
func (s *Site) Close() {
	if s.http != nil {
		s.http.Close()
	}
	for _, c := range s.closers {
		c()
	}
	s.Tunnel.Close()
}

// serveDNS answers A and AAAA queries for s.DNSNames on 10.20.0.53:53 inside
// the tunnel; anything else is NXDOMAIN.
func (s *Site) serveDNS() error {
	pc, err := s.Tunnel.Net.ListenUDP(&net.UDPAddr{IP: DNSAddr.AsSlice(), Port: 53})
	if err != nil {
		return err
	}
	s.closers = append(s.closers, func() { pc.Close() })
	go func() {
		buf := make([]byte, 1500)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			var m dnsmessage.Message
			if m.Unpack(buf[:n]) != nil || len(m.Questions) != 1 {
				continue
			}
			s.DNSLookup.Add(1)
			q := m.Questions[0]
			resp := dnsmessage.Message{
				Header:    dnsmessage.Header{ID: m.ID, Response: true, Authoritative: true, RecursionAvailable: true},
				Questions: m.Questions,
			}
			name := strings.TrimSuffix(strings.ToLower(q.Name.String()), ".")
			s.mu.Lock()
			addrs, ok := s.DNSNames[name]
			s.mu.Unlock()
			if !ok {
				resp.Header.RCode = dnsmessage.RCodeNameError
			}
			for _, a := range addrs {
				hdr := dnsmessage.ResourceHeader{Name: q.Name, Class: dnsmessage.ClassINET, TTL: 1}
				switch {
				case a.Is4() && q.Type == dnsmessage.TypeA:
					hdr.Type = dnsmessage.TypeA
					resp.Answers = append(resp.Answers, dnsmessage.Resource{Header: hdr, Body: &dnsmessage.AResource{A: a.As4()}})
				case a.Is6() && q.Type == dnsmessage.TypeAAAA:
					hdr.Type = dnsmessage.TypeAAAA
					resp.Answers = append(resp.Answers, dnsmessage.Resource{Header: hdr, Body: &dnsmessage.AAAAResource{AAAA: a.As16()}})
				}
			}
			out, err := resp.Pack()
			if err == nil {
				pc.WriteTo(out, from)
			}
		}
	}()
	return nil
}

// SetName adds or replaces a DNS answer.
func (s *Site) SetName(name string, addrs ...netip.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.DNSNames[name] = addrs
}

// ForwardTo makes the site a gateway to the machine it runs on, for the
// conformance judge: a TCP connection through the tunnel to dst (any port) is
// connected to toHost:the same port. dst must be one of the site's own
// addresses (a promiscuous NIC, which would allow any destination, breaks
// the stack's delivery in this version of netstack). It is a test tool; the production service
// has nothing like it. netstack does not export its gVisor stack, so this
// reaches for it by reflection.
func (s *Site) ForwardTo(dst netip.Addr, toHost string) error {
	v := reflect.ValueOf(s.Tunnel.Net).Elem().FieldByName("stack")
	if !v.IsValid() {
		return fmt.Errorf("netstack.Net has no stack field in this version of wireguard-go")
	}
	st := (*stack.Stack)(unsafe.Pointer(v.Pointer()))
	want := dst.As4()
	fwd := tcp.NewForwarder(st, 0, 256, func(r *tcp.ForwarderRequest) {
		id := r.ID()
		if id.LocalAddress.AsSlice() == nil || string(id.LocalAddress.AsSlice()) != string(want[:]) {
			r.Complete(true)
			return
		}
		go func() {
			c, err := net.DialTimeout("tcp", net.JoinHostPort(toHost, strconv.Itoa(int(id.LocalPort))), 3*time.Second)
			if err != nil {
				r.Complete(true) // RST: connection refused
				return
			}
			var wq waiter.Queue
			ep, terr := r.CreateEndpoint(&wq)
			if terr != nil {
				c.Close()
				r.Complete(true)
				return
			}
			r.Complete(false)
			t := gonet.NewTCPConn(&wq, ep)
			go func() { io.Copy(c, t); c.Close() }()
			go func() { io.Copy(t, c); t.Close() }()
		}()
	})
	st.SetTransportProtocolHandler(tcp.ProtocolNumber, fwd.HandlePacket)
	return nil
}

// Relay is a UDP relay between the gateway and the site that counts what
// crosses: everything between the two ends of a tunnel is ciphertext.
type Relay struct {
	Addr           string
	front, back    *net.UDPConn
	Packets, Bytes atomic.Int64
	mu             sync.Mutex
	Marker         []byte
	MarkerSeen     atomic.Bool
	gateway        *net.UDPAddr
	site           *net.UDPAddr
}

// NewRelay listens on frontPort (where the gateway's Endpoint points), sends to
// the site on sitePort from backPort.
func NewRelay(frontPort, backPort, sitePort int, marker string) (*Relay, error) {
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: frontPort})
	if err != nil {
		return nil, err
	}
	back, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: backPort})
	if err != nil {
		front.Close()
		return nil, err
	}
	r := &Relay{Addr: front.LocalAddr().String(), front: front, back: back, Marker: []byte(marker),
		site: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: sitePort}}
	pump := func(in, out *net.UDPConn, dst func() *net.UDPAddr, learn func(*net.UDPAddr)) {
		buf := make([]byte, 65535)
		for {
			n, from, err := in.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if learn != nil {
				learn(from)
			}
			r.Packets.Add(1)
			r.Bytes.Add(int64(n))
			if len(r.Marker) > 0 && strings.Contains(string(buf[:n]), string(r.Marker)) {
				r.MarkerSeen.Store(true)
			}
			if d := dst(); d != nil {
				out.WriteToUDP(buf[:n], d)
			}
		}
	}
	go pump(front, back, func() *net.UDPAddr { return r.site }, func(a *net.UDPAddr) { r.mu.Lock(); r.gateway = a; r.mu.Unlock() })
	go pump(back, front, func() *net.UDPAddr { r.mu.Lock(); defer r.mu.Unlock(); return r.gateway }, nil)
	return r, nil
}

// Close stops the relay.
func (r *Relay) Close() { r.front.Close(); r.back.Close() }

// SelfSigned returns a certificate for the given IP, signed by itself, and a
// pool that trusts it.
func SelfSigned(ip netip.Addr) (tls.Certificate, *x509.CertPool, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "wgtest"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.IP(ip.AsSlice())}, DNSNames: []string{"origin.corp.internal"},
		BasicConstraintsValid: true, IsCA: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	leaf, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool, nil
}

// ServeTLSOrigin hosts an HTTPS origin on 10.20.0.2:443 inside the tunnel.
func (s *Site) ServeTLSOrigin(cert tls.Certificate) error {
	ln, err := s.Tunnel.Net.ListenTCP(&net.TCPAddr{IP: OriginAddr.AsSlice(), Port: 443})
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "secure hello to %s", r.Host)
	}), TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}}}
	go srv.ServeTLS(ln, "", "")
	s.closers = append(s.closers, func() { srv.Close() })
	return nil
}
