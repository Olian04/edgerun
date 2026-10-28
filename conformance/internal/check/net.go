package check

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// Net says where a suite's own servers listen, and how the implementation
// under test reaches them. A suite plays every role around the implementation,
// and when that implementation runs somewhere else (another host, a container,
// another language) the suite's servers must be reachable from there.
type Net struct {
	// Listen is the host, or host:port, to bind. A suite's primary server, the
	// one an implementation must be configured with before the suite starts,
	// takes the port if one is given; every other server takes a random port
	// on the same host. Default 127.0.0.1.
	Listen string
	// Advertise is the host the implementation reaches the suite's servers at,
	// when that differs from where they bind (a container, NAT, 0.0.0.0).
	// Default: the host in Listen.
	Advertise string
}

func (n Net) split() (host, port string) {
	l := n.Listen
	if l == "" {
		return "127.0.0.1", ""
	}
	if h, p, err := net.SplitHostPort(l); err == nil {
		return h, p
	}
	return l, ""
}

// Host is the host the implementation reaches the suite at.
func (n Net) Host() string {
	if n.Advertise != "" {
		return n.Advertise
	}
	h, _ := n.split()
	if ip := net.ParseIP(h); h == "" || (ip != nil && ip.IsUnspecified()) {
		return "127.0.0.1"
	}
	return h
}

// Server is a running server of a suite.
type Server struct {
	*httptest.Server
	// URL is the base URL the implementation uses to reach it.
	URL string
	// Addr is the host:port part of URL.
	Addr string
	// Port is the port it bound.
	Port int
}

func (n Net) bind(t testing.TB, primary bool) net.Listener {
	t.Helper()
	host, port := n.split()
	if !primary || port == "" {
		port = "0"
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(host, port))
	if err != nil {
		t.Fatalf("the suite cannot listen on %s: %v", net.JoinHostPort(host, port), err)
	}
	return ln
}

func (n Net) start(t testing.TB, h http.Handler, primary, tls bool) *Server {
	t.Helper()
	ln := n.bind(t, primary)
	hs := httptest.NewUnstartedServer(h)
	hs.Listener.Close()
	hs.Listener = ln
	scheme := "http"
	if tls {
		hs.StartTLS()
		scheme = "https"
	} else {
		var p http.Protocols
		p.SetHTTP1(true)
		p.SetUnencryptedHTTP2(true)
		hs.Config.Protocols = &p
		hs.Start()
	}
	t.Cleanup(hs.Close)
	port := ln.Addr().(*net.TCPAddr).Port
	addr := net.JoinHostPort(n.Host(), strconv.Itoa(port))
	return &Server{Server: hs, URL: scheme + "://" + addr, Addr: addr, Port: port}
}

// Serve starts a plain HTTP server for h. primary marks the server the
// implementation under test is configured with in advance.
func (n Net) Serve(t testing.TB, h http.Handler, primary bool) *Server {
	return n.start(t, h, primary, false)
}

// ServeTLS starts an HTTPS server with a certificate no client trusts.
func (n Net) ServeTLS(t testing.TB, h http.Handler) *Server {
	return n.start(t, h, false, true)
}

// ClosedAddr returns an address on the suite's host that nothing listens on.
func (n Net) ClosedAddr(t testing.TB) string {
	t.Helper()
	ln := n.bind(t, false)
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return net.JoinHostPort(n.Host(), strconv.Itoa(port))
}
