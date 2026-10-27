package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"
)

// deniedError is a destination the service itself never reaches.
type deniedError struct{ msg string }

func (e *deniedError) Error() string { return e.msg }

// blocked are the address ranges a network service refuses by default, whatever
// a caller allowed: addresses that mean "this machine" or "the network behind
// it", and ones that are not routable on the public internet. A request that
// lands here is almost always an attempt to reach something it should not
// (cloud metadata services live in 169.254.0.0/16).
var blocked = mustPrefixes(
	// IPv4
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12",
	"192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "192.168.0.0/16", "198.18.0.0/15",
	"198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
	// IPv6: only 2000::/3 is global unicast (below); of that, not these.
	"2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20",
)

var globalUnicast6 = netip.MustParsePrefix("2000::/3")

func mustPrefixes(ss ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(ss))
	for i, s := range ss {
		out[i] = netip.MustParsePrefix(s)
	}
	return out
}

// reachable reports whether the service may connect to ip. An address inside
// one of the operator's allow networks is exempt from the built-in ranges.
func reachable(ip netip.Addr, allow []netip.Prefix) bool {
	ip = ip.Unmap()
	for _, p := range allow {
		if p.Contains(ip) {
			return true
		}
	}
	if ip.Is6() && !globalUnicast6.Contains(ip) {
		return false
	}
	for _, p := range blocked {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

// guardedDialer resolves a host and connects only to addresses that pass
// reachable. Every address the name resolves to is checked, not just the one
// used, and the connection is made to the checked address itself, so a name
// cannot be made to resolve somewhere harmless for the check and somewhere
// else for the connection.
type guardedDialer struct {
	allow []netip.Prefix
	// lookup resolves a host name to all of its addresses.
	lookup  func(ctx context.Context, host string) ([]netip.Addr, error)
	timeout time.Duration
}

func (g *guardedDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	var ips []netip.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		ips = []netip.Addr{ip}
	} else {
		ips, err = g.lookup(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("resolving %s: %w", host, err)
		}
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("resolving %s: no addresses", host)
	}
	for _, ip := range ips {
		if !reachable(ip, g.allow) {
			return nil, &deniedError{fmt.Sprintf("%s resolves to %s, which this service does not connect to", host, ip.Unmap())}
		}
	}
	d := net.Dialer{Timeout: g.timeout}
	var last error
	for _, ip := range ips {
		c, err := d.DialContext(ctx, network, net.JoinHostPort(ip.Unmap().String(), port))
		if err == nil {
			return c, nil
		}
		last = err
		if ctx.Err() != nil {
			break
		}
	}
	return nil, last
}

func isDenied(err error) bool {
	var d *deniedError
	return errors.As(err, &d)
}
