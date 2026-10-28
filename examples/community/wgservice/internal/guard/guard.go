// Package guard decides which destination addresses a network service never
// connects to, whatever a caller allowed (docs/protocol.md, "What a network
// service does with a Fetch", step 3): loopback, unspecified, link-local,
// private, carrier-grade NAT, multicast, broadcast, reserved, documentation and
// benchmarking ranges, and IPv4 addresses embedded in IPv6 forms.
//
// The documentation names the classes but not the ranges; the lists below are
// this implementation's reading of them (IANA special-purpose registries).
package guard

import (
	"fmt"
	"net/netip"
	"strings"
)

func p(s string) netip.Prefix { return netip.MustParsePrefix(s) }

var forbidden4 = []struct {
	net  netip.Prefix
	what string
}{
	{p("0.0.0.0/8"), "unspecified (this network)"},
	{p("10.0.0.0/8"), "private"},
	{p("100.64.0.0/10"), "carrier-grade NAT"},
	{p("127.0.0.0/8"), "loopback"},
	{p("169.254.0.0/16"), "link-local"},
	{p("172.16.0.0/12"), "private"},
	{p("192.0.0.0/24"), "IETF protocol assignments"},
	{p("192.0.2.0/24"), "documentation"},
	{p("192.88.99.0/24"), "6to4 relay anycast"},
	{p("192.168.0.0/16"), "private"},
	{p("198.18.0.0/15"), "benchmarking"},
	{p("198.51.100.0/24"), "documentation"},
	{p("203.0.113.0/24"), "documentation"},
	{p("224.0.0.0/4"), "multicast"},
	{p("240.0.0.0/4"), "reserved (including broadcast)"},
}

// Of IPv6, only global unicast (2000::/3) is a candidate at all; everything
// outside it (::/8 unspecified, loopback and IPv4-compatible, fc00::/7
// unique-local, fe80::/10 link-local, ff00::/8 multicast, ...) is refused.
var global6 = p("2000::/3")

var forbidden6 = []struct {
	net  netip.Prefix
	what string
}{
	{p("2001::/23"), "IETF protocol assignments (Teredo, benchmarking, ORCHID, ...)"},
	{p("2001:db8::/32"), "documentation"},
	{p("3fff::/20"), "documentation"},
}

var (
	nat64      = p("64:ff9b::/96")
	nat64Local = p("64:ff9b:1::/48")
	sixToFour  = p("2002::/16")
)

// Check returns nil if a connection to a may be made, and otherwise an error
// saying which class of address it is. allowed are the ranges the operator
// opened (--allow-network): an address inside one is allowed whatever class it
// is in. An IPv4 address in its IPv6 form is the IPv4 address.
func Check(a netip.Addr, allowed []netip.Prefix) error {
	a = a.WithZone("").Unmap()
	if !a.IsValid() {
		return fmt.Errorf("not an address")
	}
	for _, n := range allowed {
		if n.Contains(a) {
			return nil
		}
	}
	return Class(a)
}

// Class returns nil for an address no class forbids, otherwise the class.
func Class(a netip.Addr) error {
	a = a.WithZone("").Unmap()
	if a.Is4() {
		for _, f := range forbidden4 {
			if f.net.Contains(a) {
				return fmt.Errorf("%s address %s", f.what, a)
			}
		}
		return nil
	}
	if nat64.Contains(a) {
		// 64:ff9b::/96 (outside 2000::/3) carries an IPv4 address in its last
		// 32 bits: it is as forbidden as that address is.
		b := a.As16()
		if err := Class(netip.AddrFrom4([4]byte(b[12:16]))); err != nil {
			return fmt.Errorf("IPv6 form of a forbidden IPv4 address (NAT64): %w", err)
		}
		return nil
	}
	if !global6.Contains(a) {
		return fmt.Errorf("non-global IPv6 address %s (unspecified, loopback, unique-local, link-local, multicast or reserved)", a)
	}
	for _, f := range forbidden6 {
		if f.net.Contains(a) {
			return fmt.Errorf("%s address %s", f.what, a)
		}
	}
	b := a.As16()
	switch {
	case nat64Local.Contains(a):
		return fmt.Errorf("local-use NAT64 address %s", a)
	case sixToFour.Contains(a):
		// 2002:aabb:ccdd::/48 embeds the IPv4 address aa.bb.cc.dd.
		if err := Class(netip.AddrFrom4([4]byte(b[2:6]))); err != nil {
			return fmt.Errorf("IPv6 form of a forbidden IPv4 address (6to4): %w", err)
		}
	}
	return nil
}

// IsLocalName reports whether a host name is one that names this machine by
// definition (RFC 6761 section 6.3: localhost and everything under it). A
// resolver is allowed to answer them anywhere, so the service does not leave
// the decision to the DNS server it asks.
func IsLocalName(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	return host == "localhost" || strings.HasSuffix(host, ".localhost")
}
