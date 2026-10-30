package check

import (
	"net/netip"
	"strings"
)

// Covered reports whether a network service that declared covers carries host,
// the host of a URL, by the rules in docs/protocol.md ("Network access"):
// "*" carries everything, "*.example.com" every name under it (not the domain
// itself), "example.com" or an address that host, and a range such as
// "10.0.0.0/8" the addresses within it (a name is not judged by where it might
// resolve). It is written here, apart from the reference implementation's own,
// so that a suite cannot be wrong in the same way as the thing it judges.
func Covered(covers []string, host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	addr, isAddr := netip.Addr{}, false
	if a, err := netip.ParseAddr(host); err == nil {
		addr, isAddr = a.Unmap(), true
		host = addr.String()
	}
	for _, c := range covers {
		c = strings.ToLower(c)
		switch {
		case c == "*":
			return true
		case strings.Contains(c, "/"):
			if p, err := netip.ParsePrefix(c); err == nil && isAddr && p.Contains(addr) {
				return true
			}
		case strings.HasPrefix(c, "*."):
			suffix := c[1:] // ".example.com"
			if !isAddr && len(host) > len(suffix) && strings.HasSuffix(host, suffix) {
				return true
			}
		default:
			if a, err := netip.ParseAddr(c); err == nil {
				if isAddr && a.Unmap() == addr {
					return true
				}
			} else if !isAddr && host == c {
				return true
			}
		}
	}
	return false
}
