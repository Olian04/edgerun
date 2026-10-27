// Package policy is the vocabulary of network allow lists: what an entry
// looks like, how a list is canonicalised, and when a destination matches.
//
// It is shared by the two components that must agree on it: the orchestrator,
// which validates and stores an operator's list, and a network service, which
// enforces it. Any other implementation of either side follows the grammar in
// docs/protocol.md; this package is just the reference copy.
package policy

import (
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"
)

// MaxEntries bounds one function's allow list.
const MaxEntries = 64

// maxEntryLen bounds one entry; a DNS name is at most 253 characters.
const maxEntryLen = 262

// Rule is one parsed allow-list entry.
type Rule struct {
	// Host is a lower-case DNS name, or an IP literal in canonical form
	// (IPv6 without brackets). For a wildcard it is the suffix: for
	// "*.example.com" it is "example.com".
	Host     string
	Wildcard bool
	// Port is the port the entry names; 0 means the scheme's default port.
	Port int
}

// String returns the canonical form of the entry.
func (r Rule) String() string {
	host := r.Host
	if r.Wildcard {
		host = "*." + host
	} else if a, err := netip.ParseAddr(host); err == nil && a.Is6() {
		host = "[" + host + "]"
	}
	if r.Port != 0 {
		return host + ":" + strconv.Itoa(r.Port)
	}
	return host
}

// ParseRule parses one entry: "example.com", "example.com:8443",
// "*.example.com", "192.0.2.1", "[2001:db8::1]:8080".
func ParseRule(s string) (Rule, error) {
	orig := s
	bad := func(why string) (Rule, error) { return Rule{}, fmt.Errorf("allow entry %q: %s", orig, why) }
	if s == "" || len(s) > maxEntryLen {
		return bad("empty or too long")
	}
	s = strings.ToLower(s)
	var r Rule

	hostPart, portPart := s, ""
	switch {
	case strings.HasPrefix(s, "["): // IPv6 literal
		end := strings.IndexByte(s, ']')
		if end < 0 {
			return bad("unterminated '['")
		}
		hostPart = s[1:end]
		rest := s[end+1:]
		if rest != "" {
			if rest[0] != ':' {
				return bad("unexpected text after ']'")
			}
			portPart = rest[1:]
		}
		a, err := netip.ParseAddr(hostPart)
		if err != nil || !a.Is6() || a.Zone() != "" {
			return bad("not an IPv6 address")
		}
		r.Host = a.String()
	default:
		if i := strings.LastIndexByte(s, ':'); i >= 0 {
			hostPart, portPart = s[:i], s[i+1:]
		}
		if strings.HasPrefix(hostPart, "*.") {
			r.Wildcard = true
			hostPart = hostPart[2:]
		}
		if a, err := netip.ParseAddr(hostPart); err == nil {
			if r.Wildcard {
				return bad("a wildcard applies to names, not to addresses")
			}
			if !a.Is4() {
				return bad("an IPv6 address must be written in brackets")
			}
			r.Host = a.String()
		} else {
			if !validName(hostPart) {
				return bad("want a host name, an address, or *.suffix")
			}
			r.Host = hostPart
		}
	}
	if portPart != "" {
		p, err := strconv.Atoi(portPart)
		if err != nil || p < 1 || p > 65535 || portPart != strconv.Itoa(p) {
			return bad("the port must be a number from 1 to 65535")
		}
		r.Port = p
	} else if strings.HasSuffix(s, ":") {
		return bad("empty port")
	}
	return r, nil
}

// validName reports whether s is a DNS name of letters, digits and hyphens in
// dot-separated labels (ASCII only: internationalised names are written in
// their punycode form).
func validName(s string) bool {
	if s == "" || len(s) > 253 {
		return false
	}
	for _, label := range strings.Split(s, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

// Canonical validates entries and returns them in canonical form: lower-case,
// deduplicated, sorted. More than MaxEntries is an error.
func Canonical(entries []string) ([]string, error) {
	set := map[string]bool{}
	for _, e := range entries {
		r, err := ParseRule(e)
		if err != nil {
			return nil, err
		}
		set[r.String()] = true
	}
	if len(set) > MaxEntries {
		return nil, fmt.Errorf("an allow list may have at most %d entries, got %d", MaxEntries, len(set))
	}
	out := make([]string, 0, len(set))
	for e := range set {
		out = append(out, e)
	}
	sort.Strings(out)
	return out, nil
}

// List is a parsed allow list.
type List []Rule

// Parse parses entries. An empty list allows nothing.
func Parse(entries []string) (List, error) {
	if len(entries) > MaxEntries {
		return nil, fmt.Errorf("an allow list may have at most %d entries, got %d", MaxEntries, len(entries))
	}
	l := make(List, 0, len(entries))
	for _, e := range entries {
		r, err := ParseRule(e)
		if err != nil {
			return nil, err
		}
		l = append(l, r)
	}
	return l, nil
}

// DefaultPort returns the port a URL scheme implies, or 0 for a scheme this
// protocol does not carry.
func DefaultPort(scheme string) int {
	switch scheme {
	case "http":
		return 80
	case "https":
		return 443
	}
	return 0
}

// NormalizeHost returns the form of a URL host that rules are matched against:
// lower-case, no trailing dot, IP literals in canonical form (no brackets).
func NormalizeHost(host string) string {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	if a, err := netip.ParseAddr(host); err == nil {
		return a.String()
	}
	return host
}

// Allows reports whether a request to scheme://host:port may go ahead. host is
// matched after NormalizeHost. An entry without a port matches only the
// scheme's default port; a wildcard matches names under the suffix and not the
// suffix itself.
func (l List) Allows(scheme, host string, port int) bool {
	host = NormalizeHost(host)
	for _, r := range l {
		want := r.Port
		if want == 0 {
			want = DefaultPort(scheme)
		}
		if want == 0 || port != want {
			continue
		}
		if r.Wildcard {
			if len(host) > len(r.Host)+1 && strings.HasSuffix(host, "."+r.Host) {
				return true
			}
		} else if host == r.Host {
			return true
		}
	}
	return false
}
