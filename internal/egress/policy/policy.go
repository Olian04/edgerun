// Package policy is the vocabulary of network allow lists: what an entry
// looks like, how a list is canonicalised, and when a destination matches.
//
// It is shared by the two components that must agree on it: the orchestrator,
// which validates and stores an operator's list, and a network service, which
// enforces it. Any other implementation of either side follows the grammar in
// docs/protocol.md; this package is just the reference copy.
package policy

import (
	"errors"
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
		// An IPv4 address written in its IPv6 form is the same destination.
		r.Host = a.Unmap().String()
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
		return a.Unmap().String()
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

// --- coverage ------------------------------------------------------------------

// MaxCovers bounds what one network service may declare.
const MaxCovers = 64

// CoverKind says what a Cover names. The order is the order of specificity,
// least first: when several services could carry a destination, the one that
// names it most precisely does.
type CoverKind int

const (
	CoverAll      CoverKind = iota + 1 // "*": every destination
	CoverNetwork                       // "10.0.0.0/8": addresses within a range
	CoverWildcard                      // "*.example.com": names under a domain
	CoverHost                          // "example.com", "192.0.2.7": one host
)

// Cover is one entry of what a network service carries. Unlike an allow-list
// entry it has no port, and may be a catch-all or an address range: a service
// is placed where the network is, and says which part of it that is.
type Cover struct {
	Kind CoverKind
	// Host is the name (for CoverHost, the name or address; for CoverWildcard,
	// the suffix without "*.").
	Host string
	// Prefix is the range of a CoverNetwork.
	Prefix netip.Prefix
}

// String returns the canonical form of the entry.
func (c Cover) String() string {
	switch c.Kind {
	case CoverAll:
		return "*"
	case CoverNetwork:
		return c.Prefix.String()
	case CoverWildcard:
		return "*." + c.Host
	}
	return c.Host
}

// ParseCover parses one entry of what a network service carries: "*",
// "example.com", "*.example.com", "192.0.2.7", "2001:db8::1" or a range such as
// "10.0.0.0/8". No ports: a service carries a destination on any port.
func ParseCover(s string) (Cover, error) {
	bad := func(why string) (Cover, error) { return Cover{}, fmt.Errorf("coverage entry %q: %s", s, why) }
	if s == "" || len(s) > maxEntryLen {
		return bad("empty or too long")
	}
	l := strings.ToLower(s)
	switch {
	case l == "*":
		return Cover{Kind: CoverAll}, nil
	case strings.Contains(l, "/"):
		p, err := netip.ParsePrefix(l)
		if err != nil {
			return bad("not an address range such as 10.0.0.0/8")
		}
		if p.Addr().Zone() != "" {
			return bad("an address range has no zone")
		}
		return Cover{Kind: CoverNetwork, Prefix: p.Masked()}, nil
	case strings.HasPrefix(l, "*."):
		rest := l[2:]
		if _, err := netip.ParseAddr(rest); err == nil || !validName(rest) {
			return bad("a wildcard applies to a domain name")
		}
		return Cover{Kind: CoverWildcard, Host: rest}, nil
	}
	if a, err := netip.ParseAddr(l); err == nil {
		if a.Zone() != "" {
			return bad("an address has no zone")
		}
		return Cover{Kind: CoverHost, Host: a.Unmap().String()}, nil
	}
	if !validName(l) {
		return bad("want *, a host name, *.domain, an address or an address range")
	}
	return Cover{Kind: CoverHost, Host: l}, nil
}

// CanonicalCovers validates and returns entries in canonical form: lower-case,
// deduplicated, sorted. At least one entry is required, and at most MaxCovers.
func CanonicalCovers(entries []string) ([]string, error) {
	if len(entries) == 0 {
		return nil, errors.New("a network service must say what it carries: at least one coverage entry (\"*\" for everything)")
	}
	set := map[string]bool{}
	for _, e := range entries {
		c, err := ParseCover(e)
		if err != nil {
			return nil, err
		}
		set[c.String()] = true
	}
	if len(set) > MaxCovers {
		return nil, fmt.Errorf("a network service may declare at most %d coverage entries, got %d", MaxCovers, len(set))
	}
	out := make([]string, 0, len(set))
	for e := range set {
		out = append(out, e)
	}
	sort.Strings(out)
	return out, nil
}

// Covers is what a network service carries.
type Covers []Cover

// ParseCovers parses entries; it does not require any.
func ParseCovers(entries []string) (Covers, error) {
	out := make(Covers, 0, len(entries))
	for _, e := range entries {
		c, err := ParseCover(e)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

// MatchesHost reports whether the cover names host, a URL host (names are
// matched after NormalizeHost). A range names addresses only: a name is not
// judged by where it might resolve, which the service's own address checks do.
func (c Cover) MatchesHost(host string) bool {
	host = NormalizeHost(host)
	if host == "" {
		return false
	}
	switch c.Kind {
	case CoverAll:
		return true
	case CoverNetwork:
		a, err := netip.ParseAddr(host)
		return err == nil && c.Prefix.Contains(a.Unmap())
	case CoverWildcard:
		return len(host) > len(c.Host)+1 && strings.HasSuffix(host, "."+c.Host)
	}
	return host == c.Host
}

// Includes reports whether the cover names everything an allow-list entry can
// match: "carries the entry" means carrying all of it, since a request to any
// host the entry allows may arrive.
func (c Cover) Includes(r Rule) bool {
	if c.Kind == CoverAll {
		return true
	}
	if !r.Wildcard {
		return c.MatchesHost(r.Host)
	}
	// "*.x" allows every name under x: only a catch-all, or a wildcard for x
	// or a domain x lies under, names all of them.
	return c.Kind == CoverWildcard && (r.Host == c.Host || strings.HasSuffix(r.Host, "."+c.Host))
}

// Rank orders how precisely a cover names what it matches, most precise last:
// a host, then a wildcard (a longer suffix is more precise), then a range (a
// longer prefix is more precise), then a catch-all. It is the key by which a
// request is given to the best-fitting of several services.
func (c Cover) Rank() [2]int {
	switch c.Kind {
	case CoverHost:
		return [2]int{4, 0}
	case CoverWildcard:
		return [2]int{3, len(c.Host)}
	case CoverNetwork:
		return [2]int{2, c.Prefix.Bits()}
	}
	return [2]int{1, 0}
}

// BestFor returns the most precise cover among cs that includes the entry, and
// whether there is one.
func (cs Covers) BestFor(r Rule) (Cover, bool) {
	var best Cover
	found := false
	for _, c := range cs {
		if !c.Includes(r) {
			continue
		}
		if !found || less(best.Rank(), c.Rank()) {
			best, found = c, true
		}
	}
	return best, found
}

func less(a, b [2]int) bool { return a[0] < b[0] || a[0] == b[0] && a[1] < b[1] }

// Compare orders two ranks: negative if a is less precise than b.
func CompareRank(a, b [2]int) int {
	switch {
	case less(a, b):
		return -1
	case less(b, a):
		return 1
	}
	return 0
}

// MatchesHost reports whether any cover names host.
func (cs Covers) MatchesHost(host string) bool {
	for _, c := range cs {
		if c.MatchesHost(host) {
			return true
		}
	}
	return false
}
