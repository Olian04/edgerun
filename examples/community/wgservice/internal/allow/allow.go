// Package allow implements the two small grammars of docs/protocol.md,
// "Network access": the per-request allow list (what a function may reach) and
// a network service's coverage (what the service carries), and the matching of
// a destination against each.
//
// Nothing here is provided by the edgerun library; it is written from the
// documentation alone. See FINDINGS.md for the places where the documentation
// left a choice.
package allow

import (
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// MaxEntries is the most entries an allow list or a coverage may hold.
const MaxEntries = 64

// Kind says what an entry names.
type Kind int

const (
	// Exact is a host name: api.example.com.
	Exact Kind = iota
	// Wildcard is *.example.com: any name under it, not example.com itself.
	Wildcard
	// Address is an IP address: 192.0.2.7 or [2001:db8::1].
	Address
)

// Entry is one allow-list entry.
type Entry struct {
	Kind Kind
	Host string     // Exact: the name; Wildcard: the suffix without "*."
	Addr netip.Addr // Address
	Port int        // 0: the default port of the URL's scheme
}

// String is the canonical form of the entry.
func (e Entry) String() string {
	var h string
	switch e.Kind {
	case Wildcard:
		h = "*." + e.Host
	case Address:
		if e.Addr.Is6() {
			h = "[" + e.Addr.String() + "]"
		} else {
			h = e.Addr.String()
		}
	default:
		h = e.Host
	}
	if e.Port != 0 {
		return h + ":" + strconv.Itoa(e.Port)
	}
	return h
}

// ParseEntry parses an allow-list entry. Entries are lower-cased.
func ParseEntry(s string) (Entry, error) {
	if s == "" {
		return Entry{}, errors.New("empty entry")
	}
	if err := plain(s); err != nil {
		return Entry{}, err
	}
	s = strings.ToLower(s)
	var host string
	var port int
	switch {
	case s[0] == '[':
		end := strings.IndexByte(s, ']')
		if end < 0 {
			return Entry{}, errors.New("unterminated [")
		}
		host = s[:end+1]
		rest := s[end+1:]
		if rest != "" {
			p, err := parsePort(strings.TrimPrefix(rest, ":"), strings.HasPrefix(rest, ":"))
			if err != nil {
				return Entry{}, err
			}
			port = p
		}
	case strings.Count(s, ":") == 1:
		i := strings.IndexByte(s, ':')
		host = s[:i]
		p, err := parsePort(s[i+1:], true)
		if err != nil {
			return Entry{}, err
		}
		port = p
	case strings.Contains(s, ":"):
		// docs/protocol.md writes IPv6 addresses in brackets; a bare one is
		// ambiguous with host:port, so it is not an entry.
		return Entry{}, errors.New("an IPv6 address must be written in brackets")
	default:
		host = s
	}

	e := Entry{Port: port}
	switch {
	case strings.HasPrefix(host, "["):
		a, err := parseBracketed(host)
		if err != nil {
			return Entry{}, err
		}
		e.Kind, e.Addr = Address, a
	case strings.HasPrefix(host, "*"):
		suffix, ok := strings.CutPrefix(host, "*.")
		if !ok || suffix == "" {
			return Entry{}, errors.New("a wildcard is written *.example.com; a bare * is not an entry")
		}
		if _, isIP := parseIP(suffix); isIP {
			return Entry{}, errors.New("there are no wildcards for addresses")
		}
		name, err := hostName(suffix)
		if err != nil {
			return Entry{}, err
		}
		if _, isIP := parseIP(name); isIP {
			return Entry{}, errors.New("there are no wildcards for addresses")
		}
		e.Kind, e.Host = Wildcard, name
	default:
		if a, ok := parseIP(host); ok {
			e.Kind, e.Addr = Address, a
			break
		}
		name, err := hostName(host)
		if err != nil {
			return Entry{}, err
		}
		e.Kind, e.Host = Exact, name
	}
	return e, nil
}

// ParseList parses a request's whole allow list: at most MaxEntries.
func ParseList(entries []string) ([]Entry, error) {
	if len(entries) > MaxEntries {
		return nil, fmt.Errorf("%d entries; at most %d", len(entries), MaxEntries)
	}
	out := make([]Entry, 0, len(entries))
	for _, s := range entries {
		e, err := ParseEntry(s)
		if err != nil {
			return nil, fmt.Errorf("allow entry %q: %w", s, err)
		}
		out = append(out, e)
	}
	return out, nil
}

// plain rejects what no entry or host contains: controls, spaces, non-ASCII,
// and the characters of a URL or a path.
func plain(s string) error {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c <= 0x20 || c >= 0x7f || strings.IndexByte("/?#@\\%\"'<>`{}|^", c) >= 0 {
			return fmt.Errorf("invalid character %q", c)
		}
	}
	return nil
}

func parsePort(s string, present bool) (int, error) {
	if !present || s == "" {
		return 0, errors.New("empty port")
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("invalid port %q", s)
		}
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > 65535 {
		return 0, fmt.Errorf("port %q out of range", s)
	}
	return n, nil
}

func parseBracketed(h string) (netip.Addr, error) {
	inner := h[1 : len(h)-1]
	a, err := netip.ParseAddr(inner)
	if err != nil || a.Zone() != "" || !strings.Contains(inner, ":") {
		return netip.Addr{}, fmt.Errorf("invalid IPv6 address %q", h)
	}
	return a.Unmap(), nil
}

// parseIP parses an IP address the strict way: IPv4 as four decimal numbers
// (never 127.1, 0x7f.1, 2130706433 or 0177.0.0.1, which resolvers and URL
// parsers elsewhere read as addresses), IPv6 without a zone. An IPv4 address
// in its IPv6 form is the IPv4 address.
func parseIP(s string) (netip.Addr, bool) {
	a, err := netip.ParseAddr(s)
	if err != nil || a.Zone() != "" {
		return netip.Addr{}, false
	}
	return a.Unmap(), true
}

// hostName validates and canonicalises a host name: ASCII letters, digits,
// hyphen and underscore in labels of 1-63 characters, at most 253 in all, no
// leading or trailing hyphen on a label, case and one trailing dot not part of
// the name. A name whose last label is a number (or 0x...) would be an IPv4
// address to some resolvers; it is refused rather than guessed at.
func hostName(s string) (string, error) {
	s = strings.ToLower(strings.TrimSuffix(s, "."))
	if s == "" || len(s) > 253 {
		return "", errors.New("invalid host name length")
	}
	labels := strings.Split(s, ".")
	for _, l := range labels {
		if l == "" || len(l) > 63 || l[0] == '-' || l[len(l)-1] == '-' {
			return "", fmt.Errorf("invalid host name %q", s)
		}
		for i := 0; i < len(l); i++ {
			c := l[i]
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
				return "", fmt.Errorf("host name %q is not ASCII letters, digits and hyphens (internationalised names are punycode)", s)
			}
		}
	}
	last := labels[len(labels)-1]
	if allDigits(last) || strings.HasPrefix(last, "0x") {
		return "", fmt.Errorf("host %q looks like a number, not a name", s)
	}
	return s, nil
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}

// Dest is the destination of a request: what an allow list and a coverage are
// matched against.
type Dest struct {
	Scheme string     // "http" or "https"
	Host   string     // canonical: lower case, no trailing dot; for an address, its text
	Addr   netip.Addr // valid if the host is an address literal (IPv4 in its IPv4 form)
	Port   int        // the URL's port, or the scheme's default
}

// IsAddr reports whether the host is an address literal.
func (d Dest) IsAddr() bool { return d.Addr.IsValid() }

// DefaultPort is the port a scheme implies.
func DefaultPort(scheme string) int {
	if scheme == "https" {
		return 443
	}
	return 80
}

// ParseURL checks that raw is an absolute http or https URL with a host, no user
// information and an ASCII host, and returns it with its destination.
func ParseURL(raw string) (*url.URL, Dest, error) {
	if raw == "" {
		return nil, Dest{}, errors.New("no URL")
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] <= 0x20 || raw[i] >= 0x7f {
			return nil, Dest{}, errors.New("the URL holds a space, a control or a non-ASCII character (escape it)")
		}
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, Dest{}, fmt.Errorf("malformed URL: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, Dest{}, fmt.Errorf("scheme %q: only http and https", u.Scheme)
	}
	if u.Opaque != "" || u.Host == "" {
		return nil, Dest{}, errors.New("the URL has no host")
	}
	if u.User != nil {
		return nil, Dest{}, errors.New("the URL carries user information")
	}
	d := Dest{Scheme: scheme, Port: DefaultPort(scheme)}
	if p := u.Port(); p != "" {
		n, err := parsePort(p, true)
		if err != nil {
			return nil, Dest{}, err
		}
		d.Port = n
	} else if strings.HasSuffix(u.Host, ":") {
		return nil, Dest{}, errors.New("empty port")
	}
	h := u.Hostname()
	if h == "" {
		return nil, Dest{}, errors.New("the URL has no host")
	}
	if strings.Contains(h, ":") {
		// An IPv6 literal. Go's parser accepts a zone ("%25eth0"); there is no
		// place for one in a destination.
		a, ok := parseIP(h)
		if !ok || !a.IsValid() {
			return nil, Dest{}, fmt.Errorf("invalid IPv6 host %q", h)
		}
		d.Addr, d.Host = a, a.String()
	} else if a, ok := parseIP(h); ok {
		d.Addr, d.Host = a, a.String()
	} else {
		name, err := hostName(h)
		if err != nil {
			return nil, Dest{}, err
		}
		d.Host = name
	}
	u.Scheme = scheme
	return u, d, nil
}

// Matches reports whether the entry allows d: scheme default port or the named
// port, and the host.
func (e Entry) Matches(d Dest) bool {
	want := e.Port
	if want == 0 {
		want = DefaultPort(d.Scheme)
	}
	if d.Port != want {
		return false
	}
	switch e.Kind {
	case Address:
		return d.IsAddr() && d.Addr == e.Addr
	case Exact:
		return !d.IsAddr() && d.Host == e.Host
	case Wildcard:
		return !d.IsAddr() && len(d.Host) > len(e.Host)+1 && strings.HasSuffix(d.Host, "."+e.Host)
	}
	return false
}

// Allowed reports whether any entry of list allows d.
func Allowed(list []Entry, d Dest) bool {
	for _, e := range list {
		if e.Matches(d) {
			return true
		}
	}
	return false
}

// CoverKind says what a coverage entry names.
type CoverKind int

const (
	CoverAll CoverKind = iota
	CoverExact
	CoverWildcard
	CoverAddress
	CoverPrefix
)

// Cover is one coverage entry: what a service carries, without ports.
type Cover struct {
	Kind   CoverKind
	Host   string // CoverExact: the name; CoverWildcard: the suffix
	Addr   netip.Addr
	Prefix netip.Prefix
}

// String is the canonical form: lower case, a range as its network, an IPv4
// address in its IPv4 form.
func (c Cover) String() string {
	switch c.Kind {
	case CoverAll:
		return "*"
	case CoverWildcard:
		return "*." + c.Host
	case CoverAddress:
		return c.Addr.String()
	case CoverPrefix:
		return c.Prefix.String()
	}
	return c.Host
}

// ParseCover parses one coverage entry.
func ParseCover(s string) (Cover, error) {
	if s == "" {
		return Cover{}, errors.New("empty coverage entry")
	}
	if s == "*" {
		return Cover{Kind: CoverAll}, nil
	}
	if strings.Contains(s, "/") {
		p, err := netip.ParsePrefix(s)
		if err != nil || p.Addr().Zone() != "" {
			return Cover{}, fmt.Errorf("invalid range %q", s)
		}
		return Cover{Kind: CoverPrefix, Prefix: unmapPrefix(p.Masked())}, nil
	}
	if a, ok := parseIP(strings.ToLower(s)); ok {
		return Cover{Kind: CoverAddress, Addr: a}, nil
	}
	e, err := ParseEntry(s)
	if err != nil {
		return Cover{}, err
	}
	if e.Port != 0 {
		return Cover{}, errors.New("a coverage entry has no port")
	}
	switch e.Kind {
	case Address:
		return Cover{Kind: CoverAddress, Addr: e.Addr}, nil
	case Wildcard:
		return Cover{Kind: CoverWildcard, Host: e.Host}, nil
	}
	return Cover{Kind: CoverExact, Host: e.Host}, nil
}

// unmapPrefix writes a range of IPv4-mapped IPv6 addresses (::ffff:a.b.c.d/n,
// n >= 96) as the IPv4 range it is.
func unmapPrefix(p netip.Prefix) netip.Prefix {
	if p.Addr().Is4In6() && p.Bits() >= 96 {
		return netip.PrefixFrom(p.Addr().Unmap(), p.Bits()-96)
	}
	return p
}

// Coverage is what a service carries.
type Coverage []Cover

// ParseCoverage parses and canonicalises coverage entries: lower-cased, ranges
// as their networks, deduplicated, sorted (byte order of the canonical text,
// which is what the conformance suite checks). It returns the canonical strings
// too, for registering and for Describe.
func ParseCoverage(entries []string) (Coverage, []string, error) {
	if len(entries) == 0 {
		return nil, nil, errors.New("a service must declare at least one coverage entry")
	}
	seen := map[string]Cover{}
	for _, s := range entries {
		c, err := ParseCover(strings.ToLower(strings.TrimSpace(s)))
		if err != nil {
			return nil, nil, fmt.Errorf("covers %q: %w", s, err)
		}
		seen[c.String()] = c
	}
	if len(seen) > MaxEntries {
		return nil, nil, fmt.Errorf("%d coverage entries; at most %d", len(seen), MaxEntries)
	}
	canon := make([]string, 0, len(seen))
	for s := range seen {
		canon = append(canon, s)
	}
	sort.Strings(canon)
	cov := make(Coverage, len(canon))
	for i, s := range canon {
		cov[i] = seen[s]
	}
	return cov, canon, nil
}

// Carries reports whether the service carries the destination's host (on any
// port). A name is judged by its name alone, never by where it might resolve.
func (c Coverage) Carries(d Dest) bool {
	for _, e := range c {
		switch e.Kind {
		case CoverAll:
			return true
		case CoverExact:
			if !d.IsAddr() && d.Host == e.Host {
				return true
			}
		case CoverWildcard:
			if !d.IsAddr() && len(d.Host) > len(e.Host)+1 && strings.HasSuffix(d.Host, "."+e.Host) {
				return true
			}
		case CoverAddress:
			if d.IsAddr() && d.Addr == e.Addr {
				return true
			}
		case CoverPrefix:
			if d.IsAddr() && e.Prefix.Contains(d.Addr) {
				return true
			}
		}
	}
	return false
}
