package policy

import (
	"fmt"
	"strings"
	"testing"
)

func TestParseRule(t *testing.T) {
	for in, want := range map[string]string{
		"example.com":           "example.com",
		"Example.COM":           "example.com",
		"example.com:8443":      "example.com:8443",
		"*.example.com":         "*.example.com",
		"*.Example.com:8443":    "*.example.com:8443",
		"192.0.2.1":             "192.0.2.1",
		"192.0.2.1:8080":        "192.0.2.1:8080",
		"[2001:DB8::1]":         "[2001:db8::1]",
		"[2001:db8:0:0::1]:80":  "[2001:db8::1]:80",
		"a":                     "a",
		"xn--bcher-kva.example": "xn--bcher-kva.example",
		"a1.example":            "a1.example", // digits in a label are fine; a number as the last label is not
		"1.example":             "1.example",
	} {
		r, err := ParseRule(in)
		if err != nil || r.String() != want {
			t.Errorf("ParseRule(%q) = %v, %v; want %q", in, r, err, want)
		}
	}
	for _, in := range []string{
		"", "*", "*.", ".", "example.", ".example.com", "a..b", "-a.com", "a-.com", "a_b.com", "a b.com", "exa mple.com",
		"http://example.com", "example.com/path", "user@example.com", "example.com:", "example.com:0", "example.com:65536",
		"example.com:80a", "example.com:080", "*.192.0.2.1", "::1", "[::1", "[::1]x", "[192.0.2.1]", "[fe80::1%eth0]",
		"127.1", "2130706433", "0x7f.1", "a.0x1", "a.1", "*.0x7f", "*example.com", "a.*.com", "**.example.com", "bücher.example", strings.Repeat("a", 64) + ".com", strings.Repeat("a.", 130) + "com",
	} {
		if r, err := ParseRule(in); err == nil {
			t.Errorf("ParseRule(%q) = %v, want an error", in, r)
		}
	}
}

func TestCanonical(t *testing.T) {
	got, err := Canonical([]string{"B.example.com", "a.example.com", "b.example.com", "*.c.example.com:8443"})
	if err != nil || fmt.Sprint(got) != "[*.c.example.com:8443 a.example.com b.example.com]" {
		t.Errorf("Canonical = %v, %v", got, err)
	}
	if got, err := Canonical(nil); err != nil || len(got) != 0 {
		t.Errorf("Canonical(nil) = %v, %v", got, err)
	}
	if _, err := Canonical([]string{"ok.com", "not ok"}); err == nil {
		t.Error("one bad entry must fail the list")
	}
	var many []string
	for i := 0; i < MaxEntries+1; i++ {
		many = append(many, fmt.Sprintf("h%d.example.com", i))
	}
	if _, err := Canonical(many); err == nil {
		t.Errorf("%d entries must be refused", len(many))
	}
	if _, err := Canonical(many[:MaxEntries]); err != nil {
		t.Errorf("%d entries are allowed: %v", MaxEntries, err)
	}
	// Duplicates do not count twice.
	dup := append(append([]string{}, many[:MaxEntries]...), "H0.example.com")
	if _, err := Canonical(dup); err != nil {
		t.Errorf("a duplicate must not count against the limit: %v", err)
	}
}

func TestAllows(t *testing.T) {
	l, err := Parse([]string{"api.example.com", "*.cdn.example.com", "alt.example.com:8443", "192.0.2.1", "[2001:db8::1]:8080", "plain.example.com:80"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		scheme, host string
		port         int
		want         bool
	}{
		{"https", "api.example.com", 443, true},
		{"http", "api.example.com", 80, true},
		{"https", "API.Example.COM.", 443, true},  // case and a trailing dot are not part of the name
		{"https", "api.example.com", 8443, false}, // no port: only the scheme's default
		{"http", "api.example.com", 443, false},
		{"https", "x.api.example.com", 443, false}, // an exact entry is not a wildcard
		{"https", "example.com", 443, false},
		{"https", "a.cdn.example.com", 443, true},
		{"https", "a.b.cdn.example.com", 443, true},
		{"https", "cdn.example.com", 443, false},  // a wildcard does not match its apex
		{"https", "xcdn.example.com", 443, false}, // on a label boundary
		{"https", "a.cdn.example.com", 8443, false},
		{"https", "alt.example.com", 8443, true},
		{"https", "alt.example.com", 443, false}, // naming a port replaces the default
		{"http", "plain.example.com", 80, true},
		{"https", "plain.example.com", 80, true}, // the port is what is compared
		{"https", "192.0.2.1", 443, true},
		{"https", "192.0.2.2", 443, false},
		{"https", "2001:db8::1", 8080, true},
		{"https", "[2001:DB8:0::1]", 8080, true},
		{"https", "2001:db8::1", 443, false},
		{"ftp", "api.example.com", 21, false}, // a scheme with no default port matches no portless entry
		{"https", "", 443, false},
	} {
		if got := l.Allows(c.scheme, c.host, c.port); got != c.want {
			t.Errorf("Allows(%s, %q, %d) = %v, want %v", c.scheme, c.host, c.port, got, c.want)
		}
	}
	if (List{}).Allows("https", "api.example.com", 443) {
		t.Error("an empty list allows nothing")
	}
}

func TestParseCover(t *testing.T) {
	for in, want := range map[string]string{
		"*":                     "*",
		"Example.COM":           "example.com",
		"*.Example.com":         "*.example.com",
		"192.0.2.7":             "192.0.2.7",
		"::ffff:192.0.2.7":      "192.0.2.7",
		"2001:DB8::1":           "2001:db8::1",
		"10.1.2.3/8":            "10.0.0.0/8", // an address range is its network
		"fd00::/8":              "fd00::/8",
		"corp.internal":         "corp.internal",
		"*.corp.internal":       "*.corp.internal",
		"xn--bcher-kva.example": "xn--bcher-kva.example",
		"a1.example":            "a1.example", // digits in a label are fine; a number as the last label is not
		"1.example":             "1.example",
	} {
		c, err := ParseCover(in)
		if err != nil || c.String() != want {
			t.Errorf("ParseCover(%q) = %v, %v; want %q", in, c, err, want)
		}
	}
	for _, in := range []string{
		"", "**", "*.", "*.*", "*example.com", "a.*.com", "example.com:443", "example.com/24", "10.0.0.0/33", "10.0.0.0/",
		"*.192.0.2.1", "a b", "http://example.com", "-a.com", "a..b", "[::1]", "fe80::1%eth0", "bücher.example",
	} {
		if c, err := ParseCover(in); err == nil {
			t.Errorf("ParseCover(%q) = %v, want an error", in, c)
		}
	}
}

func TestCanonicalCovers(t *testing.T) {
	got, err := CanonicalCovers([]string{"B.example.com", "*", "10.1.1.1/8", "b.example.com"})
	if err != nil || fmt.Sprint(got) != "[* 10.0.0.0/8 b.example.com]" {
		t.Errorf("CanonicalCovers = %v, %v", got, err)
	}
	if _, err := CanonicalCovers(nil); err == nil {
		t.Error("a service that says nothing about what it carries is refused")
	}
	if _, err := CanonicalCovers([]string{"ok.com", "not ok"}); err == nil {
		t.Error("one bad entry fails the list")
	}
	var many []string
	for i := 0; i < MaxCovers+1; i++ {
		many = append(many, fmt.Sprintf("h%d.example.com", i))
	}
	if _, err := CanonicalCovers(many); err == nil {
		t.Error("too many entries")
	}
}

func TestCoverMatchesHost(t *testing.T) {
	for _, c := range []struct {
		cover, host string
		want        bool
	}{
		{"*", "anything.test", true},
		{"*", "192.0.2.1", true},
		{"example.com", "example.com", true},
		{"example.com", "EXAMPLE.com.", true},
		{"example.com", "a.example.com", false},
		{"*.example.com", "a.b.example.com", true},
		{"*.example.com", "example.com", false},
		{"*.example.com", "xexample.com", false},
		{"192.0.2.7", "192.0.2.7", true},
		{"192.0.2.7", "[::ffff:192.0.2.7]", true},
		{"10.0.0.0/8", "10.9.9.9", true},
		{"10.0.0.0/8", "11.0.0.1", false},
		{"10.0.0.0/8", "internal.corp", false}, // a name is not judged by where it might resolve
		{"fd00::/8", "fd12::1", true},
		{"fd00::/8", "[fd12::1]", true},
		{"example.com", "", false},
	} {
		cv, err := ParseCover(c.cover)
		if err != nil {
			t.Fatal(err)
		}
		if got := cv.MatchesHost(c.host); got != c.want {
			t.Errorf("%q matches %q = %v, want %v", c.cover, c.host, got, c.want)
		}
	}
}

func TestCoverIncludesAnAllowEntry(t *testing.T) {
	for _, c := range []struct {
		cover, entry string
		want         bool
	}{
		{"*", "api.example.com", true},
		{"*", "*.example.com:8443", true},
		{"example.com", "example.com:8443", true}, // a service carries a host on any port
		{"example.com", "api.example.com", false},
		{"*.example.com", "api.example.com", true},
		{"*.example.com", "*.cdn.example.com", true},
		{"*.example.com", "*.example.com", true},
		{"*.example.com", "*.com", false},
		{"*.cdn.example.com", "*.example.com", false}, // would carry only part of it
		{"api.example.com", "*.example.com", false},
		{"10.0.0.0/8", "10.1.2.3:8080", true},
		{"10.0.0.0/8", "11.1.2.3", false},
		{"10.0.0.0/8", "internal.corp", false},
		{"10.0.0.0/8", "*.corp", false},
		{"[::1]", "[::1]:80", false}, // not a coverage entry; see the next case
	} {
		cv, err := ParseCover(strings.Trim(c.cover, "[]"))
		if err != nil {
			t.Fatal(err)
		}
		r, err := ParseRule(c.entry)
		if err != nil {
			t.Fatal(err)
		}
		if c.cover == "[::1]" {
			c.want = true
		}
		if got := cv.Includes(r); got != c.want {
			t.Errorf("cover %q includes entry %q = %v, want %v", c.cover, c.entry, got, c.want)
		}
	}
}

func TestTheMostPreciseCoverWins(t *testing.T) {
	covers, err := ParseCovers([]string{"*", "10.0.0.0/8", "10.1.0.0/16", "*.example.com", "*.cdn.example.com", "api.cdn.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	for entry, want := range map[string]string{
		"api.cdn.example.com": "api.cdn.example.com",
		"img.cdn.example.com": "*.cdn.example.com",
		"www.example.com":     "*.example.com",
		"elsewhere.test":      "*",
		"10.1.2.3":            "10.1.0.0/16",
		"10.2.2.3":            "10.0.0.0/8",
		"*.cdn.example.com":   "*.cdn.example.com",
		"*.example.com":       "*.example.com",
		"*.test":              "*",
	} {
		r, _ := ParseRule(entry)
		got, ok := covers.BestFor(r)
		if !ok || got.String() != want {
			t.Errorf("best cover for %q = %v (%v), want %q", entry, got, ok, want)
		}
	}
	if _, ok := (Covers{}).BestFor(Rule{Host: "x.test"}); ok {
		t.Error("no covers carry nothing")
	}
	only, _ := ParseCovers([]string{"*.example.com"})
	r, _ := ParseRule("other.test")
	if _, ok := only.BestFor(r); ok {
		t.Error("a destination no cover names is carried by no one")
	}
	if CompareRank([2]int{4, 0}, [2]int{3, 99}) <= 0 || CompareRank([2]int{3, 5}, [2]int{3, 9}) >= 0 || CompareRank([2]int{1, 0}, [2]int{1, 0}) != 0 {
		t.Error("rank order")
	}
}

func TestSpecialUseAndLegacyHosts(t *testing.T) {
	for host, want := range map[string]bool{"localhost": true, "LOCALHOST.": true, "a.localhost": true, "localhost.example.com": false, "notlocalhost": false} {
		if got := IsLocalhostName(host); got != want {
			t.Errorf("IsLocalhostName(%q) = %v, want %v", host, got, want)
		}
	}
	for host, want := range map[string]bool{"127.1": true, "2130706433": true, "0x7f.1": true, "a.1": true, "1.2.3.4": false, "::1": false, "example.com": false, "1.example": false} {
		if got := IsLegacyNumericHost(host); got != want {
			t.Errorf("IsLegacyNumericHost(%q) = %v, want %v", host, got, want)
		}
	}
}
