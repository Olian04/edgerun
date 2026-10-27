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
		"*example.com", "a.*.com", "**.example.com", "bücher.example", strings.Repeat("a", 64) + ".com", strings.Repeat("a.", 130) + "com",
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
