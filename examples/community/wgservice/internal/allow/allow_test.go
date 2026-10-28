package allow

import (
	"fmt"
	"slices"
	"testing"
)

func dest(t *testing.T, raw string) Dest {
	t.Helper()
	_, d, err := ParseURL(raw)
	if err != nil {
		t.Fatalf("%s: %v", raw, err)
	}
	return d
}

func TestEntries(t *testing.T) {
	good := map[string]string{
		"api.example.com":           "api.example.com",
		"API.Example.COM.":          "api.example.com",
		"api.example.com:8443":      "api.example.com:8443",
		"*.example.com":             "*.example.com",
		"*.Example.com:8080":        "*.example.com:8080",
		"192.0.2.7":                 "192.0.2.7",
		"[2001:db8::1]:8080":        "[2001:db8::1]:8080",
		"[2001:DB8::1]":             "[2001:db8::1]",
		"[::ffff:192.0.2.7]":        "192.0.2.7",
		"[::ffff:192.0.2.7]:99":     "192.0.2.7:99",
		"localhost:1":               "localhost:1",
		"a_b.example.com":           "a_b.example.com",
		"xn--bcher-kva.example.com": "xn--bcher-kva.example.com",
	}
	for in, want := range good {
		e, err := ParseEntry(in)
		if err != nil || e.String() != want {
			t.Errorf("%q: %v %v, want %q", in, e, err, want)
		}
	}
	bad := []string{
		"", "*", "*.", "**.example.com", "*example.com", "a.*.example.com", "*.1.2.3.4", "*.[::1]",
		"http://example.com", "example.com/x", "example.com?x", "exa mple.com", "bücher.example",
		"example.com:", "example.com:0", "example.com:65536", "example.com:-1", "example.com:80:80",
		"2001:db8::1", "[2001:db8::1", "[::1]x", "[1.2.3.4]", "[fe80::1%eth0]", "-a.example.com", "a..example.com",
		"127.1", "2130706433", "0x7f.1", "0177.0.0.1", "1.2.3", "user@example.com", "a.example.com\n",
	}
	for _, in := range bad {
		if e, err := ParseEntry(in); err == nil {
			t.Errorf("%q: accepted as %v", in, e)
		}
	}
}

func TestMatching(t *testing.T) {
	type c struct {
		url, entry string
		want       bool
	}
	for _, tc := range []c{
		{"http://api.example.com/", "api.example.com", true},
		{"https://api.example.com/", "api.example.com", true},
		{"http://API.EXAMPLE.COM./x", "api.example.com", true},
		{"http://api.example.com:8080/", "api.example.com", false}, // no port: default only
		{"http://api.example.com:80/", "api.example.com", true},
		{"https://api.example.com:80/", "api.example.com", false},
		{"http://api.example.com:443/", "api.example.com", false},
		{"http://api.example.com:8443/", "api.example.com:8443", true},
		{"http://api.example.com/", "api.example.com:8443", false}, // naming a port replaces the default
		{"http://sub.api.example.com/", "api.example.com", false},
		{"http://api.example.com.evil.test/", "api.example.com", false},
		{"http://a.example.com/", "*.example.com", true},
		{"http://a.b.example.com/", "*.example.com", true},
		{"http://example.com/", "*.example.com", false},
		{"http://xexample.com/", "*.example.com", false},
		{"http://a.example.com.evil.test/", "*.example.com", false},
		{"http://a.example.com:81/", "*.example.com", false},
		{"http://192.0.2.7/", "192.0.2.7", true},
		{"http://192.0.2.8/", "192.0.2.7", false},
		{"http://[::ffff:192.0.2.7]/", "192.0.2.7", true},
		{"http://192.0.2.7/", "[::ffff:192.0.2.7]", true},
		{"http://[2001:db8::1]:8080/", "[2001:db8::1]:8080", true},
		{"http://[2001:db8::1]/", "[2001:db8::1]:8080", false},
		{"http://[2001:db8::1]/", "[2001:db8::1]", true},
		{"http://192.0.2.7/", "*.0.2.7", false},
	} {
		e, err := ParseEntry(tc.entry)
		if err != nil {
			if tc.want {
				t.Errorf("%s: %v", tc.entry, err)
			}
			continue
		}
		if got := e.Matches(dest(t, tc.url)); got != tc.want {
			t.Errorf("%s allow %s = %v, want %v", tc.url, tc.entry, got, tc.want)
		}
	}
	// A literal address is never matched by a host-name entry, nor a name by an address.
	e, _ := ParseEntry("example.com")
	if e.Matches(dest(t, "http://192.0.2.7/")) {
		t.Error("an address matched a name")
	}
}

func TestURLs(t *testing.T) {
	bad := []string{
		"", "echo", "/echo", "ftp://x.test/", "file:///etc/passwd", "http:///echo", "http://u:p@x.test/", "http://u@x.test/",
		"http://127.0.0.1:0/", "http://127.0.0.1:99999/", "http://bücher.example/", "http://exa mple.com/", "http://x.test:/",
		"http://127.1/", "http://2130706433/", "http://0x7f.0.0.1/", "http://0177.0.0.1/", "http://[fe80::1%25eth0]/", "http://[::1/",
		"http://x.test%2e.evil/", "http://x.test/\n", "mailto:a@b.c", "http:x.test", "//x.test/", "http://-x.test/", "http://x..test/",
	}
	for _, in := range bad {
		if _, d, err := ParseURL(in); err == nil {
			t.Errorf("%q accepted: %+v", in, d)
		}
	}
	d := dest(t, "HTTP://Example.COM.:8080/p?q=1#f")
	if d.Scheme != "http" || d.Host != "example.com" || d.Port != 8080 {
		t.Errorf("%+v", d)
	}
	d = dest(t, "http://[::ffff:127.0.0.1]/")
	if !d.IsAddr() || d.Addr.String() != "127.0.0.1" {
		t.Errorf("an IPv4-mapped host must be the IPv4 address: %+v", d)
	}
}

func TestCoverage(t *testing.T) {
	cov, canon, err := ParseCoverage([]string{"10.20.5.9/16", "*.Corp.Internal", "EXAMPLE.com", "example.com", "192.0.2.7", "FD00::/8", "::ffff:10.1.0.0/112", "2001:DB8::1"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"*.corp.internal", "10.1.0.0/16", "10.20.0.0/16", "192.0.2.7", "2001:db8::1", "example.com", "fd00::/8"}
	if !slices.Equal(canon, want) {
		t.Fatalf("canonical coverage %v, want %v", canon, want)
	}
	for url, ok := range map[string]bool{
		"http://10.20.255.1/":             true,
		"http://10.21.0.1/":               false,
		"http://a.corp.internal/":         true,
		"http://corp.internal/":           false,
		"http://example.com/":             true,
		"http://sub.example.com/":         false,
		"http://192.0.2.7:8080/":          true,
		"http://[fd12::1]/":               true,
		"http://[2001:db8::1]/":           true,
		"http://[2001:db8::2]/":           false,
		"http://10.20.0.1.corp.internal/": true,
	} {
		if got := cov.Carries(dest(t, url)); got != ok {
			t.Errorf("carries %s = %v, want %v", url, got, ok)
		}
	}
	// A name is not judged by where it might resolve: a range does not carry names.
	if cov.Carries(dest(t, "http://other.test/")) {
		t.Error("carried a name nobody named")
	}
	all, _, _ := ParseCoverage([]string{"*"})
	if !all.Carries(dest(t, "http://x.test/")) || !all.Carries(dest(t, "http://1.2.3.4/")) {
		t.Error("* carries everything")
	}
	for _, bad := range [][]string{nil, {}, {""}, {"example.com:80"}, {"http://example.com"}, {"10.0.0.0/33"}, {"*.1.2.3.4"}, {"a b"}} {
		if _, _, err := ParseCoverage(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	var many []string
	for i := 0; i < 65; i++ {
		many = append(many, fmt.Sprintf("h%d.example.com", i))
	}
	if _, _, err := ParseCoverage(many); err == nil {
		t.Error("65 entries accepted")
	}
}
