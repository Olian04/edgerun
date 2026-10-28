package guard

import (
	"net/netip"
	"testing"
)

func TestForbidden(t *testing.T) {
	for _, s := range []string{
		"0.0.0.0", "0.1.2.3", "10.0.0.1", "172.16.0.1", "172.31.255.255", "192.168.1.1", "100.64.0.1", "100.127.255.255",
		"127.0.0.1", "127.255.255.254", "169.254.169.254", "224.0.0.1", "239.255.255.255", "255.255.255.255", "240.0.0.1",
		"192.0.2.1", "198.51.100.1", "203.0.113.9", "198.18.0.1", "198.19.255.255", "192.0.0.1", "192.88.99.1",
		"::", "::1", "::2", "::ffff:127.0.0.1", "::ffff:10.0.0.1", "::ffff:169.254.169.254", "::10.0.0.1",
		"fc00::1", "fd00::1", "fe80::1", "fec0::1", "ff02::1", "100::1",
		"2001:db8::1", "2001::1", "2001:2::1", "2001:10::1", "3fff::1",
		"64:ff9b::7f00:1", "64:ff9b::a00:1", "64:ff9b::a9fe:a9fe", "64:ff9b:1::1",
		"2002:7f00:1::", "2002:a00:1::1", "2002:c0a8:101::1", "2002:a9fe:a9fe::",
	} {
		a := netip.MustParseAddr(s)
		if Check(a, nil) == nil {
			t.Errorf("%s allowed", s)
		}
	}
	for _, s := range []string{"1.1.1.1", "8.8.8.8", "93.184.216.34", "172.15.0.1", "172.32.0.1", "100.63.255.255", "100.128.0.1", "192.0.3.1", "198.17.0.1", "198.20.0.1", "223.255.255.255",
		"2606:4700::1111", "2001:4860:4860::8888", "2a00:1450::1", "64:ff9b::808:808", "2002:808:808::1", "::ffff:8.8.8.8"} {
		if err := Check(netip.MustParseAddr(s), nil); err != nil {
			t.Errorf("%s refused: %v", s, err)
		}
	}
}

func TestOperatorRanges(t *testing.T) {
	nets := []netip.Prefix{netip.MustParsePrefix("10.20.0.0/16")}
	if Check(netip.MustParseAddr("10.20.1.1"), nets) != nil || Check(netip.MustParseAddr("::ffff:10.20.1.1"), nets) != nil {
		t.Error("an opened range must be reachable, in IPv4 or IPv4-mapped form")
	}
	for _, s := range []string{"10.21.0.1", "127.0.0.1", "169.254.169.254", "64:ff9b::a14:101"} {
		if Check(netip.MustParseAddr(s), nets) == nil {
			t.Errorf("%s allowed though only 10.20/16 was opened", s)
		}
	}
}

func TestLocalNames(t *testing.T) {
	for _, n := range []string{"localhost", "LOCALHOST.", "a.localhost", "a.b.localhost."} {
		if !IsLocalName(n) {
			t.Errorf("%s", n)
		}
	}
	for _, n := range []string{"localhost.example.com", "notlocalhost", "example.com"} {
		if IsLocalName(n) {
			t.Errorf("%s", n)
		}
	}
}
