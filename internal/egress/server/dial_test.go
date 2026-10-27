package server

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"
)

func TestReachable(t *testing.T) {
	loopback := []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128")}
	for _, c := range []struct {
		ip    string
		allow []netip.Prefix
		want  bool
	}{
		{"8.8.8.8", nil, true},
		{"93.184.216.34", nil, true},
		{"2606:4700:4700::1111", nil, true},
		{"::ffff:8.8.8.8", nil, true}, // what the resolver returns for an IPv4 address: judged as IPv4
		{"127.0.0.1", nil, false},
		{"::ffff:127.0.0.1", nil, false},
		{"::1", nil, false},
		{"10.1.2.3", nil, false},
		{"::ffff:10.1.2.3", nil, false},
		{"172.31.255.255", nil, false},
		{"172.32.0.1", nil, true},
		{"192.168.0.1", nil, false},
		{"169.254.169.254", nil, false},
		{"100.64.0.1", nil, false},
		{"0.0.0.0", nil, false},
		{"255.255.255.255", nil, false},
		{"224.0.0.1", nil, false},
		{"fe80::1", nil, false},
		{"fd12:3456::1", nil, false},
		{"2002:7f00:1::", nil, false},
		{"64:ff9b::7f00:1", nil, false},
		{"2001:db8::1", nil, false},
		// An operator's allowance, whichever form the address arrives in.
		{"127.0.0.1", loopback, true},
		{"::ffff:127.0.0.1", loopback, true},
		{"::1", loopback, true},
		{"127.0.0.1", []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}, false},
		{"10.1.2.3", []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}, true},
	} {
		if got := reachable(netip.MustParseAddr(c.ip), c.allow); got != c.want {
			t.Errorf("reachable(%s, %v) = %v, want %v", c.ip, c.allow, got, c.want)
		}
	}
}

// A name that resolves to several addresses is refused if any of them is
// forbidden, and no connection is made: the one that would have been used must
// not be the only one checked.
func TestEveryResolvedAddressIsChecked(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan struct{}, 4)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			accepted <- struct{}{}
			c.Close()
		}
	}()
	_, port, _ := net.SplitHostPort(l.Addr().String())

	dial := func(allow []netip.Prefix, ips ...string) (net.Conn, error) {
		var addrs []netip.Addr
		for _, ip := range ips {
			addrs = append(addrs, netip.MustParseAddr(ip))
		}
		g := &guardedDialer{allow: allow, timeout: time.Second,
			lookup: func(context.Context, string) ([]netip.Addr, error) { return addrs, nil }}
		return g.DialContext(context.Background(), "tcp", net.JoinHostPort("rebind.example", port))
	}
	loopback := []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}

	for name, ips := range map[string][]string{
		"the forbidden address first": {"127.0.0.1", "8.8.8.8"},
		"the forbidden address last":  {"8.8.8.8", "127.0.0.1"},
		"in the middle":               {"8.8.8.8", "10.0.0.1", "1.1.1.1"},
	} {
		conn, err := dial(nil, ips...)
		var d *deniedError
		if !errors.As(err, &d) {
			t.Errorf("%s: %v, want a refusal", name, err)
		}
		if conn != nil {
			conn.Close()
		}
	}
	select {
	case <-accepted:
		t.Error("a connection was made to a name with a forbidden address")
	case <-time.After(100 * time.Millisecond):
	}

	conn, err := dial(loopback, "127.0.0.1")
	if err != nil {
		t.Fatalf("an address the operator allowed: %v", err)
	}
	conn.Close()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Error("the allowed connection never arrived")
	}

	// Several allowed addresses: the dialer moves on from one that fails.
	conn, err = dial(loopback, "127.0.0.2", "127.0.0.1")
	if err == nil {
		conn.Close()
	}
}
