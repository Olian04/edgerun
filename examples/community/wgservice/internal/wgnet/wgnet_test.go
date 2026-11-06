package wgnet

import (
	"context"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func TestParseConfig(t *testing.T) {
	priv, _ := GenerateKey()
	peer, _ := GenerateKey()
	pub, _ := peer.Public()
	psk, _ := GenerateKey()
	text := `
# a wg-quick file
[Interface]
PrivateKey = ` + priv.String() + `
Address = 10.20.0.1/32, fd00::1/128
DNS = 10.20.0.53
MTU = 1280
ListenPort = 23099

[Peer]
PublicKey = ` + pub.String() + `
PresharedKey = ` + psk.String() + `
Endpoint = 127.0.0.1:23098
AllowedIPs = 10.20.0.0/16, fd00::/64
PersistentKeepalive = 25
`
	c, err := ParseConfig(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	if c.PrivateKey != priv || c.MTU != 1280 || c.ListenPort != 23099 || len(c.Address) != 2 || c.Address[0] != netip.MustParseAddr("10.20.0.1") ||
		c.DNS[0] != netip.MustParseAddr("10.20.0.53") || len(c.Peers) != 1 {
		t.Fatalf("%+v", c)
	}
	p := c.Peers[0]
	if p.PublicKey != pub || *p.PresharedKey != psk || p.Endpoint != "127.0.0.1:23098" || len(p.AllowedIPs) != 2 || p.Keepalive != 25*time.Second {
		t.Fatalf("%+v", p)
	}
	u, err := c.uapi(context.Background())
	if err != nil || !strings.Contains(u, "persistent_keepalive_interval=25") || !strings.Contains(u, "allowed_ip=10.20.0.0/16") || !strings.Contains(u, "endpoint=127.0.0.1:23098") {
		t.Fatalf("%v\n%s", err, u)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		"[Interface]\nPrivateKey = nope\n", "[Interface]\nPostUp = iptables\n", "[Peer]\nAllowedIPs = 10.0.0.0/33\n", "PrivateKey = x\n",
		"[Interface]\nDNS = example.com\n", "[Wat]\n",
	} {
		if _, err := ParseConfig(strings.NewReader(bad)); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	if err := (Config{}).Validate(); err == nil {
		t.Error("an empty config is valid")
	}
}
