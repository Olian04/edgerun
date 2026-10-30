// Package wgnet runs a userspace WireGuard peer (wireguard-go with its gVisor
// netstack TUN) and gives access to the network inside the tunnel: dialing and,
// separately, name resolution, both only through the tunnel. No root, no kernel
// support and no host network interface are needed.
package wgnet

import (
	"bufio"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

// Key is a WireGuard key (private, public or preshared), 32 bytes.
type Key [32]byte

// ParseKey reads the base64 form that `wg genkey` prints.
func ParseKey(s string) (Key, error) {
	var k Key
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil || len(b) != 32 {
		return k, errors.New("a WireGuard key is 32 bytes in base64")
	}
	copy(k[:], b)
	return k, nil
}

func (k Key) String() string { return base64.StdEncoding.EncodeToString(k[:]) }

// GenerateKey returns a new private key.
func GenerateKey() (Key, error) {
	var k Key
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return k, err
	}
	copy(k[:], priv.Bytes())
	return k, nil
}

// Public returns the public key of a private key.
func (k Key) Public() (Key, error) {
	var pub Key
	priv, err := ecdh.X25519().NewPrivateKey(k[:])
	if err != nil {
		return pub, err
	}
	copy(pub[:], priv.PublicKey().Bytes())
	return pub, nil
}

// Peer is the other end of the tunnel.
type Peer struct {
	PublicKey    Key
	PresharedKey *Key
	// Endpoint is host:port of the peer's UDP socket; empty if the peer dials us.
	Endpoint string
	// AllowedIPs are the addresses reachable through the peer (and accepted
	// from it).
	AllowedIPs []netip.Prefix
	// Keepalive, if non-zero, is the persistent keepalive interval.
	Keepalive time.Duration
}

// Config is the whole tunnel, as a wg-quick file says it.
type Config struct {
	PrivateKey Key
	// Address are the addresses of this end inside the tunnel.
	Address []netip.Addr
	// DNS are the resolvers inside the tunnel.
	DNS        []netip.Addr
	MTU        int
	ListenPort int
	Peers      []Peer
}

// ParseConfig reads a wg-quick-like file: [Interface] with PrivateKey, Address,
// DNS, MTU and ListenPort, and [Peer] sections with PublicKey, PresharedKey,
// Endpoint, AllowedIPs and PersistentKeepalive. Keys not listed (Table, PreUp,
// ...) are rejected: a file that means something this does not do must not be
// half obeyed.
func ParseConfig(r io.Reader) (Config, error) {
	var cfg Config
	var peer *Peer
	section := ""
	sc := bufio.NewScanner(r)
	line := 0
	flush := func() {
		if peer != nil {
			cfg.Peers = append(cfg.Peers, *peer)
			peer = nil
		}
	}
	for sc.Scan() {
		line++
		s := sc.Text()
		if i := strings.IndexByte(s, '#'); i >= 0 {
			s = s[:i]
		}
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if strings.HasPrefix(s, "[") {
			flush()
			switch strings.ToLower(s) {
			case "[interface]":
				section = "interface"
			case "[peer]":
				section = "peer"
				peer = &Peer{}
			default:
				return cfg, fmt.Errorf("line %d: unknown section %s", line, s)
			}
			continue
		}
		key, val, ok := strings.Cut(s, "=")
		if !ok {
			return cfg, fmt.Errorf("line %d: want key = value", line)
		}
		key, val = strings.ToLower(strings.TrimSpace(key)), strings.TrimSpace(val)
		var err error
		switch section {
		case "interface":
			err = cfg.setInterface(key, val)
		case "peer":
			err = peer.set(key, val)
		default:
			err = errors.New("outside any section")
		}
		if err != nil {
			return cfg, fmt.Errorf("line %d: %s: %w", line, key, err)
		}
	}
	flush()
	return cfg, sc.Err()
}

func (c *Config) setInterface(key, val string) (err error) {
	switch key {
	case "privatekey":
		c.PrivateKey, err = ParseKey(val)
	case "address":
		for _, f := range splitList(val) {
			a, e := parseAddrOrPrefix(f)
			if e != nil {
				return e
			}
			c.Address = append(c.Address, a)
		}
	case "dns":
		for _, f := range splitList(val) {
			a, e := netip.ParseAddr(f)
			if e != nil {
				return fmt.Errorf("%q: DNS must be addresses (search domains are not supported)", f)
			}
			c.DNS = append(c.DNS, a)
		}
	case "mtu":
		c.MTU, err = strconv.Atoi(val)
	case "listenport":
		c.ListenPort, err = strconv.Atoi(val)
	default:
		err = errors.New("unsupported")
	}
	return err
}

func (p *Peer) set(key, val string) (err error) {
	switch key {
	case "publickey":
		p.PublicKey, err = ParseKey(val)
	case "presharedkey":
		var k Key
		k, err = ParseKey(val)
		p.PresharedKey = &k
	case "endpoint":
		p.Endpoint = val
	case "allowedips":
		for _, f := range splitList(val) {
			pf, e := netip.ParsePrefix(f)
			if e != nil {
				return e
			}
			p.AllowedIPs = append(p.AllowedIPs, pf)
		}
	case "persistentkeepalive":
		if val == "off" {
			return nil
		}
		n, e := strconv.Atoi(val)
		if e != nil || n < 0 {
			return fmt.Errorf("invalid keepalive %q", val)
		}
		p.Keepalive = time.Duration(n) * time.Second
	default:
		err = errors.New("unsupported")
	}
	return err
}

func splitList(s string) []string {
	var out []string
	for _, f := range strings.Split(s, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

// parseAddrOrPrefix reads "10.20.0.1/32" or "10.20.0.1".
func parseAddrOrPrefix(s string) (netip.Addr, error) {
	if strings.Contains(s, "/") {
		p, err := netip.ParsePrefix(s)
		return p.Addr(), err
	}
	return netip.ParseAddr(s)
}

// Validate says what is missing.
func (c Config) Validate() error {
	var zero Key
	if c.PrivateKey == zero {
		return errors.New("no private key")
	}
	if len(c.Address) == 0 {
		return errors.New("no address for this end of the tunnel")
	}
	if len(c.Peers) == 0 {
		return errors.New("no peer")
	}
	for i, p := range c.Peers {
		if p.PublicKey == zero {
			return fmt.Errorf("peer %d: no public key", i+1)
		}
		if len(p.AllowedIPs) == 0 {
			return fmt.Errorf("peer %d: no allowed IPs: nothing would be routed to it", i+1)
		}
	}
	return nil
}

// uapi renders the configuration in wireguard-go's IPC format. Endpoints must
// be IP:port there, so a host name is resolved here, once, outside the tunnel:
// the one lookup this program makes with the system resolver.
func (c Config) uapi(ctx context.Context) (string, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "private_key=%s\n", hex.EncodeToString(c.PrivateKey[:]))
	if c.ListenPort != 0 {
		fmt.Fprintf(&b, "listen_port=%d\n", c.ListenPort)
	}
	for _, p := range c.Peers {
		fmt.Fprintf(&b, "public_key=%s\n", hex.EncodeToString(p.PublicKey[:]))
		if p.PresharedKey != nil {
			fmt.Fprintf(&b, "preshared_key=%s\n", hex.EncodeToString(p.PresharedKey[:]))
		}
		if p.Endpoint != "" {
			ep, err := resolveEndpoint(ctx, p.Endpoint)
			if err != nil {
				return "", err
			}
			fmt.Fprintf(&b, "endpoint=%s\n", ep)
		}
		fmt.Fprintf(&b, "persistent_keepalive_interval=%d\n", int(p.Keepalive/time.Second))
		b.WriteString("replace_allowed_ips=true\n")
		for _, a := range p.AllowedIPs {
			fmt.Fprintf(&b, "allowed_ip=%s\n", a)
		}
	}
	return b.String(), nil
}

func resolveEndpoint(ctx context.Context, ep string) (string, error) {
	host, port, err := net.SplitHostPort(ep)
	if err != nil {
		return "", fmt.Errorf("endpoint %q: %w", ep, err)
	}
	if a, err := netip.ParseAddr(host); err == nil {
		return netip.AddrPortFrom(a, mustPort(port)).String(), nil
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil || len(ips) == 0 {
		return "", fmt.Errorf("endpoint %q: %v", ep, err)
	}
	return netip.AddrPortFrom(ips[0].Unmap(), mustPort(port)).String(), nil
}

func mustPort(s string) uint16 {
	n, _ := strconv.ParseUint(s, 10, 16)
	return uint16(n)
}

// Tunnel is a running peer.
type Tunnel struct {
	// Net dials and listens inside the tunnel.
	Net *netstack.Net
	dev *device.Device
	// hasV4/hasV6: which address families this end has addresses for.
	hasV4, hasV6 bool
}

// Up creates the netstack device and brings the WireGuard device up.
func Up(ctx context.Context, cfg Config, log *slog.Logger) (*Tunnel, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	mtu := cfg.MTU
	if mtu == 0 {
		mtu = 1420
	}
	tdev, tnet, err := netstack.CreateNetTUN(cfg.Address, cfg.DNS, mtu)
	if err != nil {
		return nil, err
	}
	level := device.LogLevelError
	if log != nil && log.Enabled(ctx, slog.LevelDebug) {
		level = device.LogLevelVerbose
	}
	dev := device.NewDevice(tdev, conn.NewDefaultBind(), device.NewLogger(level, "wireguard: "))
	conf, err := cfg.uapi(ctx)
	if err != nil {
		dev.Close()
		return nil, err
	}
	if err := dev.IpcSet(conf); err != nil {
		dev.Close()
		return nil, fmt.Errorf("configuring the tunnel: %w", err)
	}
	if err := dev.Up(); err != nil {
		dev.Close()
		return nil, fmt.Errorf("bringing the tunnel up: %w", err)
	}
	t := &Tunnel{Net: tnet, dev: dev}
	for _, a := range cfg.Address {
		t.hasV4 = t.hasV4 || a.Is4()
		t.hasV6 = t.hasV6 || a.Is6()
	}
	return t, nil
}

// Close takes the tunnel down.
func (t *Tunnel) Close() { t.dev.Close() }

// LookupHost resolves a name through the tunnel's DNS servers, and only them:
// netstack's resolver reads neither /etc/hosts nor /etc/resolv.conf and has no
// search domains. It returns every address.
func (t *Tunnel) LookupHost(ctx context.Context, host string) ([]netip.Addr, error) {
	ss, err := t.Net.LookupContextHost(ctx, host)
	if err != nil {
		return nil, err
	}
	out := make([]netip.Addr, 0, len(ss))
	for _, s := range ss {
		if a, err := netip.ParseAddr(s); err == nil {
			out = append(out, a.Unmap())
		}
	}
	return out, nil
}

// DialIP connects to one address inside the tunnel. There is no name in it: it
// is what a connection pinned to an address that was checked is made of.
func (t *Tunnel) DialIP(ctx context.Context, ap netip.AddrPort) (net.Conn, error) {
	if (ap.Addr().Is4() && !t.hasV4) || (ap.Addr().Is6() && !t.hasV6) {
		return nil, fmt.Errorf("the tunnel has no %s address", map[bool]string{true: "IPv4", false: "IPv6"}[ap.Addr().Is4()])
	}
	return t.Net.DialContextTCPAddrPort(ctx, ap)
}

// Families says which address families the tunnel has.
func (t *Tunnel) Families() (v4, v6 bool) { return t.hasV4, t.hasV6 }
