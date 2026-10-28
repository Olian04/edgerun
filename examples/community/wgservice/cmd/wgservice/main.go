// Command wgservice is an edgerun network service (egress.v1) that makes every
// upstream connection through a userspace WireGuard tunnel: the destinations it
// carries are reachable only on the other side of the tunnel, and names are
// resolved there too.
//
//	wgservice --listen 127.0.0.1:8090 --orchestrator-url http://orch:8080 \
//	  --network-token ... --egress-token ... \
//	  --covers 10.20.0.0/16,*.corp.internal --allow-network 10.20.0.0/16 \
//	  --wg-config /etc/wgservice/tunnel.conf
//
// Every flag has an environment variable (EDGERUN_EGRESS_LISTEN, ...); a flag wins.
package main

import (
	"context"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Olian04/edgerun"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"

	"example.com/edgerun-wgservice/internal/allow"
	"example.com/edgerun-wgservice/internal/lifecycle"
	"example.com/edgerun-wgservice/internal/service"
	"example.com/edgerun-wgservice/internal/wgnet"
)

const version = "0.1"

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error { *l = append(*l, strings.Split(v, ",")...); return nil }

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "wgservice:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		listen        = flag.String("listen", env("EDGERUN_EGRESS_LISTEN", ":8090"), "where workers reach this service")
		advertise     = flag.String("advertise-url", env("EDGERUN_EGRESS_ADVERTISE_URL", ""), "the URL workers reach it at (default: http://<listen>)")
		orchURL       = flag.String("orchestrator-url", env("EDGERUN_ORCHESTRATOR_URL", ""), "where to register")
		networkToken  = flag.String("network-token", env("EDGERUN_NETWORK_TOKEN", ""), "presented to the orchestrator (scope network)")
		egressToken   = flag.String("egress-token", env("EDGERUN_EGRESS_TOKEN", ""), "required of workers (scope egress)")
		covers        = flag.String("covers", env("EDGERUN_EGRESS_COVERS", ""), "what it carries, comma-separated: *, hosts, *.domain, addresses, ranges (required)")
		maxReq        = flag.Uint64("max-request-bytes", 1<<20, "largest request body it forwards")
		maxResp       = flag.Uint64("max-response-bytes", 4<<20, "largest response body it returns")
		drainTimeout  = flag.Duration("drain-timeout", 30*time.Second, "on SIGTERM: how long to wait for requests in flight")
		caFile        = flag.String("ca-file", env("EDGERUN_EGRESS_CA_FILE", ""), "PEM roots for https upstreams, instead of the system's")
		wgConf        = flag.String("wg-config", env("EDGERUN_WG_CONFIG", ""), "a wg-quick-like file: [Interface] PrivateKey, Address, DNS, MTU, ListenPort; [Peer] PublicKey, Endpoint, AllowedIPs, PersistentKeepalive")
		wgPrivateKey  = flag.String("wg-private-key", env("EDGERUN_WG_PRIVATE_KEY", ""), "overrides PrivateKey (prefer --wg-config or the environment: flags are visible in ps)")
		wgAddress     = flag.String("wg-address", "", "overrides Address: this end's address(es) inside the tunnel")
		wgDNS         = flag.String("wg-dns", "", "overrides DNS: resolver(s) inside the tunnel")
		wgListenPort  = flag.Int("wg-listen-port", 0, "overrides ListenPort (the UDP port of the tunnel)")
		wgPeerKey     = flag.String("wg-peer-public-key", "", "overrides the (first) peer's PublicKey")
		wgEndpoint    = flag.String("wg-endpoint", "", "overrides the (first) peer's Endpoint host:port")
		wgAllowedIPs  = flag.String("wg-allowed-ips", "", "overrides the (first) peer's AllowedIPs")
		wgKeepalive   = flag.Duration("wg-keepalive", 0, "overrides the (first) peer's PersistentKeepalive")
		allowNetworks listFlag
		verbose       = flag.Bool("v", false, "debug logging")
	)
	flag.Var(&allowNetworks, "allow-network", "a range the service may connect to although it is refused by default, e.g. the tunnel's (repeatable)")
	flag.Parse()
	if v := os.Getenv("EDGERUN_EGRESS_ALLOW_NETWORKS"); v != "" && len(allowNetworks) == 0 {
		allowNetworks.Set(v)
	}

	lvl := slog.LevelInfo
	if *verbose {
		lvl = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))

	if *orchURL == "" || *networkToken == "" {
		return errors.New("--orchestrator-url and --network-token are required: a service no orchestrator knows is given nothing")
	}
	if *egressToken == "" {
		return errors.New("--egress-token is required: a service that accepts everyone lets anyone use the tunnel")
	}
	cov, covText, err := allow.ParseCoverage(splitList(*covers))
	if err != nil {
		return fmt.Errorf("--covers: %w", err)
	}
	var nets []netip.Prefix
	for _, s := range allowNetworks {
		if s = strings.TrimSpace(s); s == "" {
			continue
		}
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return fmt.Errorf("--allow-network %q: %w", s, err)
		}
		nets = append(nets, p.Masked())
	}

	// The tunnel.
	var tcfg wgnet.Config
	if *wgConf != "" {
		f, err := os.Open(*wgConf)
		if err != nil {
			return err
		}
		tcfg, err = wgnet.ParseConfig(f)
		f.Close()
		if err != nil {
			return fmt.Errorf("%s: %w", *wgConf, err)
		}
	}
	if err := override(&tcfg, *wgPrivateKey, *wgAddress, *wgDNS, *wgListenPort, *wgPeerKey, *wgEndpoint, *wgAllowedIPs, *wgKeepalive); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	tun, err := wgnet.Up(ctx, tcfg, log)
	if err != nil {
		return fmt.Errorf("tunnel: %w", err)
	}
	defer tun.Close()

	var roots *x509.CertPool
	if *caFile != "" {
		pem, err := os.ReadFile(*caFile)
		if err != nil {
			return err
		}
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return fmt.Errorf("%s holds no certificates", *caFile)
		}
	}

	srv := service.New(service.Config{
		Dialer: tun, Coverage: cov, CoverageText: covText, AllowNetworks: nets,
		MaxRequestBody: *maxReq, MaxResponseBody: *maxResp, RootCAs: roots, Version: version, Log: log,
	})
	mux := http.NewServeMux()
	mux.Handle(srv.Handler(edgerun.Verifier{*egressToken: edgerun.ScopeEgress}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	endpoint := *advertise
	if endpoint == "" {
		endpoint = "http://" + ln.Addr().String()
	}
	hs := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	// h2c, as the other edgerun components serve: Connect streams over HTTP/1.1 too.
	var protos http.Protocols
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	hs.Protocols = &protos
	go func() {
		if err := hs.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("serve", "err", err)
			stop()
		}
	}()
	log.Info("serving", "listen", ln.Addr().String(), "endpoint", endpoint, "covers", covText, "allow-networks", nets)

	loopCtx, stopLoop := context.WithCancel(context.Background())
	loop := &lifecycle.Loop{
		Reg:      edgerun.NewRegistry(*orchURL, *networkToken),
		Endpoint: endpoint,
		Info:     &egressv1.ServiceInfo{Implementation: service.Implementation(version), Covers: covText},
		Log:      log, InFlight: srv.InFlight, DrainTimeout: *drainTimeout,
	}
	loop.Start(loopCtx)

	<-ctx.Done()
	log.Info("stopping: draining")
	loop.Drain(stopLoop)
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hs.Shutdown(shutdown)
	srv.Close()
	return nil
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

func override(c *wgnet.Config, priv, addr, dns string, port int, peerKey, endpoint, allowed string, keepalive time.Duration) error {
	if priv != "" {
		k, err := wgnet.ParseKey(priv)
		if err != nil {
			return fmt.Errorf("--wg-private-key: %w", err)
		}
		c.PrivateKey = k
	}
	if addr != "" {
		c.Address = nil
		for _, s := range splitList(addr) {
			p, err := netip.ParsePrefix(s)
			if err != nil {
				a, err2 := netip.ParseAddr(s)
				if err2 != nil {
					return fmt.Errorf("--wg-address %q: %w", s, err)
				}
				c.Address = append(c.Address, a)
				continue
			}
			c.Address = append(c.Address, p.Addr())
		}
	}
	if dns != "" {
		c.DNS = nil
		for _, s := range splitList(dns) {
			a, err := netip.ParseAddr(s)
			if err != nil {
				return fmt.Errorf("--wg-dns %q: %w", s, err)
			}
			c.DNS = append(c.DNS, a)
		}
	}
	if port != 0 {
		c.ListenPort = port
	}
	if peerKey != "" || endpoint != "" || allowed != "" || keepalive != 0 {
		if len(c.Peers) == 0 {
			c.Peers = []wgnet.Peer{{}}
		}
		p := &c.Peers[0]
		if peerKey != "" {
			k, err := wgnet.ParseKey(peerKey)
			if err != nil {
				return fmt.Errorf("--wg-peer-public-key: %w", err)
			}
			p.PublicKey = k
		}
		if endpoint != "" {
			p.Endpoint = endpoint
		}
		if allowed != "" {
			p.AllowedIPs = nil
			for _, s := range splitList(allowed) {
				pf, err := netip.ParsePrefix(s)
				if err != nil {
					return fmt.Errorf("--wg-allowed-ips %q: %w", s, err)
				}
				p.AllowedIPs = append(p.AllowedIPs, pf)
			}
		}
		if keepalive != 0 {
			p.Keepalive = keepalive
		}
	}
	return nil
}
