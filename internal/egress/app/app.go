// Package app assembles and runs a network service. cmd/egress and the tests
// both go through it, so what is tested is what ships.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/Olian04/edgerun"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/egress/policy"
	"github.com/Olian04/edgerun/internal/egress/registry"
	"github.com/Olian04/edgerun/internal/egress/server"
)

// Config configures a network service. Zero values select the defaults.
type Config struct {
	// Listen is the address to serve on, e.g. ":8090".
	Listen string
	// AdvertiseURL is the base URL at which workers reach this service. It may
	// be left out when Listen names a specific address.
	AdvertiseURL string
	// EgressToken is the bearer token workers present (scope "egress").
	EgressToken string

	// OrchestratorURL is where this service registers, presenting
	// NetworkToken. Registration is what makes the service exist as far as
	// functions are concerned.
	OrchestratorURL string
	NetworkToken    string
	// Covers is what this service carries: "*" for everything, or host names,
	// "*.domain" patterns, addresses and address ranges. At least one. A
	// service in a private network carries that network; one on the public
	// internet carries the rest.
	Covers []string

	// AllowNetworks are ranges the service may connect to although it refuses
	// them by default (private networks, loopback...).
	AllowNetworks []netip.Prefix
	// MaxRequestBody and MaxResponseBody bound one exchange.
	MaxRequestBody, MaxResponseBody uint64
	ShutdownTimeout                 time.Duration // Default 30s.
	// DrainGrace is how long the service keeps answering after it has said it is
	// draining, so that orchestrators stop granting it before it goes, and calls
	// from invocations that started earlier are not refused. Default 3s.
	DrainGrace time.Duration
	Version    string
	Log        *slog.Logger
}

// Server is a running network service.
type Server struct {
	cfg      Config
	http     *http.Server
	ln       net.Listener
	reg      *registry.Client
	stopReg  context.CancelFunc
	regDone  chan struct{}
	draining atomic.Bool
}

// Start begins serving and registers with the orchestrator, then returns.
func Start(_ context.Context, cfg Config) (*Server, error) {
	if cfg.Listen == "" {
		return nil, errors.New("listen address is required")
	}
	if cfg.EgressToken == "" {
		return nil, errors.New("the egress token is required")
	}
	if cfg.NetworkToken == "" {
		return nil, errors.New("the network token is required")
	}
	if u, err := url.ParseRequestURI(cfg.OrchestratorURL); err != nil || u.Host == "" {
		return nil, fmt.Errorf("orchestrator URL %q: want an http(s) URL", cfg.OrchestratorURL)
	}
	covers, err := policy.CanonicalCovers(cfg.Covers)
	if err != nil {
		return nil, err
	}
	cfg.Covers = covers
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.DrainGrace == 0 {
		cfg.DrainGrace = 3 * time.Second
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	advertise := cfg.AdvertiseURL
	if advertise == "" {
		addr := ln.Addr().(*net.TCPAddr)
		if addr.IP.IsUnspecified() {
			ln.Close()
			return nil, errors.New("advertise URL is required when listening on all interfaces")
		}
		advertise = "http://" + addr.String()
	}

	svc := server.New(server.Config{
		Covers: cfg.Covers, AllowNetworks: cfg.AllowNetworks,
		MaxRequestBody: cfg.MaxRequestBody, MaxResponseBody: cfg.MaxResponseBody,
		Version: cfg.Version, Log: cfg.Log,
	})
	mux := http.NewServeMux()
	mux.Handle(svc.Handler(authn.Verifier{cfg.EgressToken: server.Scope}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	// No write timeout: responses stream, and the call's deadline bounds them.
	s := &Server{cfg: cfg, ln: ln, regDone: make(chan struct{}), http: &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout: 2 * time.Minute, Protocols: &protocols}}
	go func() {
		if err := s.http.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			cfg.Log.Error("serve", "error", err)
		}
	}()
	for _, p := range cfg.AllowNetworks {
		cfg.Log.Warn("connecting to this network is allowed, although it is refused by default", "network", p.String())
	}

	s.reg = registry.New(registry.Config{
		Endpoint: advertise,
		Info:     serviceInfo(cfg),
		Registry: edgerun.NewRegistry(cfg.OrchestratorURL, cfg.NetworkToken),
		State: func() egressv1.ServiceState {
			if s.draining.Load() {
				return egressv1.ServiceState_SERVICE_STATE_DRAINING
			}
			return egressv1.ServiceState_SERVICE_STATE_READY
		},
		Log: cfg.Log,
	})
	regCtx, cancel := context.WithCancel(context.Background())
	s.stopReg = cancel
	go func() { defer close(s.regDone); s.reg.Run(regCtx) }()

	cfg.Log.Info("egress listening", "addr", ln.Addr().String(), "advertise", advertise, "covers", cfg.Covers, "orchestrator", cfg.OrchestratorURL)
	return s, nil
}

func serviceInfo(cfg Config) *egressv1.ServiceInfo {
	return &egressv1.ServiceInfo{
		Implementation: server.Implementation(cfg.Version),
		Covers:         cfg.Covers,
	}
}

// Addr is the address the service listens on.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Stop shuts down gracefully: it tells the orchestrator to send nothing new,
// lets requests in flight finish up to the shutdown timeout, then leaves.
func (s *Server) Stop(ctx context.Context) error {
	s.draining.Store(true)
	// Nothing new is sent here once the orchestrator knows; do not leave before it does.
	hctx, hcancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := s.reg.HeartbeatNow(hctx); err != nil {
		s.cfg.Log.Warn("could not tell the orchestrator this service is draining", "error", err)
	}
	hcancel()
	// Grants are made when an invocation starts, so older invocations may still
	// call: stay up for a grace period before closing the door.
	select {
	case <-time.After(s.cfg.DrainGrace):
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(ctx, s.cfg.ShutdownTimeout)
	defer cancel()
	err := s.http.Shutdown(sctx)
	if err != nil {
		s.http.Close()
	}
	s.stopReg()
	<-s.regDone
	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()
	if derr := s.reg.Deregister(dctx); derr != nil {
		s.cfg.Log.Warn("deregister failed", "error", derr)
	}
	return err
}
