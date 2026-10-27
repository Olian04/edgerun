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
	"time"

	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/egress/server"
)

// Config configures a network service. Zero values select the defaults.
type Config struct {
	// Listen is the address to serve on, e.g. ":8090".
	Listen string
	// Token is the bearer token workers present (scope "egress").
	Token string
	// AllowNetworks are ranges the service may connect to although it refuses
	// them by default (private networks, loopback...).
	AllowNetworks []netip.Prefix
	// MaxRequestBody and MaxResponseBody bound one exchange.
	MaxRequestBody, MaxResponseBody uint64
	ShutdownTimeout                 time.Duration // Default 30s.
	Version                         string
	Log                             *slog.Logger
}

// Server is a running network service.
type Server struct {
	cfg  Config
	http *http.Server
	ln   net.Listener
}

// Start begins serving and returns.
func Start(_ context.Context, cfg Config) (*Server, error) {
	if cfg.Listen == "" {
		return nil, errors.New("listen address is required")
	}
	if cfg.Token == "" {
		return nil, errors.New("the egress token is required")
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	svc := server.New(server.Config{
		AllowNetworks: cfg.AllowNetworks, MaxRequestBody: cfg.MaxRequestBody, MaxResponseBody: cfg.MaxResponseBody,
		Version: cfg.Version, Log: cfg.Log,
	})
	mux := http.NewServeMux()
	mux.Handle(svc.Handler(authn.Verifier{cfg.Token: server.Scope}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	// No write timeout: responses stream, and the call's deadline bounds them.
	s := &Server{cfg: cfg, ln: ln, http: &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout: 2 * time.Minute, Protocols: &protocols}}
	go func() {
		if err := s.http.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			cfg.Log.Error("serve", "error", err)
		}
	}()
	for _, p := range cfg.AllowNetworks {
		cfg.Log.Warn("connecting to this network is allowed, although it is refused by default", "network", p.String())
	}
	cfg.Log.Info("egress listening", "addr", ln.Addr().String())
	return s, nil
}

// Addr is the address the service listens on.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Stop shuts down gracefully, letting requests in flight finish up to the
// shutdown timeout.
func (s *Server) Stop(ctx context.Context) error {
	sctx, cancel := context.WithTimeout(ctx, s.cfg.ShutdownTimeout)
	defer cancel()
	if err := s.http.Shutdown(sctx); err != nil {
		s.http.Close()
		return err
	}
	return nil
}
