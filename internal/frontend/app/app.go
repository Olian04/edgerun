// Package app assembles and runs a frontend. cmd/frontend and the end-to-end
// tests both go through it, so what is tested is what ships.
package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/Olian04/edgerun/internal/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/Olian04/edgerun"
	"github.com/Olian04/edgerun/internal/frontend"
)

// Config configures a frontend. Zero values select the defaults.
type Config struct {
	// Listen is the public HTTP address, e.g. ":8000".
	Listen string
	// HealthListen serves /healthz on a separate port, so no function's route
	// can shadow it. Empty disables it.
	HealthListen    string
	OrchestratorURL string
	IngressToken    string

	RequestTimeout time.Duration // whole-invocation budget. Default 30s.
	TrustForwarded bool          // see frontend.Config
	// LimitRefresh is how often the orchestrator's body limit is re-read.
	// Default 30s.
	LimitRefresh    time.Duration
	ShutdownTimeout time.Duration // Default 30s.

	Log *slog.Logger
}

// Server is a running frontend.
type Server struct {
	cfg     Config
	http    *http.Server
	ln      net.Listener
	health  *http.Server
	hln     net.Listener
	cancel  context.CancelFunc
	stopped chan struct{}
}

// Start begins serving and returns. The orchestrator need not be reachable yet:
// until it answers, the default body limit applies and requests that cannot be
// forwarded fail with 503.
func Start(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.Listen == "" {
		return nil, errors.New("listen address is required")
	}
	if u, err := url.ParseRequestURI(cfg.OrchestratorURL); err != nil || u.Host == "" {
		return nil, fmt.Errorf("orchestrator URL %q: want an http(s) URL", cfg.OrchestratorURL)
	}
	if cfg.IngressToken == "" {
		return nil, errors.New("the ingress token is required")
	}
	if cfg.LimitRefresh == 0 {
		cfg.LimitRefresh = 30 * time.Second
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 256
	transport.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	client := edgerun.NewIngress(cfg.OrchestratorURL, cfg.IngressToken, edgerun.WithHTTPClient(&http.Client{Transport: transport}))
	fe := frontend.New(frontend.Config{
		Ingress: client, RequestTimeout: cfg.RequestTimeout, TrustForwarded: cfg.TrustForwarded, Log: cfg.Log,
	})

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	s := &Server{cfg: cfg, ln: ln, stopped: make(chan struct{})}
	// No write timeout: responses stream. Slow clients are bounded per write.
	s.http = &http.Server{Handler: otelhttp.NewHandler(fe, "edgerun.frontend",
		otelhttp.WithPropagators(telemetry.Propagator()), otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method })), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 60 * time.Second,
		IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 64 << 10}

	if cfg.HealthListen != "" {
		hln, err := net.Listen("tcp", cfg.HealthListen)
		if err != nil {
			ln.Close()
			return nil, fmt.Errorf("health listen: %w", err)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
		s.hln, s.health = hln, &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go s.health.Serve(hln)
	}

	bg, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go func() { defer close(s.stopped); fe.RunRefresh(bg, cfg.LimitRefresh) }()
	go func() {
		if err := s.http.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			cfg.Log.Error("serve", "error", err)
		}
	}()
	cfg.Log.Info("frontend listening", "addr", ln.Addr().String(), "orchestrator", cfg.OrchestratorURL)
	return s, nil
}

// Addr is the public address.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Stop shuts down gracefully: it stops accepting connections and lets requests
// in flight finish, up to the shutdown timeout.
func (s *Server) Stop(ctx context.Context) error {
	sctx, cancel := context.WithTimeout(ctx, s.cfg.ShutdownTimeout)
	defer cancel()
	err := s.http.Shutdown(sctx)
	if err != nil {
		s.http.Close()
	}
	if s.health != nil {
		s.health.Close()
	}
	s.cancel()
	<-s.stopped
	return err
}
