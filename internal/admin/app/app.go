// Package app runs the admin UI as a server. cmd/admin and the tests go
// through it, so what is tested is what ships.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/Olian04/edgerun/internal/admin"
	"github.com/Olian04/edgerun/internal/admin/auth"
)

// Config configures the admin server.
type Config struct {
	Listen          string
	OrchestratorURL string
	AdminToken      string
	Users           []auth.User
	SessionKey      []byte
	SecureCookies   bool
	ShutdownTimeout time.Duration // Default 30s.
	Log             *slog.Logger
}

// Server is a running admin UI.
type Server struct {
	cfg  Config
	http *http.Server
	ln   net.Listener
}

// Start begins serving and returns.
func Start(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.Listen == "" {
		return nil, errors.New("listen address is required")
	}
	if u, err := url.ParseRequestURI(cfg.OrchestratorURL); err != nil || u.Host == "" {
		return nil, fmt.Errorf("orchestrator URL %q: want an http(s) URL", cfg.OrchestratorURL)
	}
	if cfg.AdminToken == "" {
		return nil, errors.New("the admin token is required")
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	h, err := admin.Handler(admin.HandlerConfig{
		Control: admin.NewControl(http.DefaultClient, cfg.OrchestratorURL, cfg.AdminToken),
		Users:   cfg.Users, SessionKey: cfg.SessionKey, SecureCookies: cfg.SecureCookies, Log: cfg.Log,
	})
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	// ReadTimeout bounds a module upload (webui asks for it); there is no write
	// timeout because pages stream.
	s := &Server{cfg: cfg, ln: ln, http: &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout: 5 * time.Minute, IdleTimeout: 2 * time.Minute}}
	go func() {
		if err := s.http.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			cfg.Log.Error("serve", "error", err)
		}
	}()
	cfg.Log.Info("admin listening", "addr", ln.Addr().String(), "orchestrator", cfg.OrchestratorURL)
	return s, nil
}

// Addr is the address the admin UI listens on.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Stop shuts down gracefully.
func (s *Server) Stop(ctx context.Context) error {
	sctx, cancel := context.WithTimeout(ctx, s.cfg.ShutdownTimeout)
	defer cancel()
	if err := s.http.Shutdown(sctx); err != nil {
		s.http.Close()
		return err
	}
	return nil
}
