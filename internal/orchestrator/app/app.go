// Package app assembles and runs an orchestrator. cmd/orchestrator and the
// end-to-end tests both go through it, so what is tested is what ships.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/orchestrator/control"
	"github.com/Olian04/edgerun/internal/orchestrator/fleet"
	"github.com/Olian04/edgerun/internal/orchestrator/invoke"
	"github.com/Olian04/edgerun/internal/orchestrator/janitor"
	"github.com/Olian04/edgerun/internal/orchestrator/migrate"
	"github.com/Olian04/edgerun/internal/orchestrator/netreg"
	"github.com/Olian04/edgerun/internal/orchestrator/record"
	"github.com/Olian04/edgerun/internal/orchestrator/secrets"
	"github.com/Olian04/edgerun/internal/orchestrator/snapshot"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
	"github.com/Olian04/edgerun/migrations"
)

// Config configures an orchestrator. Zero values select the defaults.
type Config struct {
	// Listen is the address all services are served on, e.g. ":8080".
	Listen      string
	DatabaseURL string

	// Inbound credentials, one per role. They must be distinct: a token has
	// exactly one scope.
	IngressToken string // presented by frontends
	WorkerToken  string // presented by workers
	AdminToken   string // presented by the admin UI and CLIs
	// ExecutionToken is what the orchestrator presents to workers.
	ExecutionToken string
	// NetworkToken is presented by network services that register. Without
	// one the orchestrator accepts no registrations, and functions have no
	// network.
	NetworkToken string

	// SecretKey seals secret environment values at rest (32 bytes).
	SecretKey []byte

	HeartbeatInterval   time.Duration // dictated to workers. Default 5s.
	RefreshEvery        time.Duration // routing/worker view. Default 1s.
	MaxRequestBodyBytes uint64        // Default 6 MiB.
	MaxModuleBytes      uint64        // Default 32 MiB.
	InvocationRetention time.Duration // Default 14 days.
	JanitorEvery        time.Duration // Default 1 hour.
	ShutdownTimeout     time.Duration // Default 30s.

	Version string
	Log     *slog.Logger
}

func (c *Config) setDefaults() {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	if c.RefreshEvery == 0 {
		c.RefreshEvery = time.Second
	}
	if c.InvocationRetention == 0 {
		c.InvocationRetention = 14 * 24 * time.Hour
	}
	if c.JanitorEvery == 0 {
		c.JanitorEvery = time.Hour
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	if c.Version == "" {
		c.Version = "dev"
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

func (c *Config) validate() error {
	if c.Listen == "" {
		return errors.New("listen address is required")
	}
	if c.DatabaseURL == "" {
		return errors.New("database URL is required")
	}
	tokens := map[string]string{"ingress": c.IngressToken, "worker": c.WorkerToken, "admin": c.AdminToken}
	if c.NetworkToken != "" {
		tokens["network"] = c.NetworkToken
	}
	seen := map[string]string{}
	for role, tok := range tokens {
		if tok == "" {
			return fmt.Errorf("the %s token is required", role)
		}
		if other, dup := seen[tok]; dup {
			return fmt.Errorf("the %s and %s tokens are the same: each role needs its own, since a token has one scope", role, other)
		}
		seen[tok] = role
	}
	if c.ExecutionToken == "" {
		return errors.New("the execution token is required")
	}
	if len(c.SecretKey) != secrets.KeySize {
		return fmt.Errorf("the secret key must be %d bytes", secrets.KeySize)
	}
	return nil
}

// Server is a running orchestrator.
type Server struct {
	cfg     Config
	pool    *pgxpool.Pool
	http    *http.Server
	ln      net.Listener
	rec     *record.Recorder
	cancel  context.CancelFunc
	bg      sync.WaitGroup
	stop    sync.Once
	stopErr error
}

// Start migrates the database, begins serving and returns. Call Stop to shut
// down gracefully.
func Start(ctx context.Context, cfg Config) (*Server, error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	log := cfg.Log

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("database: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			pool.Close()
		}
	}()
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("database: %w", err)
	}
	applied, err := migrate.Up(ctx, pool, migrations.FS)
	if err != nil {
		return nil, fmt.Errorf("migrating: %w", err)
	}
	if len(applied) > 0 {
		log.Info("applied migrations", "versions", applied)
	}

	box, err := secrets.NewBox(cfg.SecretKey)
	if err != nil {
		return nil, err
	}
	st := store.New(pool, box)

	cache := snapshot.New(st, 3*cfg.HeartbeatInterval, log)
	if err := cache.Refresh(ctx); err != nil {
		return nil, fmt.Errorf("loading routing: %w", err)
	}
	rec := record.New(st, log, record.Options{})

	verifier := authn.Verifier{
		cfg.IngressToken: invoke.IngressScope,
		cfg.WorkerToken:  fleet.WorkerScope,
		cfg.AdminToken:   control.AdminScope,
	}
	if cfg.NetworkToken != "" {
		verifier[cfg.NetworkToken] = netreg.Scope
	}
	mux := http.NewServeMux()
	ingress := invoke.New(invoke.Config{
		Cache: cache, Recorder: rec, ExecutionToken: cfg.ExecutionToken,
		MaxRequestBodyBytes: cfg.MaxRequestBodyBytes, Version: cfg.Version, Log: log,
	})
	mux.Handle(ingress.Handler(verifier))
	fleet.New(fleet.Config{Store: st, HeartbeatInterval: cfg.HeartbeatInterval, OnChange: cache.Poke, Log: log}).Mount(mux, verifier)
	if cfg.NetworkToken != "" {
		netreg.New(netreg.Config{Store: st, HeartbeatInterval: cfg.HeartbeatInterval, OnChange: cache.Poke, Log: log}).Mount(mux, verifier)
	}
	control.New(control.Config{
		Store: st, MaxModuleBytes: cfg.MaxModuleBytes, AliveWithin: 3 * cfg.HeartbeatInterval, Version: cfg.Version, Log: log,
		Refresh: func(ctx context.Context) {
			if err := cache.Refresh(ctx); err != nil {
				log.Warn("refresh after a write failed; other readers will catch up", "error", err)
			}
		},
	}).Mount(mux, verifier)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		pctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(pctx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok\n"))
	})

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	// No write timeout: responses stream, and the invocation deadline bounds them.
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute, Protocols: &protocols}

	bgCtx, cancel := context.WithCancel(context.Background())
	s := &Server{cfg: cfg, pool: pool, http: srv, ln: ln, rec: rec, cancel: cancel}
	s.bg.Add(2)
	go func() { defer s.bg.Done(); cache.Run(bgCtx, cfg.RefreshEvery) }()
	go func() {
		defer s.bg.Done()
		janitor.Run(bgCtx, janitor.Config{Store: st, InvocationRetention: cfg.InvocationRetention, Log: log}, cfg.JanitorEvery)
	}()
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("serve", "error", err)
		}
	}()
	log.Info("orchestrator listening", "addr", ln.Addr().String(), "version", cfg.Version)
	ok = true
	return s, nil
}

// Addr is the address the orchestrator listens on.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Stop shuts down gracefully: it stops accepting connections, lets in-flight
// invocations finish (up to the shutdown timeout), flushes invocation records
// and closes the database. It is safe to call more than once.
func (s *Server) Stop(ctx context.Context) error {
	s.stop.Do(func() {
		sctx, cancel := context.WithTimeout(ctx, s.cfg.ShutdownTimeout)
		defer cancel()
		var errs []error
		if err := s.http.Shutdown(sctx); err != nil {
			errs = append(errs, fmt.Errorf("draining requests: %w", err))
			s.http.Close()
		}
		s.cancel()
		s.bg.Wait()
		// Records of the invocations that just finished are still queued.
		if err := s.rec.Close(sctx); err != nil {
			errs = append(errs, fmt.Errorf("flushing invocation records: %w", err))
		}
		s.pool.Close()
		s.stopErr = errors.Join(errs...)
	})
	return s.stopErr
}
