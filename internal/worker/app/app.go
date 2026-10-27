// Package app assembles and runs a worker. cmd/worker and the conformance
// tests both go through it, so what is tested is what ships.
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

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/worker/cache"
	"github.com/Olian04/edgerun/internal/worker/fleetclient"
	"github.com/Olian04/edgerun/internal/worker/runtime"
	"github.com/Olian04/edgerun/internal/worker/server"
)

// WorkerScope is the token scope a worker presents to the orchestrator.
const WorkerScope = "worker"

// Config configures a worker.
type Config struct {
	// Listen is the address ExecutionService is served on, e.g. ":9000".
	Listen string
	// AdvertiseURL is how the orchestrator reaches this worker. It may be
	// omitted only when Listen names a concrete address (e.g. 127.0.0.1:0).
	AdvertiseURL    string
	OrchestratorURL string
	// WorkerToken is presented to the orchestrator (Fleet, Artifact).
	WorkerToken string
	// ExecutionToken is required of the orchestrator on ExecutionService.
	ExecutionToken string

	MaxConcurrency uint32
	Labels         map[string]string
	Version        string

	Runtime            runtime.Config
	ArtifactCacheBytes int64         // default 256 MiB
	MaxArtifactBytes   int64         // default 64 MiB
	DrainTimeout       time.Duration // default 30s

	Log *slog.Logger
}

func (c *Config) setDefaults() {
	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = 8
	}
	if c.ArtifactCacheBytes == 0 {
		c.ArtifactCacheBytes = 256 << 20
	}
	if c.MaxArtifactBytes == 0 {
		c.MaxArtifactBytes = 64 << 20
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = 30 * time.Second
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
	if _, err := url.ParseRequestURI(c.OrchestratorURL); err != nil {
		return fmt.Errorf("orchestrator URL: %w", err)
	}
	// An empty token would match an empty credential; refuse it outright.
	if c.WorkerToken == "" || c.ExecutionToken == "" {
		return errors.New("both the worker token and the execution token are required")
	}
	return nil
}

// Run serves until ctx is done, then shuts down gracefully: it reports
// DRAINING, lets in-flight invocations finish (up to DrainTimeout),
// deregisters, and stops serving. This is the path SIGTERM takes.
func Run(ctx context.Context, cfg Config) error {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return err
	}
	log := cfg.Log

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	advertise := cfg.AdvertiseURL
	if advertise == "" {
		addr := ln.Addr().(*net.TCPAddr)
		if addr.IP.IsUnspecified() {
			ln.Close()
			return errors.New("advertise URL is required when listening on all interfaces")
		}
		advertise = "http://" + addr.String()
	}

	engine, err := runtime.New(cfg.Runtime)
	if err != nil {
		ln.Close()
		return err
	}
	defer engine.Close(context.Background())

	hc := &http.Client{}
	artifactClient := workerv1connect.NewArtifactServiceClient(hc, cfg.OrchestratorURL,
		connect.WithInterceptors(authn.Client(cfg.WorkerToken)))
	artifacts := cache.New(cache.ConnectFetcher{Client: artifactClient, MaxBytes: cfg.MaxArtifactBytes}, cfg.ArtifactCacheBytes)

	info := &workerv1.WorkerInfo{
		Implementation: implementation(cfg.Version),
		Abis:           []string{runtime.ABI},
		MaxConcurrency: cfg.MaxConcurrency,
		Labels:         cfg.Labels,
	}
	srv := server.New(info, engine, artifacts, log)

	mux := http.NewServeMux()
	mux.Handle(srv.Handler(authn.Verifier{cfg.ExecutionToken: server.ExecuteScope}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if srv.State() == workerv1.WorkerState_WORKER_STATE_DRAINING {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok\n"))
	})
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	httpSrv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second, Protocols: &protocols}

	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.Serve(ln) }()
	log.Info("worker listening", "addr", ln.Addr().String(), "advertise", advertise, "max_concurrency", cfg.MaxConcurrency)

	fleet := fleetclient.New(fleetclient.Config{
		Endpoint: advertise, Info: info, Source: srv, Log: log,
		Fleet: workerv1connect.NewFleetServiceClient(hc, cfg.OrchestratorURL,
			connect.WithInterceptors(authn.Client(cfg.WorkerToken))),
	})
	// Heartbeats must outlive ctx: they keep reporting DRAINING while we wait.
	hbCtx, stopHeartbeats := context.WithCancel(context.Background())
	hbDone := make(chan struct{})
	go func() { defer close(hbDone); fleet.Run(hbCtx) }()

	select {
	case err := <-serveErr:
		stopHeartbeats()
		<-hbDone
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}

	log.Info("shutting down: draining", "timeout", cfg.DrainTimeout)
	srv.Drain()
	fleet.Poke()
	select {
	case <-srv.Idle():
	case <-time.After(cfg.DrainTimeout):
		log.Warn("drain timeout; stopping with invocations still running", "in_flight", srv.InFlight())
	}
	stopHeartbeats()
	<-hbDone

	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := fleet.Deregister(dctx); err != nil {
		log.Warn("deregister failed", "error", err)
	}
	if err := httpSrv.Shutdown(dctx); err != nil {
		log.Warn("http shutdown", "error", err)
		httpSrv.Close()
	}
	return nil
}

func implementation(version string) *commonv1.Implementation {
	return &commonv1.Implementation{
		Name:      "edgerun-worker-wazero",
		Version:   version,
		Protocols: []string{"edgerun.worker.v1"},
	}
}
