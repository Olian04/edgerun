// Command admin serves the operator UI for an edgerun orchestrator.
//
//	admin                       run the server
//	admin hash-password         read a password on stdin, print its bcrypt hash
package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Olian04/edgerun/internal/admin/app"
	"github.com/Olian04/edgerun/internal/admin/auth"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "hash-password" {
		hashPassword()
		return
	}

	var cfg app.Config
	var users, key string
	flag.StringVar(&cfg.Listen, "listen", env("EDGERUN_ADMIN_LISTEN", ":9090"), "address to serve on")
	flag.StringVar(&cfg.OrchestratorURL, "orchestrator-url", env("EDGERUN_ORCHESTRATOR_URL", ""), "orchestrator base URL")
	flag.StringVar(&cfg.AdminToken, "admin-token", env("EDGERUN_ADMIN_TOKEN", ""), "token presented to the orchestrator")
	flag.StringVar(&users, "users", env("EDGERUN_ADMIN_USERS", ""), "operators: name:role:bcrypt-hash entries separated by ';' (role is admin or viewer)")
	flag.StringVar(&key, "session-key", env("EDGERUN_ADMIN_SESSION_KEY", ""), "base64 of 32+ random bytes that sign sessions; random per start if unset")
	flag.BoolVar(&cfg.SecureCookies, "secure-cookies", false, "mark the session cookie Secure (set when served over HTTPS)")
	flag.DurationVar(&cfg.AutoRefresh, "auto-refresh", 10*time.Second, "reload the read-only pages (overview, workers, invocations) this often; 0 disables")
	flag.Parse()
	cfg.Log = slog.New(slog.NewJSONHandler(os.Stderr, nil))

	us, err := auth.ParseUsers(users)
	if err != nil {
		fatal(cfg.Log, fmt.Errorf("users: %w", err))
	}
	cfg.Users = us
	if key != "" {
		b, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			fatal(cfg.Log, fmt.Errorf("session key is not valid base64: %w", err))
		}
		cfg.SessionKey = b
	} else {
		cfg.Log.Warn("no session key configured: everyone is signed out whenever the admin restarts")
		cfg.SessionKey = auth.NewSessionKey()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	srv, err := app.Start(ctx, cfg)
	if err != nil {
		fatal(cfg.Log, err)
	}
	<-ctx.Done()
	cfg.Log.Info("shutting down")
	if err := srv.Stop(context.Background()); err != nil {
		cfg.Log.Error("shutdown", "error", err)
		os.Exit(1)
	}
}

func hashPassword() {
	fmt.Fprintln(os.Stderr, "Password (one line on stdin):")
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && line == "" {
		fatal(slog.Default(), fmt.Errorf("reading the password: %w", err))
	}
	pw := strings.TrimRight(line, "\r\n")
	if pw == "" {
		fatal(slog.Default(), fmt.Errorf("the password is empty"))
	}
	h, err := auth.HashPassword(pw)
	if err != nil {
		fatal(slog.Default(), err)
	}
	fmt.Println(h)
}

func fatal(log *slog.Logger, err error) {
	log.Error("admin failed", "error", err)
	os.Exit(1)
}
