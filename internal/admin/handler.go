package admin

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Olian04/edgerun/internal/admin/auth"
)

// HandlerConfig configures the complete admin site: sign-in, the webui app
// behind it, and a health endpoint.
type HandlerConfig struct {
	Control *Control
	Users   []auth.User
	// SessionKey signs session cookies (32+ bytes).
	SessionKey    []byte
	SecureCookies bool
	SessionTTL    time.Duration
	// MaxModuleBytes bounds uploads. Zero asks the orchestrator, falling back
	// to 32 MiB if it cannot be reached.
	MaxModuleBytes int64
	Log            *slog.Logger
}

// Handler assembles the admin site.
//
//	/login, /logout   sign in and out (ours; webui has no authentication)
//	/admin/...        the webui app, only for signed-in operators
//	/healthz          liveness, open
//	/                 redirects to /admin/
func Handler(cfg HandlerConfig) (http.Handler, error) {
	if cfg.Control == nil {
		return nil, errors.New("a control client is required")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	a, err := auth.New(auth.Config{Users: cfg.Users, SessionKey: cfg.SessionKey, SecureCookies: cfg.SecureCookies, SessionTTL: cfg.SessionTTL})
	if err != nil {
		return nil, err
	}
	maxModule := cfg.MaxModuleBytes
	if maxModule == 0 {
		maxModule = 32 << 20
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if info, err := cfg.Control.Info(ctx); err == nil && info.GetMaxModuleBytes() > 0 {
			maxModule = int64(info.GetMaxModuleBytes())
		} else if err != nil {
			cfg.Log.Warn("could not read the orchestrator's module limit; assuming 32 MiB", "error", err)
		}
		cancel()
	}

	site, err := App(Config{Control: cfg.Control, MaxModuleBytes: maxModule, Log: cfg.Log}).Compile("/admin")
	if err != nil {
		// A broken declaration must stop the process, not serve a page of
		// compiler errors to operators.
		return nil, err
	}

	mux := http.NewServeMux()
	mux.Handle("GET /login.css", auth.LoginCSS())
	mux.Handle("/login", a.Login("/admin/"))
	mux.Handle("/logout", a.Logout())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.Handle("/admin/", a.Require(site))
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin/", http.StatusSeeOther)
	})
	return mux, nil
}
