// Package external runs the conformance suites against an implementation that
// is already running, wherever it is and whatever it is written in. It is what
// cmd/edgerun-conformance is made of.
//
// A suite starts the implementation it judges through a Target. Here, nothing
// is started: the implementation is where the operator put it, and the suite
// is given its address. Because a suite also plays every role around the
// implementation (the orchestrator a worker dials, the workers an orchestrator
// calls, the web servers a network service reaches), the implementation must
// in turn be configured to reach the suite: PrintConfig says how.
package external

import (
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	egressconf "github.com/Olian04/edgerun/conformance/egress"
	frontendconf "github.com/Olian04/edgerun/conformance/frontend"
	"github.com/Olian04/edgerun/conformance/internal/check"
	orchestratorconf "github.com/Olian04/edgerun/conformance/orchestrator"
	workerconf "github.com/Olian04/edgerun/conformance/worker"
)

// Roles are the roles that have a suite.
var Roles = []string{"frontend", "orchestrator", "worker", "egress"}

// Config says what to test and how the suite is reached.
type Config struct {
	// Role is one of Roles.
	Role string
	// Target is the implementation under test: host:port or a URL. For a
	// frontend, orchestrator or network service, where it listens; for a
	// worker, the endpoint it registers with (what it advertises).
	Target string
	// TargetDefault, for a network service, is a second instance in its default
	// configuration, which the suite needs to test what a service refuses
	// whatever it is asked to allow. Without it that group is skipped.
	TargetDefault string
	// Listen and Advertise say where the suite's own servers listen, and the
	// host the implementation reaches them at (check.Net).
	Listen, Advertise string
	// StopCmd, run with sh -c, asks the worker or network service under test
	// to shut down as SIGTERM does; StopCmdDefault does the same for
	// TargetDefault. Without one the groups that need the service to leave are
	// skipped.
	StopCmd, StopCmdDefault string
	// Wait is how long to wait for the implementation to connect to the suite.
	Wait time.Duration

	// Tokens and settings the implementation must be configured with; the
	// suite's defaults for any left empty.
	IngressToken, WorkerToken, AdminToken, NetworkToken, ExecutionToken, EgressToken string
	HeartbeatInterval, RequestTimeout                                                time.Duration
	MaxConcurrency                                                                   uint32
}

// Validate checks what the role needs.
func (c Config) Validate() error {
	known := false
	for _, r := range Roles {
		known = known || r == c.Role
	}
	if !known {
		return fmt.Errorf("unknown role %q: want one of %s", c.Role, strings.Join(Roles, ", "))
	}
	if c.Target == "" {
		return fmt.Errorf("--target is required: where the %s under test is", c.Role)
	}
	if c.Role != "orchestrator" {
		if _, port, err := net.SplitHostPort(c.Listen); err != nil || port == "" || port == "0" {
			return fmt.Errorf("--listen host:port is required for role %s, with a port: the %s must be configured to reach the suite "+
				"before it starts, so the suite's port cannot be chosen at random", c.Role, c.Role)
		}
	}
	return nil
}

func normURL(target string) string {
	if strings.Contains(target, "://") {
		return strings.TrimSuffix(target, "/")
	}
	return "http://" + target
}

func stopper(t testing.TB, cmd string) func() {
	if cmd == "" {
		return nil
	}
	return func() {
		out, err := exec.Command("sh", "-c", cmd).CombinedOutput()
		if err != nil {
			t.Errorf("--stop-cmd %q: %v\n%s", cmd, err, out)
		}
	}
}

// Tests returns the suite for cfg.Role, ready to be run as a test.
func Tests(cfg Config) ([]testing.InternalTest, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()
	n := check.Net{Listen: cfg.Listen, Advertise: cfg.Advertise}
	announce := func(t testing.TB, what string, env any) {
		t.Logf("testing the %s at %s; the suite is at %s", cfg.Role, what, n.Host())
	}
	var f func(t *testing.T)
	switch cfg.Role {
	case "frontend":
		f = func(t *testing.T) {
			frontendconf.Run(t, frontendconf.Target{
				Env: frontendconf.Env{IngressToken: cfg.IngressToken, RequestTimeout: cfg.RequestTimeout},
				Net: n, Wait: cfg.Wait,
				Launch: func(t testing.TB, env frontendconf.Env) string {
					announce(t, cfg.Target, env)
					return normURL(cfg.Target)
				},
			})
		}
	case "orchestrator":
		f = func(t *testing.T) {
			orchestratorconf.Run(t, orchestratorconf.Target{
				Env: orchestratorconf.Env{
					IngressToken: cfg.IngressToken, WorkerToken: cfg.WorkerToken, AdminToken: cfg.AdminToken,
					NetworkToken: cfg.NetworkToken, ExecutionToken: cfg.ExecutionToken, HeartbeatInterval: cfg.HeartbeatInterval,
				},
				Net: n,
				Launch: func(t testing.TB, env orchestratorconf.Env) string {
					announce(t, cfg.Target, env)
					return normURL(cfg.Target)
				},
			})
		}
	case "worker":
		f = func(t *testing.T) {
			workerconf.Run(t, workerconf.Target{
				Env: workerconf.Env{
					WorkerToken: cfg.WorkerToken, ExecutionToken: cfg.ExecutionToken, EgressToken: cfg.EgressToken,
					MaxConcurrency: cfg.MaxConcurrency, HeartbeatInterval: cfg.HeartbeatInterval,
				},
				Net: n, Endpoint: cfg.Target, Wait: cfg.Wait,
				Launch: func(t testing.TB, env workerconf.Env) func() {
					announce(t, cfg.Target, env)
					return stopper(t, cfg.StopCmd)
				},
			})
		}
	case "egress":
		f = func(t *testing.T) {
			launches := 0
			egressconf.Run(t, egressconf.Target{
				Env: egressconf.Env{EgressToken: cfg.EgressToken, NetworkToken: cfg.NetworkToken, HeartbeatInterval: cfg.HeartbeatInterval},
				Net: n,
				Launch: func(t testing.TB, env egressconf.Env) egressconf.Instance {
					launches++
					if launches == 1 {
						announce(t, cfg.Target, env)
						return egressconf.Instance{URL: normURL(cfg.Target), Stop: stopper(t, cfg.StopCmd)}
					}
					if cfg.TargetDefault == "" {
						return egressconf.Instance{}
					}
					return egressconf.Instance{URL: normURL(cfg.TargetDefault), Stop: stopper(t, cfg.StopCmdDefault)}
				},
			})
		}
	}
	return []testing.InternalTest{{Name: strings.ToUpper(cfg.Role[:1]) + cfg.Role[1:] + "Conformance", F: f}}, nil
}

// withDefaults fills tokens and settings left empty with the suite's own.
func (c Config) withDefaults() Config {
	d := func(v *string, def string) {
		if *v == "" {
			*v = def
		}
	}
	d(&c.IngressToken, "conformance-ingress-token")
	d(&c.WorkerToken, "conformance-worker-token")
	d(&c.AdminToken, "conformance-admin-token")
	d(&c.NetworkToken, "conformance-network-token")
	d(&c.ExecutionToken, "conformance-execution-token")
	d(&c.EgressToken, "conformance-egress-token")
	if c.HeartbeatInterval == 0 {
		switch c.Role {
		case "worker":
			c.HeartbeatInterval = 100 * time.Millisecond
		default:
			c.HeartbeatInterval = 200 * time.Millisecond
		}
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 3 * time.Second
	}
	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = 2
	}
	return c
}

// Defaults returns c with the suite's own tokens and settings in place of any
// left empty, which is what Tests and PrintConfig use.
func (c Config) Defaults() Config { return c.withDefaults() }

// PrintConfig says how the implementation under test must be configured to be
// judged: what the suite will present and require, and where the suite is.
func PrintConfig(w io.Writer, cfg Config) {
	cfg = cfg.withDefaults()
	n := check.Net{Listen: cfg.Listen, Advertise: cfg.Advertise}
	suite := ""
	if _, port, err := net.SplitHostPort(cfg.Listen); err == nil && port != "" && port != "0" {
		suite = "http://" + net.JoinHostPort(n.Host(), port)
	}
	p := func(f string, a ...any) { fmt.Fprintf(w, f+"\n", a...) }
	p("Configuring the %s under test", cfg.Role)
	p("")
	switch cfg.Role {
	case "frontend":
		p("Start the suite first: it listens on %s as the orchestrator, and waits up to %v for the frontend to ask it for its limits.", cfg.Listen, waitOr(cfg.Wait))
		p("Then start the frontend, listening where --target says (%s), with:", cfg.Target)
		p("  orchestrator URL     %s", suite)
		p("  ingress token        %s   (presented to the orchestrator)", cfg.IngressToken)
		p("  request timeout      %v     (the budget given to each invocation)", cfg.RequestTimeout)
		p("  forwarded headers    not trusted (the default)")
		p("")
		p("  edgerun's own:  frontend --orchestrator-url %s --ingress-token %s --request-timeout %v --listen %s", suite, cfg.IngressToken, cfg.RequestTimeout, cfg.Target)
	case "orchestrator":
		p("The orchestrator must be new and empty, and used by nothing else. Configure it with:")
		p("  ingress token        %s", cfg.IngressToken)
		p("  worker token         %s", cfg.WorkerToken)
		p("  admin token          %s", cfg.AdminToken)
		p("  network token        %s", cfg.NetworkToken)
		p("  execution token      %s   (what it presents to workers)", cfg.ExecutionToken)
		p("  heartbeat interval   %v      (what it dictates to workers and network services)", cfg.HeartbeatInterval)
		p("")
		p("The orchestrator calls the suite's fake workers: they listen on %s at random ports, and the orchestrator must reach them at %s.", n.Listen, n.Host())
		p("Use --listen and --advertise if that is not where the orchestrator can reach this machine.")
		p("")
		p("  edgerun's own:  orchestrator --ingress-token %s --worker-token %s --admin-token %s --network-token %s --execution-token %s --heartbeat-interval %v ...",
			cfg.IngressToken, cfg.WorkerToken, cfg.AdminToken, cfg.NetworkToken, cfg.ExecutionToken, cfg.HeartbeatInterval)
	case "worker":
		p("Start the suite first: it listens on %s as the orchestrator (and hosts the network services the worker is routed to).", cfg.Listen)
		p("Then start the worker, reachable at %s (what it advertises), with:", cfg.Target)
		p("  orchestrator URL     %s", suite)
		p("  worker token         %s   (presented to the orchestrator)", cfg.WorkerToken)
		p("  execution token      %s   (required of the orchestrator)", cfg.ExecutionToken)
		p("  egress token         %s   (presented to network services; a worker that has none offers no wagi-2)", cfg.EgressToken)
		p("  max concurrency      %d", cfg.MaxConcurrency)
		p("")
		p("  edgerun's own:  worker --orchestrator-url %s --worker-token %s --execution-token %s --egress-token %s --max-concurrency %d --advertise-url %s --listen ...",
			suite, cfg.WorkerToken, cfg.ExecutionToken, cfg.EgressToken, cfg.MaxConcurrency, normURL(cfg.Target))
		p("")
		p("The worker suite builds guest modules: it needs a Go toolchain, or an edgerun-conformance built with `make conformance`.")
		p("Without --stop-cmd the suite cannot shut the worker down, and skips the group that checks it drains and leaves.")
	case "egress":
		p("Start the suite first: it listens on %s as the orchestrator's registry, and hosts the web servers the service is asked to reach.", cfg.Listen)
		p("Then start the network service, reachable at %s, with:", cfg.Target)
		p("  orchestrator URL     %s   (it must register there)", suite)
		p("  network token        %s   (presented to the orchestrator)", cfg.NetworkToken)
		p("  egress token         %s   (required of workers)", cfg.EgressToken)
		p("  carries              %s,*.covered.test   (or \"*\")", n.Host())
		p("  allowed to connect   %s   (the web servers the suite runs: a range the service refuses by default)", n.Host())
		p("")
		p("and, for what a service refuses however it is asked, a second instance in its default configuration (--target-default):")
		p("  the same tokens and orchestrator, carries \"*\", and no ranges allowed.")
		p("")
		p("  edgerun's own:  egress --listen %s --orchestrator-url %s --network-token %s --egress-token %s --covers %s,*.covered.test --allow-network %s/32",
			cfg.Target, suite, cfg.NetworkToken, cfg.EgressToken, n.Host(), n.Host())
		p("The suite adapts to what a service declares it carries; it needs these two to carry its own web servers.")
	}
}

func waitOr(d time.Duration) time.Duration {
	if d == 0 {
		return 15 * time.Second
	}
	return d
}
