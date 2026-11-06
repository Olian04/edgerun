// Package external runs the conformance suites against an implementation that
// is already running, wherever it is and whatever it is written in. It is what
// cmd/edgerun-conformance is made of.
//
// A suite starts the implementation it judges through a Target. Here, nothing
// is started: the implementation is where the operator put it, and the suite
// is given its address. Because a suite also plays every other component around
// the implementation (the orchestrator a worker dials, the workers an
// orchestrator calls, the web servers a network service reaches), the
// implementation must in turn be configured to reach the suite: PrintConfig says
// how.
//
// What is judged is named in one of two ways. A Service is one platform service,
// and the implementation under test is its host (or, with Client, the component
// that is its client). A Profile is a component: every service it hosts, run in
// turn against the same implementation, and then the component's duties as a
// client of other services.
package external

import (
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	artifactsconf "github.com/Olian04/edgerun/conformance/artifacts"
	catalogconf "github.com/Olian04/edgerun/conformance/catalog"
	edgeconf "github.com/Olian04/edgerun/conformance/edge"
	egressconf "github.com/Olian04/edgerun/conformance/egress"
	executionconf "github.com/Olian04/edgerun/conformance/execution"
	ingressconf "github.com/Olian04/edgerun/conformance/ingress"
	"github.com/Olian04/edgerun/conformance/internal/check"
	membershipconf "github.com/Olian04/edgerun/conformance/membership"
	plansconf "github.com/Olian04/edgerun/conformance/plans"
	recordsconf "github.com/Olian04/edgerun/conformance/records"
	routingconf "github.com/Olian04/edgerun/conformance/routing"
)

// Components are the kinds of component that can be judged.
const (
	Frontend       = "frontend"
	Orchestrator   = "orchestrator"
	Worker         = "worker"
	NetworkService = "network-service"
)

// Profiles are the components that have a profile: each is the set of services
// it hosts, and its duties as a client.
var Profiles = []string{Frontend, Orchestrator, Worker, NetworkService}

// Services are the platform services that have a suite, and the component that
// hosts each.
var Services = []string{"edge", "ingress", "execution", "membership", "artifacts", "egress", "catalog", "routing", "plans", "records"}

// host is the component that hosts each service.
var host = map[string]string{
	"edge": Frontend, "ingress": Orchestrator, "membership": Orchestrator, "artifacts": Orchestrator,
	"catalog": Orchestrator, "routing": Orchestrator, "plans": Orchestrator, "records": Orchestrator,
	"execution": Worker, "egress": NetworkService,
}

// clients says, for the services that have a client suite, which components can
// be judged as their client (the value of Config.Client).
var clients = map[string][]string{
	"execution":  {Orchestrator},
	"membership": {Worker, NetworkService},
	"artifacts":  {Worker},
	"egress":     {Worker},
}

// hosted lists the services a component hosts, in the order a profile runs them.
var hosted = map[string][]string{
	Frontend:       {"edge"},
	Orchestrator:   {"ingress", "membership", "artifacts", "catalog", "routing", "plans", "records"},
	Worker:         {"execution"},
	NetworkService: {"egress"},
}

// clientDuties lists the services a component is the client of and has a suite
// for, in the order a profile runs them. The one that stops the component (a
// graceful shutdown) is last.
var clientDuties = map[string][]string{
	Frontend:       nil, // its duties as the client of Ingress are judged by the Edge suite itself
	Orchestrator:   {"execution"},
	Worker:         {"artifacts", "egress", "membership"},
	NetworkService: {"membership"},
}

// Config says what to test and how the suite is reached.
type Config struct {
	// Service names one platform service to judge: its host is the
	// implementation under test. Exactly one of Service and Profile is set.
	Service string
	// Client, with Service, judges the implementation as a client of that service
	// instead of its host: the component that is the client (see ClientsOf).
	Client string
	// Profile names a component: every service it hosts is judged against the
	// implementation, then its duties as a client of other services.
	Profile string

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

// ClientsOf returns the components that can be judged as the client of service
// (the values of Config.Client), or nil if the service has no client suite.
func ClientsOf(service string) []string { return clients[service] }

func in(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// Component is the component under test: the host of Service, the component
// named by Client, or Profile.
func (c Config) Component() string {
	switch {
	case c.Profile != "":
		return c.Profile
	case c.Client != "":
		return c.Client
	}
	return host[c.Service]
}

// Validate checks what the request needs.
func (c Config) Validate() error {
	switch {
	case c.Service == "" && c.Profile == "":
		return fmt.Errorf("say what to test: --service one of %s, or --profile one of %s", strings.Join(Services, ", "), strings.Join(Profiles, ", "))
	case c.Service != "" && c.Profile != "":
		return fmt.Errorf("--service and --profile are alternatives: --service judges one service, --profile every service a component hosts")
	case c.Profile != "" && !in(Profiles, c.Profile):
		return fmt.Errorf("unknown profile %q: want one of %s", c.Profile, strings.Join(Profiles, ", "))
	case c.Service != "" && !in(Services, c.Service):
		return fmt.Errorf("unknown service %q: want one of %s", c.Service, strings.Join(Services, ", "))
	case c.Client != "" && c.Service == "":
		return fmt.Errorf("--client says which component is judged as the client of --service; a profile already includes its component's client duties")
	case c.Client != "" && !in(clients[c.Service], c.Client):
		if len(clients[c.Service]) == 0 {
			return fmt.Errorf("service %s has no client suite (a client's duties are judged where the service says: see docs/services.md)", c.Service)
		}
		return fmt.Errorf("--client for service %s must be one of %s, not %q", c.Service, strings.Join(clients[c.Service], ", "), c.Client)
	}
	if c.Target == "" {
		return fmt.Errorf("--target is required: where the %s under test is", c.Component())
	}
	if c.Component() != Orchestrator {
		if _, port, err := net.SplitHostPort(c.Listen); err != nil || port == "" || port == "0" {
			return fmt.Errorf("--listen host:port is required when judging a %s, with a port: the %s must be configured to reach the suite "+
				"before it starts, so the suite's port cannot be chosen at random", c.Component(), c.Component())
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

// Name is how a test of a service is named in the output: "Membership",
// "MembershipAsWorker" for a client. The Go test driver runs top-level tests
// in order, so a profile reports each in turn.
func testName(component, service, client string) string {
	title := func(s string) string {
		var out string
		for _, w := range strings.Split(s, "-") {
			out += strings.ToUpper(w[:1]) + w[1:]
		}
		return out
	}
	if client != "" {
		return title(service) + "ConformanceAs" + title(client)
	}
	return title(service) + "Conformance"
}

// unit is one suite to run.
type unit struct {
	service, client string
	run             func(t *testing.T)
}

// Tests returns the suites for cfg, ready to be run as tests, in the order they
// must run.
func Tests(cfg Config) ([]testing.InternalTest, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()
	n := check.Net{Listen: cfg.Listen, Advertise: cfg.Advertise}
	component := cfg.Component()
	announce := func(t testing.TB) {
		t.Logf("testing the %s at %s; the suite is at %s", component, cfg.Target, n.Host())
	}

	// Targets for each kind of component. Each suite gets its own, because a
	// Launch is per run (and the network service's counts its calls).
	frontend := func() edgeconf.Target {
		return edgeconf.Target{
			Env: edgeconf.Env{IngressToken: cfg.IngressToken, RequestTimeout: cfg.RequestTimeout},
			Net: n, Wait: cfg.Wait,
			Launch: func(t testing.TB, env edgeconf.Env) string { announce(t); return normURL(cfg.Target) },
		}
	}
	orchestrator := func() ingressconf.Target {
		return ingressconf.Target{
			Env: ingressconf.Env{
				IngressToken: cfg.IngressToken, WorkerToken: cfg.WorkerToken, AdminToken: cfg.AdminToken,
				NetworkToken: cfg.NetworkToken, ExecutionToken: cfg.ExecutionToken, HeartbeatInterval: cfg.HeartbeatInterval,
			},
			Net:    n,
			Launch: func(t testing.TB, env ingressconf.Env) string { announce(t); return normURL(cfg.Target) },
		}
	}
	worker := func() executionconf.Target {
		return executionconf.Target{
			Env: executionconf.Env{
				WorkerToken: cfg.WorkerToken, ExecutionToken: cfg.ExecutionToken, EgressToken: cfg.EgressToken,
				MaxConcurrency: cfg.MaxConcurrency, HeartbeatInterval: cfg.HeartbeatInterval,
			},
			Net: n, Endpoint: cfg.Target, Wait: cfg.Wait,
			Launch: func(t testing.TB, env executionconf.Env) func() { announce(t); return stopper(t, cfg.StopCmd) },
		}
	}
	netService := func() egressconf.Target {
		launches := 0
		return egressconf.Target{
			Env: egressconf.Env{EgressToken: cfg.EgressToken, NetworkToken: cfg.NetworkToken, HeartbeatInterval: cfg.HeartbeatInterval},
			Net: n,
			Launch: func(t testing.TB, env egressconf.Env) egressconf.Instance {
				launches++
				if launches == 1 {
					announce(t)
					return egressconf.Instance{URL: normURL(cfg.Target), Stop: stopper(t, cfg.StopCmd)}
				}
				if cfg.TargetDefault == "" {
					return egressconf.Instance{}
				}
				return egressconf.Instance{URL: normURL(cfg.TargetDefault), Stop: stopper(t, cfg.StopCmdDefault)}
			},
		}
	}

	// hostRun and clientRun are the suites of a service, by who is judged.
	hostRun := func(service string) func(t *testing.T) {
		switch service {
		case "edge":
			return func(t *testing.T) { edgeconf.Run(t, frontend()) }
		case "ingress":
			return func(t *testing.T) { ingressconf.Run(t, orchestrator()) }
		case "membership":
			return func(t *testing.T) { membershipconf.Run(t, orchestrator()) }
		case "artifacts":
			return func(t *testing.T) { artifactsconf.Run(t, orchestrator()) }
		case "catalog":
			return func(t *testing.T) { catalogconf.Run(t, orchestrator()) }
		case "routing":
			return func(t *testing.T) { routingconf.Run(t, orchestrator()) }
		case "plans":
			return func(t *testing.T) { plansconf.Run(t, orchestrator()) }
		case "records":
			return func(t *testing.T) { recordsconf.Run(t, orchestrator()) }
		case "execution":
			return func(t *testing.T) { executionconf.Run(t, worker()) }
		case "egress":
			return func(t *testing.T) { egressconf.Run(t, netService()) }
		}
		return nil
	}
	clientRun := func(service, client string) func(t *testing.T) {
		switch service + "/" + client {
		case "execution/" + Orchestrator:
			return func(t *testing.T) { executionconf.RunClient(t, orchestrator()) }
		case "membership/" + Worker:
			return func(t *testing.T) { membershipconf.RunWorkerClient(t, worker()) }
		case "membership/" + NetworkService:
			return func(t *testing.T) { membershipconf.RunNetworkServiceClient(t, netService()) }
		case "artifacts/" + Worker:
			return func(t *testing.T) { artifactsconf.RunClient(t, worker()) }
		case "egress/" + Worker:
			return func(t *testing.T) { egressconf.RunClient(t, worker()) }
		}
		return nil
	}

	var units []unit
	if cfg.Profile != "" {
		for _, s := range hosted[cfg.Profile] {
			units = append(units, unit{service: s, run: hostRun(s)})
		}
		for _, s := range clientDuties[cfg.Profile] {
			units = append(units, unit{service: s, client: cfg.Profile, run: clientRun(s, cfg.Profile)})
		}
	} else if cfg.Client != "" {
		units = append(units, unit{service: cfg.Service, client: cfg.Client, run: clientRun(cfg.Service, cfg.Client)})
	} else {
		units = append(units, unit{service: cfg.Service, run: hostRun(cfg.Service)})
	}
	var tests []testing.InternalTest
	for _, u := range units {
		tests = append(tests, testing.InternalTest{Name: testName(component, u.service, u.client), F: u.run})
	}
	return tests, nil
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
		switch c.Component() {
		case Worker:
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
	component := cfg.Component()
	switch {
	case cfg.Profile != "":
		p("Configuring the %s under test (profile: every service it hosts, then its duties as a client)", component)
		p("  services: %s", strings.Join(append(append([]string{}, hosted[component]...), asClient(clientDuties[component])...), ", "))
	case cfg.Client != "":
		p("Configuring the %s under test (as a client of %s)", component, cfg.Service)
	default:
		p("Configuring the %s under test (as the host of %s)", component, cfg.Service)
	}
	p("")
	switch component {
	case Frontend:
		p("Start the suite first: it listens on %s as the orchestrator, and waits up to %v for the frontend to ask it for its limits.", cfg.Listen, waitOr(cfg.Wait))
		p("Then start the frontend, listening where --target says (%s), with:", cfg.Target)
		p("  orchestrator URL     %s", suite)
		p("  ingress token        %s   (presented to the orchestrator)", cfg.IngressToken)
		p("  request timeout      %v     (the budget given to each invocation)", cfg.RequestTimeout)
		p("  forwarded headers    not trusted (the default)")
		p("")
		p("  edgerun's own:  frontend --orchestrator-url %s --ingress-token %s --request-timeout %v --listen %s", suite, cfg.IngressToken, cfg.RequestTimeout, cfg.Target)
	case Orchestrator:
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
		p("")
		p("The services run one after another against this one orchestrator, which keeps what earlier ones created; each uses names of its own.")
	case Worker:
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
		p("The worker suites build guest modules: they need a Go toolchain, or an edgerun-conformance built with `make conformance`.")
		p("Without --stop-cmd the suite cannot shut the worker down, and skips the group that checks it drains and leaves.")
		if cfg.Profile != "" {
			p("A profile runs the suites in turn against the one worker: each starts its own orchestrator at %s, which the worker must find", cfg.Listen)
			p("again (it keeps heartbeating, is told to register again, and does). The membership suite runs last because it stops the worker.")
		}
	case NetworkService:
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
		p("Without --stop-cmd (and --stop-cmd-default) the suite cannot stop the service, and skips the groups that check it drains and deregisters.")
		if cfg.Profile != "" {
			p("A profile runs the egress suite and then the membership suite against the same service; the membership suite runs last because it stops it.")
		}
	}
}

func asClient(services []string) []string {
	var out []string
	for _, s := range services {
		out = append(out, s+" (as a client)")
	}
	return out
}

func waitOr(d time.Duration) time.Duration {
	if d == 0 {
		return 15 * time.Second
	}
	return d
}
