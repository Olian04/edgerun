// Command edgerun-conformance judges whether an implementation keeps its side of
// edgerun's protocols, from outside: it is pointed at an address, told what to
// judge, and talks to it over the wire, playing every other component itself.
//
//	edgerun-conformance --service membership --target 10.0.0.5:8080
//	edgerun-conformance --profile worker --target 10.0.0.5:9000 --listen :7000 --advertise 10.0.0.2
//
// --service judges one platform service: the implementation at --target is its
// host (add --client COMPONENT to judge the component that is its client
// instead). --profile judges a component: every service it hosts, in turn
// against the same target, then its duties as a client of other services where a
// suite has them. Services are edge, ingress, execution, membership, artifacts,
// egress, catalog, routing, plans and records; components are frontend,
// orchestrator, worker and network-service.
//
// Because the suite plays the components around the implementation, the
// implementation must be configured to reach the suite; --print-config says how,
// for what is judged and the options given. The exit status is 0 if everything
// passed, 1 if anything failed, 2 for a usage error.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Olian04/edgerun/conformance/external"
)

const usage = `usage: edgerun-conformance (--service SERVICE [--client COMPONENT] | --profile COMPONENT) --target HOST:PORT [options]

Judge an implementation that is already running. What is judged:

  --service SERVICE   one platform service, with the implementation at --target as its HOST.
                      SERVICE is one of: %[1]s
                      Hosts: edge is hosted by a frontend; execution by a worker; egress by a
                      network service; the others by an orchestrator.
  --client COMPONENT  with --service: judge the component that is the CLIENT of the service instead
                      (the implementation at --target is that component; the suite plays the host).
                      Services with a client suite, and the components that can be judged:
                      %[3]s
  --profile COMPONENT a component: every service it hosts, run in turn against --target, then its
                      duties as a client of other services. COMPONENT is one of: %[2]s
                        frontend         edge (which also proves its duties as the client of ingress)
                        orchestrator     ingress, membership, artifacts, catalog, routing, plans,
                                         records; then execution (as the client, against fake workers)
                        worker           execution; then artifacts, egress and membership (as the
                                         client of each; membership last, it stops the worker)
                        network-service  egress; then membership (as the client; it stops the service)

What each component under test needs (--print-config prints exactly what, for the options given):

  frontend         --listen HOST:PORT with a port: the suite is the frontend's orchestrator.
  orchestrator     must be new, empty and used by nothing else; it calls the suite's fake workers,
                   so --advertise must be a host it can reach. --listen needs no port.
  worker           --listen with a port (the suite is the worker's orchestrator); --target is the
                   endpoint the worker advertises; --stop-cmd shuts it down as SIGTERM does, for the
                   checks that it drains and leaves.
  network-service  --listen with a port; --target-default is a second instance in its default
                   configuration, for what a service refuses however it is asked; --stop-cmd and
                   --stop-cmd-default shut them down as SIGTERM does.

Options:
`

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	// --role was replaced by --service and --profile.
	for _, a := range args {
		if a == "--" {
			break
		}
		if a == "-role" || a == "--role" || strings.HasPrefix(a, "-role=") || strings.HasPrefix(a, "--role=") {
			fmt.Fprintln(stderr, "edgerun-conformance: --role no longer exists. Use --service SERVICE to judge one platform service "+
				"(edge, ingress, execution, membership, artifacts, egress, catalog, routing, plans, records), or "+
				"--profile COMPONENT to judge every service a component hosts (frontend, orchestrator, worker, network-service). "+
				"The old roles are --profile frontend, --profile orchestrator, --profile worker and --profile network-service "+
				"(egress was the network service). See --help.")
			return 2
		}
	}
	var cfg external.Config
	var (
		printConfig bool
		runPattern  string
		verbose     bool
		timeout     time.Duration
	)
	fs := flag.NewFlagSet("edgerun-conformance", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.Service, "service", "", "judge one platform service, the implementation at --target being its host: "+strings.Join(external.Services, ", "))
	fs.StringVar(&cfg.Client, "client", "", "with --service: judge the component that is the client of the service instead of its host (see above)")
	fs.StringVar(&cfg.Profile, "profile", "", "judge a component: every service it hosts, then its duties as a client: "+strings.Join(external.Profiles, ", "))
	fs.StringVar(&cfg.Target, "target", "", "the implementation under test, host:port or a URL (for a worker, the endpoint it advertises)")
	fs.StringVar(&cfg.TargetDefault, "target-default", "", "network service: a second one, in its default configuration, for what it refuses whatever it is asked to allow")
	fs.StringVar(&cfg.Listen, "listen", "127.0.0.1", "where the suite's own servers listen, host or host:port (a port is required unless an orchestrator is judged)")
	fs.StringVar(&cfg.Advertise, "advertise", "", "the host the implementation reaches the suite at, if not the one it listens on")
	fs.StringVar(&cfg.StopCmd, "stop-cmd", "", "worker, network service: a shell command that shuts the implementation under test down as SIGTERM does")
	fs.StringVar(&cfg.StopCmdDefault, "stop-cmd-default", "", "network service: the same for --target-default")
	fs.DurationVar(&cfg.Wait, "wait", 60*time.Second, "how long to wait for the implementation to connect to the suite")
	fs.StringVar(&cfg.IngressToken, "ingress-token", "", "token frontends present to the orchestrator (default: the suite's own)")
	fs.StringVar(&cfg.WorkerToken, "worker-token", "", "token workers present to the orchestrator")
	fs.StringVar(&cfg.AdminToken, "admin-token", "", "token operators present to the orchestrator")
	fs.StringVar(&cfg.NetworkToken, "network-token", "", "token network services present to the orchestrator")
	fs.StringVar(&cfg.ExecutionToken, "execution-token", "", "token the orchestrator presents to workers")
	fs.StringVar(&cfg.EgressToken, "egress-token", "", "token workers present to network services")
	fs.DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", 0, "heartbeat interval the orchestrator dictates (default: the suite's own)")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", 0, "frontend: the budget it gives each invocation (default 3s)")
	var maxConc uint
	fs.UintVar(&maxConc, "max-concurrency", 0, "worker: invocations it runs at once (default 2)")
	fs.BoolVar(&printConfig, "print-config", false, "print how to configure the implementation under test for what is judged, and exit")
	fs.StringVar(&runPattern, "run", "", "run only the groups and tests whose names match this regular expression (a/b/c selects nested ones)")
	fs.BoolVar(&verbose, "v", true, "print each test as it runs")
	fs.DurationVar(&timeout, "timeout", 30*time.Minute, "give up after this long")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), usage, strings.Join(external.Services, ", "), strings.Join(external.Profiles, ", "), clientsHelp())
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	cfg.MaxConcurrency = uint32(maxConc)

	if printConfig {
		if cfg.Service == "" && cfg.Profile == "" {
			fmt.Fprintln(stderr, "edgerun-conformance: --print-config needs --service or --profile")
			return 2
		}
		if cfg.Target == "" {
			cfg.Target = "HOST:PORT"
		}
		if err := validateWhat(cfg); err != nil {
			fmt.Fprintln(stderr, "edgerun-conformance:", err)
			return 2
		}
		external.PrintConfig(stdout, cfg)
		return 0
	}
	tests, err := external.Tests(cfg)
	if err != nil {
		fmt.Fprintln(stderr, "edgerun-conformance:", err)
		return 2
	}
	fmt.Fprintln(stderr, "edgerun-conformance: configure the implementation as follows, then run again if it is not yet:")
	external.PrintConfig(stderr, cfg)
	fmt.Fprintln(stderr)
	return runTests(tests, runPattern, verbose, timeout, stderr)
}

// validateWhat checks the naming of what is judged, without the rest of what a
// run needs (a target, a listen port).
func validateWhat(cfg external.Config) error {
	probe := cfg
	probe.Target, probe.Listen = "HOST:PORT", "127.0.0.1:1"
	return probe.Validate()
}

func clientsHelp() string {
	var lines []string
	for _, s := range external.Services {
		if c := external.ClientsOf(s); len(c) > 0 {
			lines = append(lines, s+": "+strings.Join(c, ", "))
		}
	}
	return strings.Join(lines, "; ")
}

// runTests drives the suites with package testing's own driver, which also gives
// them -run selection, timing and exit status.
func runTests(tests []testing.InternalTest, run string, verbose bool, timeout time.Duration, stderr io.Writer) int {
	testing.Init()
	args := []string{"-test.timeout=" + timeout.String()}
	if verbose {
		args = append(args, "-test.v=true")
	}
	if run != "" {
		var names []string
		for _, t := range tests {
			names = append(names, regexp.QuoteMeta(t.Name))
		}
		args = append(args, "-test.run=^("+strings.Join(names, "|")+")$/"+run)
	}
	if err := flag.CommandLine.Parse(args); err != nil {
		fmt.Fprintln(stderr, "edgerun-conformance:", err)
		return 2
	}
	// testing.Main exits the process itself.
	testing.Main(func(pat, str string) (bool, error) {
		return regexp.MatchString(pat, str)
	}, tests, nil, nil)
	return 0
}
