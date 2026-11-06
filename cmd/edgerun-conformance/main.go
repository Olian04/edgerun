// Command edgerun-conformance judges whether an implementation of one of
// edgerun's roles keeps its side of the protocols, from outside: it is pointed
// at an address, told which role to test, and talks to it over the wire,
// playing every other role itself.
//
//	edgerun-conformance --role worker --target 10.0.0.5:9000 --listen :7000 --advertise 10.0.0.2
//
// Roles are frontend, orchestrator, worker and egress (the network service).
// Because the suite plays the roles around the implementation, the
// implementation must be configured to reach the suite; --print-config says
// how, for the role and the options given. The exit status is 0 if everything
// passed, 1 if anything failed, 2 for a usage error.
package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Olian04/edgerun/conformance/external"
)

func main() {
	var cfg external.Config
	var (
		printConfig bool
		run         string
		verbose     bool
		timeout     time.Duration
	)
	fs := flag.NewFlagSet("edgerun-conformance", flag.ContinueOnError)
	fs.StringVar(&cfg.Role, "role", "", "the role to test: "+strings.Join(external.Roles, ", "))
	fs.StringVar(&cfg.Target, "target", "", "the implementation under test, host:port or a URL (for a worker, the endpoint it advertises)")
	fs.StringVar(&cfg.TargetDefault, "target-default", "", "egress: a second network service, in its default configuration, for what it refuses whatever it is asked to allow")
	fs.StringVar(&cfg.Listen, "listen", "127.0.0.1", "where the suite's own servers listen, host or host:port (a port is required for every role but orchestrator)")
	fs.StringVar(&cfg.Advertise, "advertise", "", "the host the implementation reaches the suite at, if not the one it listens on")
	fs.StringVar(&cfg.StopCmd, "stop-cmd", "", "worker, egress: a shell command that shuts the implementation under test down as SIGTERM does")
	fs.StringVar(&cfg.StopCmdDefault, "stop-cmd-default", "", "egress: the same for --target-default")
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
	fs.BoolVar(&printConfig, "print-config", false, "print how to configure the implementation under test for this role, and exit")
	fs.StringVar(&run, "run", "", "run only the groups and tests whose names match this regular expression (a/b/c selects nested ones)")
	fs.BoolVar(&verbose, "v", true, "print each test as it runs")
	fs.DurationVar(&timeout, "timeout", 30*time.Minute, "give up after this long")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: edgerun-conformance --role ROLE --target HOST:PORT [options]\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	cfg.MaxConcurrency = uint32(maxConc)

	if printConfig {
		if cfg.Role == "" {
			fmt.Fprintln(os.Stderr, "edgerun-conformance: --print-config needs --role")
			os.Exit(2)
		}
		if cfg.Target == "" {
			cfg.Target = "HOST:PORT"
		}
		external.PrintConfig(os.Stdout, cfg)
		return
	}
	tests, err := external.Tests(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "edgerun-conformance:", err)
		os.Exit(2)
	}
	fmt.Fprintln(os.Stderr, "edgerun-conformance: configure the implementation as follows, then run again if it is not yet:")
	external.PrintConfig(os.Stderr, cfg)
	fmt.Fprintln(os.Stderr)

	// The suites are written against package testing; run them with its own
	// driver, which also gives them -run selection, timing and exit status.
	testing.Init()
	args := []string{"-test.timeout=" + timeout.String()}
	if verbose {
		args = append(args, "-test.v=true")
	}
	if run != "" {
		args = append(args, "-test.run="+tests[0].Name+"/"+run)
	}
	if err := flag.CommandLine.Parse(args); err != nil {
		fmt.Fprintln(os.Stderr, "edgerun-conformance:", err)
		os.Exit(2)
	}
	testing.Main(func(pat, str string) (bool, error) {
		ok, err := regexp.MatchString(pat, str)
		return ok, err
	}, tests, nil, nil)
}
