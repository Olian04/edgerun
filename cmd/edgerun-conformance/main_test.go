package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Olian04/edgerun/conformance/external"
)

// These tests are about the command line: what it accepts, what it refuses and
// says so, and what it prints. They run the suites against nothing.

func runCLI(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errb bytes.Buffer
	code = run(args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestUsageNamesEveryServiceAndProfile(t *testing.T) {
	code, _, usage := runCLI(t, "--help")
	if code != 0 {
		t.Errorf("--help exits %d", code)
	}
	for _, s := range external.Services {
		if !strings.Contains(usage, s) {
			t.Errorf("the help does not mention service %q:\n%s", s, usage)
		}
	}
	for _, p := range external.Profiles {
		if !strings.Contains(usage, p) {
			t.Errorf("the help does not mention profile %q", p)
		}
	}
	for _, flag := range []string{"--service", "--profile", "--client", "--target", "--listen", "--advertise", "--stop-cmd", "--target-default", "--print-config"} {
		if !strings.Contains(usage, flag) && !strings.Contains(usage, strings.TrimPrefix(flag, "-")) {
			t.Errorf("the help does not mention %s", flag)
		}
	}
	if strings.Contains(usage, "--role") || strings.Contains(usage, "-role ") {
		t.Errorf("the help still mentions --role:\n%s", usage)
	}
}

// Every service the help lists as having a client suite must be accepted, and the
// help must not promise one that Tests refuses.
func TestEveryAdvertisedClientIsAccepted(t *testing.T) {
	for _, s := range external.Services {
		for _, c := range external.ClientsOf(s) {
			cfg := external.Config{Service: s, Client: c, Target: "127.0.0.1:1", Listen: "127.0.0.1:1"}
			tests, err := external.Tests(cfg)
			if err != nil || len(tests) != 1 {
				t.Errorf("--service %s --client %s: %d tests, %v", s, c, len(tests), err)
			}
		}
	}
}

func TestProfilesRunTheirServices(t *testing.T) {
	want := map[string][]string{
		"frontend":        {"EdgeConformance"},
		"orchestrator":    {"IngressConformance", "MembershipConformance", "ArtifactsConformance", "CatalogConformance", "RoutingConformance", "PlansConformance", "RecordsConformance", "ExecutionConformanceAsOrchestrator"},
		"worker":          {"ExecutionConformance", "ArtifactsConformanceAsWorker", "EgressConformanceAsWorker", "MembershipConformanceAsWorker"},
		"network-service": {"EgressConformance", "MembershipConformanceAsNetworkService"},
	}
	for profile, names := range want {
		tests, err := external.Tests(external.Config{Profile: profile, Target: "127.0.0.1:1", Listen: "127.0.0.1:1"})
		if err != nil {
			t.Errorf("--profile %s: %v", profile, err)
			continue
		}
		var got []string
		for _, test := range tests {
			got = append(got, test.Name)
		}
		if strings.Join(got, ",") != strings.Join(names, ",") {
			t.Errorf("--profile %s runs %v, want %v", profile, got, names)
		}
	}
	// The suite that stops the component under test runs last.
	for _, profile := range []string{"worker", "network-service"} {
		tests, _ := external.Tests(external.Config{Profile: profile, Target: "127.0.0.1:1", Listen: "127.0.0.1:1"})
		if last := tests[len(tests)-1].Name; !strings.HasPrefix(last, "Membership") {
			t.Errorf("--profile %s ends with %s: the membership suite stops the component and must be last", profile, last)
		}
	}
}

func TestRoleIsRefusedWithAMessageNamingTheNewFlags(t *testing.T) {
	code, _, stderr := runCLI(t, "--role", "worker", "--target", "x:1")
	if code != 2 {
		t.Errorf("exit %d, want 2", code)
	}
	for _, want := range []string{"--role no longer exists", "--service", "--profile", "network-service"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the message does not say %q:\n%s", want, stderr)
		}
	}
}

func TestUsageErrorsExitTwo(t *testing.T) {
	for name, args := range map[string][]string{
		"nothing to judge":              {"--target", "x:1"},
		"both service and profile":      {"--service", "ingress", "--profile", "orchestrator", "--target", "x:1"},
		"an unknown service":            {"--service", "nonsense", "--target", "x:1"},
		"an unknown profile":            {"--profile", "nonsense", "--target", "x:1"},
		"a client of a service without": {"--service", "catalog", "--client", "orchestrator", "--target", "x:1"},
		"a client that is not one":      {"--service", "membership", "--client", "frontend", "--target", "x:1", "--listen", "127.0.0.1:1"},
		"a client with a profile":       {"--profile", "worker", "--client", "worker", "--target", "x:1", "--listen", "127.0.0.1:1"},
		"no target":                     {"--service", "ingress"},
		"a worker needs a listen port":  {"--profile", "worker", "--target", "x:1"},
		"an unknown flag":               {"--frobnicate"},
	} {
		code, _, stderr := runCLI(t, args...)
		if code != 2 {
			t.Errorf("%s: exit %d, want 2 (stderr: %s)", name, code, stderr)
		}
		if stderr == "" {
			t.Errorf("%s: nothing was said", name)
		}
	}
}

func TestPrintConfigSaysWhatTheComponentNeeds(t *testing.T) {
	for _, c := range []struct {
		args []string
		want []string
	}{
		{[]string{"--service", "membership"}, []string{"orchestrator", "worker token", "network token", "admin token", "host of membership"}},
		{[]string{"--service", "edge", "--listen", "127.0.0.1:7000"}, []string{"frontend", "orchestrator URL", "http://127.0.0.1:7000"}},
		{[]string{"--service", "egress", "--listen", "127.0.0.1:7000"}, []string{"network service", "network token", "--target-default"}},
		{[]string{"--service", "membership", "--client", "worker", "--listen", "127.0.0.1:7000"}, []string{"worker", "as a client of membership", "worker token", "stop-cmd"}},
		{[]string{"--profile", "worker", "--listen", "127.0.0.1:7000"}, []string{"profile", "execution, artifacts (as a client), egress (as a client), membership (as a client)", "orchestrator URL"}},
		{[]string{"--profile", "orchestrator"}, []string{"ingress, membership, artifacts, catalog, routing, plans, records, execution (as a client)", "execution token"}},
	} {
		code, out, stderr := runCLI(t, append(c.args, "--print-config")...)
		if code != 0 {
			t.Errorf("%v: exit %d: %s", c.args, code, stderr)
			continue
		}
		for _, w := range c.want {
			if !strings.Contains(out, w) {
				t.Errorf("%v: the configuration does not say %q:\n%s", c.args, w, out)
			}
		}
	}
}
