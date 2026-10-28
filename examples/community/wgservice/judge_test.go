package wgservice_test

import (
	"bytes"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"example.com/edgerun-wgservice/internal/wgtest"
)

const portJudgeSite2 = 23033

// TestJudge points edgerun's own conformance executable at wgservice
// (`--role egress`), the way docs/protocol.md "Conformance" describes.
//
// The suite hosts its web servers on its own machine and expects the service
// to reach them. A tunnel-only service cannot, so the test makes the machine a
// destination *inside* the tunnel: the suite advertises 10.20.0.2 as its host,
// listens on 127.0.0.1, and the site end of the tunnel forwards connections
// for 10.20.0.2 to 127.0.0.1 (wgtest.Site.ForwardTo). The service is told, by
// its own flags, to register with the suite at 127.0.0.1 (not through the
// tunnel) and to carry and be allowed to connect to 10.20.0.2.
func TestJudge(t *testing.T) {
	if testing.Short() {
		t.Skip("takes a minute")
	}
	// As --print-config asks (the suite's host only), and as an operator would
	// configure a service inside a private network (the whole range).
	t.Run("as print-config says", func(t *testing.T) { judge(t, "10.20.0.2,*.covered.test", "10.20.0.2/32") })
	t.Run("a whole private range", func(t *testing.T) { judge(t, "10.20.0.0/16,*.covered.test", "10.20.0.0/16") })
}

func judge(t *testing.T, covers, allowNet string) {
	site, err := wgtest.NewSite(portJudgeSite, map[string][]netip.Addr{})
	if err != nil {
		t.Fatal(err)
	}
	defer site.Close()
	if err := site.ForwardTo(wgtest.OriginAddr, "127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	// The second instance (default configuration) has a tunnel of its own to
	// nowhere in particular: it must refuse everything forbidden before
	// connecting, so it never needs one.
	site2, err := wgtest.NewSite(portJudgeSite2, map[string][]netip.Addr{})
	if err != nil {
		t.Fatal(err)
	}
	defer site2.Close()
	conf1 := filepath.Join(tmpRoot, "judge1.conf")
	conf2 := filepath.Join(tmpRoot, "judge2.conf")
	os.WriteFile(conf1, []byte(site.GatewayConfig(fmt.Sprintf("127.0.0.1:%d", portJudgeSite), portJudgeGW1)), 0o600)
	os.WriteFile(conf2, []byte(site2.GatewayConfig(fmt.Sprintf("127.0.0.1:%d", portJudgeSite2), portJudgeGW2)), 0o600)

	suiteURL := fmt.Sprintf("http://127.0.0.1:%d", portSuite) // how the service reaches the suite: not through the tunnel
	common := func(port int, covers, conf string, extra ...string) []string {
		return append([]string{
			"--listen", fmt.Sprintf("127.0.0.1:%d", port), "--advertise-url", fmt.Sprintf("http://127.0.0.1:%d", port),
			"--orchestrator-url", suiteURL, "--network-token", "conformance-network-token", "--egress-token", "conformance-egress-token",
			"--covers", covers, "--wg-config", conf,
		}, extra...)
	}
	// Exactly what --print-config asks of a service: carry the suite's host and
	// *.covered.test, be allowed to connect to the suite's host; the second
	// carries "*" and is allowed nothing.
	open := start(t, "judge-open", nil, wgserviceBin(t), common(portJudgeOpen, covers, conf1, "--allow-network", allowNet)...)
	closed := start(t, "judge-default", nil, wgserviceBin(t), common(portJudgeClose, "*", conf2)...)
	stopCmd := func(p *process) string { return fmt.Sprintf("kill -TERM %d", p.cmd.Process.Pid) }

	args := []string{
		"--role", "egress", "--target", fmt.Sprintf("127.0.0.1:%d", portJudgeOpen), "--target-default", fmt.Sprintf("127.0.0.1:%d", portJudgeClose),
		"--listen", fmt.Sprintf("127.0.0.1:%d", portSuite), "--advertise", "10.20.0.2",
		"--stop-cmd", stopCmd(open), "--stop-cmd-default", stopCmd(closed), "--timeout", "10m",
	}
	cmd := exec.Command(conformanceBin(t), args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	runErr := cmd.Run()
	os.WriteFile(filepath.Join(tmpRoot, "logs", "judge-conformance.txt"), out.Bytes(), 0o644)
	if p := os.Getenv("WGSERVICE_JUDGE_OUT"); p != "" {
		os.WriteFile(p, out.Bytes(), 0o644)
	}

	pass, fail, skip := summarize(out.String())
	t.Logf("conformance: %d passed, %d failed, %d skipped", len(pass), len(fail), len(skip))
	for _, f := range fail {
		t.Logf("FAIL %s", f)
	}
	for _, s := range skip {
		t.Logf("SKIP %s", s)
	}
	if runErr != nil {
		unexpected := []string{}
		for _, f := range fail {
			if _, ok := knownFailures[f]; !ok {
				unexpected = append(unexpected, f)
			}
		}
		if len(unexpected) > 0 || len(fail) == 0 {
			t.Errorf("the conformance executable failed (%v); unexpected failures: %v\n%s", runErr, unexpected, tail(filepath.Join(tmpRoot, "logs", "judge-conformance.txt"), 6000))
		}
	}
	for f := range knownFailures {
		found := false
		for _, g := range fail {
			found = found || g == f
		}
		if !found {
			t.Errorf("%s is listed as a known failure but did not fail: update judge_test.go and FINDINGS.md", f)
		}
	}
	_ = time.Second
}

var resultLine = regexp.MustCompile(`^\s*--- (PASS|FAIL|SKIP): (\S+) `)

// summarize returns the leaf test names (a/b/c) by outcome.
func summarize(out string) (pass, fail, skip []string) {
	for _, l := range strings.Split(out, "\n") {
		m := resultLine.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		switch m[1] {
		case "PASS":
			pass = append(pass, m[2])
		case "FAIL":
			fail = append(fail, m[2])
		case "SKIP":
			skip = append(skip, m[2])
		}
	}
	// A parent is reported FAIL when a child is: keep leaves only.
	leaf := func(names []string, all []string) []string {
		var out []string
		for _, n := range names {
			isParent := false
			for _, o := range all {
				if o != n && strings.HasPrefix(o, n+"/") {
					isParent = true
				}
			}
			if !isParent {
				out = append(out, n)
			}
		}
		sort.Strings(out)
		return out
	}
	all := append(append(append([]string{}, pass...), fail...), skip...)
	return leaf(pass, all), leaf(fail, fail), leaf(skip, all)
}

// knownFailures: cases the suite fails that are explained in FINDINGS.md.
var knownFailures = map[string]string{}
