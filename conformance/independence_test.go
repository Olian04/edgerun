package conformance_test

import (
	"os/exec"
	"strings"
	"testing"
)

// The conformance suites are only evidence if they cannot be wrong in the same
// way as what they judge. So they depend on the generated protocol code (gen/)
// and on each other (conformance/), and on nothing else of this module: not on
// any implementation, and not on the library that wraps the protocol. This test
// is what keeps it so, for every package under conformance/ (tests included).
func TestSuitesDependOnlyOnGeneratedCode(t *testing.T) {
	const module = "github.com/Olian04/edgerun"
	out, err := exec.Command("go", "list", "-test", "-deps", "-f", "{{.ImportPath}}", module+"/conformance/...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	seen := 0
	for _, pkg := range strings.Fields(string(out)) {
		// Test variants are listed as "path [path.test]" or "path.test".
		pkg = strings.TrimSuffix(strings.Fields(pkg + " ")[0], ".test")
		if pkg != module && !strings.HasPrefix(pkg, module+"/") {
			continue // the standard library and third-party modules
		}
		seen++
		rest := strings.TrimPrefix(pkg, module+"/")
		if strings.HasPrefix(rest, "gen/") || strings.HasPrefix(rest, "conformance") {
			continue
		}
		t.Errorf("a conformance package depends on %s: suites may depend only on gen/ and conformance/, so that "+
			"an implementation cannot be wrong in the same way as the test that judges it", pkg)
	}
	if seen < 10 {
		t.Fatalf("only %d packages of the module were listed: the check is not looking at what it should\n%s", seen, out)
	}
}
