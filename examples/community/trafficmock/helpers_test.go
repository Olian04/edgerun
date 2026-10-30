package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

const mockHost = "mock.test"

func writeFile(t testing.TB, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// tm runs the tool in-process, as `edgerun-trafficmock run ...`, and returns
// its exit code, JSON report and text output.
func tm(t testing.TB, args ...string) (int, *Report, string) {
	t.Helper()
	jsonPath := filepath.Join(t.TempDir(), "report.json")
	var out, errb bytes.Buffer
	code := realMain(append([]string{"run", "-json", jsonPath}, args...), &out, &errb)
	var rep *Report
	if b, err := os.ReadFile(jsonPath); err == nil {
		rep = &Report{}
		if err := json.Unmarshal(b, rep); err != nil {
			t.Fatalf("report is not JSON: %v", err)
		}
	}
	if errb.Len() > 0 {
		t.Logf("stderr: %s", errb.String())
	}
	return code, rep, out.String()
}

// runOn runs a scenario against a stack with the invocation lookup on.
func runOn(t testing.TB, s *Stack, scenario string, extra ...string) (int, *Report, string) {
	t.Helper()
	args := []string{"-scenario", writeFile(t, "s.yaml", scenario), "-url", s.URL, "-token", ingressToken,
		"-admin-url", s.URL, "-admin-token", adminToken}
	return tm(t, append(args, extra...)...)
}

// warm makes the worker compile the guest before the measured part starts
// (the first invocation of a module costs about a second).
func warm(t testing.TB, s *Stack, host string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		res, err := s.Ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: host, Path: "/status/200"}})
		if err != nil {
			t.Fatalf("warm-up: %v", err)
		}
		res.ReadAll()
	}
}

func atLeast(t testing.TB, what string, got, lo float64) {
	t.Helper()
	if got < lo {
		t.Errorf("%s = %v, want >= %v", what, got, lo)
	}
}

func between(t testing.TB, what string, got, lo, hi float64) {
	t.Helper()
	if got < lo || got > hi {
		t.Errorf("%s = %v, want within [%v, %v]", what, got, lo, hi)
	}
}

func mustRead(t testing.TB, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
