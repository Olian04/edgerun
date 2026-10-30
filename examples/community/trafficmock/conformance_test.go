package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestFrontendConformance points the frontend conformance suite at the
// listener mode of this tool: the suite plays the orchestrator and the HTTP
// clients, the listener is the frontend under test.
func TestFrontendConformance(t *testing.T) {
	requireStack(t)
	if !env.conformOK {
		t.Skip("no edgerun-conformance binary")
	}
	suitePort, frontPort := freePort(t), freePort(t)
	suite := fmt.Sprintf("127.0.0.1:%d", suitePort)
	front := fmt.Sprintf("127.0.0.1:%d", frontPort)
	dir := filepath.Join(env.root, "conformance")
	os.MkdirAll(dir, 0o755)

	// What the suite says to configure (--print-config), in short:
	//   orchestrator URL http://<suite>, ingress token conformance-ingress-token,
	//   request timeout 3s, forwarded headers not trusted.
	cmd := exec.Command(filepath.Join(env.bin, "edgerun-conformance"), "--role", "frontend", "--target", front, "--listen", suite, "--timeout", "5m")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	time.Sleep(500 * time.Millisecond) // the suite starts first
	startProc(t, dir, "listener", nil, env.tmBin, "listen", "-addr", front, "-url", "http://"+suite,
		"-token", "conformance-ingress-token", "-request-timeout", "3s", "-quiet")
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Minute):
		cmd.Process.Signal(syscall.SIGKILL)
		t.Fatal("the suite did not finish")
	}
	os.WriteFile(filepath.Join(dir, "suite.out"), out.Bytes(), 0o644)
	lines := strings.Split(out.String(), "\n")
	var failed []string
	for _, l := range lines {
		if strings.Contains(l, "FAIL") || strings.Contains(l, "--- FAIL") {
			failed = append(failed, l)
		}
	}
	pass, fail := strings.Count(out.String(), "--- PASS"), strings.Count(out.String(), "--- FAIL")
	t.Logf("conformance suite: %d passed, %d failed\nsuite output (head):\n%s\n(tail):\n%s", pass, fail, strings.Join(lines[:min(len(lines), 25)], "\n"), tail(lines, 12))
	if err != nil {
		t.Errorf("the conformance suite reports failures: %v\n%s", err, strings.Join(failed, "\n"))
	}
	_ = context.Background
}

func tail(lines []string, n int) string {
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
