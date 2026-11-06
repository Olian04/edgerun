package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The real binary, as a user would run it.
func TestCLI(t *testing.T) {
	h := newHarness(t)
	h.R.Commit("c", files("m.wasm", env.hello, "edgerun.yaml", `
functions:
  - {name: hello, module: m.wasm, env: {GREETING: CLI}}
routes:
  - {host: hello.test, path: /, function: hello}
`))
	bin := filepath.Join(env.bin, "edgerun-gitops")
	cmd := func(args ...string) *exec.Cmd {
		c := exec.Command(bin, append([]string{"--repo", h.R.URL, "--orchestrator", h.S.URL, "--workdir", filepath.Join(h.S.dir, "cli-clone")}, args...)...)
		c.Env = append(os.Environ(), "EDGERUN_ADMIN_TOKEN="+adminToken)
		return c
	}
	run := func(args ...string) (int, string) {
		c := cmd(args...)
		var b bytes.Buffer
		c.Stdout, c.Stderr = &b, &b
		err := c.Run()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		return code, b.String()
	}

	if code, out := run("--dry-run", "--detailed-exitcode"); code != 2 || !strings.Contains(out, "+ hello: create function") {
		t.Fatalf("dry run: %d\n%s", code, out)
	}
	if code, out := run("--once"); code != 0 {
		t.Fatalf("once: %d\n%s", code, out)
	}
	if got := h.S.mustInvoke(t, "hello.test", "/"); !strings.HasPrefix(got, "CLI, world!") {
		t.Fatal(got)
	}
	if code, out := run("--dry-run", "--detailed-exitcode"); code != 0 || !strings.Contains(out, "in sync") {
		t.Fatalf("second dry run: %d\n%s", code, out)
	}

	// A change the orchestrator would refuse: the dry run says so and exits 1 (not 2:
	// it is not a difference, it is a failure), the real run fails the same way, and
	// neither creates anything.
	h.R.Commit("bad", files("edgerun.yaml", `
functions:
  - {name: hello, module: m.wasm, env: {GREETING: CLI}}
  - {name: bad, module: m.wasm, egress: ["a.example.com:99999"]}
routes:
  - {host: hello.test, path: /, function: hello}
`))
	if code, out := run("--dry-run", "--detailed-exitcode"); code != 1 || !strings.Contains(out, "rejected by the orchestrator (Validate)") || !strings.Contains(out, "99999") {
		t.Fatalf("dry run of a rejected change: %d\n%s", code, out)
	}
	if code, out := run("--once"); code != 1 || !strings.Contains(out, "99999") {
		t.Fatalf("apply of a rejected change: %d\n%s", code, out)
	}
	if _, ok := h.S.functions(t)["bad"]; ok {
		t.Fatal("a rejected function was created")
	}
	h.R.Revert()

	if code, out := run("--once", "--file", "nope.yaml"); code != 1 || !strings.Contains(out, "nope.yaml") {
		t.Fatalf("missing file: %d\n%s", code, out)
	}

	// The poll loop: runs until signalled, exits 0.
	c := cmd("--interval", "300ms")
	var b syncBuffer
	c.Stdout, c.Stderr = &b, &b
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a poll", func() bool { return strings.Count(b.String(), "in sync") >= 2 })
	c.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("loop exit: %v\n%s", err, b.String())
		}
	case <-time.After(5 * time.Second):
		c.Process.Kill()
		t.Fatal("loop did not stop")
	}
}
