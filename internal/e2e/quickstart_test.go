package e2e

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Olian04/edgerun/internal/ctl"
)

// repoRoot finds the module root, so the example can be built from source.
func repoRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil || len(bytes.TrimSpace(out)) == 0 {
		t.Fatalf("cannot find the module root: %v", err)
	}
	return filepath.Dir(strings.TrimSpace(string(out)))
}

// TestQuickstart is the journey the README describes, for real: build the
// example guest to WebAssembly, deploy it with edgerunctl only, and fetch it
// over plain HTTP through the frontend.
func TestQuickstart(t *testing.T) {
	wasm := filepath.Join(t.TempDir(), "hello.wasm")
	build := exec.Command("go", "build", "-trimpath", "-o", wasm, "./examples/hello")
	build.Dir = repoRoot(t)
	build.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building examples/hello: %v\n%s", err, out)
	}

	s := newStack(t)
	s.startWorker(4)
	w := s.startFrontend(30 * time.Second)

	env := map[string]string{"EDGERUN_URL": "http://" + s.addr, "EDGERUN_ADMIN_TOKEN": adminToken}
	edgerunctl := func(args ...string) string {
		t.Helper()
		var out, errb bytes.Buffer
		if code := ctl.Run(context.Background(), args, func(k string) string { return env[k] }, &out, &errb); code != 0 {
			t.Fatalf("edgerunctl %s: exit %d: %s", strings.Join(args, " "), code, errb.String())
		}
		return out.String()
	}
	edgerunctl("create", "hello")
	edgerunctl("deploy", "hello", wasm, "--activate")
	edgerunctl("route", "hello.localhost", "/", "hello")
	edgerunctl("set", "hello", "GREETING", "Howdy")

	var r reply
	eventually(t, "the example to answer over HTTP", 120*time.Second, func() bool {
		r = w.do("GET", "hello.localhost", "/greet?name=edgerun", "")
		return r.status == 200
	})
	for _, want := range []string{"Howdy, edgerun!", "You asked for GET /greet from 127.0.0.1"} {
		if !strings.Contains(r.body, want) {
			t.Errorf("the response lacks %q:\n%s", want, r.body)
		}
	}
	if ct := r.header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content-type = %q", ct)
	}
}
