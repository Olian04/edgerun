package ctl_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Olian04/edgerun/internal/ctl"
	orchapp "github.com/Olian04/edgerun/internal/orchestrator/app"
	"github.com/Olian04/edgerun/internal/orchestrator/orchtest"
	"github.com/Olian04/edgerun/internal/testpg"
)

func TestMain(m *testing.M) { testpg.Main(m) }

const adminToken = "admin-token"

type rig struct {
	t   *testing.T
	url string
}

func newRig(t *testing.T) *rig {
	t.Helper()
	orch, err := orchapp.Start(context.Background(), orchapp.Config{
		Listen: "127.0.0.1:0", DatabaseURL: testpg.DSN(t),
		IngressToken: "i", WorkerToken: "w", AdminToken: adminToken, ExecutionToken: "x",
		SecretKey: orchtest.Key, HeartbeatInterval: 200 * time.Millisecond, RefreshEvery: 100 * time.Millisecond,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { orch.Stop(context.Background()) })
	return &rig{t: t, url: "http://" + orch.Addr()}
}

// run executes edgerunctl with the rig's credentials.
func (r *rig) run(args ...string) (code int, stdout, stderr string) {
	r.t.Helper()
	var out, errb bytes.Buffer
	env := map[string]string{"EDGERUN_URL": r.url, "EDGERUN_ADMIN_TOKEN": adminToken}
	code = ctl.Run(context.Background(), args, func(k string) string { return env[k] }, &out, &errb)
	return code, out.String(), errb.String()
}

func (r *rig) mustRun(args ...string) string {
	r.t.Helper()
	code, out, errb := r.run(args...)
	if code != 0 {
		r.t.Fatalf("edgerunctl %s: exit %d: %s", strings.Join(args, " "), code, errb)
	}
	return out
}

func TestTheWholeLifecycleFromTheCommandLine(t *testing.T) {
	r := newRig(t)
	dir := t.TempDir()
	module := filepath.Join(dir, "m.wasm")
	os.WriteFile(module, []byte("\x00asm\x01\x00\x00\x00"), 0o644)

	if out := r.mustRun("create", "hello"); !strings.Contains(out, "created hello") {
		t.Errorf("create: %q", out)
	}
	if out := r.mustRun("functions"); !strings.Contains(out, "hello") || !strings.Contains(out, "NAME") {
		t.Errorf("functions: %q", out)
	}

	// Flags may come before or after the positional arguments.
	if out := r.mustRun("deploy", "hello", module, "--memory-mib", "64"); !strings.Contains(out, "version 1 uploaded") {
		t.Errorf("deploy: %q", out)
	}
	if out := r.mustRun("deploy", "--activate", "hello", module); !strings.Contains(out, "version 2 uploaded and live") {
		t.Errorf("deploy --activate: %q", out)
	}
	vs := r.mustRun("versions", "hello")
	if !strings.Contains(vs, "v2") || !strings.Contains(vs, "live") || !strings.Contains(vs, "64 MiB") {
		t.Errorf("versions:\n%s", vs)
	}
	if out := r.mustRun("activate", "hello", "v1"); !strings.Contains(out, "version 1 is live") {
		t.Errorf("activate: %q", out)
	}
	if out := r.mustRun("functions"); !strings.Contains(out, "v1") {
		t.Errorf("the list should show the live version: %q", out)
	}

	if out := r.mustRun("route", "api.example.com", "/v1", "hello"); !strings.Contains(out, "routed api.example.com/v1 to hello") {
		t.Errorf("route: %q", out)
	}
	rs := r.mustRun("routes")
	if !strings.Contains(rs, "api.example.com") || !strings.Contains(rs, "/v1") || !strings.Contains(rs, "hello") {
		t.Errorf("routes:\n%s", rs)
	}

	r.mustRun("set", "hello", "GREETING", "Hi")
	r.mustRun("set", "hello", "API_KEY", "s3cret", "--secret")
	env := r.mustRun("env", "hello")
	if !strings.Contains(env, "GREETING") || !strings.Contains(env, "Hi") || !strings.Contains(env, "(secret)") {
		t.Errorf("env:\n%s", env)
	}
	if strings.Contains(env, "s3cret") {
		t.Error("a secret's value must never be printed")
	}
	r.mustRun("unset", "hello", "GREETING")
	if env := r.mustRun("env", "hello"); strings.Contains(env, "GREETING") {
		t.Error("unset did not remove the variable")
	}

	// Output a person (or a script) can read.
	if out := r.mustRun("workers"); !strings.HasPrefix(out, "ENDPOINT") {
		t.Errorf("workers: %q", out)
	}
	if out := r.mustRun("invocations", "--limit", "5"); !strings.HasPrefix(out, "TIME") {
		t.Errorf("invocations: %q", out)
	}

	id := strings.Fields(strings.Split(r.mustRun("routes"), "\n")[1])
	r.mustRun("unroute", id[len(id)-1])
	r.mustRun("delete", "hello")
	if out := r.mustRun("functions"); strings.Contains(out, "hello") {
		t.Errorf("delete: %q", out)
	}
}

func TestErrorsAreSaidPlainlyWithTheRightExitCode(t *testing.T) {
	r := newRig(t)
	r.mustRun("create", "dup")
	for name, tc := range map[string]struct {
		args []string
		code int
		want string
	}{
		"duplicate":        {[]string{"create", "dup"}, 1, "already exists"},
		"bad name":         {[]string{"create", "Bad Name"}, 1, "lowercase"},
		"unknown function": {[]string{"delete", "nope"}, 1, `no function named "nope"`},
		"unknown version":  {[]string{"activate", "dup", "9"}, 1, "no version 9"},
		"missing file":     {[]string{"deploy", "dup", "/no/such/file"}, 1, "no such file"},
		"unknown command":  {[]string{"frobnicate"}, 2, "unknown command"},
		"missing argument": {[]string{"create"}, 2, "usage: edgerunctl create NAME"},
		"bad version":      {[]string{"activate", "dup", "abc"}, 2, "VERSION must be a number"},
		"reserved env":     {[]string{"set", "dup", "REQUEST_METHOD", "x"}, 1, "reserved"},
	} {
		code, _, errb := r.run(tc.args...)
		if code != tc.code || !strings.Contains(errb, tc.want) {
			t.Errorf("%s: exit %d stderr %q, want exit %d containing %q", name, code, errb, tc.code, tc.want)
		}
		if strings.Contains(errb, "connect") || strings.Contains(errb, "rpc") {
			t.Errorf("%s: protocol framing leaked into the message: %q", name, errb)
		}
	}
}

func TestMissingCredentialsAndHelp(t *testing.T) {
	var out, errb bytes.Buffer
	none := func(string) string { return "" }
	if code := ctl.Run(context.Background(), []string{"functions"}, none, &out, &errb); code != 2 || !strings.Contains(errb.String(), "--url") {
		t.Errorf("no credentials: %d %q", code, errb.String())
	}
	out.Reset()
	if code := ctl.Run(context.Background(), []string{"help"}, none, &out, &errb); code != 0 || !strings.Contains(out.String(), "usage:") {
		t.Errorf("help: %d", code)
	}
	if code := ctl.Run(context.Background(), nil, none, &out, &errb); code != 0 {
		t.Errorf("no arguments should print help, got %d", code)
	}
	// A wrong token is an authentication error, not a crash.
	r := newRig(t)
	env := map[string]string{"EDGERUN_URL": r.url, "EDGERUN_ADMIN_TOKEN": "wrong"}
	errb.Reset()
	if code := ctl.Run(context.Background(), []string{"functions"}, func(k string) string { return env[k] }, &out, &errb); code != 1 || errb.Len() == 0 {
		t.Errorf("wrong token: %d %q", code, errb.String())
	}
}
