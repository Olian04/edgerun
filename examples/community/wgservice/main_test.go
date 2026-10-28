package wgservice_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Ports: only 23000-23099 are used.
const (
	portSuite      = 23001 // the conformance suite (as the orchestrator)
	portJudgeOpen  = 23002 // the service, configured for the suite's web servers
	portJudgeClose = 23003 // the service in its default configuration

	portTunnelSite  = 23010 // tunnel test: the site's UDP socket
	portTunnelGW    = 23011 // the gateway's
	portTunnelRelay = 23012 // a relay between them that counts what crosses
	portTunnelBack  = 23013

	portOrch      = 23020 // chain test
	portWorker    = 23021
	portChainSvc  = 23022
	portChainSite = 23023
	portChainGW   = 23024
	portChainSvc2 = 23025 // a second service (default-configured: forbids everything private)

	portJudgeSite = 23030 // judge: the site and the two gateways
	portJudgeGW1  = 23031
	portJudgeGW2  = 23032
)

var (
	repoRoot string // the edgerun checkout this module replaces
	modRoot  string // this module
	tmpRoot  string // our own unique temp dir
)

func TestMain(m *testing.M) {
	var err error
	modRoot, _ = os.Getwd()
	repoRoot, _ = filepath.Abs(filepath.Join(modRoot, "..", "..", ".."))
	tmpRoot, err = os.MkdirTemp("", "wgservice-test-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Chmod(tmpRoot, 0o755)
	code := m.Run()
	stopPG()
	os.RemoveAll(tmpRoot)
	os.Exit(code)
}

// ---- building: everything goes into tmpRoot/bin ----

var (
	buildMu sync.Mutex
	built   = map[string]string{}
)

// build compiles target (a package path) in dir into tmpRoot/bin/name, once.
func build(t testing.TB, name, dir string, env []string, args ...string) string {
	t.Helper()
	buildMu.Lock()
	defer buildMu.Unlock()
	if p, ok := built[name]; ok {
		return p
	}
	out := filepath.Join(tmpRoot, "bin", name)
	cmd := exec.Command("go", append([]string{"build", "-o", out}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %s: %v\n%s", name, err, b)
	}
	built[name] = out
	return out
}

func wgserviceBin(t testing.TB) string { return build(t, "wgservice", modRoot, nil, "./cmd/wgservice") }
func orchestratorBin(t testing.TB) string {
	return build(t, "orchestrator", repoRoot, nil, "./cmd/orchestrator")
}
func workerBin(t testing.TB) string { return build(t, "worker", repoRoot, nil, "./cmd/worker") }

// conformanceBin is the conformance executable with the guests compiled in
// (the Makefile's `conformance` target, built into our own directory).
func conformanceBin(t testing.TB) string {
	return build(t, "edgerun-conformance", repoRoot, nil, "-tags", "prebuilt", "./cmd/edgerun-conformance")
}

func fetchWasm(t testing.TB) []byte {
	t.Helper()
	p := build(t, "fetch.wasm", repoRoot, []string{"GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0"}, "-trimpath", "./examples/fetch")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// ---- processes ----

type process struct {
	cmd  *exec.Cmd
	log  string
	done chan struct{}
}

// start runs a program, logging to tmpRoot/logs, and stops it with the test.
func start(t testing.TB, name string, env []string, bin string, args ...string) *process {
	t.Helper()
	os.MkdirAll(filepath.Join(tmpRoot, "logs"), 0o755)
	logPath := filepath.Join(tmpRoot, "logs", name+".log")
	f, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &process{cmd: cmd, log: logPath, done: make(chan struct{})}
	go func() { cmd.Wait(); f.Close(); close(p.done) }()
	t.Cleanup(func() {
		p.stop()
		if t.Failed() {
			t.Logf("---- %s log ----\n%s", name, tail(logPath, 4000))
		}
	})
	return p
}

// stop sends SIGTERM and waits (SIGKILL after 15 s).
func (p *process) stop() {
	select {
	case <-p.done:
		return
	default:
	}
	p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
		p.cmd.Process.Kill()
		<-p.done
	}
}

func tail(path string, n int) string {
	b, _ := os.ReadFile(path)
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(b)
}

func eventually(t testing.TB, what string, d time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// ---- PostgreSQL (devstack.sh's way: a private cluster on a unix socket) ----

var pg struct {
	sync.Once
	err    error
	dir    string
	bin    string
	asRoot bool
	n      int
	mu     sync.Mutex
}

func pgBin() string {
	if p, err := exec.LookPath("initdb"); err == nil {
		return filepath.Dir(p)
	}
	m, _ := filepath.Glob("/usr/lib/postgresql/*/bin")
	if len(m) > 0 {
		return m[len(m)-1]
	}
	return ""
}

func pgRun(args ...string) ([]byte, error) {
	if pg.asRoot {
		args = append([]string{"-u", "postgres", "--"}, args...)
		return exec.Command("runuser", args...).CombinedOutput()
	}
	return exec.Command(args[0], args[1:]...).CombinedOutput()
}

func startPG() error {
	pg.bin = pgBin()
	if pg.bin == "" {
		return fmt.Errorf("PostgreSQL server binaries (initdb, pg_ctl) not found")
	}
	u, _ := user.Current()
	pg.asRoot = u != nil && u.Uid == "0"
	pg.dir = filepath.Join(tmpRoot, "pg")
	if err := os.MkdirAll(pg.dir, 0o755); err != nil {
		return err
	}
	if pg.asRoot {
		if b, err := exec.Command("chown", "postgres:postgres", pg.dir).CombinedOutput(); err != nil {
			return fmt.Errorf("chown: %v %s", err, b)
		}
	}
	data := filepath.Join(pg.dir, "data")
	if b, err := pgRun(filepath.Join(pg.bin, "initdb"), "-D", data, "-A", "trust", "-U", "postgres", "--no-sync", "-E", "UTF8"); err != nil {
		return fmt.Errorf("initdb: %v\n%s", err, b)
	}
	opts := fmt.Sprintf("-c listen_addresses='' -c unix_socket_directories=%s -c fsync=off", pg.dir)
	if b, err := pgRun(filepath.Join(pg.bin, "pg_ctl"), "-D", data, "-l", filepath.Join(pg.dir, "pg.log"), "-o", opts, "-w", "start"); err != nil {
		return fmt.Errorf("pg_ctl start: %v\n%s", err, b)
	}
	return nil
}

func stopPG() {
	if pg.dir == "" || pg.err != nil {
		return
	}
	pgRun(filepath.Join(pg.bin, "pg_ctl"), "-D", filepath.Join(pg.dir, "data"), "-m", "fast", "stop")
}

// newDatabase returns the DSN of a fresh, empty database.
func newDatabase(t testing.TB) string {
	t.Helper()
	pg.Do(func() { pg.err = startPG() })
	if pg.err != nil {
		t.Fatalf("PostgreSQL: %v", pg.err)
	}
	pg.mu.Lock()
	pg.n++
	name := fmt.Sprintf("wgsvc%d", pg.n)
	pg.mu.Unlock()
	if b, err := pgRun(filepath.Join(pg.bin, "createdb"), "-h", pg.dir, "-U", "postgres", name); err != nil {
		t.Fatalf("createdb: %v\n%s", err, b)
	}
	return fmt.Sprintf("postgres://postgres@/%s?host=%s", name, pg.dir)
}

// ---- small things ----

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func ctx10(t testing.TB) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return c
}

var _ = io.Discard
var _ = bytes.NewReader
var _ = strings.Contains
