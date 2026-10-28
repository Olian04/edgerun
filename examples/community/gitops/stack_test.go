package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

// This file builds a real edgerun stack out of real processes: a private
// PostgreSQL, the orchestrator and a worker, compiled from the edgerun checkout
// two directories up. Nothing of edgerun's internals is imported.
//
// Ports are taken from 21000-21099; every file lives under one temp directory.

const (
	ingressToken   = "t-ingress"
	workerToken    = "t-worker"
	adminToken     = "t-admin"
	executionToken = "t-exec"
	secretKey      = "ZGV2LW9ubHktc2VjcmV0LWtleS0zMi1ieXRlcyEhISE="
)

var env struct {
	root     string // temp dir of this test run
	repoRoot string // the edgerun checkout
	bin      string
	pgBin    string
	pgSock   string
	pgUser   string // run postgres tools as this OS user ("" = current)
	hello    []byte // examples/hello built for wasip1
	guest    map[string][]byte
	err      error
}

var nextPort atomic.Int32

func TestMain(m *testing.M) {
	code := func() int {
		root, err := os.MkdirTemp("", "gitops-e2e-")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		os.Chmod(root, 0o755)
		env.root = root
		defer os.RemoveAll(root)
		nextPort.Store(21000 + int32(os.Getpid()%50))
		env.err = setupEnv()
		if env.err == nil {
			defer stopPG()
		}
		return m.Run()
	}()
	os.Exit(code)
}

func sh(dir string, extraEnv []string, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), extraEnv...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return nil
}

func setupEnv() error {
	var err error
	if env.repoRoot, err = filepath.Abs("../../.."); err != nil {
		return err
	}
	env.bin = filepath.Join(env.root, "bin")
	os.MkdirAll(env.bin, 0o755)

	// 1. Compile the components and the guests.
	for _, c := range []string{"orchestrator", "worker"} {
		if err := sh(env.repoRoot, nil, "go", "build", "-o", filepath.Join(env.bin, c), "./cmd/"+c); err != nil {
			return err
		}
	}
	wasm := []string{"GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0"}
	hello := filepath.Join(env.bin, "hello.wasm")
	if err := sh(env.repoRoot, wasm, "go", "build", "-trimpath", "-o", hello, "./examples/hello"); err != nil {
		return err
	}
	if env.hello, err = os.ReadFile(hello); err != nil {
		return err
	}
	env.guest = map[string][]byte{}
	for _, v := range []string{"v1", "v2", "v3"} {
		out := filepath.Join(env.bin, "guest-"+v+".wasm")
		if err := sh(".", wasm, "go", "build", "-trimpath", "-ldflags", "-X main.version="+v, "-o", out, "./testdata/guest"); err != nil {
			return err
		}
		if env.guest[v], err = os.ReadFile(out); err != nil {
			return err
		}
	}
	if err := sh(".", nil, "go", "build", "-o", filepath.Join(env.bin, "edgerun-gitops"), "."); err != nil {
		return err
	}

	// 2. A private PostgreSQL (the logic of scripts/devstack.sh).
	initdb, err := exec.LookPath("initdb")
	if err != nil {
		g, _ := filepath.Glob("/usr/lib/postgresql/*/bin/initdb")
		if len(g) == 0 {
			return fmt.Errorf("PostgreSQL server binaries (initdb, pg_ctl) not found")
		}
		initdb = g[len(g)-1]
	}
	env.pgBin = filepath.Dir(initdb)
	pgdir := filepath.Join(env.root, "pg")
	os.MkdirAll(pgdir, 0o755)
	env.pgSock = pgdir
	if os.Getuid() == 0 {
		u, err := user.Lookup("postgres")
		if err != nil {
			return fmt.Errorf("running as root needs a postgres OS user: %w", err)
		}
		uid, _ := strconv.Atoi(u.Uid)
		gid, _ := strconv.Atoi(u.Gid)
		os.Chown(pgdir, uid, gid)
		env.pgUser = "postgres"
	}
	data := filepath.Join(pgdir, "data")
	if err := pg("initdb", "-D", data, "-A", "trust", "-U", "postgres", "--no-sync", "-E", "UTF8"); err != nil {
		return err
	}
	return pg("pg_ctl", "-D", data, "-l", filepath.Join(pgdir, "postgres.log"),
		"-o", "-c listen_addresses='' -c unix_socket_directories="+pgdir+" -c fsync=off", "-w", "start")
}

func pg(tool string, args ...string) error {
	name, a := filepath.Join(env.pgBin, tool), args
	if env.pgUser != "" {
		name, a = "runuser", append([]string{"-u", env.pgUser, "--", name}, args...)
	}
	return sh(env.root, nil, name, a...)
}

func stopPG() {
	pg("pg_ctl", "-D", filepath.Join(env.pgSock, "data"), "-m", "fast", "stop")
}

// freePort returns a port of 21000-21099 nothing is listening on.
func freePort(t testing.TB) int {
	t.Helper()
	for i := 0; i < 100; i++ {
		p := 21000 + int(nextPort.Add(1)-21000)%100
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		if err == nil {
			l.Close()
			return p
		}
	}
	t.Fatal("no free port in 21000-21099")
	return 0
}

// Stack is one orchestrator (with its own empty database) and one worker.
type Stack struct {
	URL string
	Ctl *edgerun.Control
	Ing *edgerun.Ingress
	dir string
}

func startProc(t testing.TB, dir, name string, extra []string, bin string, args ...string) *exec.Cmd {
	t.Helper()
	log, err := os.Create(filepath.Join(dir, name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(filepath.Join(env.bin, bin), args...)
	cmd.Env = append(os.Environ(), extra...)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			cmd.Process.Kill()
			<-done
		}
		log.Close()
	})
	return cmd
}

func waitHTTP(t testing.TB, url string, proc *exec.Cmd, logfile string) {
	t.Helper()
	for i := 0; i < 150; i++ {
		if r, err := http.Get(url); err == nil {
			r.Body.Close()
			if r.StatusCode == 200 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	b, _ := os.ReadFile(logfile)
	t.Fatalf("%s did not come up:\n%s", url, b)
}

func newStack(t *testing.T) *Stack {
	t.Helper()
	if env.err != nil {
		if os.Getenv("GITOPS_REQUIRE_STACK") == "1" {
			t.Fatalf("stack unavailable: %v", env.err)
		}
		t.Skipf("stack unavailable (set GITOPS_REQUIRE_STACK=1 to make this fail): %v", env.err)
	}
	dir := filepath.Join(env.root, strings.NewReplacer("/", "_").Replace(t.Name()))
	os.MkdirAll(dir, 0o755)

	dbname := "db_" + strconv.Itoa(int(time.Now().UnixNano()%1e9))
	if err := pg("psql", "-h", env.pgSock, "-U", "postgres", "-d", "postgres", "-c", "CREATE DATABASE "+dbname); err != nil {
		t.Fatal(err)
	}
	dsn := "postgres://postgres@/" + dbname + "?host=" + env.pgSock

	op, wp := freePort(t), freePort(t)
	orchURL := fmt.Sprintf("http://127.0.0.1:%d", op)
	orch := startProc(t, dir, "orchestrator", []string{
		"EDGERUN_DATABASE_URL=" + dsn, fmt.Sprintf("EDGERUN_ORCHESTRATOR_LISTEN=127.0.0.1:%d", op),
		"EDGERUN_INGRESS_TOKEN=" + ingressToken, "EDGERUN_WORKER_TOKEN=" + workerToken, "EDGERUN_ADMIN_TOKEN=" + adminToken,
		"EDGERUN_EXECUTION_TOKEN=" + executionToken, "EDGERUN_SECRET_KEY=" + secretKey,
	}, "orchestrator", "--heartbeat-interval", "1s")
	waitHTTP(t, orchURL+"/healthz", orch, filepath.Join(dir, "orchestrator.log"))
	startProc(t, dir, "worker", []string{
		"EDGERUN_ORCHESTRATOR_URL=" + orchURL, "EDGERUN_WORKER_TOKEN=" + workerToken, "EDGERUN_EXECUTION_TOKEN=" + executionToken,
		fmt.Sprintf("EDGERUN_WORKER_LISTEN=127.0.0.1:%d", wp), fmt.Sprintf("EDGERUN_WORKER_ADVERTISE_URL=http://127.0.0.1:%d", wp),
	}, "worker", "--drain-timeout", "2s")

	s := &Stack{URL: orchURL, dir: dir, Ctl: edgerun.NewControl(orchURL, adminToken), Ing: edgerun.NewIngress(orchURL, ingressToken)}
	// Wait until the worker has registered and is alive.
	deadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err := s.Ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
		cancel()
		if err == nil && len(res.Workers) > 0 && res.Workers[0].Alive {
			break
		}
		if time.Now().After(deadline) {
			b, _ := os.ReadFile(filepath.Join(dir, "worker.log"))
			t.Fatalf("worker never became alive: %v\n%s", err, b)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return s
}

// Invoke calls a function the way a frontend would, through the library.
func (s *Stack) Invoke(t testing.TB, host, path string) (int, string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	res, err := s.Ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: host, Path: path}})
	if err != nil {
		return 0, "", err
	}
	body, err := res.ReadAll()
	return res.Status(), strings.TrimSpace(string(body)), err
}

func (s *Stack) mustInvoke(t testing.TB, host, path string) string {
	t.Helper()
	st, body, err := s.Invoke(t, host, path)
	if err != nil || st != 200 {
		t.Fatalf("invoke %s%s: status %d, err %v, body %q", host, path, st, err, body)
	}
	return body
}

func (s *Stack) functions(t testing.TB) map[string]*controlv1.Function {
	t.Helper()
	out := map[string]*controlv1.Function{}
	for f, err := range s.Ctl.AllFunctions(context.Background()) {
		if err != nil {
			t.Fatal(err)
		}
		out[f.Name] = f
	}
	return out
}

func (s *Stack) versions(t testing.TB, fnID string) []*controlv1.FunctionVersion {
	t.Helper()
	var out []*controlv1.FunctionVersion
	for v, err := range s.Ctl.AllVersions(context.Background(), fnID) {
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

func (s *Stack) routes(t testing.TB) map[string]*controlv1.Route {
	t.Helper()
	out := map[string]*controlv1.Route{}
	for r, err := range s.Ctl.AllRoutes(context.Background()) {
		if err != nil {
			t.Fatal(err)
		}
		out[r.HostPattern+r.PathPrefix] = r
	}
	return out
}

// Repo is a bare repository standing in for GitHub, and a work tree to push from.
type Repo struct {
	t         testing.TB
	bare, wd  string
	URL, Name string
}

func newRepo(t testing.TB, dir string) *Repo {
	t.Helper()
	r := &Repo{t: t, bare: filepath.Join(dir, "origin.git"), wd: filepath.Join(dir, "work")}
	r.URL = "file://" + r.bare
	r.git(dir, "init", "-q", "--bare", "-b", "main", r.bare)
	os.MkdirAll(r.wd, 0o755)
	r.git(r.wd, "init", "-q", "-b", "main")
	r.git(r.wd, "remote", "add", "origin", r.URL)
	return r
}

func (r *Repo) git(dir string, args ...string) string {
	r.t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		r.t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out.String())
	}
	return strings.TrimSpace(out.String())
}

// Commit writes the files (a nil value deletes one), commits and pushes to main.
func (r *Repo) Commit(msg string, files map[string][]byte) string {
	r.t.Helper()
	for p, b := range files {
		full := filepath.Join(r.wd, p)
		if b == nil {
			os.Remove(full)
			continue
		}
		os.MkdirAll(filepath.Dir(full), 0o755)
		if err := os.WriteFile(full, b, 0o644); err != nil {
			r.t.Fatal(err)
		}
	}
	r.git(r.wd, "add", "-A")
	r.git(r.wd, "commit", "-q", "-m", msg)
	r.git(r.wd, "push", "-q", "origin", "HEAD:main")
	return r.git(r.wd, "rev-parse", "HEAD")
}

// Revert reverts the newest commit and pushes the result.
func (r *Repo) Revert() {
	r.t.Helper()
	r.git(r.wd, "revert", "--no-edit", "HEAD")
	r.git(r.wd, "push", "-q", "origin", "HEAD:main")
}

// Harness drives the GitOps service in-process against a stack and a repo.
type Harness struct {
	t      testing.TB
	S      *Stack
	R      *Repo
	Env    map[string]string // the "process environment" secrets are read from
	Mutate func(*Config)
	G      *GitOps
	out    *bytes.Buffer
}

func (h *Harness) config() *Config {
	c := &Config{
		Repo: h.R.URL, Branch: "main", File: "edgerun.yaml", WorkDir: filepath.Join(h.S.dir, "clone"),
		Orchestrator: h.S.URL, AdminToken: adminToken, Owner: "test", Interval: time.Second, RPCTimeout: 20 * time.Second,
		Getenv: func(k string) (string, bool) { v, ok := h.Env[k]; return v, ok },
	}
	if h.Mutate != nil {
		h.Mutate(c)
	}
	return c
}

// Run does one pass and returns its result, its error and what it printed.
func (h *Harness) Run(dry bool) (*Result, error, string) {
	h.t.Helper()
	c := h.config()
	c.DryRun = dry
	h.out = &bytes.Buffer{}
	if h.G == nil || h.G.c.Owner != c.Owner {
		h.G = New(c, h.out)
	}
	h.G.out, h.G.c, h.G.rec.Prune, h.G.rec.Adopt, h.G.rec.AllowEmpty = h.out, c, c.Prune, c.Adopt, c.AllowEmpty
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := h.G.Reconcile(ctx)
	return res, err, h.out.String()
}

func newHarness(t *testing.T) *Harness {
	s := newStack(t)
	return &Harness{t: t, S: s, R: newRepo(t, s.dir), Env: map[string]string{"GITOPS_FINGERPRINT_KEY": "k-1"}}
}
