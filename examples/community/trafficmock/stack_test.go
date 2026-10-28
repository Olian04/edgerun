package main

import (
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
)

// This file builds a real edgerun stack out of real processes: a private
// PostgreSQL, an orchestrator and workers, compiled from a *snapshot* of the
// edgerun repository (git worktree of HEAD: the working tree of a repository
// that other people are editing is not a stable thing to build), and the test
// guest of this module. Nothing of edgerun's internals is imported.
//
// Set TRAFFICMOCK_BIN to a directory holding prebuilt orchestrator, worker and
// edgerun-conformance binaries to skip the snapshot build.
// Ports are taken from 24000-24099; every file lives under one temp directory.

const (
	ingressToken   = "t-ingress"
	workerToken    = "t-worker"
	adminToken     = "t-admin"
	executionToken = "t-exec"
	secretKey      = "ZGV2LW9ubHktc2VjcmV0LWtleS0zMi1ieXRlcyEhISE="
)

var env struct {
	root      string
	bin       string // orchestrator, worker, edgerun-conformance
	tmBin     string // edgerun-trafficmock
	pgBin     string
	pgSock    string
	pgUser    string
	guest     []byte // testdata/guest
	hello     []byte // examples/hello
	err       error
	repoRoot  string
	conformOK bool
}

var nextPort atomic.Int32

func TestMain(m *testing.M) {
	code := func() int {
		root, err := os.MkdirTemp("", "trafficmock-")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		os.Chmod(root, 0o755)
		env.root = root
		defer os.RemoveAll(root)
		nextPort.Store(int32(os.Getpid() % 90))
		env.err = setupEnv()
		if env.err == nil {
			defer stopPG()
		} else {
			fmt.Fprintln(os.Stderr, "stack unavailable:", env.err)
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
	here, _ := os.Getwd()
	env.bin = filepath.Join(env.root, "bin")
	os.MkdirAll(env.bin, 0o755)

	// 1. edgerun's binaries, from a snapshot of HEAD.
	wasm := []string{"GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0"}
	helloOut := filepath.Join(env.bin, "hello.wasm")
	if pre := os.Getenv("TRAFFICMOCK_BIN"); pre != "" {
		env.bin = pre
		helloOut = filepath.Join(env.root, "hello.wasm")
		if err := sh(env.repoRoot, wasm, "go", "build", "-trimpath", "-o", helloOut, "./examples/hello"); err != nil {
			return err
		}
	} else {
		snap := filepath.Join(env.root, "snap")
		if err := sh(env.repoRoot, nil, "git", "worktree", "add", "--detach", snap, "HEAD"); err != nil {
			return err
		}
		defer sh(env.repoRoot, nil, "git", "worktree", "remove", "--force", snap)
		if err := sh(snap, nil, "go", "build", "-o", env.bin+"/", "./cmd/orchestrator", "./cmd/worker", "./cmd/edgerun-conformance"); err != nil {
			return err
		}
		if err := sh(snap, wasm, "go", "build", "-trimpath", "-o", helloOut, "./examples/hello"); err != nil {
			return err
		}
	}
	if env.hello, err = os.ReadFile(helloOut); err != nil {
		return err
	}
	_, cerr := os.Stat(filepath.Join(env.bin, "edgerun-conformance"))
	env.conformOK = cerr == nil

	// 2. This module: the guest and the tool.
	guestOut := filepath.Join(env.root, "guest.wasm")
	if err := sh(here, wasm, "go", "build", "-trimpath", "-o", guestOut, "./testdata/guest"); err != nil {
		return err
	}
	if env.guest, err = os.ReadFile(guestOut); err != nil {
		return err
	}
	env.tmBin = filepath.Join(env.root, "edgerun-trafficmock")
	if err := sh(here, nil, "go", "build", "-o", env.tmBin, "."); err != nil {
		return err
	}

	// 3. A private PostgreSQL (the logic of scripts/devstack.sh).
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
		"-o", "-c listen_addresses='' -c unix_socket_directories="+pgdir+" -c fsync=off -c max_connections=300", "-w", "start")
}

func pg(tool string, args ...string) error {
	name, a := filepath.Join(env.pgBin, tool), args
	if env.pgUser != "" {
		name, a = "runuser", append([]string{"-u", env.pgUser, "--", name}, args...)
	}
	return sh(env.root, nil, name, a...)
}

func stopPG() { pg("pg_ctl", "-D", filepath.Join(env.pgSock, "data"), "-m", "fast", "stop") }

// freePort returns a port of 24000-24099 nothing is listening on.
func freePort(t testing.TB) int {
	t.Helper()
	for i := 0; i < 100; i++ {
		p := 24000 + int(nextPort.Add(1))%100
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		if err == nil {
			l.Close()
			return p
		}
	}
	t.Fatal("no free port in 24000-24099")
	return 0
}

func requireStack(t testing.TB) {
	t.Helper()
	if env.err != nil {
		if os.Getenv("TRAFFICMOCK_REQUIRE_STACK") == "1" {
			t.Fatalf("stack unavailable: %v", env.err)
		}
		t.Skipf("stack unavailable (set TRAFFICMOCK_REQUIRE_STACK=1 to make this fail): %v", env.err)
	}
}

func startProc(t testing.TB, dir, name string, extra []string, bin string, args ...string) *exec.Cmd {
	t.Helper()
	log, err := os.Create(filepath.Join(dir, name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, args...)
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

func waitHTTP(t testing.TB, url, logfile string) {
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

// Stack is one orchestrator (with its own empty database) and some workers.
type Stack struct {
	URL      string
	Ctl      *edgerun.Control
	Ing      *edgerun.Ingress
	dir      string
	FnID     string
	WorkerID []string
	OrchPID  int
	WorkPIDs []int
}

type StackOpts struct {
	Workers     int // default 1
	Concurrency int // per worker, default 8
	Guest       []byte
}

func newStack(t testing.TB, o StackOpts) *Stack {
	t.Helper()
	requireStack(t)
	if o.Workers == 0 {
		o.Workers = 1
	}
	if o.Concurrency == 0 {
		o.Concurrency = 8
	}
	dir := filepath.Join(env.root, strings.NewReplacer("/", "_").Replace(t.Name()))
	os.MkdirAll(dir, 0o755)

	dbname := "db_" + strconv.Itoa(int(time.Now().UnixNano()%1e9))
	if err := pg("psql", "-h", env.pgSock, "-U", "postgres", "-d", "postgres", "-c", "CREATE DATABASE "+dbname); err != nil {
		t.Fatal(err)
	}
	dsn := "postgres://postgres@/" + dbname + "?host=" + env.pgSock

	op := freePort(t)
	orchURL := fmt.Sprintf("http://127.0.0.1:%d", op)
	orch := startProc(t, dir, "orchestrator", []string{
		"EDGERUN_DATABASE_URL=" + dsn, fmt.Sprintf("EDGERUN_ORCHESTRATOR_LISTEN=127.0.0.1:%d", op),
		"EDGERUN_INGRESS_TOKEN=" + ingressToken, "EDGERUN_WORKER_TOKEN=" + workerToken, "EDGERUN_ADMIN_TOKEN=" + adminToken,
		"EDGERUN_EXECUTION_TOKEN=" + executionToken, "EDGERUN_SECRET_KEY=" + secretKey,
	}, filepath.Join(env.bin, "orchestrator"), "--heartbeat-interval", "1s")
	waitHTTP(t, orchURL+"/healthz", filepath.Join(dir, "orchestrator.log"))
	var workPIDs []int
	for i := 0; i < o.Workers; i++ {
		wp := freePort(t)
		w := startProc(t, dir, fmt.Sprintf("worker%d", i), []string{
			"EDGERUN_ORCHESTRATOR_URL=" + orchURL, "EDGERUN_WORKER_TOKEN=" + workerToken, "EDGERUN_EXECUTION_TOKEN=" + executionToken,
			fmt.Sprintf("EDGERUN_WORKER_LISTEN=127.0.0.1:%d", wp), fmt.Sprintf("EDGERUN_WORKER_ADVERTISE_URL=http://127.0.0.1:%d", wp),
		}, filepath.Join(env.bin, "worker"), "--drain-timeout", "2s", "--max-concurrency", strconv.Itoa(o.Concurrency))
		workPIDs = append(workPIDs, w.Process.Pid)
	}

	s := &Stack{URL: orchURL, dir: dir, Ctl: edgerun.NewControl(orchURL, adminToken), Ing: edgerun.NewIngress(orchURL, ingressToken), OrchPID: orch.Process.Pid, WorkPIDs: workPIDs}
	deadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err := s.Ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
		cancel()
		alive := 0
		if err == nil {
			for _, w := range res.Workers {
				if w.Alive {
					alive++
				}
			}
		}
		if alive >= o.Workers {
			for _, w := range res.Workers {
				s.WorkerID = append(s.WorkerID, w.Id)
			}
			break
		}
		if time.Now().After(deadline) {
			b, _ := os.ReadFile(filepath.Join(dir, "worker0.log"))
			t.Fatalf("workers never became alive: %v\n%s", err, b)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return s
}

// Deploy creates a function from a module and routes host / to it.
func (s *Stack) Deploy(t testing.TB, name, host string, module []byte) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fn, err := s.Ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fn.Function.Id, Module: module, Abi: "wagi-1", Activate: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: "/", FunctionId: fn.Function.Id}); err != nil {
		t.Fatal(err)
	}
	return fn.Function.Id
}

var _ = commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED

// cpuTicks is utime+stime of a process, in clock ticks (100 per second).
func cpuTicks(pid int) int64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	s := string(b)
	f := strings.Fields(s[strings.LastIndexByte(s, ')')+2:])
	if len(f) < 13 {
		return 0
	}
	u, _ := strconv.ParseInt(f[11], 10, 64)
	k, _ := strconv.ParseInt(f[12], 10, 64)
	return u + k
}
