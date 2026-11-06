package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Olian04/edgerun/internal/orchestrator/orchtest"
	"github.com/Olian04/edgerun/internal/testpg"
)

// These tests run the executable the way an operator would: every component is
// a separate process built from cmd/, and the suite is pointed at it by address.
// They prove that what the in-repo tests check in-process also holds for an
// implementation the suite did not start.

var bin struct {
	once sync.Once
	dir  string
	err  error
}

func TestMain(m *testing.M) { testpg.Main(m) }

// build compiles the binaries once.
func build(t *testing.T) string {
	t.Helper()
	bin.once.Do(func() {
		// A fixed place in the user's cache, so that a later run only relinks.
		root, err := os.UserCacheDir()
		if err != nil {
			root = os.TempDir()
		}
		bin.dir = filepath.Join(root, "edgerun-conformance-bin")
		if bin.err = os.MkdirAll(bin.dir, 0o755); bin.err != nil {
			return
		}
		args := []string{"build", "-o", bin.dir + "/"}
		for _, c := range []string{"orchestrator", "worker", "frontend", "egress", "edgerun-conformance"} {
			args = append(args, "github.com/Olian04/edgerun/cmd/"+c)
		}
		out, err := exec.Command("go", args...).CombinedOutput()
		if err != nil {
			bin.err = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if bin.err != nil {
		t.Fatal(bin.err)
	}
	return bin.dir
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

// proc is a running process, killed when the test ends.
type proc struct {
	cmd  *exec.Cmd
	out  *bytes.Buffer
	done chan struct{} // closed when it has exited
	err  error         // how, once done is closed
}

func start(t *testing.T, name string, args ...string) *proc {
	t.Helper()
	p := &proc{cmd: exec.Command(filepath.Join(build(t), name), args...), out: &bytes.Buffer{}, done: make(chan struct{})}
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
			p.cmd.Process.Kill()
			<-p.done
		}
	})
	return p
}

// waitPort waits until something accepts connections at addr.
func waitPort(t *testing.T, addr string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("nothing is listening at %s after %v", addr, within)
}

// finish waits for the suite to exit and requires success.
func (p *proc) finish(t *testing.T, within time.Duration) {
	t.Helper()
	select {
	case <-p.done:
		if p.err != nil {
			t.Fatalf("the suite failed: %v\n%s", p.err, tail(p.out.String(), 6000))
		}
		if !strings.Contains(p.out.String(), "PASS") {
			t.Fatalf("the suite did not report PASS:\n%s", tail(p.out.String(), 3000))
		}
	case <-time.After(within):
		t.Fatalf("the suite did not finish within %v:\n%s", within, tail(p.out.String(), 6000))
	}
}

func tail(s string, n int) string {
	if len(s) > n {
		return "..." + s[len(s)-n:]
	}
	return s
}

const (
	ingressToken   = "conformance-ingress-token"
	workerToken    = "conformance-worker-token"
	adminToken     = "conformance-admin-token"
	networkToken   = "conformance-network-token"
	executionToken = "conformance-execution-token"
	egressToken    = "conformance-egress-token"
)

func TestTheOrchestratorSuiteJudgesAProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs several processes")
	}
	port := freePort(t)
	start(t, "orchestrator", "--listen", "127.0.0.1:"+port, "--database-url", testpg.DSN(t),
		"--ingress-token", ingressToken, "--worker-token", workerToken, "--admin-token", adminToken,
		"--network-token", networkToken, "--execution-token", executionToken,
		"--secret-key", base64.StdEncoding.EncodeToString(orchtest.Key), "--heartbeat-interval", "200ms")
	waitPort(t, "127.0.0.1:"+port, 60*time.Second)
	suite := start(t, "edgerun-conformance", "--role", "orchestrator", "--target", "127.0.0.1:"+port)
	suite.finish(t, 90*time.Second)
}

func TestTheFrontendSuiteJudgesAProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs several processes")
	}
	suitePort, fePort := freePort(t), freePort(t)
	suite := start(t, "edgerun-conformance", "--role", "frontend", "--target", "127.0.0.1:"+fePort, "--listen", "127.0.0.1:"+suitePort)
	waitPort(t, "127.0.0.1:"+suitePort, 60*time.Second)
	start(t, "frontend", "--listen", "127.0.0.1:"+fePort, "--health-listen", "", "--orchestrator-url", "http://127.0.0.1:"+suitePort,
		"--ingress-token", ingressToken, "--request-timeout", "3s")
	suite.finish(t, 5*time.Minute)
}

func TestTheWorkerSuiteJudgesAProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs several processes")
	}
	suitePort, workerPort := freePort(t), freePort(t)
	pidfile := filepath.Join(t.TempDir(), "worker.pid")
	suite := start(t, "edgerun-conformance", "--role", "worker", "--target", "127.0.0.1:"+workerPort,
		"--listen", "127.0.0.1:"+suitePort, "--stop-cmd", "kill -TERM $(cat "+pidfile+")")
	waitPort(t, "127.0.0.1:"+suitePort, 60*time.Second)
	w := start(t, "worker", "--listen", "127.0.0.1:"+workerPort, "--advertise-url", "http://127.0.0.1:"+workerPort,
		"--orchestrator-url", "http://127.0.0.1:"+suitePort, "--worker-token", workerToken,
		"--execution-token", executionToken, "--egress-token", egressToken, "--max-concurrency", "2")
	os.WriteFile(pidfile, []byte(strconv.Itoa(w.cmd.Process.Pid)), 0o644)
	suite.finish(t, 10*time.Minute)
}

func TestTheEgressSuiteJudgesTwoProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs several processes")
	}
	suitePort, openPort, defaultPort := freePort(t), freePort(t), freePort(t)
	dir := t.TempDir()
	openPid, defaultPid := filepath.Join(dir, "open.pid"), filepath.Join(dir, "default.pid")
	suite := start(t, "edgerun-conformance", "--role", "egress",
		"--target", "127.0.0.1:"+openPort, "--target-default", "127.0.0.1:"+defaultPort, "--listen", "127.0.0.1:"+suitePort,
		"--stop-cmd", "kill -TERM $(cat "+openPid+")", "--stop-cmd-default", "kill -TERM $(cat "+defaultPid+")")
	waitPort(t, "127.0.0.1:"+suitePort, 60*time.Second)
	common := []string{"--orchestrator-url", "http://127.0.0.1:" + suitePort, "--network-token", networkToken, "--egress-token", egressToken}
	open := start(t, "egress", append([]string{"--listen", "127.0.0.1:" + openPort, "--covers", "127.0.0.1,*.covered.test", "--allow-network", "127.0.0.1/32"}, common...)...)
	def := start(t, "egress", append([]string{"--listen", "127.0.0.1:" + defaultPort, "--covers", "*"}, common...)...)
	os.WriteFile(openPid, []byte(strconv.Itoa(open.cmd.Process.Pid)), 0o644)
	os.WriteFile(defaultPid, []byte(strconv.Itoa(def.cmd.Process.Pid)), 0o644)
	suite.finish(t, 10*time.Minute)
}
