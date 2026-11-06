package e2e

// Guest containment, with guests of our own: what a hostile module can and
// cannot do to the host, to the network, to its limits, and to other functions.

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

func (f *secFn) callBudget(path, query, body string, budget time.Duration) result {
	f.s.t.Helper()
	return f.s.call(req{host: f.host, path: path, query: query, method: "POST", body: body, budget: budget})
}

func secWaitRuns(t *testing.T, f *secFn) {
	t.Helper()
	eventually(t, "the function to run", 120*time.Second, func() bool { return f.s.call(req{host: f.host, path: "/noop"}).err == nil })
}

// The guest has no files, no host environment, no sockets, and cannot run programs.
func TestSecurityGuestCannotReachTheHost(t *testing.T) {
	t.Setenv("EDGERUN_HOST_CANARY", "host-secret-canary-4711")
	t.Setenv("DATABASE_URL", "postgres://admin:hunter2canary@db/prod")
	s := newStack(t)
	s.secNetWorker(4)
	fn := s.secDeploy("probe", "probe.test", secGuest(t), "wagi-2", nil)
	s.setEnv(fn.id, "MY_OWN", "mine", false)
	secWaitRuns(t, fn)

	// Environment: the CGI variables, the function's own, and nothing of the worker's process.
	env := string(fn.do("/env", "", "").body)
	for _, kv := range os.Environ() {
		k, v, _ := strings.Cut(kv, "=")
		if len(v) >= 6 && strings.Contains(env, v) && !strings.Contains("GATEWAY_INTERFACE=CGI/1.1 SERVER_PROTOCOL=HTTP/1.1", v) {
			t.Errorf("the guest sees the host's %s=%.12q...", k, v)
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(env), "\n") {
		k, _, _ := strings.Cut(line, "=")
		switch {
		case strings.HasPrefix(k, "HTTP_"), k == "MY_OWN":
		case map[string]bool{"GATEWAY_INTERFACE": true, "SERVER_PROTOCOL": true, "REQUEST_METHOD": true, "SERVER_NAME": true, "PATH_INFO": true,
			"QUERY_STRING": true, "REMOTE_ADDR": true, "CONTENT_LENGTH": true, "CONTENT_TYPE": true}[k]:
		default:
			t.Errorf("the guest has an environment variable nobody gave it: %q", line)
		}
	}

	// Files: nothing is mounted.
	files := string(fn.do("/files", "", "").body)
	for _, line := range strings.Split(files, "\n") {
		switch {
		case strings.HasPrefix(line, "read "), strings.HasPrefix(line, "readdir "):
			if !strings.Contains(line, "n=0 ") {
				t.Errorf("the guest read something from the host: %s", line)
			}
		case strings.HasPrefix(line, "create "), strings.HasPrefix(line, "write "):
			if strings.Contains(line, "ok=true") || strings.Contains(line, "err=<nil>") {
				t.Errorf("the guest wrote to the host: %s", line)
			}
		}
	}
	if strings.Contains(files, "root:x:") {
		t.Errorf("the guest read /etc/passwd: %s", files)
	}

	// Programs and sockets.
	if out := string(fn.do("/exec", "", "").body); !strings.Contains(out, "err=") || strings.Contains(out, "err=<nil>") || strings.Contains(out, "uid=") {
		t.Errorf("the guest ran a program: %s", out)
	}
	for name, addr := range map[string]string{"orchestrator": s.addr, "worker": s.workerAddr(t), "egress": s.secRealEgress(t, []string{"*"}), "an unused port": freePort(t)} {
		out := string(fn.do("/dial", "addr="+addr, "").body)
		if strings.Contains(out, "conn=true") {
			t.Errorf("the guest opened a socket to the %s (%s): %s", name, addr, out)
		}
		// (Go's wasip1 net package is an in-process fake: a "listener" opens no host port, and a dial
		// to a real address is refused, which is what is judged above.)
	}
}

// Through wagi-2, with the function allowed to name them, the guest still cannot
// reach what is not the network's to give: loopback, the orchestrator, workers,
// network services, the frontend, the metadata address, private ranges. (The
// database listens on a unix socket here, so there is no port to ask for.)
func TestSecurityGuestNetworkCannotReachTheDeployment(t *testing.T) {
	s := newStack(t)
	s.secNetWorker(8)
	egAddr := s.secRealEgress(t, []string{"*"}) // the default service: refuses loopback, private, link-local
	web := s.startFrontend(10 * time.Second)
	workerAddr := s.workerAddr(t)
	fn := s.secDeploy("netprobe", "netprobe.test", secNetModule(t), "wagi-2", nil)
	eventually(t, "the function to run", 120*time.Second, func() bool { return s.call(req{host: "netprobe.test", path: "/noop"}).err == nil })

	targets := map[string]string{
		"orchestrator": s.addr, "worker": workerAddr, "network service": egAddr, "frontend": strings.TrimPrefix(web.base, "http://"),
		"the metadata address": "169.254.169.254:80", "a private address": "10.0.0.1:80", "an unspecified address": "0.0.0.0:80",
	}
	var names []string
	for n := range targets {
		names = append(names, n)
	}
	sort.Strings(names)
	var allow []string
	for _, n := range names {
		allow = append(allow, targets[n])
	}
	for _, listed := range []bool{false, true} {
		if listed {
			fn.allow(allow...)
		} else {
			fn.allow()
		}
		for _, n := range names {
			for _, path := range []string{"/healthz", "/edgerun.control.v1.InspectionService/ListWorkers"} {
				method := "GET"
				if strings.Contains(path, "edgerun") {
					method = "POST"
				}
				code, resp := fn.fetch("/fetch", fmt.Sprintf("%s http://%s%s\nAuthorization: Bearer %s\nContent-Type: application/json\n\n{}", method, targets[n], path, adminToken))
				if code >= 0 {
					t.Errorf("(listed=%v) the guest got a %d-byte response from %s%s: %.200q", listed, code, n, path, resp)
				}
				if listed && code != -1 && code != -4 {
					t.Errorf("(listed) %s%s: code %d, want -1 (denied by the service)", n, path, code)
				}
			}
		}
	}
}

// Two functions run at the same time: each sees its own request and its own
// environment and nothing of the other's, whatever the schedule.
func TestSecurityGuestInvocationsAreIsolated(t *testing.T) {
	s := newStack(t)
	s.secNetWorker(16)
	mod := secGuest(t)
	a := s.secDeploy("tenant-a", "a.test", mod, "wagi-2", nil)
	b := s.secDeploy("tenant-b", "b.test", mod, "wagi-2", nil)
	s.setEnv(a.id, "SECRET_A", "alpha-secret-0001", true)
	s.setEnv(a.id, "ONLY_IN_A", "a-plain", false)
	s.setEnv(b.id, "SECRET_B", "bravo-secret-0002", true)
	s.setEnv(b.id, "ONLY_IN_B", "b-plain", false)
	secWaitRuns(t, a)
	secWaitRuns(t, b)

	type run struct {
		fn    *secFn
		own   string
		other []string
		body  string
	}
	var runs []run
	for i := 0; i < 24; i++ {
		runs = append(runs,
			run{a, "alpha-secret-0001", []string{"bravo-secret-0002", "SECRET_B", "ONLY_IN_B", "b-plain", "b-body-"}, fmt.Sprintf("a-body-%d", i)},
			run{b, "bravo-secret-0002", []string{"alpha-secret-0001", "SECRET_A", "ONLY_IN_A", "a-plain", "a-body-"}, fmt.Sprintf("b-body-%d", i)})
	}
	var mu sync.Mutex
	secParallel(runs, 16, func(r run) {
		res := r.fn.do("/hold", "ms=300", r.body)
		mu.Lock()
		defer mu.Unlock()
		if res.err != nil {
			t.Errorf("%s: %v", r.body, res.err)
			return
		}
		out := string(res.body)
		if !strings.Contains(out, r.own) || !strings.Contains(out, "body="+r.body+"\n") {
			t.Errorf("%s did not see its own secret and body:\n%.300s", r.body, out)
		}
		for _, x := range r.other {
			if strings.Contains(out, x) {
				t.Errorf("ISOLATION: %s saw %q, which belongs to the other function", r.body, x)
			}
		}
		if strings.Count(out, "body=") != 1 {
			t.Errorf("%s: more than one body in the output", r.body)
		}
	})
}

// Memory a guest wrote is not there for the next guest to read.
func TestSecurityGuestMemoryIsNotReused(t *testing.T) {
	s := newStack(t)
	s.secNetWorker(1) // one slot: every invocation runs on the same worker, one after another
	mod := secGuest(t)
	a := s.secDeploy("mem-a", "mema.test", mod, "wagi-2", nil)
	b := s.secDeploy("mem-b", "memb.test", mod, "wagi-2", nil)
	secWaitRuns(t, a)
	pat := "TENANT-A-SECRET-PATTERN-0123456789"
	for i := 0; i < 2; i++ {
		if r := a.do("/plant", "mb=24&p="+pat, ""); r.err != nil || !strings.Contains(string(r.body), "planted") {
			t.Fatalf("plant: %v %q", r.err, r.body)
		}
		r := b.do("/scan", "mb=32&p="+pat, "")
		if r.err != nil {
			t.Fatal(r.err)
		}
		if !strings.Contains(string(r.body), "nonzero=0 pattern=0") {
			t.Errorf("round %d: another function's memory is visible: %s", i, r.body)
		}
		// The same function again, too: a fresh instance per invocation.
		r = a.do("/scan", "mb=32&p="+pat, "")
		if !strings.Contains(string(r.body), "nonzero=0 pattern=0") {
			t.Errorf("round %d: the function's own earlier invocation is visible: %s", i, r.body)
		}
	}
}

// Limits hold: memory, time, output, stderr. Each failure is typed, fast, and
// leaves the worker's slot free and the process's heap bounded.
func TestSecurityGuestLimits(t *testing.T) {
	s := newStack(t)
	s.secNetWorker(1) // one slot: a leaked slot makes the next call fail
	lim := func(mem, out uint64) func(*controlv1.CreateVersionRequest) {
		return func(r *controlv1.CreateVersionRequest) {
			r.Limits = &commonv1.Limits{MemoryLimitBytes: mem, MaxResponseBytes: out}
		}
	}
	small := s.secDeploy("lim-small", "small.test", s.wasm, "wagi-1", lim(16<<20, 1<<20))
	huge := s.secDeploy("lim-huge", "huge.test", s.wasm, "wagi-1", lim(1<<40, 1<<40)) // beyond anything the worker allows
	mine := s.secDeploy("lim-mine", "mine.test", secGuest(t), "wagi-2", lim(0, 0))
	secWaitRuns(t, small)
	free := func(f *secFn, what string) {
		t.Helper()
		eventually(t, "the slot to be free after "+what, 15*time.Second, func() bool {
			return f.s.call(req{host: f.host, path: "/echo"}).err == nil || f.s.call(req{host: f.host, path: "/noop"}).err == nil
		})
	}
	typed := func(what string, r result, want commonv1.ErrorReason, within time.Duration) {
		t.Helper()
		if reason(r.err) != want {
			t.Errorf("%s: %v, want %v", what, r.err, want)
		}
		if r.elapsed > within {
			t.Errorf("%s took %v, want within %v", what, r.elapsed, within)
		}
	}

	h := secMeasure()
	typed("allocating 1 GiB with a 16 MiB limit", small.callBudget("/memhog", "mb=1024", "", 20*time.Second), commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, 15*time.Second)
	free(small, "the memory limit")
	typed("allocating 3 GiB under the largest limit the worker has", huge.callBudget("/memhog", "mb=3072", "", 30*time.Second), commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, 25*time.Second)
	free(huge, "the largest memory limit")
	grew, _ := h.Stop()
	t.Logf("memory attacks: heap peak +%d MiB", grew/secMiB)
	if grew > 1500*secMiB {
		t.Errorf("a guest allowed 512 MiB grew the process's heap by %d MiB", grew/secMiB)
	}

	for _, path := range []string{"/spin", "/spinsys", "/sleepforever", "/sleep"} {
		f := small
		q := "ms=3600000"
		if path == "/spinsys" || path == "/sleepforever" {
			f = mine
			q = ""
		}
		typed("running forever: "+path, f.callBudget(path, q, "", 1500*time.Millisecond), commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, 8*time.Second)
		free(f, path)
	}

	h = secMeasure()
	typed("writing 2 GiB to stdout against a 1 MiB limit", small.callBudget("/big", "bytes=2147483648", "", 20*time.Second), commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE, 15*time.Second)
	free(small, "the output limit")
	typed("writing 2 GiB to stdout under the largest limit", huge.callBudget("/big", "bytes=2147483648", "", 30*time.Second), commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE, 25*time.Second)
	free(huge, "the largest output limit")
	grew, _ = h.Stop()
	if grew > 600*secMiB {
		t.Errorf("a guest writing without end grew the heap by %d MiB (the response limit is at most 32 MiB)", grew/secMiB)
	}

	// stderr: unbounded writes, bounded record.
	h = secMeasure()
	r := mine.callBudget("/stderrflood", "mb=4096", "", 5*time.Second)
	grew, _ = h.Stop()
	if r.err != nil && reason(r.err) != commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED {
		t.Errorf("a stderr flood: %v", r.err)
	}
	if r.elapsed > 12*time.Second {
		t.Errorf("a stderr flood held the invocation for %v against a 5s budget", r.elapsed)
	}
	if grew > 300*secMiB {
		t.Errorf("a stderr flood grew the heap by %d MiB", grew/secMiB)
	}
	free(mine, "the stderr flood")
	ok := mine.callBudget("/stderrflood", "mb=2", "", 20*time.Second)
	if ok.err != nil {
		t.Errorf("a 2 MiB stderr: %v", ok.err)
	}
	eventually(t, "the records", 20*time.Second, func() bool { return len(s.invocations(mine.id)) >= 2 })
	for _, inv := range s.invocations(mine.id) {
		if len(inv.Stderr) > 64<<10 {
			t.Errorf("an invocation record keeps %d bytes of stderr: the protocol bounds it at 64 KiB", len(inv.Stderr))
		}
	}
	// And the error message carried to the frontend is bounded too (docs: at most 4 KiB of stderr).
	if r := mine.callBudget("/stderrflood", "mb=1", "", 20*time.Second); r.err != nil {
		t.Logf("stderr of a failing call: %d bytes in the error", len(r.err.Error()))
	}
	_ = strconv.Itoa
}
