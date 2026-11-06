package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

// Three ingress instances at the same time against one orchestrator, and a
// check that every response was delivered to the instance that made the
// request. The instances are:
//
//	fe   edgerun's own frontend (./cmd/frontend, built from the snapshot): real
//	     HTTP in, ingress.v1 out. Driven by a Go HTTP client in this test.
//	mka  edgerun-trafficmock, a separate process, closed-loop scenario
//	mkb  edgerun-trafficmock, a separate process, open-loop scenario with spikes
//
// All three send to ONE function on ONE host (shared.test), so their requests
// are interleaved on the same function and the same workers. The function
// echoes the request body and the request id it was given.
//
// What proves delivery to the right instance:
//  1. Every request body is stamped with the id of the request (and the run id
//     of the instance) and padded to a size that varies from a few bytes to
//     about 1 MiB. The response must be the echo of that body, byte for byte.
//     A response meant for another request cannot be that (the stamps differ),
//     nor can a truncated or spliced one.
//  2. The id the guest saw (HTTP_X_REQUEST_ID, set by the worker from the
//     request header) is echoed in the body prefix and in a header, and the
//     frontend must also return its own X-Request-Id.
//  3. The orchestrator's own invocation records (control.v1) are the ground
//     truth: each record's request_id has the prefix of exactly one instance,
//     the number of records per instance is the number of requests that
//     instance sent, and for every request the invocation id the instance was
//     given is the id of the record with that request id (and with the path,
//     method, status and error reason the instance observed).

const (
	multiHost = "shared.test"
	multiSlot = 6 // slots per worker
)

// ---------------------------------------------------------------- a counting proxy

// execProxy sits between the orchestrator and a worker and counts the Execute
// attempts the orchestrator makes on that worker. The orchestrator retries
// NO_CAPACITY (and an unreachable worker) on the next worker, which shows
// nowhere else: attempts beyond one per invocation are its retries.
//
// It is a plain TCP forwarder: it understands no HTTP, it only recognises the
// request line of an Execute call (the orchestrator talks HTTP/1.1 to workers)
// in the stream from the orchestrator. A first version used httputil.ReverseProxy;
// it tore a response now and then (see FINDINGS.md), which a byte forwarder cannot.
type execProxy struct {
	URL      string
	Attempts atomic.Int64
	mu       sync.Mutex
	Errors   []string // what the proxy itself saw go wrong with a connection
}

var executeLine = []byte("ExecutionService/Execute HTTP/1.1")

func (p *execProxy) note(format string, a ...any) {
	p.mu.Lock()
	p.Errors = append(p.Errors, fmt.Sprintf(format, a...))
	p.mu.Unlock()
}

// countingCopy copies src to dst and counts the Execute request lines in it.
func (p *execProxy) countingCopy(dst io.Writer, src io.Reader) {
	buf := make([]byte, 64<<10)
	var carry []byte
	for {
		n, err := src.Read(buf)
		if n > 0 {
			data := append(carry, buf[:n]...)
			p.Attempts.Add(int64(bytes.Count(data, executeLine)))
			carry = append([]byte(nil), data[max(0, len(data)-len(executeLine)+1):]...)
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func startExecProxy(t testing.TB, port int, backend string) *execProxy {
	t.Helper()
	u, _ := url.Parse(backend)
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	px := &execProxy{URL: fmt.Sprintf("http://127.0.0.1:%d", port)}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				b, err := net.DialTimeout("tcp", u.Host, 5*time.Second)
				if err != nil {
					px.note("dial %s: %v", u.Host, err)
					c.Close()
					return
				}
				done := make(chan struct{})
				go func() { // worker -> orchestrator
					io.Copy(c, b)
					c.(*net.TCPConn).CloseWrite()
					close(done)
				}()
				px.countingCopy(b, c)
				b.(*net.TCPConn).CloseWrite()
				<-done
				c.Close()
				b.Close()
			}()
		}
	}()
	return px
}

// ---------------------------------------------------------------- what an instance saw

// seen is what one instance knows about one of its requests.
type seen struct {
	ReqID, InvID, Path string
	Outcome            string // ok, refused (NO_CAPACITY), failed (any other error)
	Status             int    // the function's status for ok
	Reason             string // for refused and failed
	Mismatch           string // not empty: the response was NOT the one for this request
}

type instance struct {
	Name, Prefix string
	Start, End   time.Time // when its load ran
	Peak         int64     // most requests in flight at once
	Released     int       // requests it sent
	Results      map[string]*seen
	Duplicates   []string
	Report       *Report // the mock instances
	Procs        string  // where its output is
}

func (in *instance) count(outcome string) (n int) {
	for _, s := range in.Results {
		if s.Outcome == outcome {
			n++
		}
	}
	return
}

func (in *instance) mismatches() (out []*seen) {
	for _, s := range in.Results {
		if s.Mismatch != "" {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ReqID < out[j].ReqID })
	return
}

// ---------------------------------------------------------------- the frontend's client

// feClient is the HTTP load client for edgerun's frontend: closed-loop
// goroutines, unique stamped bodies of mixed sizes, every response verified.
type feClient struct {
	base, run, prefix string
	hc                *http.Client
	jitterOnly        bool
	pool              []byte

	seq              atomic.Uint64
	inflight, peak   atomic.Int64
	mu               sync.Mutex
	results          []*seen
	failureDetails   []string // transport-level failures, a bounded sample
	transportFailure atomic.Int64
}

func newFEClient(base, run string, jitterOnly bool) *feClient {
	c := &feClient{base: base, run: run, prefix: "fe-" + run + "-", jitterOnly: jitterOnly,
		hc: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256, IdleConnTimeout: 30 * time.Second}, Timeout: 60 * time.Second}}
	c.pool = make([]byte, 1<<20+512)
	r := rand.New(rand.NewPCG(99, 1))
	for i := range c.pool {
		c.pool[i] = byte(r.Uint32())
	}
	return c
}

// size draws a padding size: mostly small, a few up to about 1 MiB.
func feSize(r *rand.Rand) int {
	switch x := r.Float64(); {
	case x < 0.35:
		return r.IntN(17) // a few bytes
	case x < 0.65:
		return 1<<10 + r.IntN(7<<10)
	case x < 0.85:
		return 16<<10 + r.IntN(48<<10)
	case x < 0.95:
		return 128<<10 + r.IntN(128<<10)
	default:
		return 512<<10 + r.IntN(512<<10)
	}
}

func (c *feClient) run1(ctx context.Context, r *rand.Rand) {
	n := c.seq.Add(1)
	rid := c.prefix + strconv.FormatUint(n, 10)
	path := "/echo-with-id"
	if c.jitterOnly || r.Float64() < 0.3 {
		path = "/jitter/200" // sleeps a random time up to 200 ms: responses return out of order
	}
	pad := feSize(r)
	off := r.IntN(len(c.pool) - pad)
	body := append([]byte("fe1 "+rid+" "+c.run+"\n"), c.pool[off:off+pad]...)

	res := &seen{ReqID: rid, Path: path}
	defer func() {
		c.mu.Lock()
		c.results = append(c.results, res)
		c.mu.Unlock()
	}()
	for n, p := c.inflight.Add(1), c.peak.Load(); n > p && !c.peak.CompareAndSwap(p, n); p = c.peak.Load() {
	}
	defer c.inflight.Add(-1)

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
	req.Host = multiHost
	req.Header.Set("X-Request-Id", rid)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.hc.Do(req)
	if err != nil {
		res.Outcome, res.Reason = "failed", "transport: "+err.Error()
		c.transportFailure.Add(1)
		return
	}
	got, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	res.Status = resp.StatusCode
	res.InvID = resp.Header.Get("X-Edgerun-Invocation-Id")
	if err != nil {
		res.Outcome, res.Reason = "failed", "body: "+err.Error()
		c.transportFailure.Add(1)
		return
	}
	// the frontend echoes the id of the request in X-Request-Id, on every response
	if h := resp.Header.Get("X-Request-Id"); h != rid {
		res.Mismatch = fmt.Sprintf("X-Request-Id %q, sent %q", h, rid)
	}
	switch {
	case resp.StatusCode == http.StatusOK && len(got) == 0 && res.InvID == "":
		// 200 OK with no body and no invocation id: the frontend wrote nothing at all
		// (it did that when the orchestrator said "cancelled"; Go's net/http turns an
		// unwritten response into this).
		res.Outcome, res.Reason = "failed", "SILENT_EMPTY_200"
	case resp.StatusCode == http.StatusServiceUnavailable:
		res.Outcome, res.Reason = "refused", "NO_CAPACITY"
	case resp.StatusCode != http.StatusOK:
		res.Outcome, res.Reason = "failed", "HTTP "+strconv.Itoa(resp.StatusCode)+" "+strings.TrimSpace(string(got[:min(len(got), 80)]))
	default:
		res.Outcome = "ok"
		want := append([]byte(rid+"\n"), body...)
		if !bytes.Equal(got, want) {
			ec := newEchoCheck(verifyEchoWithID, rid, body)
			ec.Write(got)
			res.Mismatch += " body: " + ec.finish(rid)
		}
		if h := resp.Header.Get("X-Echo-Request-Id"); h != rid {
			res.Mismatch += fmt.Sprintf(" X-Echo-Request-Id %q", h)
		}
		if res.InvID == "" {
			res.Mismatch += " no X-Edgerun-Invocation-Id"
		}
	}
	res.Mismatch = strings.TrimSpace(res.Mismatch)
}

// Run drives `users` closed loops until end, then waits for them.
func (c *feClient) Run(ctx context.Context, users int, think time.Duration, end time.Time) {
	var wg sync.WaitGroup
	for u := 0; u < users; u++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(7, uint64(u)))
			for time.Now().Before(end) && ctx.Err() == nil {
				c.run1(ctx, r)
				time.Sleep(time.Duration(r.Float64() * 2 * float64(think)))
			}
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------- the mock instances

type multiCfg struct {
	name       string
	dur        time.Duration
	jitterOnly bool          // every request sleeps a random time up to 300 ms before echoing
	killAt     time.Duration // > 0: SIGKILL the busiest worker this long after the start
	feUsers    int
	feThink    time.Duration
	mkaUsers   int
	mkbRate    float64
	mkbSpike   float64
}

func (c multiCfg) path(normal string) string {
	if c.jitterOnly {
		return "/jitter/300"
	}
	return normal
}

// tmplPaths is the path of each template, for the check against the records.
func (c multiCfg) tmplPaths() map[string]string {
	return map[string]string{
		"small": c.path("/echo-with-id"), "mid": c.path("/echo-with-id"), "large": c.path("/echo-with-id"),
		"slow": "/jitter/300", "plain": c.path("/echo"), "plain-big": c.path("/echo"),
	}
}

func (c multiCfg) scenarioA() string {
	v := "verify: echo-with-id, verify_header: x-echo-request-id, method: POST"
	return fmt.Sprintf(`
name: mock-a
defaults: {host: %[1]s, budget: 25s}
requests:
  - {name: small, weight: 5, path: %[2]s, %[3]s, body: {stamp: true, size_min: 0, size_max: 512}}
  - {name: mid, weight: 2, path: %[2]s, %[3]s, body: {stamp: true, size_min: 1KiB, size_max: 64KiB, kind: text}}
  - {name: large, weight: 0.4, path: %[2]s, %[3]s, body: {stamp: true, size_min: 256KiB, size_max: 1MiB}}
  - {name: slow, weight: 3, path: /jitter/300, %[3]s, body: {stamp: true, size_min: 0, size_max: 4KiB}}
load: {model: closed, users: %[4]d, think: 10ms, think_dist: uniform, duration: %[5]s}
`, multiHost, c.path("/echo-with-id"), v, c.mkaUsers, c.dur)
}

func (c multiCfg) scenarioB() string {
	d := c.dur.Seconds()
	pv := "echo" // what a response to /echo is; /jitter answers with the id in front
	if c.jitterOnly {
		pv = "echo-with-id"
	}
	return fmt.Sprintf(`
name: mock-b
defaults: {host: %[1]s, budget: 25s}
requests:
  - {name: plain, weight: 4, method: POST, path: %[2]s, verify: %[10]s, verify_header: X-Echo-Request-Id, body: {stamp: true, size_min: 0, size_max: 2KiB, kind: zeros}}
  - {name: plain-big, weight: 0.3, method: POST, path: %[2]s, verify: %[10]s, body: {stamp: true, size_min: 100KiB, size_max: 1000KiB, kind: text}}
  - {name: slow, weight: 3, method: POST, path: /jitter/300, verify: echo-with-id, verify_header: x-echo-request-id, body: {stamp: true, size_min: 1, size_max: 8KiB}}
load:
  model: spike
  rate: %[3]g
  max_in_flight: 500
  duration: %[4]s
  spikes:
    - {at: "%.1[5]fs", duration: "%.1[6]fs", rate: %[7]g}
    - {at: "%.1[8]fs", duration: "%.1[9]fs", rate: %[7]g}
`, multiHost, c.path("/echo"), c.mkbRate, c.dur, d*0.30, d*0.15, c.mkbSpike, d*0.65, d*0.12, pv)
}

type mockProc struct {
	inst *instance
	cmd  *exec.Cmd
	out  string
	json string
	reqs string
	done chan struct{}
	werr error
}

func startMock(t *testing.T, s *Stack, dir, name, run string, seed uint64, scenario string) *mockProc {
	t.Helper()
	m := &mockProc{done: make(chan struct{})}
	m.out, m.json, m.reqs = filepath.Join(dir, name+".out"), filepath.Join(dir, name+".json"), filepath.Join(dir, name+".requests.jsonl")
	sp := filepath.Join(dir, name+".yaml")
	if err := os.WriteFile(sp, []byte(scenario), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(m.out)
	if err != nil {
		t.Fatal(err)
	}
	m.cmd = exec.Command(env.tmRaceBin, "run", "-scenario", sp, "-url", s.URL, "-token", ingressToken,
		"-admin-url", s.URL, "-admin-token", adminToken, "-run-id", run, "-seed", strconv.FormatUint(seed, 10),
		"-json", m.json, "-requests-log", m.reqs,
		"-expect", "echo_mismatches==0", "-expect", "echo_checked>100", "-expect", "dropped==0", "-expect", "canceled==0")
	m.cmd.Stdout, m.cmd.Stderr = out, out
	m.inst = &instance{Name: name, Prefix: "tm-" + run + "-", Results: map[string]*seen{}, Procs: m.out}
	if err := m.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.cmd.Process.Kill() })
	go func() { m.werr = m.cmd.Wait(); out.Close(); close(m.done) }()
	return m
}

// load reads what a finished mock instance saw: its report and its request log.
func (m *mockProc) load(t *testing.T, c multiCfg) {
	t.Helper()
	<-m.done
	b, err := os.ReadFile(m.json)
	if err != nil {
		o, _ := os.ReadFile(m.out)
		t.Fatalf("%s: no report (%v):\n%s", m.inst.Name, m.werr, o)
	}
	rep := &Report{}
	if err := json.Unmarshal(b, rep); err != nil {
		t.Fatalf("%s: %v", m.inst.Name, err)
	}
	m.inst.Report = rep
	m.inst.Start = rep.Started
	m.inst.End = rep.Started.Add(time.Duration(rep.LoadSeconds * float64(time.Second)))
	m.inst.Peak = rep.Totals.PeakInFlight
	m.inst.Released = int(rep.Totals.Sent)

	f, err := os.Open(m.reqs)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	paths := c.tmplPaths()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var r Result
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("%s: request log: %v", m.inst.Name, err)
		}
		s := &seen{ReqID: r.RequestID, InvID: r.InvocationID, Path: paths[r.Template], Status: r.FuncStatus, Reason: r.Reason}
		switch r.Outcome {
		case OutOK:
			s.Outcome = "ok"
			if r.Verify != VerifyOK {
				s.Mismatch = r.Verify + ": " + r.VerifyDetail
			}
		case OutPlatformError:
			s.Outcome = "failed"
			if r.Reason == "NO_CAPACITY" {
				s.Outcome = "refused"
			}
		default:
			s.Outcome, s.Reason = "failed", string(r.Outcome)+" "+r.Reason
		}
		if _, dup := m.inst.Results[r.RequestID]; dup {
			m.inst.Duplicates = append(m.inst.Duplicates, r.RequestID)
		}
		m.inst.Results[r.RequestID] = s
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------- the test

type multiResult struct {
	insts          []*instance
	records        map[string]*controlv1.Invocation // by request id
	otherRecords   int
	attempts       int64
	proxied        bool
	killedWorker   string
	killedAt       time.Time
	killedInFlight int
}

func multiDuration() time.Duration {
	if v := os.Getenv("TRAFFICMOCK_MULTI_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	if testing.Short() {
		return 8 * time.Second
	}
	return 25 * time.Second
}

func startFrontend(t *testing.T, s *Stack) (base, logPath string, stop func()) {
	t.Helper()
	port, health := freePort(t), freePort(t)
	logPath = filepath.Join(s.dir, "frontend.log")
	cmd := startProc(t, s.dir, "frontend", []string{
		"EDGERUN_ORCHESTRATOR_URL=" + s.URL, "EDGERUN_INGRESS_TOKEN=" + ingressToken,
	}, filepath.Join(env.bin, "frontend"),
		"--listen", fmt.Sprintf("127.0.0.1:%d", port), "--health-listen", fmt.Sprintf("127.0.0.1:%d", health))
	waitHTTP(t, fmt.Sprintf("http://127.0.0.1:%d/healthz", health), logPath)
	return fmt.Sprintf("http://127.0.0.1:%d", port), logPath, func() {
		cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			cmd.Process.Kill()
		}
	}
}

func runMulti(t *testing.T, c multiCfg) *multiResult {
	t.Helper()
	requireStack(t)
	if _, err := os.Stat(filepath.Join(env.bin, "frontend")); err != nil {
		t.Skipf("no frontend binary in %s: %v", env.bin, err)
	}
	const workers = 3
	s := newStack(t, StackOpts{Workers: workers, Concurrency: multiSlot, CountExecute: c.killAt == 0 && os.Getenv("TRAFFICMOCK_NOPROXY") == ""})
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t, s.dir, s.Proxies...)
		}
	})
	s.Deploy(t, "shared-echo", multiHost, env.guest)
	warm(t, s, multiHost)
	// warm every worker's compiled copy of the module: a worker's first invocation costs about a second
	{
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		var wg sync.WaitGroup
		for i := 0; i < workers*multiSlot; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				warmOne(ctx, s)
			}()
		}
		wg.Wait()
		cancel()
	}

	feBase, feLog, stopFE := startFrontend(t, s)
	fe := newFEClient(feBase, "x"+strconv.Itoa(os.Getpid()%1000), c.jitterOnly)
	fei := &instance{Name: "frontend", Prefix: fe.prefix, Results: map[string]*seen{}, Procs: feLog}

	ma := startMock(t, s, s.dir, "mock-a", "mka", 11, c.scenarioA())
	mb := startMock(t, s, s.dir, "mock-b", "mkb", 22, c.scenarioB())

	// All three are started within a few hundred milliseconds of each other.
	// The Go client begins its load at the same moment the mocks begin theirs.
	time.Sleep(300 * time.Millisecond)
	res := &multiResult{}
	baseAttempts := int64(0)
	for _, px := range s.Proxies {
		baseAttempts += px.Attempts.Load()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fei.Start = time.Now()
	end := fei.Start.Add(c.dur)
	var killWG sync.WaitGroup
	if c.killAt > 0 {
		killWG.Add(1)
		go func() {
			defer killWG.Done()
			time.Sleep(time.Until(fei.Start.Add(c.killAt)))
			res.killedWorker, res.killedInFlight = killBusiestWorker(t, s)
			res.killedAt = time.Now()
		}()
	}
	fe.Run(ctx, c.feUsers, c.feThink, end)
	fei.End = time.Now()
	killWG.Wait()
	stopFE() // flushes its access log
	ma.load(t, c)
	mb.load(t, c)
	for _, m := range []*mockProc{ma, mb} {
		if m.werr != nil {
			o, _ := os.ReadFile(m.out)
			t.Errorf("%s exited with %v:\n%s", m.inst.Name, m.werr, o)
		}
	}

	fei.Released = int(fe.seq.Load())
	fei.Peak = fe.peak.Load()
	for _, r := range fe.results {
		if _, dup := fei.Results[r.ReqID]; dup {
			fei.Duplicates = append(fei.Duplicates, r.ReqID)
		}
		fei.Results[r.ReqID] = r
	}
	res.insts = []*instance{fei, ma.inst, mb.inst}

	for _, px := range s.Proxies {
		res.attempts += px.Attempts.Load()
	}
	res.attempts -= baseAttempts
	res.proxied = len(s.Proxies) > 0
	res.records, res.otherRecords = fetchRecords(t, s, res.insts)
	checkFrontendLog(t, feLog, fei)
	return res
}

// dumpLogs prints the warnings and errors of the processes of a failed run.
func dumpLogs(t *testing.T, dir string, proxies ...*execProxy) {
	for i, px := range proxies {
		px.mu.Lock()
		for _, e := range px.Errors {
			t.Logf("proxy %d: %s", i, e)
		}
		px.mu.Unlock()
	}
	logs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	for _, l := range logs {
		b, _ := os.ReadFile(l)
		n := 0
		for _, line := range strings.Split(string(b), "\n") {
			if (strings.Contains(line, `"level":"WARN"`) || strings.Contains(line, `"level":"ERROR"`) || strings.Contains(line, "level=WARN") || strings.Contains(line, "level=ERROR")) && !strings.Contains(line, "slots busy") && n < 15 {
				t.Logf("%s: %s", filepath.Base(l), line)
				n++
			}
		}
	}
}

func warmOne(ctx context.Context, s *Stack) {
	res, err := s.Ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: multiHost, Path: "/sleep/300"}})
	if err == nil {
		res.ReadAll()
	}
}

// killBusiestWorker SIGKILLs the worker with the most invocations in flight.
func killBusiestWorker(t *testing.T, s *Stack) (id string, inFlight int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, err := s.Ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
	if err != nil {
		t.Errorf("ListWorkers: %v", err)
		return "", 0
	}
	var best *controlv1.WorkerStatus
	for _, w := range ws.Workers {
		if w.Alive && (best == nil || w.InFlight > best.InFlight) {
			best = w
		}
	}
	if best == nil {
		t.Error("no live worker to kill")
		return "", 0
	}
	for i, u := range s.WorkURLs {
		if u == best.Endpoint {
			if err := syscall.Kill(s.WorkPIDs[i], syscall.SIGKILL); err != nil {
				t.Errorf("kill: %v", err)
			}
			return best.Id, int(best.InFlight)
		}
	}
	t.Errorf("no process for worker endpoint %s (have %v)", best.Endpoint, s.WorkURLs)
	return "", 0
}

// fetchRecords reads every invocation record of the orchestrator, waiting for
// the recorder to catch up, and sorts them out by instance.
func fetchRecords(t *testing.T, s *Stack, insts []*instance) (map[string]*controlv1.Invocation, int) {
	t.Helper()
	want := 0
	for _, in := range insts {
		want += len(in.Results)
	}
	var recs map[string]*controlv1.Invocation
	var other int
	var all []*controlv1.Invocation
	deadline := time.Now().Add(60 * time.Second)
	for {
		all = all[:0]
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		for rec, err := range s.Ctl.AllInvocations(ctx, "") {
			if err != nil {
				t.Fatalf("ListInvocations: %v", err)
			}
			all = append(all, rec)
		}
		cancel()
		recs, other = map[string]*controlv1.Invocation{}, 0
		for _, rec := range all {
			if rec.RequestId == "" {
				other++ // the warm-up requests have no request id
				continue
			}
			recs[rec.RequestId] = rec
		}
		if len(all)-other >= want || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	// the same list is checked for duplicates as a list, not as a map
	t.Logf("orchestrator holds %d invocation records (%d without a request id: the warm-up)", len(all), other)
	if len(recs) != len(all)-other {
		t.Errorf("%d records but %d distinct request ids: a request id has two records", len(all)-other, len(recs))
	}
	return recs, other
}

// checkFrontendLog compares the frontend process's own access log with what
// the client saw: the frontend wrote the same invocation id for the same request id.
func checkFrontendLog(t *testing.T, path string, fe *instance) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Errorf("frontend log: %v", err)
		return
	}
	defer f.Close()
	n, bad := 0, 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var l struct {
			Msg, RequestID, InvocationID string
			Status                       int
		}
		var raw map[string]any
		if json.Unmarshal(sc.Bytes(), &raw) != nil || raw["msg"] != "request" {
			continue
		}
		l.RequestID, _ = raw["request_id"].(string)
		l.InvocationID, _ = raw["invocation_id"].(string)
		st, _ := raw["status"].(float64)
		l.Status = int(st)
		if !strings.HasPrefix(l.RequestID, fe.Prefix) {
			continue // the warm-up
		}
		n++
		r := fe.Results[l.RequestID]
		if r == nil || r.InvID != l.InvocationID || (r.Outcome == "ok" && r.Status != l.Status) {
			bad++
			if bad < 5 {
				t.Errorf("frontend log says %+v, the client saw %+v", l, r)
			}
		}
	}
	answered := 0
	for _, r := range fe.Results {
		if r.Status != 0 {
			answered++
		}
	}
	if n != answered {
		t.Errorf("the frontend logged %d requests of this run, the client got %d answers", n, answered)
	}
	t.Logf("frontend access log: %d requests of this run, every invocation id the same as in the client's view (%d disagreements)", n, bad)
}

// verifyMulti asserts everything the test is about.
func verifyMulti(t *testing.T, c multiCfg, res *multiResult) {
	t.Helper()
	insts := res.insts
	byPrefix := func(reqID string) *instance {
		var found *instance
		for _, in := range insts {
			if strings.HasPrefix(reqID, in.Prefix) {
				if found != nil {
					t.Errorf("request id %q has the prefix of two instances", reqID)
				}
				found = in
			}
		}
		return found
	}
	for _, a := range insts {
		for _, b := range insts {
			if a != b && (strings.HasPrefix(a.Prefix, b.Prefix) || strings.HasPrefix(b.Prefix, a.Prefix)) {
				t.Fatalf("the prefixes %q and %q overlap", a.Prefix, b.Prefix)
			}
		}
	}

	// 1. Every instance: every request answered exactly once, none of the answers wrong.
	total := 0
	for _, in := range insts {
		total += len(in.Results)
		if len(in.Duplicates) > 0 {
			t.Errorf("%s: request ids answered twice: %v", in.Name, in.Duplicates)
		}
		if len(in.Results) != in.Released {
			t.Errorf("%s: sent %d requests, %d distinct answers", in.Name, in.Released, len(in.Results))
		}
		if in.Released < 150 {
			t.Errorf("%s: only %d requests: the test did not exercise it", in.Name, in.Released)
		}
		if mm := in.mismatches(); len(mm) > 0 {
			for i, m := range mm {
				if i < 10 {
					t.Errorf("MISMATCH %s %s (invocation %s): %s", in.Name, m.ReqID, m.InvID, m.Mismatch)
				}
			}
			t.Errorf("%s: %d responses were NOT the response to their request", in.Name, len(mm))
		}
		if in.Report != nil {
			if e := in.Report.Echo; e == nil || e.Mismatches != 0 || e.Checked != in.Report.Totals.OK {
				t.Errorf("%s: the instance's own verification: %+v (ok %d)", in.Name, e, in.Report.Totals.OK)
			}
			if in.Report.Invocations != nil {
				iv := in.Report.Invocations
				if iv.Found != iv.Expected || iv.IDMismatch != 0 || iv.StatusMismatch != 0 {
					t.Errorf("%s: its own record lookup: %+v", in.Name, iv)
				}
			}
			for _, w := range in.Report.Warnings {
				if strings.Contains(w, "send lag") { // a note on this machine's load, not on delivery
					t.Logf("%s: %s", in.Name, w)
					continue
				}
				t.Errorf("%s: warning in the report: %s", in.Name, w)
			}
		}
		if in.Name != "frontend" && in.Report != nil && c.killAt == 0 && in.Report.Totals.StreamFailures != 0 {
			t.Errorf("%s: %d stream failures", in.Name, in.Report.Totals.StreamFailures)
		}
	}

	// 2. The orchestrator's records.
	recsPer := map[*instance]int{}
	owner := map[string]*instance{} // invocation id -> the instance its request id belongs to
	for reqID, rec := range res.records {
		in := byPrefix(reqID)
		if in == nil {
			t.Errorf("record %s has request id %q, which belongs to no instance", rec.Id, reqID)
			continue
		}
		recsPer[in]++
		if prev := owner[rec.Id]; prev != nil {
			t.Errorf("invocation id %s is on two records", rec.Id)
		}
		owner[rec.Id] = in
	}
	if res.otherRecords > 3*multiSlot+3 {
		t.Errorf("%d records with no request id (only the warm-up has none)", res.otherRecords)
	}
	sum := 0
	for _, in := range insts {
		sum += recsPer[in]
		if recsPer[in] != len(in.Results) {
			t.Errorf("%s: %d records, but it sent %d requests", in.Name, recsPer[in], len(in.Results))
		}
	}
	if len(res.records) != sum || sum != total {
		t.Errorf("records %d, per-instance sum %d, requests sent %d", len(res.records), sum, total)
	}

	// 3. Request by request: what the instance was told is what the record says.
	pairs, refusedRecs, failedOnKilled, failedElsewhere, canceledByPeer := 0, 0, 0, 0, 0
	for _, in := range insts {
		for id, r := range in.Results {
			rec := res.records[id]
			if rec == nil {
				t.Errorf("%s: no record for %s (%s)", in.Name, id, r.Outcome)
				continue
			}
			if rec.Method != "POST" || (r.Path != "" && rec.Path != r.Path) {
				t.Errorf("%s %s: sent POST %s, the record says %s %s", in.Name, id, r.Path, rec.Method, rec.Path)
			}
			switch r.Outcome {
			case "ok":
				pairs++
				if r.InvID == "" || rec.Id != r.InvID {
					t.Errorf("%s %s: the instance was given invocation %q, the record of this request id is %q", in.Name, id, r.InvID, rec.Id)
				}
				if o := owner[r.InvID]; o != in {
					t.Errorf("%s %s: invocation %q belongs to a request of %v", in.Name, id, r.InvID, o)
				}
				if int(rec.Status) != r.Status || rec.ErrorReason != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED || r.Status != 200 {
					t.Errorf("%s %s: status %d/reason %v in the record, %d seen", in.Name, id, rec.Status, rec.ErrorReason, r.Status)
				}
			case "refused":
				refusedRecs++
				if rec.ErrorReason != commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY {
					t.Errorf("%s %s: refused as NO_CAPACITY, the record says %v", in.Name, id, rec.ErrorReason)
				}
			default: // failed
				if c.killAt == 0 {
					t.Errorf("%s %s: failed (%s) with no worker killed; record: %v status %d", in.Name, id, r.Reason, rec.ErrorReason, rec.Status)
				}
				silent := r.Reason == "SILENT_EMPTY_200" || r.Reason == ReasonCanceledByPeer
				if silent {
					canceledByPeer++
					if rec.Status != 0 || rec.ErrorReason != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
						t.Errorf("%s %s: %s, but the record says status %d reason %v", in.Name, id, r.Reason, rec.Status, rec.ErrorReason)
					}
				} else if rec.ErrorReason == commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
					t.Errorf("%s %s: failed (%s) but the record has no error reason", in.Name, id, r.Reason)
				}
				if rec.WorkerId == res.killedWorker && res.killedWorker != "" {
					failedOnKilled++
				} else {
					failedElsewhere++
					t.Errorf("%s %s: failed (%s), record worker %s is not the killed one (%s); reason %v", in.Name, id, r.Reason, rec.WorkerId, res.killedWorker, rec.ErrorReason)
				}
			}
		}
	}

	// 4. The instances really did overlap, and overload really happened.
	var peak int64
	lo, hi := insts[0].Start, insts[0].End
	for _, in := range insts {
		peak += in.Peak
		if in.Start.After(lo) {
			lo = in.Start
		}
		if in.End.Before(hi) {
			hi = in.End
		}
	}
	if hi.Sub(lo) < c.dur*8/10 {
		t.Errorf("the three instances overlapped for %v of %v", hi.Sub(lo), c.dur)
	}
	if peak <= int64(3*multiSlot) {
		t.Errorf("the peaks in flight add up to %d, not more than the %d slots", peak, 3*multiSlot)
	}

	// The numbers.
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n%s: %d workers x %d slots; instances overlapped for %.1fs of %v\n", c.name, 3, multiSlot, hi.Sub(lo).Seconds(), c.dur)
	for _, in := range insts {
		fmt.Fprintf(&sb, "  %-9s sent %5d  ok %5d  refused(NO_CAPACITY) %4d  failed %3d  mismatches %d  records %5d  peak in flight %3d\n",
			in.Name, in.Released, in.count("ok"), in.count("refused"), in.count("failed"), len(in.mismatches()), recsPer[in], in.Peak)
	}
	fmt.Fprintf(&sb, "  total requests %d = records %d; (request id, invocation id) pairs verified against the records: %d\n", total, len(res.records), pairs)
	if res.proxied {
		fmt.Fprintf(&sb, "  Execute attempts on the workers: %d for %d invocations: %d retries between workers (NO_CAPACITY); %d invocations ended refused\n",
			res.attempts, len(res.records), res.attempts-int64(len(res.records)), refusedRecs)
		if res.attempts <= int64(len(res.records)) {
			t.Errorf("no NO_CAPACITY retry between workers happened (%d attempts for %d invocations): the load was too light", res.attempts, len(res.records))
		}
	}
	if c.killAt > 0 {
		fmt.Fprintf(&sb, "  killed worker %s at +%.1fs with %d in flight; %d requests failed, all on that worker (%d elsewhere); %d refused (NO_CAPACITY)\n",
			res.killedWorker, res.killedAt.Sub(insts[0].Start).Seconds(), res.killedInFlight, failedOnKilled, failedElsewhere, refusedRecs)
		fmt.Fprintf(&sb, "  of the failures, %d were an orchestrator answer of \"cancelled\" that nobody asked for (a frontend answers that with a silent empty 200)\n", canceledByPeer)
		if res.killedWorker == "" {
			t.Error("no worker was killed")
		}
		if failedOnKilled > 2*multiSlot {
			t.Errorf("%d requests failed, but the killed worker has only %d slots", failedOnKilled, multiSlot)
		}
	}
	t.Log(sb.String())
}

func multiBase() multiCfg {
	return multiCfg{dur: multiDuration(), feUsers: 10, feThink: 15 * time.Millisecond, mkaUsers: 6, mkbRate: 25, mkbSpike: 100}
}

func TestMultiIngressSameInstanceDelivery(t *testing.T) {
	c := multiBase()
	c.name = "three instances, one shared host"
	res := runMulti(t, c)
	verifyMulti(t, c, res)
}

func TestMultiIngressOutOfOrderAndWorkerKill(t *testing.T) {
	c := multiBase()
	c.name = "random sleep before echo, one worker SIGKILLed mid-run"
	c.jitterOnly = true
	c.killAt = c.dur * 4 / 10
	// every request sleeps ~150 ms on average, so the same number of users keep more slots busy
	c.feUsers, c.mkaUsers, c.mkbRate, c.mkbSpike = 8, 5, 20, 60
	res := runMulti(t, c)
	verifyMulti(t, c, res)
}
