package e2e

// Helpers shared by the security_* tests. Everything here is named sec* so it
// cannot collide with the helpers of the other e2e tests, which are not edited.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Olian04/edgerun"
	"github.com/Olian04/edgerun/conformance/guest"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/internal/authn"
	egressapp "github.com/Olian04/edgerun/internal/egress/app"
	"github.com/Olian04/edgerun/internal/egress/policy"
	"github.com/Olian04/edgerun/internal/egress/registry"
	"github.com/Olian04/edgerun/internal/egress/server"
	workerapp "github.com/Olian04/edgerun/internal/worker/app"
)

// secBuild compiles guest source for wasip1, cached by hash (like conformance/guest).
func secBuild(t testing.TB, name, src string) []byte {
	t.Helper()
	sum := sha256.Sum256([]byte(src))
	file := name + "-" + hex.EncodeToString(sum[:6]) + ".wasm"
	root, err := os.UserCacheDir()
	if err != nil {
		root = os.TempDir()
	}
	dir := filepath.Join(root, "edgerun-security")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cached := filepath.Join(dir, file)
	if b, err := os.ReadFile(cached); err == nil {
		return b
	}
	work := t.TempDir()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("building %s: %v", name, err)
		}
	}
	must(os.WriteFile(filepath.Join(work, "main.go"), []byte(src), 0o644))
	must(os.WriteFile(filepath.Join(work, "go.mod"), []byte("module "+name+"\n\ngo 1.21\n"), 0o644))
	out := filepath.Join(work, "out.wasm")
	cmd := exec.Command("go", "build", "-trimpath", "-o", out, ".")
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0", "GOFLAGS=-mod=mod")
	if msg, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building %s: %v\n%s", name, err, msg)
	}
	b, err := os.ReadFile(out)
	must(err)
	tmp, err := os.CreateTemp(dir, "tmp-*")
	must(err)
	_, werr := tmp.Write(b)
	must(werr)
	must(tmp.Close())
	must(os.Rename(tmp.Name(), cached))
	return b
}

func secGuest(t testing.TB) []byte { return secBuild(t, "secguest", secguestSource) }

// secNetWorker starts a worker that can run wagi-2 (it has the egress token).
func (s *stack) secNetWorker(maxConcurrency uint32) {
	s.t.Helper()
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- workerapp.Run(wctx, workerapp.Config{
			Listen: "127.0.0.1:0", OrchestratorURL: "http://" + s.addr,
			WorkerToken: workerToken, ExecutionToken: executionToken, EgressToken: egressToken,
			MaxConcurrency: maxConcurrency, DrainTimeout: 10 * time.Second, Log: quiet,
		})
	}()
	s.t.Cleanup(func() { cancel(); <-done })
}

// secEgressCfg configures a network service the tests can see into.
type secEgressCfg struct {
	Covers []string
	Allow  []netip.Prefix
	// Lookup replaces name resolution (nil: the system resolver). It is counted.
	Lookup func(ctx context.Context, host string) ([]netip.Addr, error)
}

type secEgress struct {
	Addr    string
	Lookups atomic.Int64
	lookupd []string
	mu      sync.Mutex
}

// LookedUp returns the names resolved so far, in order.
func (e *secEgress) LookedUp() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.lookupd...)
}

// secStartEgress runs the reference network service (server.Service, the same
// code cmd/egress runs) with a resolver of the test's own, registered with the
// stack's orchestrator exactly as egress/app does.
func (s *stack) secStartEgress(cfg secEgressCfg) *secEgress {
	s.t.Helper()
	covers, err := policy.CanonicalCovers(cfg.Covers)
	if err != nil {
		s.t.Fatal(err)
	}
	out := &secEgress{}
	var lookup func(ctx context.Context, host string) ([]netip.Addr, error)
	if cfg.Lookup != nil {
		lookup = func(ctx context.Context, host string) ([]netip.Addr, error) {
			out.Lookups.Add(1)
			out.mu.Lock()
			out.lookupd = append(out.lookupd, host)
			out.mu.Unlock()
			return cfg.Lookup(ctx, host)
		}
	}
	svc := server.New(server.Config{Covers: covers, AllowNetworks: cfg.Allow, Lookup: lookup, Log: quiet, DialTimeout: 3 * time.Second})
	mux := http.NewServeMux()
	mux.Handle(svc.Handler(authn.Verifier{egressToken: server.Scope}))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		s.t.Fatal(err)
	}
	hs := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go hs.Serve(ln)
	out.Addr = ln.Addr().String()
	reg := registry.New(registry.Config{
		Endpoint: "http://" + out.Addr,
		Info:     &egressv1.ServiceInfo{Implementation: server.Implementation(""), Covers: covers},
		Registry: edgerun.NewRegistry("http://"+s.addr, networkToken),
		Log:      quiet,
	})
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); reg.Run(rctx) }()
	s.t.Cleanup(func() { cancel(); <-done; hs.Close() })
	// Wait until the orchestrator lists it as alive: grants are only made to live services.
	eventually(s.t, "the network service to register", 30*time.Second, func() bool {
		r, err := s.ctl.ListNetworkServices(ctx, &controlv1.ListNetworkServicesRequest{})
		if err != nil {
			return false
		}
		for _, n := range r.Services {
			if n.Endpoint == "http://"+out.Addr && n.Alive {
				return true
			}
		}
		return false
	})
	return out
}

// secSeen is one request an origin received.
type secSeen struct {
	URI, Host string
	Header    http.Header
}

// secOrigin is a web server on loopback that counts what reaches it.
type secOrigin struct {
	srv  *httptest.Server
	Addr string // 127.0.0.1:port
	Port int
	mu   sync.Mutex
	seen []secSeen
}

func secStartOrigin(t testing.TB) *secOrigin {
	t.Helper()
	o := &secOrigin{}
	o.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.mu.Lock()
		o.seen = append(o.seen, secSeen{URI: r.RequestURI, Host: r.Host, Header: r.Header.Clone()})
		o.mu.Unlock()
		if r.URL.Path == "/loop" {
			w.Header().Set("Location", "/loop")
			w.WriteHeader(http.StatusFound)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/redir") {
			w.Header().Set("Location", r.URL.Query().Get("to"))
			w.WriteHeader(http.StatusFound)
			return
		}
		fmt.Fprintf(w, "origin-answer:%s", r.RequestURI)
	}))
	t.Cleanup(o.srv.Close)
	o.Addr = strings.TrimPrefix(o.srv.URL, "http://")
	_, p, _ := net.SplitHostPort(o.Addr)
	o.Port, _ = strconv.Atoi(p)
	return o
}

// Hits counts requests whose URI starts with prefix.
func (o *secOrigin) Hits(prefix string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, s := range o.seen {
		if strings.HasPrefix(s.URI, prefix) {
			n++
		}
	}
	return n
}

func (o *secOrigin) Seen(prefix string) []secSeen {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []secSeen
	for _, s := range o.seen {
		if strings.HasPrefix(s.URI, prefix) {
			out = append(out, s)
		}
	}
	return out
}

func (o *secOrigin) Total() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.seen)
}

// secFn is a deployed function the tests drive over the ingress API.
type secFn struct {
	s    *stack
	id   string
	host string
}

// secDeploy creates a function from module (wagi-2 or wagi-1), routes host to it.
func (s *stack) secDeploy(name, host string, module []byte, abi string, limits func(*controlv1.CreateVersionRequest)) *secFn {
	s.t.Helper()
	fid := s.createFunction(name)
	req := &controlv1.CreateVersionRequest{FunctionId: fid, Module: module, Abi: abi, Activate: true}
	if limits != nil {
		limits(req)
	}
	if _, err := s.ctl.CreateVersion(ctx, req); err != nil {
		s.t.Fatal(err)
	}
	s.route(host, "/", fid)
	return &secFn{s: s, id: fid, host: host}
}

func (f *secFn) allow(entries ...string) {
	f.s.t.Helper()
	if _, err := f.s.ctl.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: f.id, Allow: entries}); err != nil {
		f.s.t.Fatal(err)
	}
}

// do calls the function, retrying while the worker has no free slot (NO_CAPACITY
// is part of the protocol; a test that fires many calls at once must expect it).
func (f *secFn) do(path, query, body string) result {
	f.s.t.Helper()
	var r result
	for i := 0; i < 200; i++ {
		r = f.s.call(req{host: f.host, path: path, query: query, method: "POST", body: body})
		if r.err == nil || reason(r.err) != commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY {
			return r
		}
		time.Sleep(20 * time.Millisecond)
	}
	return r
}

// fetch makes the netguest-style /fetch call: the guest performs reqText and
// reports "code=N". It returns N (or 1<<30 if the invocation itself failed).
func (f *secFn) fetch(path, reqText string) (code int, resp string) {
	f.s.t.Helper()
	r := f.do(path, "", reqText)
	if r.err != nil {
		return 1 << 30, r.err.Error()
	}
	b := string(r.body)
	line, rest, _ := strings.Cut(b, "\n")
	line = strings.TrimPrefix(line, "code=")
	n, err := strconv.Atoi(line)
	if err != nil {
		return 1 << 30, b
	}
	return n, rest
}

// netguest module (wagi-2) for the corpus tests.
func secNetModule(t testing.TB) []byte { return guest.BuildNet(t) }

// secParallel runs fn for each item, at most n at a time.
func secParallel[T any](items []T, n int, fn func(T)) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, n)
	for _, it := range items {
		it := it
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			fn(it)
		}()
	}
	wg.Wait()
}

// secHeap samples the process's heap while a function runs and reports the peak
// (HeapInuse) and the total bytes allocated, so a test can say "memory stayed
// bounded". Every component of the stack runs in this process.
type secHeap struct {
	stop       chan struct{}
	done       chan struct{}
	startTotal uint64
	startInuse uint64
	peak       atomic.Uint64
}

func secMeasure() *secHeap {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	h := &secHeap{stop: make(chan struct{}), done: make(chan struct{}), startTotal: m.TotalAlloc, startInuse: m.HeapInuse}
	h.peak.Store(m.HeapInuse)
	go func() {
		defer close(h.done)
		t := time.NewTicker(5 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-h.stop:
				return
			case <-t.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapInuse > h.peak.Load() {
					h.peak.Store(m.HeapInuse)
				}
			}
		}
	}()
	return h
}

// Stop returns the growth of the heap peak over the start, and the bytes allocated since.
func (h *secHeap) Stop() (peakGrowth, allocated uint64) {
	close(h.stop)
	<-h.done
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if p := h.peak.Load(); p > h.startInuse {
		peakGrowth = p - h.startInuse
	}
	return peakGrowth, m.TotalAlloc - h.startTotal
}

const secMiB = 1 << 20

// secRealEgress starts the network service exactly as cmd/egress does (egress/app) and returns its address.
func (s *stack) secRealEgress(t testing.TB, covers []string) string {
	t.Helper()
	eg, err := egressapp.Start(ctx, egressapp.Config{
		Listen: "127.0.0.1:0", EgressToken: egressToken, OrchestratorURL: "http://" + s.addr, NetworkToken: networkToken,
		Covers: covers, DrainGrace: 100 * time.Millisecond, Log: quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eg.Stop(context.Background()) })
	return eg.Addr()
}
