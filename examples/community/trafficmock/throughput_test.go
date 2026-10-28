package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

// Throughput and fidelity. The question: how many requests per second can ONE
// instance of the tool drive before the library or the protocol (not this
// tool's own code) is the limit?
//
// Three layers are measured, so each can be told apart:
//  1. the library alone: a bare loop of Invoke + read-to-end against a fake
//     orchestrator in another process (no engine, no statistics);
//  2. the engine: the same through scenarios, histograms, the works;
//  3. a real orchestrator and worker (which is the limit long before 1 or 2).
//
// The machine has 4 cores, shared by the tool, the fake or real stack and the
// load generator's own scheduler: absolute numbers are for comparison.

// recordingTransport notes whether messages were compressed on the wire.
type recordingTransport struct {
	rt             http.RoundTripper
	accept         atomic.Value // the Connect-Accept-Encoding the client sent
	gzipped, total atomic.Int64
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if v := req.Header.Get("Connect-Accept-Encoding"); v != "" {
		r.accept.Store(v)
	}
	res, err := r.rt.RoundTrip(req)
	if err == nil {
		r.total.Add(1)
		if res.Header.Get("Connect-Content-Encoding") == "gzip" {
			r.gzipped.Add(1)
		}
	}
	return res, err
}

type memDelta struct{ mallocs, bytes uint64 }

func measureMem(f func()) memDelta {
	runtime.GC()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	f()
	runtime.ReadMemStats(&b)
	return memDelta{b.Mallocs - a.Mallocs, b.TotalAlloc - a.TotalAlloc}
}

// libLoop: conc goroutines of Invoke + read to the end, for d. It uses nothing of the tool but the HTTP client setup.
func libLoop(ing *edgerun.Ingress, conc int, d time.Duration, path string) (n int64, errs int64, mem memDelta, bytes int64) {
	var count, errCount, byteCount atomic.Int64
	stop := time.Now().Add(d)
	mem = measureMem(func() {
		var wg sync.WaitGroup
		for range conc {
			wg.Add(1)
			go func() {
				defer wg.Done()
				buf := make([]byte, 64<<10)
				req := &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: mockHost, Path: path}}
				for time.Now().Before(stop) {
					r, err := ing.Invoke(context.Background(), req)
					if err != nil {
						errCount.Add(1)
						continue
					}
					for {
						k, err := r.Body.Read(buf)
						byteCount.Add(int64(k))
						if err != nil {
							if err != io.EOF {
								errCount.Add(1)
							}
							break
						}
					}
					r.Body.Close()
					count.Add(1)
				}
			}()
		}
		wg.Wait()
	})
	return count.Load(), errCount.Load(), mem, byteCount.Load()
}

var lastFakePID int

func fakeProc(t testing.TB, extra ...string) string {
	t.Helper()
	requireStack(t)
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	lastFakePID = startProc(t, env.root, fmt.Sprintf("fake%d", port), nil, env.tmBin, append([]string{"fake", "-addr", addr, "-token", ingressToken}, extra...)...).Process.Pid
	waitHTTP(t, "http://"+addr+"/healthz", "")
	return "http://" + addr
}

func TestThroughputLibraryOnly(t *testing.T) {
	url := fakeProc(t)
	dur := 2 * time.Second
	conc := 128
	if raceOn {
		dur, conc = time.Second, 32
	}
	type variant struct {
		name string
		http func() *http.Client
		gzip bool
	}
	clients := []variant{
		{"default client (http.DefaultClient)", func() *http.Client { return http.DefaultClient }, true},
		{"h1 tuned", func() *http.Client { return newHTTPClient("1", 0) }, true},
		{"h1 tuned, no gzip", func() *http.Client { return newHTTPClient("1", 0) }, false},
		{"h2c", func() *http.Client { return newHTTPClient("2", 0) }, true},
		{"h2c, no gzip", func() *http.Client { return newHTTPClient("2", 0) }, false},
	}
	sizes := []struct {
		name, path string
	}{{"0.1 KB", "/bytes/100"}, {"1 MiB", "/bytes/1048576"}}
	if os.Getenv("TRAFFICMOCK_BENCH") == "" {
		clients = clients[1:]
		sizes = sizes[:1]
	}
	var table strings.Builder
	fmt.Fprintf(&table, "\nlibrary alone, %d concurrent, %v each, fake orchestrator in another process, GOMAXPROCS=%d\n", conc, dur, runtime.GOMAXPROCS(0))
	fmt.Fprintf(&table, "%-38s %-8s %9s %9s %10s %10s %7s\n", "client", "payload", "req/s", "MB/s", "allocs/req", "KiB/req", "gzipped")
	for _, sz := range sizes {
		for _, c := range clients {
			rt := &recordingTransport{rt: c.http().Transport}
			if rt.rt == nil {
				rt.rt = http.DefaultTransport
			}
			opts := []edgerun.Option{edgerun.WithHTTPClient(&http.Client{Transport: rt})}
			if !c.gzip {
				opts = append(opts, edgerun.WithClientOptions(connect.WithAcceptCompression("gzip", nil, nil)))
			}
			ing := edgerun.NewIngress(url, ingressToken, opts...)
			libLoop(ing, conc, 300*time.Millisecond, sz.path) // warm up
			n, errs, mem, bytes := libLoop(ing, conc, dur, sz.path)
			if errs > 0 {
				t.Errorf("%s: %d errors", c.name, errs)
			}
			gz := "no"
			if g, tot := rt.gzipped.Load(), rt.total.Load(); g > 0 {
				gz = fmt.Sprintf("%d%%", 100*g/tot)
			}
			fmt.Fprintf(&table, "%-38s %-8s %9.0f %9.1f %10.0f %10.1f %7s\n", c.name, sz.name, float64(n)/dur.Seconds(), float64(bytes)/dur.Seconds()/1e6,
				float64(mem.mallocs)/float64(n), float64(mem.bytes)/float64(n)/1024, gz)
		}
	}
	t.Log(table.String())
}

func TestThroughputEngine(t *testing.T) {
	url := fakeProc(t)
	dur := 3 * time.Second
	if raceOn {
		dur = time.Second
	}
	scen := func(load string) string {
		return fmt.Sprintf("defaults: {host: mock.test, budget: 10s}\nrequests: [{path: /bytes/100}]\nload: %s\n", load)
	}
	var out strings.Builder
	fmt.Fprintf(&out, "\nengine (scenario, statistics, report) against the fake in another process, %v, GOMAXPROCS=%d\n", dur, runtime.GOMAXPROCS(0))
	for _, c := range []struct {
		name string
		load string
		args []string
	}{
		{"closed, 128 users, h1", fmt.Sprintf("{model: closed, users: 128, duration: %v}", dur), nil},
		{"closed, 128 users, h2c", fmt.Sprintf("{model: closed, users: 128, duration: %v}", dur), []string{"-http", "2"}},
		{"closed, 128 users, h1, request log", fmt.Sprintf("{model: closed, users: 128, duration: %v}", dur), []string{"-requests-log", t.TempDir() + "/l.jsonl"}},
		{"open 3000/s, h1", fmt.Sprintf("{model: constant, rate: 3000, duration: %v}", dur), nil},
		{"open 15000/s, h1 (overload?)", fmt.Sprintf("{model: constant, rate: 15000, duration: %v, max_in_flight: 2000}", dur), nil},
	} {
		self0, fake0 := selfTicks(), cpuTicks(lastFakePID)
		code, rep, _ := tm(t, append([]string{"-scenario", writeFile(t, "s.yaml", scen(c.load)), "-url", url, "-token", ingressToken, "-quiet"}, c.args...)...)
		if code != 0 || rep == nil {
			t.Fatalf("%s: exit %d", c.name, code)
		}
		n := float64(rep.Totals.Attempted)
		fmt.Fprintf(&out, "  %-36s released %8.0f/s completed %8.0f/s  errors %d dropped %d  p50 %.2fms p99 %.2fms  send lag p99 %.2fms  peak in flight %d  cpu us/request: tool %.0f fake %.0f\n",
			c.name, rep.Totals.RPSSent, rep.Totals.RPSCompleted, rep.Totals.PlatformErrors, rep.Totals.Dropped, rep.Latency["total"].P50, rep.Latency["total"].P99,
			rep.Latency["send_lag"].P99, rep.Totals.PeakInFlight, float64(selfTicks()-self0)*1e4/n, float64(cpuTicks(lastFakePID)-fake0)*1e4/n)
		if rep.Totals.PlatformErrors > 0 {
			t.Errorf("%s: errors %v", c.name, rep.Reasons)
		}
	}
	t.Log(out.String())
}

func TestThroughputRealStack(t *testing.T) {
	s := newStack(t, StackOpts{Concurrency: 64})
	s.Deploy(t, "mock", mockHost, env.guest)
	warm(t, s, mockHost)
	var out strings.Builder
	fmt.Fprintf(&out, "\nreal orchestrator + 1 worker (64 slots) + the tool, GOMAXPROCS=%d\n", runtime.GOMAXPROCS(0))
	dur := "4s"
	for _, c := range []struct {
		name string
		args []string
		path string
	}{
		{"closed 8 users, h1", []string{"-users", "8"}, "/status/200"},
		{"closed 32 users, h1", []string{"-users", "32"}, "/status/200"},
		{"closed 32 users, h2c", []string{"-users", "32", "-http", "2"}, "/status/200"},
		{"closed 32 users, h2c, no gzip", []string{"-users", "32", "-http", "2", "-no-compression"}, "/status/200"},
		{"closed 32 users, h1, no gzip", []string{"-users", "32", "-no-compression"}, "/status/200"},
	} {
		scen := fmt.Sprintf("defaults: {host: mock.test, budget: 20s}\nrequests: [{path: %s}]\nload: {model: closed, users: 32, duration: %s}\n", c.path, dur)
		orch0, work0, self0 := cpuTicks(s.OrchPID), cpuTicks(s.WorkPIDs[0]), selfTicks()
		code, rep, _ := runOn(t, s, scen, append(c.args, "-quiet")...)
		if code != 0 || rep == nil {
			t.Fatalf("%s: exit %d", c.name, code)
		}
		n := float64(rep.Totals.OK)
		cpuMs := func(d int64) float64 { return float64(d) * 10 / n } // ticks of 10 ms, per request
		fmt.Fprintf(&out, "  %-32s %7.0f req/s  ok %d errors %d %v  p50 %.1fms p99 %.1fms ttfb p50 %.1fms   cpu ms/request: orchestrator %.2f worker %.2f this tool %.2f\n", c.name, rep.Totals.RPSCompleted, rep.Totals.OK,
			rep.Totals.PlatformErrors, reasonsOf(rep), rep.Latency["total"].P50, rep.Latency["total"].P99, rep.Latency["ttfb"].P50,
			cpuMs(cpuTicks(s.OrchPID)-orch0), cpuMs(cpuTicks(s.WorkPIDs[0])-work0), cpuMs(selfTicks()-self0))
	}
	t.Log(out.String())
}

func reasonsOf(r *Report) map[string]int64 {
	m := map[string]int64{}
	for k, v := range r.Reasons {
		m[k] = v.Count
	}
	return m
}

func selfTicks() int64 { return cpuTicks(os.Getpid()) }
