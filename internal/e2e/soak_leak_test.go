package e2e

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

// A worker that came, compiled a guest, served and left (graceful) must give
// back what it held: starting and stopping workers over and over may not grow
// the process, as a soak with worker churn would otherwise only suspect.
//
// KNOWN DEFECT MO-6 (low): worker/server.New registers an OpenTelemetry observable
// callback and never unregisters it (the orchestrator's invoke.instruments does,
// in close()). Whether or not an SDK is installed, the meter keeps the callback,
// and the callback keeps the Server, its Engine (compiled modules) and its
// artifact cache: about 9 MiB per worker lifetime for one small guest, never
// freed. cmd/worker runs one worker per process, so only a process that starts
// workers repeatedly (tests, an embedder) pays.
func TestSoakWorkerStartStopDoesNotRetainMemory(t *testing.T) {

	c := newMoCluster(t, moOpts{n: 1})
	ctl := c.A().control()
	fid := moCreateFn(t, ctl, "leak")
	moDeploy(t, ctl, fid, "wagi-1", c.wasm)
	moSetEnv(t, ctl, fid, "FN", "leak")
	moRoute(t, ctl, "leak.test", "/", fid)

	heap := func() float64 {
		moCloseIdle()
		for range 3 {
			runtime.GC()
		}
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return float64(ms.HeapInuse) / (1 << 20)
	}
	var series []float64
	var gor []int
	const cycles = 8
	for i := range cycles {
		w := c.startWorker(t, 4, moOnly(c.A()))
		moEventually(t, "the worker to serve", 90*time.Second, func() bool {
			r := moInvoke(c.A().ingress(), "leak.test", "/", fmt.Sprint("l", i), 0, 30*time.Second)
			return r.err == nil && r.status == 200
		})
		w.stop()
		if w.exitErr != nil {
			t.Fatal(w.exitErr)
		}
		moEventually(t, "its goroutines to end", 20*time.Second, func() bool { time.Sleep(200 * time.Millisecond); return true })
		series = append(series, heap())
		gor = append(gor, runtime.NumGoroutine())
	}
	if p := os.Getenv("SOAK_HEAP_PROFILE"); p != "" {
		if f, err := os.Create(p); err == nil {
			pprof.WriteHeapProfile(f)
			f.Close()
		}
	}
	t.Logf("heap in use after each of %d worker lifetimes (one guest compiled in each): %.1f MiB; goroutines %v", cycles, series, gor)
	if grown := series[cycles-1] - series[1]; grown > 40 {
		t.Errorf("heap grew %.1f MiB over %d more worker lifetimes: a stopped worker retains what it compiled", grown, cycles-2)
	}
	if gor[cycles-1] > gor[1]+30 {
		t.Errorf("goroutines grew from %d to %d over %d worker lifetimes", gor[1], gor[cycles-1], cycles-2)
	}
}
