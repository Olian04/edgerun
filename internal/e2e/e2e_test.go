package e2e

import (
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// TestDeployAndInvoke walks the operator's and the frontend's whole life with
// one function: upload, deploy, call, configure, break, roll back.
func TestDeployAndInvoke(t *testing.T) {
	s := newStack(t)
	s.startWorker(4)
	fid := s.createFunction("hello")
	v1 := s.uploadVersion(fid, s.wasm, &commonv1.Limits{MemoryLimitBytes: 32 << 20}, true)
	s.route("*", "/", fid)
	s.waitServing("e2e.test", "/echo")

	t.Run("request and response survive the whole path", func(t *testing.T) {
		big := strings.Repeat("0123456789abcdef", 100_000) // 1.6 MB
		res := s.mustCall(req{method: "POST", path: "/echo", query: "a=b", body: big})
		if res.status() != 200 || string(res.body) != big {
			t.Fatalf("status %d, body %d bytes (want %d)", res.status(), len(res.body), len(big))
		}
		if res.head.InvocationId == "" {
			t.Error("the invocation id must reach the frontend")
		}
		var ct string
		for _, h := range res.head.Http.Headers {
			if h.Name == "content-type" {
				ct = h.Value
			}
		}
		if ct != "text/x-e2e" {
			t.Errorf("content-type = %q: request headers must reach the guest and its response headers must come back", ct)
		}
	})

	t.Run("environment: plain and secret", func(t *testing.T) {
		s.setEnv(fid, "GREETING", "hello", false)
		s.setEnv(fid, "API_KEY", "s3cr3t-value", true)
		// No sleeps: a write through the control API is visible to the next request.
		env := string(s.mustCall(req{path: "/env"}).body)
		for _, want := range []string{"GREETING=hello\n", "API_KEY=s3cr3t-value\n", "REQUEST_METHOD=GET\n", "SERVER_NAME=e2e.test\n"} {
			if !strings.Contains(env, want) {
				t.Errorf("guest environment lacks %q:\n%s", want, env)
			}
		}
		listed, _ := s.fn.ListEnv(ctx, connect.NewRequest(&controlv1.ListEnvRequest{FunctionId: fid}))
		for _, e := range listed.Msg.Entries {
			if e.Name == "API_KEY" && (e.Value != "" || !e.Secret) {
				t.Errorf("secrets are write-only over the control API: %+v", e)
			}
		}
	})

	t.Run("a function's own error status is not a platform error", func(t *testing.T) {
		res := s.mustCall(req{path: "/status", query: "code=503"})
		if res.status() != 503 {
			t.Errorf("status = %d", res.status())
		}
	})

	t.Run("a crashing function is a typed platform error with its stderr", func(t *testing.T) {
		res := s.call(req{path: "/exit", query: "code=3"})
		if reason(res.err) != commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED {
			t.Fatalf("err = %v", res.err)
		}
		if !strings.Contains(res.err.Error(), "exiting on request") {
			t.Errorf("the guest's stderr should travel with a trap (for operators): %v", res.err)
		}
	})

	t.Run("limits come from the deployed version, and rollback changes behaviour at once", func(t *testing.T) {
		// v1 has a 32 MiB limit: a 60 MiB allocation traps.
		if reason(s.call(req{path: "/memhog", query: "mb=60"}).err) != commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED {
			t.Fatal("v1 (32 MiB) should not fit 60 MiB")
		}
		// v2: same code, a roomier limit.
		v2 := s.uploadVersion(fid, padded(s.wasm), &commonv1.Limits{MemoryLimitBytes: 256 << 20}, true)
		if res := s.call(req{path: "/memhog", query: "mb=60"}); res.err != nil {
			t.Fatalf("v2 (256 MiB) must fit it, immediately after deploy: %v", res.err)
		}
		// Rollback is just another pointer move.
		if _, err := s.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: v1})); err != nil {
			t.Fatal(err)
		}
		if reason(s.call(req{path: "/memhog", query: "mb=60"}).err) != commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED {
			t.Error("after rollback to v1 the 32 MiB limit must apply again")
		}
		s.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: v2}))
	})

	t.Run("routing by host and path", func(t *testing.T) {
		other := s.createFunction("other")
		s.uploadVersion(other, s.wasm, nil, true)
		s.setEnv(other, "WHO", "the-other-function", false)
		// The whole path reaches the function, so prefixes are chosen to land on
		// paths the guest answers distinctively ("/zzz" is its 404).
		s.route("api.example.com", "/zzz", other)
		s.route("*.example.com", "/", other)

		first := func(host, path string) string { return string(s.mustCall(req{host: host, path: path}).body) }
		if got := s.mustCall(req{host: "API.Example.COM:8443", path: "/zzz/deep"}); got.status() != 404 || string(got.body) != "unknown path\n" {
			t.Errorf("exact host + prefix (case and port insensitive) must reach the other function: %d %q", got.status(), got.body)
		}
		if got := first("www.example.com", "/env"); !strings.Contains(got, "WHO=the-other-function\n") {
			t.Error("a wildcard host must reach the other function")
		}
		if got := first("api.example.com", "/env"); !strings.Contains(got, "WHO=the-other-function\n") {
			t.Error("a host-specific route with no matching prefix must fall through to the wildcard host, not to '*'")
		}
		if got := first("unrelated.test", "/env"); strings.Contains(got, "WHO=") {
			t.Error("'*' must reach the first function")
		}
		if got := first("example.com", "/env"); strings.Contains(got, "WHO=") {
			t.Error("'*.example.com' must not match its own apex")
		}
	})

	t.Run("no route, no deployment, body limit", func(t *testing.T) {
		empty := s.createFunction("undeployed")
		s.route("undeployed.test", "/", empty)
		if r := s.call(req{host: "undeployed.test", path: "/"}); reason(r.err) != commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND {
			t.Errorf("function without a version: %v", r.err)
		}
		// Just over the limit: the orchestrator answers with the typed reason.
		if r := s.call(req{method: "POST", path: "/echo", body: strings.Repeat("x", 4<<20+1)}); reason(r.err) != commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE {
			t.Errorf("4 MiB + 1 byte against a 4 MiB limit: %v", r.err)
		}
		// Far over: the transport's own message cap rejects it before any
		// decoding, with no typed reason. This is why frontends must enforce the
		// limit reported by Describe themselves (docs/protocol.md).
		if r := s.call(req{method: "POST", path: "/echo", body: strings.Repeat("x", 5<<20)}); connect.CodeOf(r.err) != connect.CodeResourceExhausted {
			t.Errorf("5 MiB body: %v", r.err)
		}
	})

	t.Run("every routed invocation is recorded", func(t *testing.T) {
		var recs []*controlv1.Invocation
		eventually(t, "records to be flushed", 10*time.Second, func() bool {
			recs = s.invocations(fid)
			return len(recs) >= 8
		})
		byPath := map[string]*controlv1.Invocation{}
		for _, r := range recs {
			byPath[r.Path+"?"+""] = r
		}
		var echo, trapped, status503 *controlv1.Invocation
		for _, r := range recs {
			switch {
			case r.Path == "/echo" && r.Method == "POST" && echo == nil:
				echo = r
			case r.Path == "/exit":
				trapped = r
			case r.Path == "/status":
				status503 = r
			}
		}
		if echo == nil || echo.Status != 200 || echo.FunctionId != fid || echo.WorkerId == "" || echo.VersionId == "" ||
			echo.DurationMicros == 0 || echo.PeakMemoryBytes == 0 || echo.StartTime == nil || echo.ErrorReason != 0 {
			t.Errorf("echo record = %+v", echo)
		}
		if trapped == nil || trapped.ErrorReason != commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED || !strings.Contains(string(trapped.Stderr), "exiting on request") {
			t.Errorf("trap record = %+v", trapped)
		}
		if status503 == nil || status503.Status != 503 || status503.ErrorReason != 0 {
			t.Errorf("a function's 503 is a success with status 503: %+v", status503)
		}
		// Unrouted requests are not recorded (a scan would flood the table).
		for _, r := range recs {
			if r.FunctionId == "" {
				t.Errorf("record without a function: %+v", r)
			}
		}
	})
}

// TestFailoverAndCapacity kills and adds workers under traffic.
func TestFailoverAndCapacity(t *testing.T) {
	s := newStack(t)
	w1, w2 := s.startWorker(4), s.startWorker(4)
	fid := s.createFunction("svc")
	s.uploadVersion(fid, s.wasm, nil, true)
	s.route("*", "/", fid)
	eventually(t, "both workers alive", 60*time.Second, func() bool { return s.aliveWorkers() == 2 })
	s.waitServing("e2e.test", "/echo")

	t.Run("a burst above one worker's slots spills to the next worker", func(t *testing.T) {
		// Every call for a module prefers the same worker (warm code), which has
		// 4 slots. Six concurrent slow calls fit the fleet's 8 slots but not
		// that worker's 4: the overflow is refused (never queued) and retried
		// on the other worker, so every call succeeds and both workers serve.
		var wg sync.WaitGroup
		for i := range 6 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if r := s.call(req{path: "/sleep", query: "ms=600"}); r.err != nil {
					t.Errorf("call %d: %v", i, r.err)
				}
			}()
		}
		wg.Wait()
		eventually(t, "records", 10*time.Second, func() bool { return len(s.invocations(fid)) >= 6 })
		workers := map[string]int{}
		for _, r := range s.invocations(fid) {
			if r.Path == "/sleep" {
				workers[r.WorkerId]++
			}
		}
		if len(workers) != 2 {
			t.Errorf("served by %d worker(s) %v; the overflow must spill to the second", len(workers), workers)
		}
	})

	t.Run("beyond the whole fleet's capacity the answer is NO_CAPACITY, not a queue", func(t *testing.T) {
		var wg sync.WaitGroup
		var mu sync.Mutex
		refused, served := 0, 0
		for range 14 { // fleet capacity is 8
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := s.call(req{path: "/sleep", query: "ms=800"})
				mu.Lock()
				defer mu.Unlock()
				switch {
				case r.err == nil:
					served++
				case reason(r.err) == commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY:
					refused++
				default:
					t.Errorf("unexpected: %v", r.err)
				}
			}()
		}
		wg.Wait()
		if served < 8 || refused == 0 || served+refused != 14 {
			t.Errorf("served %d, refused %d of 14: every slot should be used and the excess refused", served, refused)
		}
	})

	t.Run("a worker leaving is invisible to callers", func(t *testing.T) {
		w1.stop() // graceful: drains, deregisters
		eventually(t, "one worker left", 10*time.Second, func() bool { return s.aliveWorkers() == 1 })
		for i := range 20 {
			if r := s.call(req{path: "/echo"}); r.err != nil {
				t.Fatalf("call %d after a worker left: %v", i, r.err)
			}
		}
	})

	t.Run("no workers is NO_CAPACITY, and a new worker restores service", func(t *testing.T) {
		w2.stop()
		eventually(t, "no workers", 10*time.Second, func() bool { return s.aliveWorkers() == 0 })
		eventually(t, "the orchestrator to notice", 10*time.Second, func() bool {
			return reason(s.call(req{path: "/echo"}).err) == commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY
		})
		s.startWorker(4)
		s.waitServing("e2e.test", "/echo")
	})
}

// TestCapacityIsNeverQueued: a one-slot worker refuses a second concurrent call
// at once instead of making it wait.
func TestCapacityIsNeverQueued(t *testing.T) {
	s := newStack(t)
	s.startWorker(1)
	fid := s.createFunction("svc")
	s.uploadVersion(fid, s.wasm, nil, true)
	s.route("*", "/", fid)
	s.waitServing("e2e.test", "/echo")

	slow := make(chan result, 1)
	go func() { slow <- s.call(req{path: "/sleep", query: "ms=1500"}) }()
	time.Sleep(300 * time.Millisecond) // let it occupy the slot

	start := time.Now()
	second := s.call(req{path: "/echo"})
	if reason(second.err) != commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY {
		t.Fatalf("second call = %v, want NO_CAPACITY", second.err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("NO_CAPACITY took %v: it must be immediate, not a queue", d)
	}
	if r := <-slow; r.err != nil {
		t.Errorf("the admitted call was affected: %v", r.err)
	}
	if r := s.call(req{path: "/echo"}); r.err != nil {
		t.Errorf("the slot must be free again: %v", r.err)
	}
}

// TestDeadlinesStopGuestsAndFreeSlots: a runaway function costs its budget, and
// nothing more.
func TestDeadlinesStopGuestsAndFreeSlots(t *testing.T) {
	s := newStack(t)
	s.startWorker(1) // one slot: a leak would be immediately visible
	fid := s.createFunction("svc")
	s.uploadVersion(fid, s.wasm, nil, true)
	s.route("*", "/", fid)
	s.waitServing("e2e.test", "/echo")

	for _, path := range []string{"/spin", "/sleep?ms=60000"} {
		p, q, _ := strings.Cut(path, "?")
		res := s.call(req{path: p, query: q, budget: 600 * time.Millisecond})
		if reason(res.err) != commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED {
			t.Fatalf("%s: %v", path, res.err)
		}
		if res.elapsed > 10*time.Second {
			t.Errorf("%s: a 600ms budget took %v", path, res.elapsed)
		}
		// The slot must come back: with a leak this call would be NO_CAPACITY.
		eventually(t, "the slot to be released after "+path, 10*time.Second, func() bool {
			return s.call(req{path: "/echo"}).err == nil
		})
	}
}

// TestOrchestratorRestartIsInvisible: all state is in PostgreSQL, and workers
// and frontends simply carry on.
func TestOrchestratorRestartIsInvisible(t *testing.T) {
	s := &stack{t: t, dsn: testpgDSN(t)}
	s.wasm = wasmBytes(t)
	addr := freePort(t)
	s.startOrchestrator(addr)
	s.startWorker(4)
	fid := s.createFunction("persist")
	s.uploadVersion(fid, s.wasm, nil, true)
	s.route("*", "/", fid)
	s.setEnv(fid, "KEPT", "across-restarts", true)
	s.waitServing("e2e.test", "/env")

	if err := s.orch.Stop(ctx); err != nil {
		t.Fatalf("graceful stop: %v", err)
	}
	if r := s.call(req{path: "/echo"}); r.err == nil {
		t.Fatal("expected the stopped orchestrator to refuse requests")
	}

	s.startOrchestrator(addr) // same address, same database
	t.Cleanup(func() { s.orch.Stop(ctx) })
	eventually(t, "service to resume", 60*time.Second, func() bool { return s.call(req{path: "/env"}).err == nil })
	if got := string(s.mustCall(req{path: "/env"}).body); !strings.Contains(got, "KEPT=across-restarts\n") {
		t.Errorf("routes, versions and sealed secrets must survive a restart:\n%s", got)
	}
	// The worker never re-registered or noticed: its registration was in the database.
	if n := s.aliveWorkers(); n != 1 {
		t.Errorf("alive workers = %d", n)
	}
}

// padded returns wasm with a trailing custom section: still a valid module, but
// with a different digest, so it uploads as a distinct version.
func padded(wasm []byte) []byte {
	payload := append([]byte{3}, "pad"...)
	payload = append(payload, make([]byte, 16)...)
	out := append([]byte{}, wasm...)
	out = append(out, 0, byte(len(payload)))
	return append(out, payload...)
}
