package orchsuite

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

// ExecutionClient judges the orchestrator as the client of worker.v1
// ExecutionService: what it asks of a worker (the request, the function, the
// credentials, the budget, the destinations the function may reach), which worker
// it picks, how it retries, and how it reports what a worker answers. The suite
// plays the workers: scripted fake workers that register through Membership and
// answer whatever the test says.
func ExecutionClient(t *testing.T, target Target) {
	h := start(t, target)
	t.Run("Forwarding", h.invokeForwarding)
	t.Run("ABIScheduling", h.abiScheduling)
	t.Run("Retries", h.retries)
	t.Run("ErrorPassthrough", h.errorPassthrough)
	t.Run("TraceContext", h.traceContext)
	// The destinations a function may reach are decided by Catalog (SetEgress) and
	// Membership (which network services are registered), and delivered to the
	// worker in ExecuteRequest.egress: so it is judged here, as what Execute carries.
	t.Run("EgressReachesTheWorker", h.egressReachesTheWorker)
	t.Run("NetworkRouting", h.networkRouting)
}

// stack is a worker, and a function deployed to it.
func (h *harness) stack(t testing.TB, b behaviour) (*fakeWorker, *fixture) {
	t.Helper()
	abi := h.abi()
	w := h.worker(t, []string{abi}, b)
	return w, h.deploy(t, abi)
}

// reached invokes f until w has been asked to run something, whatever it
// answered: for tests whose worker answers with a failure, where "served"
// has no success to wait for.
func (h *harness) reached(t testing.TB, f *fixture, w *fakeWorker) {
	t.Helper()
	check.Eventually(t, "the worker to be scheduled", settle, func() bool {
		h.invoke(t, call{host: f.host})
		return w.callCount() > 0
	})
}

// invokeForwarding checks that what the frontend sent arrives at the worker as
// the protocol says, and what the worker answers arrives back unchanged.
func (h *harness) invokeForwarding(t *testing.T) {
	abi := h.abi()
	answer := []*commonv1.Header{hdr("x-answer", "1"), hdr("x-answer", "2"), hdr("content-type", "text/x-conformance")}
	w := h.worker(t, []string{abi}, respondWith(201, "payload", "a log line", answer))

	limits := &commonv1.Limits{MemoryLimitBytes: 48 << 20, MaxResponseBytes: 2 << 20}
	fn := h.createFunction(t, h.name("fwd"))
	module := h.module("fwd")
	v := h.createVersion(t, fn.GetId(), abi, module, limits, true)
	host := h.host()
	h.createRoute(t, host, "/", fn.GetId())
	h.setEnv(t, fn.GetId(), "PLAIN", "visible", false)
	h.setEnv(t, fn.GetId(), "SECRET", "s3cret", true)
	h.served(t, host)

	body := []byte("binary \x00\x01\xff body")
	reqHeaders := []*commonv1.Header{hdr("x-one", "a"), hdr("x-one", "b"), hdr("content-type", "text/plain")}
	before := w.callCount()
	r := h.mustInvoke(t, call{
		method: "POST", host: host, path: "/p/q", rawQuery: "a=1&b=%20x", body: body, headers: reqHeaders,
		requestID: "req-" + h.run, budget: 20 * time.Second,
	})
	calls := w.calls()
	if len(calls) != before+1 {
		t.Fatalf("one Invoke made %d calls to the worker", len(calls)-before)
	}
	got := calls[len(calls)-1]
	req := got.req

	t.Run("the request", func(t *testing.T) {
		hr := req.GetRequest()
		if hr.GetMethod() != "POST" || hr.GetHost() != host || hr.GetPath() != "/p/q" || hr.GetRawQuery() != "a=1&b=%20x" {
			t.Errorf("request line: %s %s %s ? %s", hr.GetMethod(), hr.GetHost(), hr.GetPath(), hr.GetRawQuery())
		}
		if string(hr.GetBody()) != string(body) {
			t.Errorf("body = %q", hr.GetBody())
		}
		if hr.GetRemoteAddr() != "192.0.2.7:4321" {
			t.Errorf("remote_addr = %q, want the one the frontend reported", hr.GetRemoteAddr())
		}
		if fmt.Sprint(hr.GetHeaders()) != fmt.Sprint(reqHeaders) {
			t.Errorf("headers = %v, want them unchanged and in order: %v", hr.GetHeaders(), reqHeaders)
		}
	})
	t.Run("the function", func(t *testing.T) {
		if req.GetArtifactDigest() != sha(module) || req.GetArtifactDigest() != v.GetArtifactDigest() {
			t.Errorf("artifact_digest = %s, want %s", req.GetArtifactDigest(), sha(module))
		}
		if req.GetAbi() != abi {
			t.Errorf("abi = %q, want %q", req.GetAbi(), abi)
		}
		if req.GetLimits().GetMemoryLimitBytes() != limits.MemoryLimitBytes || req.GetLimits().GetMaxResponseBytes() != limits.MaxResponseBytes {
			t.Errorf("limits = %v, want %v", req.GetLimits(), limits)
		}
		env := map[string]string{}
		for _, e := range req.GetEnv() {
			env[e.GetName()] = e.GetValue()
		}
		if env["PLAIN"] != "visible" || env["SECRET"] != "s3cret" {
			t.Errorf("env = %v: secrets are write-only for operators, but the worker needs them in clear", env)
		}
	})
	t.Run("the call", func(t *testing.T) {
		if got.header.Get("Authorization") != "Bearer "+h.env.ExecutionToken {
			t.Errorf("Authorization = %q, want the execution token", got.header.Get("Authorization"))
		}
		ms, err := strconv.Atoi(got.header.Get("Connect-Timeout-Ms"))
		if err != nil || ms <= 0 || ms > 20_000 {
			t.Errorf("Connect-Timeout-Ms = %q: the client's deadline (20s) must reach the worker", got.header.Get("Connect-Timeout-Ms"))
		}
		if req.GetInvocationId() == "" || req.GetInvocationId() != r.invocationID {
			t.Errorf("invocation_id: worker %q, frontend %q: they must be the same, and set", req.GetInvocationId(), r.invocationID)
		}
	})
	t.Run("the response", func(t *testing.T) {
		if r.head.GetHttp().GetStatus() != 201 {
			t.Errorf("status = %d", r.head.GetHttp().GetStatus())
		}
		if fmt.Sprint(r.head.GetHttp().GetHeaders()) != fmt.Sprint(answer) {
			t.Errorf("headers = %v, want %v", r.head.GetHttp().GetHeaders(), answer)
		}
		if string(r.body) != "payload" {
			t.Errorf("body = %q", r.body)
		}
	})
	t.Run("invocations have distinct ids", func(t *testing.T) {
		again := h.mustInvoke(t, call{host: host})
		if again.invocationID == "" || again.invocationID == r.invocationID {
			t.Errorf("invocation ids %q and %q must be distinct", r.invocationID, again.invocationID)
		}
	})
}

func (h *harness) abiScheduling(t *testing.T) {
	a, b := h.abi(), h.abi()
	only := h.worker(t, []string{a}, respond("a"))
	fa := h.deploy(t, a)
	fb := h.deploy(t, b)
	h.served(t, fa.host)

	t.Run("no worker supports the ABI", func(t *testing.T) {
		r := h.invoke(t, call{host: fb.host})
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
		if only.callCount() != 1 { // only the warm-up of the other function
			for _, c := range only.calls() {
				if c.req.GetAbi() != a {
					t.Errorf("a module built for %q was sent to a worker that does not list it", c.req.GetAbi())
				}
			}
		}
	})

	t.Run("one worker, several ABIs", func(t *testing.T) {
		h.worker(t, []string{a, b}, respond("both"))
		check.Eventually(t, "a worker for the second ABI", settle, func() bool {
			return h.invoke(t, call{host: fb.host}).err == nil
		})
		if r := h.mustInvoke(t, call{host: fb.host}); bodyOf(r) != "both" {
			t.Errorf("served by %q, want the worker that lists the ABI", bodyOf(r))
		}
	})
}

// many deploys n functions, so that n different digests make the scheduler
// try the workers in different orders.
func (h *harness) many(t testing.TB, abi string, n int) []*fixture {
	t.Helper()
	var out []*fixture
	for i := 0; i < n; i++ {
		out = append(out, h.deploy(t, abi))
	}
	return out
}

func (h *harness) retries(t *testing.T) {
	const n = 24

	t.Run("NO_CAPACITY goes to the next worker", func(t *testing.T) {
		abi := h.abi()
		busy := h.worker(t, []string{abi}, fail(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, "full"))
		free := h.worker(t, []string{abi}, respond("ok"))
		fs := h.many(t, abi, n)
		h.served(t, fs[0].host)
		check.Eventually(t, "the full worker to be tried at least once", settle, func() bool {
			for _, f := range fs {
				if r := h.invoke(t, call{host: f.host}); r.err != nil {
					t.Fatalf("a refusal by one worker must not fail the request while another can serve it: %v", r.err)
				}
			}
			return busy.callCount() > 0
		})
		if free.callCount() < n {
			t.Errorf("the free worker ran %d of the %d requests", free.callCount(), n)
		}
	})

	t.Run("at most three attempts", func(t *testing.T) {
		abi := h.abi()
		var ws []*fakeWorker
		for i := 0; i < 4; i++ {
			ws = append(ws, h.worker(t, []string{abi}, fail(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, "full")))
		}
		f := h.deploy(t, abi)
		total := func() (n int) {
			for _, w := range ws {
				n += w.callCount()
			}
			return
		}
		attempts := func() int {
			before := total()
			r := h.invoke(t, call{host: f.host})
			if r.err != nil {
				check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY)
			}
			return total() - before
		}
		// Every worker is refusing, so each request is refused after however many
		// attempts the orchestrator makes. Watch it for several refresh intervals,
		// by which time all four workers are known: it must settle at three.
		check.Eventually(t, "three attempts", settle, func() bool { return attempts() == 3 })
		for end := time.Now().Add(3 * time.Second); time.Now().Before(end); {
			if n := attempts(); n != 3 {
				t.Fatalf("%d attempts with four refusing workers, want 3", n)
			}
		}
	})

	t.Run("an unreachable worker is skipped", func(t *testing.T) {
		abi := h.abi()
		gone := h.worker(t, []string{abi}, respond("ok"))
		live := h.worker(t, []string{abi}, respond("ok"))
		fs := h.many(t, abi, n)
		h.served(t, fs[0].host)
		// Both workers must be known to the orchestrator before one goes away, or
		// there would be no other to try. Which is asked first depends on the module.
		check.Eventually(t, "both workers to be scheduled", settle, func() bool {
			for _, f := range fs {
				h.invoke(t, call{host: f.host})
			}
			return live.callCount() > 0 && gone.callCount() > 0
		})
		gone.closeListener() // still heartbeating: the orchestrator thinks it is there
		for _, f := range fs {
			if r := h.invoke(t, call{host: f.host}); r.err != nil || bodyOf(r) != "ok" {
				t.Fatalf("a worker that cannot be reached cannot have run the request, so another must be tried: %q, %v", bodyOf(r), r.err)
			}
		}
		if live.callCount() < n {
			t.Errorf("the live worker ran %d of %d requests", live.callCount(), n)
		}
	})

	// For these, which worker is asked first depends on the module, so many
	// modules are tried until both workers have been asked first at least once;
	// then every request must have cost exactly one attempt.
	exactlyOnce := func(t *testing.T, abi string, a, b *fakeWorker, check1 func(f *fixture, r result)) {
		t.Helper()
		fs := h.many(t, abi, n)
		attempts := func() int { return a.callCount() + b.callCount() }
		check.Eventually(t, "both workers to be asked", settle, func() bool {
			for _, f := range fs {
				h.invoke(t, call{host: f.host})
			}
			return a.callCount() > 0 && b.callCount() > 0
		})
		for _, f := range fs {
			before := attempts()
			r := h.invoke(t, call{host: f.host})
			check1(f, r)
			if got := attempts() - before; got != 1 {
				t.Errorf("%d attempts, want exactly 1", got)
			}
		}
	}

	t.Run("a failure that may have had effects is not retried", func(t *testing.T) {
		for _, reason := range []commonv1.ErrorReason{
			commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED,
			commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED,
			commonv1.ErrorReason_ERROR_REASON_INTERNAL,
		} {
			abi := h.abi()
			a := h.worker(t, []string{abi}, fail(reason, "failed"))
			b := h.worker(t, []string{abi}, fail(reason, "failed"))
			exactlyOnce(t, abi, a, b, func(_ *fixture, r result) { check.WantReason(t, r.err, reason) })
		}
	})

	t.Run("nothing is retried after the response began", func(t *testing.T) {
		for _, reason := range []commonv1.ErrorReason{commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED} {
			abi := h.abi()
			a := h.worker(t, []string{abi}, headThenFail(reason))
			b := h.worker(t, []string{abi}, headThenFail(reason))
			exactlyOnce(t, abi, a, b, func(_ *fixture, r result) {
				if r.head == nil || string(r.body) != "partial" {
					t.Errorf("%v: the part of the response that was produced must reach the client: head %v, body %q", reason, r.head, r.body)
				}
				check.WantReason(t, r.err, reason) // and the stream must end in an error, not as if it had finished
			})
		}
	})
}

func (h *harness) errorPassthrough(t *testing.T) {
	t.Run("worker errors keep their reason", func(t *testing.T) {
		for _, reason := range []commonv1.ErrorReason{
			commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED,
			commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED,
			commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH,
			commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE,
			commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND,
			commonv1.ErrorReason_ERROR_REASON_INTERNAL,
		} {
			w, f := h.stack(t, fail(reason, "the worker's message"))
			h.reached(t, f, w)
			r := h.invoke(t, call{host: f.host})
			check.WantReason(t, r.err, reason)
		}
	})

	t.Run("untyped worker errors do not leak", func(t *testing.T) {
		w, f := h.stack(t, func(context.Context, *workerv1.ExecuteRequest, *connect.ServerStream[workerv1.ExecuteResponse]) error {
			return connect.NewError(connect.CodeUnknown, errors.New("secret-internal-detail: dial postgres://user:pw@db"))
		})
		h.reached(t, f, w)
		r := h.invoke(t, call{host: f.host})
		msg := check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_INTERNAL)
		if strings.Contains(msg, "secret-internal-detail") || strings.Contains(r.err.Error(), "secret-internal-detail") {
			t.Errorf("an error the worker did not type must not be forwarded as it is: %v", r.err)
		}
	})

	t.Run("the deadline reaches the worker and is enforced", func(t *testing.T) {
		started := make(chan struct{}, 8)
		ended := make(chan error, 8)
		w, f := h.stack(t, func(ctx context.Context, req *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
			if req.GetRequest().GetPath() != "/slow" {
				return respond("warm")(ctx, req, st)
			}
			started <- struct{}{}
			select {
			case <-ctx.Done():
				ended <- ctx.Err()
				return ctx.Err()
			case <-time.After(30 * time.Second):
				ended <- nil
				return respond("too late")(ctx, req, st)
			}
		})
		h.served(t, f.host)
		_ = w
		begin := time.Now()
		r := h.invoke(t, call{host: f.host, path: "/slow", budget: time.Second})
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED)
		if took := time.Since(begin); took > 10*time.Second {
			t.Errorf("answered after %v with a budget of 1s", took)
		}
		select {
		case err := <-ended:
			if err == nil {
				t.Error("the worker was never told to stop")
			}
		case <-time.After(10 * time.Second):
			t.Error("the worker was still running 10s after the budget ran out: the deadline must reach it")
		}
	})

	t.Run("a client that goes away stops the worker", func(t *testing.T) {
		ended := make(chan error, 8)
		started := make(chan struct{}, 8)
		_, f := h.stack(t, func(ctx context.Context, req *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
			if req.GetRequest().GetPath() != "/hang" {
				return respond("warm")(ctx, req, st)
			}
			started <- struct{}{}
			select {
			case <-ctx.Done():
				ended <- ctx.Err()
			case <-time.After(30 * time.Second):
				ended <- nil
			}
			return ctx.Err()
		})
		h.served(t, f.host)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			st, err := h.ing.Invoke(ctx, connect.NewRequest(&ingressv1.InvokeRequest{
				Request: &commonv1.HttpRequest{Method: "GET", Host: f.host, Path: "/hang"}}))
			if err == nil {
				for st.Receive() {
				}
				st.Close()
			}
		}()
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("the request never reached the worker")
		}
		cancel()
		select {
		case err := <-ended:
			if err == nil {
				t.Error("the worker kept running for a client that had gone")
			}
		case <-time.After(10 * time.Second):
			t.Error("cancelling the client's call did not stop the worker")
		}
		// And the record says why it has no status: the caller gave up.
		check.Eventually(t, "the record of the abandoned invocation", settle, func() bool {
			for _, r := range h.records(t, f.fn.GetId(), 0) {
				if r.GetPath() == "/hang" {
					return r.GetErrorReason() == commonv1.ErrorReason_ERROR_REASON_CANCELLED && r.GetStatus() == 0
				}
			}
			return false
		})
	})
}

// egressReachesTheWorker: the allow list the operator set is what the worker is
// told, with nothing added, and a function that was given none is told none.
func (h *harness) egressReachesTheWorker(t *testing.T) {
	ctx, cancel := ctxT(60 * time.Second)
	defer cancel()
	svc := h.netService(t, "*")
	w, f := h.stack(t, respond("ok"))
	h.served(t, f.host)

	h.mustInvoke(t, call{host: f.host})
	if got := lastGrants(w); len(got) != 0 {
		t.Errorf("a function nobody opened up was told %v: closed is the default", got)
	}

	// From here on every check is the very next request.
	set := func(allow ...string) {
		t.Helper()
		if _, err := h.fn.SetEgress(ctx, connect.NewRequest(&controlv1.SetEgressRequest{FunctionId: f.fn.GetId(), Allow: allow})); err != nil {
			t.Fatal(err)
		}
	}
	set("api.example.com", "*.cdn.example.com:8443")
	check.Eventually(t, "the service to be known", settle, func() bool {
		h.mustInvoke(t, call{host: f.host})
		return len(lastGrants(w)) == 2
	})
	want := "[*.cdn.example.com:8443=" + svc.endpoint + " api.example.com=" + svc.endpoint + "]"
	if got := fmt.Sprint(lastGrants(w)); got != want {
		t.Errorf("the worker was told %s, want %s: the function's list in canonical form, each with its service", got, want)
	}
	set()
	h.mustInvoke(t, call{host: f.host})
	if got := lastGrants(w); len(got) != 0 {
		t.Errorf("a list that was cleared reached the worker as %v", got)
	}
	// A new version keeps the list: it belongs to the function.
	set("kept.example.com")
	h.createVersion(t, f.fn.GetId(), f.abi, h.module("v2"), nil, true)
	h.mustInvoke(t, call{host: f.host})
	if got := fmt.Sprint(lastGrants(w)); got != "[kept.example.com="+svc.endpoint+"]" {
		t.Errorf("after a deploy the worker was told %s: the list is the function's, not a version's", got)
	}
}

// traceContext: the orchestrator continues the trace of an Invoke in the Execute
// it makes, and says in the record which trace the invocation belongs to
// (docs/protocol.md, "Observability").
func (h *harness) traceContext(t *testing.T) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	w, f := h.stack(t, respond("ok"))
	h.served(t, f.host)
	before := len(w.calls())
	h.mustInvoke(t, call{host: f.host, requestID: "trace-" + h.run, traceparent: "00-" + traceID + "-00f067aa0ba902b7-01"})

	calls := w.calls()
	if len(calls) <= before {
		t.Fatal("the worker was not asked to run anything")
	}
	tp := calls[len(calls)-1].header.Get("Traceparent")
	if parts := strings.Split(tp, "-"); len(parts) != 4 || parts[1] != traceID {
		t.Errorf("the Execute carries traceparent %q: an Invoke that carried a trace is continued in the call to the worker (trace id %s)", tp, traceID)
	}
	if b := calls[len(calls)-1].header.Get("Baggage"); b != "" {
		t.Errorf("the Execute carries baggage %q: it is not propagated", b)
	}

	check.Eventually(t, "the record to name the trace", settle, func() bool {
		for _, r := range h.records(t, f.fn.GetId(), 0) {
			if r.GetRequestId() == "trace-"+h.run {
				return r.GetTraceId() == traceID
			}
		}
		return false
	})
}
