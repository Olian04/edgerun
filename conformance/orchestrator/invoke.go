package orchestratorconf

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

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

// --- forwarding ------------------------------------------------------------------

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

// invokeStreaming: the response is forwarded as it is produced, not held back
// until it is complete.
func (h *harness) invokeStreaming(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	abi := h.abi()
	h.worker(t, []string{abi}, func(ctx context.Context, req *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
		if req.GetRequest().GetPath() != "/stream" {
			return respond("warm")(ctx, req, st)
		}
		if err := send(st, head(200)); err != nil {
			return err
		}
		if err := send(st, chunk("first,")); err != nil {
			return err
		}
		select {
		case <-release: // the client has seen the first chunk
		case <-time.After(10 * time.Second):
			return check.Error(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "the first chunk was never forwarded")
		}
		for _, c := range []string{"second,", "third"} {
			if err := send(st, chunk(c)); err != nil {
				return err
			}
		}
		return send(st, summary(""))
	})
	f := h.deploy(t, abi)
	h.served(t, f.host)

	ctx, cancel := ctxT(30 * time.Second)
	defer cancel()
	st, err := h.ing.Invoke(ctx, connect.NewRequest(&ingressv1.InvokeRequest{
		Request: &commonv1.HttpRequest{Method: "GET", Host: f.host, Path: "/stream"}}))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var body []byte
	gotHead := false
	for st.Receive() {
		switch p := st.Msg().GetPart().(type) {
		case *ingressv1.InvokeResponse_Head:
			gotHead = true
		case *ingressv1.InvokeResponse_Chunk:
			body = append(body, p.Chunk...)
			once.Do(func() { close(release) })
		}
	}
	if err := st.Err(); err != nil {
		t.Fatalf("a response held back until it was complete cannot be streamed to a client: %v", err)
	}
	if !gotHead || string(body) != "first,second,third" {
		t.Errorf("head %v, body %q", gotHead, body)
	}
}

// invokeValidation: what the orchestrator refuses before it picks a worker.
func (h *harness) invokeValidation(t *testing.T) {
	ctx, cancel := ctxT(60 * time.Second)
	defer cancel()

	t.Run("no request", func(t *testing.T) {
		st, err := h.ing.Invoke(ctx, connect.NewRequest(&ingressv1.InvokeRequest{}))
		if err == nil {
			defer st.Close()
			for st.Receive() {
			}
			err = st.Err()
		}
		check.WantCode(t, err, connect.CodeInvalidArgument)
	})

	t.Run("no route", func(t *testing.T) {
		r := h.invoke(t, call{host: "nothing-routed-" + h.run + ".conformance.test", path: "/"})
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)
	})

	t.Run("a function without an active version", func(t *testing.T) {
		fn := h.createFunction(t, h.name("inv-nov"))
		host := h.host()
		h.createRoute(t, host, "/", fn.GetId())
		h.createVersion(t, fn.GetId(), "wagi-1", h.module("inactive"), nil, false)
		check.WantReason(t, h.invoke(t, call{host: host}).err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)
	})

	t.Run("body size", func(t *testing.T) {
		d, err := h.ing.Describe(ctx, connect.NewRequest(&ingressv1.DescribeRequest{}))
		if err != nil {
			t.Fatal(err)
		}
		limit := int(d.Msg.GetMaxRequestBodyBytes())
		_, f := h.stack(t, respond("ok"))
		h.served(t, f.host)
		r := h.invoke(t, call{method: "POST", host: f.host, body: make([]byte, limit+1)})
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE)
		if r := h.invoke(t, call{method: "POST", host: f.host, body: make([]byte, limit)}); r.err != nil {
			t.Errorf("a body of exactly max_request_body_bytes (%d) must be accepted: %v", limit, r.err)
		}
	})
}

// --- routing -----------------------------------------------------------------------

// routed deploys a function that answers with its own name and routes
// (host, prefix) to it.
func (h *harness) routed(t testing.TB, abi, host, prefix, who string) *controlv1.Function {
	t.Helper()
	fn := h.createFunction(t, h.name("rt"))
	h.createVersion(t, fn.GetId(), abi, h.module(who), nil, true)
	h.setEnv(t, fn.GetId(), "WHO", who, false)
	h.createRoute(t, host, prefix, fn.GetId())
	return fn
}

func (h *harness) routingPrecedence(t *testing.T) {
	abi := h.abi()
	h.worker(t, []string{abi}, echoEnv("WHO"))
	b := fmt.Sprintf("p%s.prec.test", h.run)

	h.routed(t, abi, "a."+b, "/", "exact")
	h.routed(t, abi, "*."+b, "/api", "wild-api")
	h.routed(t, abi, "*."+b, "/api/v1", "wild-api-v1")
	h.routed(t, abi, "*."+b, "/", "wild-root")
	h.routed(t, abi, "*.deep."+b, "/", "deep")
	h.routed(t, abi, "c."+b, "/only", "c-only")

	// Wait for the orchestrator to have seen the worker: the control changes
	// above are visible at once, the new worker is not promised to be.
	h.served(t, "a."+b)

	for _, c := range []struct{ host, path, want, why string }{
		{"a." + b, "/api/v1/x", "exact", "an exact host beats a wildcard even where the wildcard's prefix is longer"},
		{"b." + b, "/api/v1/x", "wild-api-v1", "the longest matching prefix wins"},
		{"b." + b, "/api/v1", "wild-api-v1", "a prefix matches the path equal to it"},
		{"b." + b, "/api/v2", "wild-api", "a shorter prefix when the longer one does not match"},
		{"b." + b, "/api", "wild-api", "a prefix matches the path equal to it"},
		{"b." + b, "/apix", "wild-root", "prefixes match whole path segments: /api is not a prefix of /apix"},
		{"b." + b, "/api/v1x", "wild-api", "prefixes match whole path segments: /api/v1 is not a prefix of /api/v1x"},
		{"b." + b, "/other", "wild-root", "the root prefix matches everything"},
		{"x.deep." + b, "/api", "deep", "the wildcard with the longer suffix is more specific"},
		{"c." + b, "/only/y", "c-only", "an exact host with a matching prefix"},
		{"c." + b, "/api", "wild-api", "an exact host with no matching prefix falls through to less specific hosts"},
		{"A." + strings.ToUpper(b), "/", "exact", "hosts are compared case-insensitively"},
		{"a." + b + ":8080", "/", "exact", "the port is not part of the host"},
		{"a." + b + ".", "/", "exact", "a trailing dot is not part of the host"},
		{"b." + b, "/x/../api/v1", "wild-root", "paths are matched as given, not cleaned"},
	} {
		r := h.invoke(t, call{host: c.host, path: c.path})
		if r.err != nil || bodyOf(r) != c.want {
			t.Errorf("%s%s: got %q (%v), want %q: %s", c.host, c.path, bodyOf(r), r.err, c.want, c.why)
		}
	}

	t.Run("a wildcard does not match its apex", func(t *testing.T) {
		check.WantReason(t, h.invoke(t, call{host: b, path: "/"}).err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)
	})
	t.Run("a host nothing routes", func(t *testing.T) {
		check.WantReason(t, h.invoke(t, call{host: "x." + h.run + ".elsewhere.test"}).err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)
	})
	t.Run("a catch-all", func(t *testing.T) {
		fn := h.routed(t, abi, "*", "/", "catch-all")
		defer func() {
			// Leave no catch-all behind for the groups that expect nothing to match.
			ctx, cancel := ctxT(settle)
			defer cancel()
			h.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: fn.GetId()}))
		}()
		if r := h.invoke(t, call{host: "anything." + h.run + ".elsewhere.test", path: "/x"}); r.err != nil || bodyOf(r) != "catch-all" {
			t.Errorf("a * route must catch what nothing else matches: %q, %v", bodyOf(r), r.err)
		}
		if r := h.invoke(t, call{host: "a." + b}); bodyOf(r) != "exact" {
			t.Errorf("a * route must not shadow a more specific host: %q", bodyOf(r))
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

// --- retries -----------------------------------------------------------------------

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

// --- errors ------------------------------------------------------------------------

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
	})
}

// --- read your writes ----------------------------------------------------------------

func (h *harness) readYourWrites(t *testing.T) {
	ctx, cancel := ctxT(60 * time.Second)
	defer cancel()
	abi := h.abi()
	h.worker(t, []string{abi}, func(_ context.Context, req *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
		v := ""
		for _, e := range req.GetEnv() {
			if e.GetName() == "V" {
				v = e.GetValue()
			}
		}
		return respond(req.GetArtifactDigest()+"|"+v)(nil, req, st)
	})
	fn := h.createFunction(t, h.name("ryw"))
	m1, m2 := h.module("one"), h.module("two")
	v1 := h.createVersion(t, fn.GetId(), abi, m1, nil, true)
	host := h.host()
	route := h.createRoute(t, host, "/", fn.GetId())
	h.served(t, host)

	// From here on nothing is awaited: every check is the very next request.
	want := func(what, digest, v string) {
		t.Helper()
		r := h.invoke(t, call{host: host})
		if r.err != nil || bodyOf(r) != digest+"|"+v {
			t.Errorf("%s: got %q (%v), want %q", what, bodyOf(r), r.err, digest+"|"+v)
		}
	}
	want("deployed", sha(m1), "")

	h.setEnv(t, fn.GetId(), "V", "1", false)
	want("a variable was set", sha(m1), "1")
	h.setEnv(t, fn.GetId(), "V", "2", true)
	want("a variable was replaced by a secret", sha(m1), "2")
	if _, err := h.fn.DeleteEnv(ctx, connect.NewRequest(&controlv1.DeleteEnvRequest{FunctionId: fn.GetId(), Name: "V"})); err != nil {
		t.Fatal(err)
	}
	want("a variable was deleted", sha(m1), "")

	h.createVersion(t, fn.GetId(), abi, m2, nil, true)
	want("a new version was activated on creation", sha(m2), "")
	if _, err := h.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: fn.GetId(), VersionId: v1.GetId()})); err != nil {
		t.Fatal(err)
	}
	want("rolled back", sha(m1), "")

	if _, err := h.rt.DeleteRoute(ctx, connect.NewRequest(&controlv1.DeleteRouteRequest{RouteId: route.GetId()})); err != nil {
		t.Fatal(err)
	}
	if r := h.invoke(t, call{host: host}); r.err == nil {
		t.Error("a deleted route still routes")
	} else {
		check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)
	}
	h.createRoute(t, host, "/", fn.GetId())
	want("a route was created", sha(m1), "")

	if _, err := h.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: fn.GetId()})); err != nil {
		t.Fatal(err)
	}
	check.WantReason(t, h.invoke(t, call{host: host}).err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)
}

// --- records ---------------------------------------------------------------------------

func (h *harness) records(t testing.TB, fnID string, pageSize uint32) []*controlv1.Invocation {
	t.Helper()
	ctx, cancel := ctxT(2 * settle)
	defer cancel()
	var out []*controlv1.Invocation
	tok := ""
	for i := 0; i < 10000; i++ {
		r, err := h.insp.ListInvocations(ctx, connect.NewRequest(&controlv1.ListInvocationsRequest{FunctionId: fnID, PageSize: pageSize, PageToken: tok}))
		if err != nil {
			t.Fatalf("ListInvocations: %v", err)
		}
		out = append(out, r.Msg.GetInvocations()...)
		if tok = r.Msg.GetNextPageToken(); tok == "" {
			return out
		}
	}
	t.Fatal("ListInvocations never ended")
	return nil
}

func (h *harness) invocationRecords(t *testing.T) {
	trapMsg := "trapped: exit status 3"
	w, f := h.stack(t, func(ctx context.Context, req *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
		switch req.GetRequest().GetPath() {
		case "/trap":
			return check.Error(commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, trapMsg)
		default:
			return respondWith(201, "made", "a log line", nil)(ctx, req, st)
		}
	})
	h.served(t, f.host)

	r := h.mustInvoke(t, call{method: "POST", host: f.host, path: "/made", requestID: "rec-ok-" + h.run})
	bad := h.invoke(t, call{host: f.host, path: "/trap", requestID: "rec-bad-" + h.run})
	check.WantReason(t, bad.err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED)
	// A request nothing routes leaves no record.
	h.invoke(t, call{host: "unrouted-" + h.run + ".conformance.test", requestID: "rec-none-" + h.run})

	find := func(requestID string) *controlv1.Invocation {
		for _, rec := range h.records(t, f.fn.GetId(), 0) {
			if rec.GetRequestId() == requestID {
				return rec
			}
		}
		return nil
	}
	var ok, failed *controlv1.Invocation
	check.Eventually(t, "the invocation records to appear", settle, func() bool {
		ok, failed = find("rec-ok-"+h.run), find("rec-bad-"+h.run)
		return ok != nil && failed != nil
	})

	t.Run("a successful invocation", func(t *testing.T) {
		if ok.GetId() != r.invocationID {
			t.Errorf("id = %q, want the invocation id the frontend got (%q)", ok.GetId(), r.invocationID)
		}
		if ok.GetFunctionId() != f.fn.GetId() || ok.GetVersionId() != f.version.GetId() || ok.GetWorkerId() != w.workerID() {
			t.Errorf("function/version/worker = %s/%s/%s, want %s/%s/%s", ok.GetFunctionId(), ok.GetVersionId(), ok.GetWorkerId(),
				f.fn.GetId(), f.version.GetId(), w.workerID())
		}
		if ok.GetMethod() != "POST" || ok.GetPath() != "/made" || ok.GetStatus() != 201 {
			t.Errorf("%s %s -> %d", ok.GetMethod(), ok.GetPath(), ok.GetStatus())
		}
		if ok.GetErrorReason() != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
			t.Errorf("error_reason = %v on a success", ok.GetErrorReason())
		}
		if string(ok.GetStderr()) != "a log line" || ok.GetDurationMicros() != 1234 || ok.GetPeakMemoryBytes() != 1<<20 {
			t.Errorf("stderr %q, duration %d, peak memory %d: want what the worker's summary reported", ok.GetStderr(), ok.GetDurationMicros(), ok.GetPeakMemoryBytes())
		}
		if ok.GetStartTime() == nil || time.Since(ok.GetStartTime().AsTime()) > time.Minute {
			t.Errorf("start_time = %v", ok.GetStartTime())
		}
	})

	t.Run("a failed invocation", func(t *testing.T) {
		if failed.GetErrorReason() != commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED || failed.GetStatus() != 0 {
			t.Errorf("reason %v, status %d: want FUNCTION_TRAPPED and no status (no response was produced)", failed.GetErrorReason(), failed.GetStatus())
		}
		if !strings.Contains(string(failed.GetStderr()), trapMsg) {
			t.Errorf("stderr = %q: a failure with no summary records the error's message", failed.GetStderr())
		}
	})

	t.Run("unrouted requests leave no record", func(t *testing.T) {
		for _, rec := range h.records(t, "", 0) {
			if rec.GetRequestId() == "rec-none-"+h.run {
				t.Error("a request that matched no function was recorded")
			}
		}
	})

	t.Run("newest first, paged", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			h.mustInvoke(t, call{host: f.host, path: "/more"})
		}
		check.Eventually(t, "six records", settle, func() bool { return len(h.records(t, f.fn.GetId(), 0)) >= 6 })
		all := h.records(t, f.fn.GetId(), 0)
		for _, size := range []uint32{1, 2, 4} {
			paged := h.records(t, f.fn.GetId(), size)
			if len(paged) != len(all) {
				t.Fatalf("page size %d returned %d records, want %d", size, len(paged), len(all))
			}
			seen := map[string]bool{}
			for i, rec := range paged {
				if seen[rec.GetId()] {
					t.Errorf("page size %d: record %s twice", size, rec.GetId())
				}
				seen[rec.GetId()] = true
				if rec.GetId() != all[i].GetId() {
					t.Errorf("page size %d: record %d differs from the unpaged order", size, i)
				}
			}
		}
		for i := 1; i < len(all); i++ {
			if all[i].GetStartTime().AsTime().After(all[i-1].GetStartTime().AsTime()) {
				t.Error("records are not newest first")
			}
		}
		// Every function's records are in the unfiltered list too.
		if got := len(h.records(t, "", 500)); got < len(all) {
			t.Errorf("the unfiltered list has %d records, fewer than one function's %d", got, len(all))
		}
	})
}

// functionDeletion: a deleted function takes its configuration with it and
// leaves its history.
func (h *harness) functionDeletion(t *testing.T) {
	ctx, cancel := ctxT(60 * time.Second)
	defer cancel()
	_, f := h.stack(t, respond("bye"))
	h.setEnv(t, f.fn.GetId(), "GONE", "soon", false)
	h.createVersion(t, f.fn.GetId(), f.abi, h.module("second"), nil, false)
	h.served(t, f.host)
	h.mustInvoke(t, call{host: f.host, requestID: "del-" + h.run})
	check.Eventually(t, "the record", settle, func() bool { return len(h.records(t, f.fn.GetId(), 0)) > 0 })
	route := ""
	for _, r := range h.allRoutes(t, 0) {
		if r.GetFunctionId() == f.fn.GetId() {
			route = r.GetId()
		}
	}
	if route == "" {
		t.Fatal("the function's route is not listed")
	}

	if _, err := h.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: f.fn.GetId()})); err != nil {
		t.Fatal(err)
	}
	if h.hasRoute(t, route) {
		t.Error("the function's route survived it")
	}
	_, err := h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: f.fn.GetId()}))
	check.WantCode(t, err, connect.CodeNotFound)
	_, err = h.fn.ListEnv(ctx, connect.NewRequest(&controlv1.ListEnvRequest{FunctionId: f.fn.GetId()}))
	check.WantCode(t, err, connect.CodeNotFound)
	check.WantReason(t, h.invoke(t, call{host: f.host}).err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)

	recs := h.records(t, f.fn.GetId(), 0)
	if len(recs) == 0 || recs[0].GetRequestId() != "del-"+h.run {
		t.Errorf("a deleted function's invocation records are kept: %v", recs)
	}
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
