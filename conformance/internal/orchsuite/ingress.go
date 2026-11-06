package orchsuite

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

// Ingress judges the orchestrator as the host of ingress.v1 IngressService: the
// frontend hands it an HTTP request and it answers with the function's response.
// The suite plays the frontend (and the operator and fake workers that give the
// request somewhere to go).
func Ingress(t *testing.T, target Target) {
	h := start(t, target)
	t.Run("Describe", h.ingressDescribe)
	t.Run("Authentication", h.ingressAuthentication)
	t.Run("NoWorkers", h.noWorkers)
	t.Run("Streaming", h.invokeStreaming)
	t.Run("Validation", h.invokeValidation)
	t.Run("Messages", h.invokeMessages)
	// Read-your-writes is a rule of control.v1 (Catalog, Routing), whose proof is
	// the very next Invoke: it is judged here, where Invoke is the call under test.
	t.Run("ReadYourWrites", h.readYourWrites)
}

func (h *harness) ingressDescribe(t *testing.T) {
	ctx, cancel := ctxT(settle)
	defer cancel()
	ing, err := h.ing.Describe(ctx, connect.NewRequest(&ingressv1.DescribeRequest{}))
	if err != nil {
		t.Fatalf("ingress Describe: %v", err)
	}
	if ing.Msg.GetImplementation().GetName() == "" {
		t.Error("ingress: the implementation has no name")
	}
	if !contains(ing.Msg.GetImplementation().GetProtocols(), "edgerun.ingress.v1") {
		t.Errorf("ingress: protocols %v do not include edgerun.ingress.v1", ing.Msg.GetImplementation().GetProtocols())
	}
	if ing.Msg.GetMaxRequestBodyBytes() == 0 {
		t.Error("ingress: max_request_body_bytes must be reported (frontends enforce it)")
	}
}

func (h *harness) ingressAuthentication(t *testing.T) {
	check.Authentication(t, h.scopes(), "ingress", []string{h.env.ExecutionToken}, func(token string) error {
		ctx, cancel := ctxT(settle)
		defer cancel()
		_, err := ingressv1connect.NewIngressServiceClient(http.DefaultClient, h.url, check.Bearer(token)).Describe(ctx, connect.NewRequest(&ingressv1.DescribeRequest{}))
		return err
	})
}

// scopes are the tokens the orchestrator knows, by scope.
func (h *harness) scopes() map[string]string {
	return map[string]string{"ingress": h.env.IngressToken, "worker": h.env.WorkerToken, "admin": h.env.AdminToken, "network": h.env.NetworkToken}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// noWorkers: with nobody alive to run it, a function answers NO_CAPACITY.
func (h *harness) noWorkers(t *testing.T) {
	check.Eventually(t, "every earlier worker to be gone", settle, func() bool { return h.aliveCount(t, kindWorker) == 0 })
	f := h.deploy(t, h.abi())
	var r result
	check.Eventually(t, "the orchestrator to see the empty fleet", settle, func() bool {
		r = h.invoke(t, call{host: f.host})
		return r.err != nil
	})
	check.WantReason(t, r.err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY)
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
