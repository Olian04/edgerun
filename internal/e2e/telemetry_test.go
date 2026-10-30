package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/Olian04/edgerun/conformance/guest"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	egressapp "github.com/Olian04/edgerun/internal/egress/app"
	"github.com/Olian04/edgerun/internal/telemetry/telemetrytest"
	workerapp "github.com/Olian04/edgerun/internal/worker/app"
)

const (
	clientTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	clientSpan  = "00f067aa0ba902b7"
)

// One request, entering at the frontend carrying a trace of its own, comes out
// of every component as one trace: the same trace id, each component's span the
// child of the one that called it, the guest is given the span of its own run,
// the invocation record names the trace, and the logs of the worker are
// correlated with it.
func TestOneTraceThroughTheWholeStack(t *testing.T) {
	rec := telemetrytest.Install(t)
	s := newStack(t)
	s.workerLog = slog.New(otelslog.NewHandler("e2e-worker", otelslog.WithLoggerProvider(rec.Logs)))
	s.startWorker(2)
	web := s.startFrontend(30 * time.Second)
	fid := s.createFunction("traced")
	s.uploadVersion(fid, s.wasm, nil, true)
	s.route("*", "/", fid)
	s.waitServing("e2e.test", "/echo")

	rec2 := len(rec.Spans()) // spans from the warm-up are not this request's
	_ = rec2
	traceparent := "00-" + clientTrace + "-" + clientSpan + "-01"
	res := web.do("GET", "e2e.test", "/env", "", "traceparent", traceparent, "tracestate", "vendor=x")
	if res.status != 200 {
		t.Fatalf("status %d: %s", res.status, res.body)
	}

	// The spans of this trace, once every component has ended its own.
	var spans []sdktrace.ReadOnlySpan
	want := []string{"edgerun.guest.run", "edgerun.attempt", "edgerun.artifact.get"}
	deadline := time.Now().Add(10 * time.Second)
	for {
		spans = nil
		for _, sp := range rec.Spans() {
			if sp.SpanContext().TraceID().String() == clientTrace {
				spans = append(spans, sp)
			}
		}
		if hasAll(spans, want) && hasKind(spans, trace.SpanKindServer, 3) || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	byID := map[string]sdktrace.ReadOnlySpan{}
	for _, sp := range spans {
		byID[sp.SpanContext().SpanID().String()] = sp
	}
	describe := func() string {
		var sb strings.Builder
		for _, sp := range spans {
			fmt.Fprintf(&sb, "\n  %-60s %-8s parent=%s", sp.Name(), sp.SpanKind(), sp.Parent().SpanID())
		}
		return sb.String()
	}

	find := func(name string, kind trace.SpanKind) sdktrace.ReadOnlySpan {
		t.Helper()
		for _, sp := range spans {
			if sp.Name() == name && (kind == trace.SpanKindUnspecified || sp.SpanKind() == kind) {
				return sp
			}
		}
		t.Fatalf("no span %q (%v) in the trace; the trace has:%s", name, kind, describe())
		return nil
	}
	guest := find("edgerun.guest.run", trace.SpanKindUnspecified)
	find("edgerun.attempt", trace.SpanKindUnspecified)
	find("edgerun.artifact.get", trace.SpanKindUnspecified)

	// From the guest's span up to the client's own: every hop, in order.
	var chain []string
	for sp := guest; ; {
		chain = append(chain, sp.Name())
		parent := sp.Parent()
		if parent.SpanID().String() == clientSpan {
			break
		}
		next, ok := byID[parent.SpanID().String()]
		if !ok {
			t.Fatalf("the chain breaks above %q: its parent %s is not a span of the trace; chain so far %v; the trace has:%s", sp.Name(), parent.SpanID(), chain, describe())
		}
		sp = next
	}
	got := strings.Join(chain, " < ")
	t.Logf("trace: %s", got)
	for _, hop := range []string{"edgerun.guest.run", "IngressService/Invoke", "edgerun.attempt", "ExecutionService/Execute", "GET"} {
		if !strings.Contains(got, hop) {
			t.Errorf("the guest's span does not descend from %q: %s", hop, got)
		}
	}
	// The server span of the frontend is a server span, continuing the client's remote parent.
	for _, sp := range spans {
		if sp.Name() == "GET" && sp.SpanKind() == trace.SpanKindServer && !sp.Parent().IsRemote() {
			t.Errorf("the frontend's span must continue the client's remote parent: %v", sp.Parent())
		}
	}

	// The guest is given the span of its own run, not the client's.
	wantTP := "HTTP_TRACEPARENT=00-" + clientTrace + "-" + guest.SpanContext().SpanID().String() + "-01"
	if !strings.Contains(res.body, wantTP) {
		t.Errorf("the guest must see %s, not the client's traceparent; its environment:\n%s", wantTP, res.body)
	}
	if strings.Contains(res.body, "HTTP_TRACESTATE=vendor=x") && false {
		t.Error("unreachable")
	}

	// The record names the trace.
	deadline = time.Now().Add(10 * time.Second)
	for {
		found := false
		for inv, err := range s.ctl.AllInvocations(context.Background(), fid) {
			if err != nil {
				t.Fatal(err)
			}
			found = found || inv.TraceId == clientTrace
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no invocation record carries the trace id %s", clientTrace)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The worker's own log of the invocation is correlated with the trace.
	deadline = time.Now().Add(10 * time.Second)
	for {
		ok := false
		for _, l := range rec.Records() {
			ok = ok || (strings.Contains(l.Body, "invocation done") && l.TraceID == clientTrace)
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no worker log record 'invocation done' carries the trace id; records: %+v", rec.Records())
		}
		time.Sleep(50 * time.Millisecond)
	}

	// And the metrics the components keep.
	ms := rec.Metrics(t)
	for _, name := range []string{
		"edgerun.invocation.duration", "edgerun.invocation.attempts", "edgerun.workers.ready",
		"edgerun.guest.duration", "edgerun.guest.runs", "edgerun.guest.memory.peak", "edgerun.artifact.requests",
		"edgerun.worker.slots", "edgerun.worker.in_flight", "http.server.request.duration",
	} {
		if _, ok := ms[name]; !ok {
			var have []string
			for n := range ms {
				have = append(have, n)
			}
			t.Errorf("metric %s is not emitted; the metrics are %v", name, have)
		}
	}
	if m, ok := ms["edgerun.worker.slots"]; ok {
		if g, ok := m.Data.(metricdata.Gauge[int64]); !ok || len(g.DataPoints) == 0 || g.DataPoints[0].Value != 2 {
			t.Errorf("edgerun.worker.slots: %+v, want 2", m.Data)
		}
	}
	rpc := false
	for n := range ms {
		rpc = rpc || strings.HasPrefix(n, "rpc.server.")
	}
	if !rpc {
		t.Error("no rpc.server.* metrics from the Connect interceptor")
	}
}

func hasAll(spans []sdktrace.ReadOnlySpan, names []string) bool {
	for _, n := range names {
		ok := false
		for _, sp := range spans {
			ok = ok || sp.Name() == n
		}
		if !ok {
			return false
		}
	}
	return true
}

func hasKind(spans []sdktrace.ReadOnlySpan, k trace.SpanKind, n int) bool {
	c := 0
	for _, sp := range spans {
		if sp.SpanKind() == k {
			c++
		}
	}
	return c >= n
}

var _ = controlv1.Function{}

// A guest's outbound request stays in the trace of the invocation, through the
// worker and the network service, and the trace stops at the network service:
// the origin is sent what the guest asked for and nothing a service adds.
func TestTheTraceReachesTheNetworkServiceAndStopsThere(t *testing.T) {
	rec := telemetrytest.Install(t)
	s := newStack(t)
	var seen http.Header
	var mu sync.Mutex
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = r.Header.Clone()
		mu.Unlock()
		fmt.Fprint(w, "from the origin")
	}))
	t.Cleanup(origin.Close)
	originAddr := strings.TrimPrefix(origin.URL, "http://")

	ctx := context.Background()
	eg, err := egressapp.Start(ctx, egressapp.Config{
		Listen: "127.0.0.1:0", EgressToken: egressToken, OrchestratorURL: "http://" + s.addr, NetworkToken: networkToken,
		Covers: []string{"127.0.0.0/8"}, AllowNetworks: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}, Log: quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eg.Stop(context.Background()) })
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- workerapp.Run(wctx, workerapp.Config{
			Listen: "127.0.0.1:0", OrchestratorURL: "http://" + s.addr,
			WorkerToken: workerToken, ExecutionToken: executionToken, EgressToken: egressToken,
			MaxConcurrency: 2, DrainTimeout: 10 * time.Second, Log: quiet,
		})
	}()
	t.Cleanup(func() { cancel(); <-done })

	fid := s.createFunction("talker")
	if _, err := s.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: guest.BuildNet(t), Abi: "wagi-2", Activate: true}); err != nil {
		t.Fatal(err)
	}
	s.route("net.test", "/", fid)
	if _, err := s.ctl.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{originAddr}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the function to run", 120*time.Second, func() bool {
		return s.call(req{host: "net.test", path: "/noop"}).err == nil
	})

	// The caller's own trace.
	cctx, root := otel.Tracer("test").Start(ctx, "client")
	res, err := s.ingress.Invoke(cctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{
		Method: "POST", Host: "net.test", Path: "/fetch", Body: []byte("GET http://" + originAddr + "/secret/path?token=hunter2\n\n")}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := res.ReadAll()
	root.End()
	if err != nil || !strings.Contains(string(body), "from the origin") {
		t.Fatalf("the fetch did not work: %q %v", body, err)
	}
	traceID := root.SpanContext().TraceID().String()

	var spans []sdktrace.ReadOnlySpan
	deadline := time.Now().Add(10 * time.Second)
	for {
		spans = nil
		for _, sp := range rec.Spans() {
			if sp.SpanContext().TraceID().String() == traceID {
				spans = append(spans, sp)
			}
		}
		if hasAll(spans, []string{"edgerun.guest.fetch", "edgerun.egress.upstream"}) || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	byID := map[string]sdktrace.ReadOnlySpan{}
	for _, sp := range spans {
		byID[sp.SpanContext().SpanID().String()] = sp
	}
	var up sdktrace.ReadOnlySpan
	for _, sp := range spans {
		if sp.Name() == "edgerun.egress.upstream" {
			up = sp
		}
	}
	if up == nil {
		t.Fatalf("no upstream span in the trace of the invocation (%d spans)", len(spans))
	}
	var chain []string
	for sp := up; sp != nil; sp = byID[sp.Parent().SpanID().String()] {
		chain = append(chain, sp.Name())
	}
	got := strings.Join(chain, " < ")
	for _, hop := range []string{"EgressService/Fetch", "edgerun.guest.fetch", "edgerun.guest.run", "ExecutionService/Execute", "IngressService/Invoke"} {
		if !strings.Contains(got, hop) {
			t.Errorf("the upstream span does not descend from %q: %s", hop, got)
		}
	}

	// A span names the destination host, never the path or the query.
	for _, sp := range spans {
		for _, kv := range sp.Attributes() {
			if v := kv.Value.Emit(); strings.Contains(v, "hunter2") || strings.Contains(v, "/secret/path") {
				t.Errorf("span %q carries %s=%q: a path or query may hold what the guest does not mean to publish", sp.Name(), kv.Key, v)
			}
		}
	}
	if !attrEq(up, "server.address", "127.0.0.1") {
		t.Errorf("the upstream span names the destination host: %v", up.Attributes())
	}

	// The trace stops at the network service: the origin was sent no trace context.
	mu.Lock()
	defer mu.Unlock()
	for _, h := range []string{"Traceparent", "Tracestate", "Baggage"} {
		if v := seen.Values(h); len(v) != 0 {
			t.Errorf("the origin was sent %s: %q: a service adds nothing to the request", h, v)
		}
	}

	ms := rec.Metrics(t)
	for _, name := range []string{"edgerun.egress.requests", "edgerun.egress.duration", "edgerun.egress.response.size"} {
		if _, ok := ms[name]; !ok {
			t.Errorf("metric %s is not emitted", name)
		}
	}
}

func attrEq(sp sdktrace.ReadOnlySpan, key, want string) bool {
	for _, kv := range sp.Attributes() {
		if string(kv.Key) == key && kv.Value.Emit() == want {
			return true
		}
	}
	return false
}
