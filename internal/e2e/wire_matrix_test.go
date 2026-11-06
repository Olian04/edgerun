package e2e

// Part 2, wire neutrality: every service of every role is called over Connect
// (protobuf and JSON), gRPC and gRPC-Web, over HTTP/1.1 and over h2c, with
// request and response gzip on and off. What comes back must not depend on any of
// it. gRPC needs HTTP/2, so that pair is not in the matrix.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

type wireVariant struct {
	proto             string // connect, connect-json, grpc, grpc-web
	http2             bool
	reqGzip, respGzip bool
}

func (v wireVariant) name() string {
	h := "h1"
	if v.http2 {
		h = "h2c"
	}
	return fmt.Sprintf("%s/%s/reqgzip=%v/respgzip=%v", v.proto, h, v.reqGzip, v.respGzip)
}

func wireVariants() []wireVariant {
	var out []wireVariant
	for _, p := range []string{"connect", "connect-json", "grpc", "grpc-web"} {
		for _, h2 := range []bool{false, true} {
			if p == "grpc" && !h2 {
				continue // gRPC is HTTP/2 only
			}
			for _, rq := range []bool{false, true} {
				for _, rs := range []bool{false, true} {
					out = append(out, wireVariant{p, h2, rq, rs})
				}
			}
		}
	}
	return out
}

// seen is what a transport observed of the exchanges it carried.
type seen struct {
	mu                        sync.Mutex
	protoMajor                map[int]int
	reqGzipSeen, respGzipSeen bool
	contentTypes              map[string]bool
}

type recTransport struct {
	rt http.RoundTripper
	s  *seen
}

func (r recTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := r.rt.RoundTrip(req)
	if err != nil {
		return res, err
	}
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	r.s.protoMajor[res.ProtoMajor]++
	r.s.contentTypes[strings.SplitN(req.Header.Get("Content-Type"), ";", 2)[0]] = true
	for _, h := range []string{"Content-Encoding", "Connect-Content-Encoding", "Grpc-Encoding"} {
		if req.Header.Get(h) == "gzip" {
			r.s.reqGzipSeen = true
		}
		if res.Header.Get(h) == "gzip" {
			r.s.respGzipSeen = true
		}
	}
	return res, nil
}

func (v wireVariant) client() (*http.Client, *seen, []connect.ClientOption) {
	var p http.Protocols
	if v.http2 {
		p.SetUnencryptedHTTP2(true)
	} else {
		p.SetHTTP1(true)
	}
	s := &seen{protoMajor: map[int]int{}, contentTypes: map[string]bool{}}
	hc := &http.Client{Transport: recTransport{rt: &http.Transport{Protocols: &p}, s: s}}
	var opts []connect.ClientOption
	switch v.proto {
	case "connect-json":
		opts = append(opts, connect.WithProtoJSON())
	case "grpc":
		opts = append(opts, connect.WithGRPC())
	case "grpc-web":
		opts = append(opts, connect.WithGRPCWeb())
	}
	if v.reqGzip {
		opts = append(opts, connect.WithSendGzip())
	}
	if !v.respGzip {
		opts = append(opts, connect.WithInterceptors(noResponseCompression{})) // gzip is not accepted back
	}
	return hc, s, opts
}

type wireClients struct {
	ing   *edgerun.Ingress
	ctl   *edgerun.Control
	fleet *edgerun.Fleet
	art   *edgerun.Artifacts
	reg   *edgerun.Registry
	exec  *edgerun.Execution
	eg    *edgerun.Egress
}

func (e *cenv) wireClients(hc *http.Client, opts []connect.ClientOption) wireClients {
	o := []edgerun.Option{edgerun.WithHTTPClient(hc), edgerun.WithClientOptions(opts...)}
	orch := e.orch.base
	return wireClients{
		ing:   edgerun.NewIngress(orch, ingressToken, o...),
		ctl:   edgerun.NewControl(orch, adminToken, o...),
		fleet: edgerun.NewFleet(orch, workerToken, o...),
		art:   edgerun.NewArtifacts(orch, workerToken, o...),
		reg:   edgerun.NewRegistry(orch, networkToken, o...),
		exec:  edgerun.NewExecution(e.worker.base, executionToken, o...),
		eg:    edgerun.NewEgress(e.egress.base, egressToken, o...),
	}
}

func sha(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:8]) + fmt.Sprintf("/%d", len(b))
}

func headersOf(h *commonv1.HttpResponseHead) string {
	var hs []string
	for _, x := range h.GetHeaders() {
		hs = append(hs, fmt.Sprintf("%s=%q", x.GetName(), x.GetValue()))
	}
	sort.Strings(hs)
	return fmt.Sprintf("%d %s", h.GetStatus(), strings.Join(hs, ";"))
}

// wireCall is one RPC, reduced to a string that must not depend on the wire.
type wireCall struct {
	name string
	do   func(e *cenv, c wireClients, n int) (string, error)
}

func wireCalls() []wireCall {
	binary := make([]byte, 150_000)
	for i := range binary {
		binary[i] = byte(i * 7)
	}
	compressible := []byte(strings.Repeat("compressible ", 20000))
	msg := func(m proto.Message) string { return normalize(m) }
	return []wireCall{
		{"ingress.Describe", func(e *cenv, c wireClients, n int) (string, error) {
			r, err := c.ing.Describe(ctx, &ingressv1.DescribeRequest{})
			if err != nil {
				return "", err
			}
			return msg(r), nil
		}},
		{"ingress.Invoke (binary body, bytes headers)", func(e *cenv, c wireClients, n int) (string, error) {
			res, err := c.ing.Invoke(ctx, &ingressv1.InvokeRequest{RequestId: "wire", Request: &commonv1.HttpRequest{Method: "POST", Host: "compat.test", Path: "/echo",
				Body: binary, Headers: []*commonv1.Header{{Name: "content-type", Value: []byte("application/x-\xff\xfe")}}}})
			if err != nil {
				return "", err
			}
			body, err := res.ReadAll()
			return headersOf(res.Head) + " " + sha(body), err
		}},
		{"ingress.Invoke (compressible body)", func(e *cenv, c wireClients, n int) (string, error) {
			res, err := c.ing.Invoke(ctx, &ingressv1.InvokeRequest{RequestId: "wire", Request: &commonv1.HttpRequest{Method: "POST", Host: "compat.test", Path: "/echo", Body: compressible}})
			if err != nil {
				return "", err
			}
			body, err := res.ReadAll()
			return headersOf(res.Head) + " " + sha(body), err
		}},
		{"ingress.Invoke (bytes in a response header)", func(e *cenv, c wireClients, n int) (string, error) {
			res, err := c.ing.Invoke(ctx, &ingressv1.InvokeRequest{RequestId: "wire", Request: &commonv1.HttpRequest{Method: "GET", Host: "compat.test", Path: "/rawheaders"}})
			if err != nil {
				return "", err
			}
			body, err := res.ReadAll()
			return headersOf(res.Head) + " " + sha(body), err
		}},
		{"control.GetFunction", func(e *cenv, c wireClients, n int) (string, error) {
			r, err := c.ctl.GetFunction(ctx, &controlv1.GetFunctionRequest{FunctionId: e.fid})
			if err != nil {
				return "", err
			}
			return msg(r), nil
		}},
		{"control.ListFunctions", func(e *cenv, c wireClients, n int) (string, error) {
			r, err := c.ctl.ListFunctions(ctx, &controlv1.ListFunctionsRequest{PageSize: 5, Name: "compat"})
			if err != nil {
				return "", err
			}
			return msg(r), nil
		}},
		{"control.ListRoutes", func(e *cenv, c wireClients, n int) (string, error) {
			r, err := c.ctl.ListRoutes(ctx, &controlv1.ListRoutesRequest{FunctionId: e.fid})
			if err != nil {
				return "", err
			}
			return msg(r), nil
		}},
		{"control.Validate (a plan)", func(e *cenv, c wireClients, n int) (string, error) {
			r, err := c.ctl.Validate(ctx, &controlv1.ValidateRequest{Steps: []*controlv1.Step{
				{Bind: "f", Request: &controlv1.Step_CreateFunction{CreateFunction: &controlv1.CreateFunctionRequest{Name: "wire-plan", Labels: map[string]string{"a": "b"}}}},
				{Request: &controlv1.Step_CreateRoute{CreateRoute: &controlv1.CreateRouteRequest{HostPattern: "wire.example", PathPrefix: "/", FunctionId: "$f"}}},
				{Request: &controlv1.Step_CreateRoute{CreateRoute: &controlv1.CreateRouteRequest{HostPattern: "", PathPrefix: "/", FunctionId: "$f"}}}, // fails, as an answer
			}})
			if err != nil {
				return "", err
			}
			return msg(r), nil
		}},
		{"control.Describe", func(e *cenv, c wireClients, n int) (string, error) {
			r, err := c.ctl.Describe(ctx, &controlv1.DescribeRequest{})
			if err != nil {
				return "", err
			}
			return msg(r), nil
		}},
		{"control.ListWorkers (shape)", func(e *cenv, c wireClients, n int) (string, error) {
			r, err := c.ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
			if err != nil {
				return "", err
			}
			return fmt.Sprint(len(r.Workers) > 0), nil
		}},
		{"fleet.Register/Heartbeat/Deregister", func(e *cenv, c wireClients, n int) (string, error) {
			rr, err := c.fleet.Register(ctx, &workerv1.RegisterRequest{Endpoint: fmt.Sprintf("http://127.0.0.1:%d", 30000+n),
				Info: &workerv1.WorkerInfo{Abis: []string{"wire-fake"}, MaxConcurrency: 1, Labels: map[string]string{"k": "v"}}})
			if err != nil {
				return "", err
			}
			hb, err := c.fleet.Heartbeat(ctx, &workerv1.HeartbeatRequest{WorkerId: rr.WorkerId, State: workerv1.WorkerState_WORKER_STATE_DRAINING, InFlight: 3})
			if err != nil {
				return "", err
			}
			if _, err := c.fleet.Deregister(ctx, &workerv1.DeregisterRequest{WorkerId: rr.WorkerId}); err != nil {
				return "", err
			}
			return msg(rr) + msg(hb), nil
		}},
		{"artifact.GetArtifact (the module, verified)", func(e *cenv, c wireClients, n int) (string, error) {
			b, err := c.art.GetArtifact(ctx, e.digest)
			return sha(b), err
		}},
		{"registry.Register/Heartbeat/Deregister", func(e *cenv, c wireClients, n int) (string, error) {
			rr, err := c.reg.Register(ctx, &egressv1.RegisterRequest{Endpoint: fmt.Sprintf("http://127.0.0.1:%d", 40000+n),
				Info: &egressv1.ServiceInfo{Covers: []string{"*.wire.example"}}})
			if err != nil {
				return "", err
			}
			hb, err := c.reg.Heartbeat(ctx, &egressv1.HeartbeatRequest{ServiceId: rr.ServiceId, State: egressv1.ServiceState_SERVICE_STATE_DRAINING})
			if err != nil {
				return "", err
			}
			if _, err := c.reg.Deregister(ctx, &egressv1.DeregisterRequest{ServiceId: rr.ServiceId}); err != nil {
				return "", err
			}
			return msg(rr) + msg(hb), nil
		}},
		{"execution.Describe", func(e *cenv, c wireClients, n int) (string, error) {
			r, err := c.exec.Describe(ctx, &workerv1.DescribeRequest{})
			if err != nil {
				return "", err
			}
			return msg(r), nil
		}},
		{"execution.Execute", func(e *cenv, c wireClients, n int) (string, error) {
			res, err := c.exec.Execute(ctx, &workerv1.ExecuteRequest{InvocationId: "wire", ArtifactDigest: e.digest, Abi: "wagi-1",
				Limits:  &commonv1.Limits{MaxResponseBytes: 8 << 20},
				Request: &commonv1.HttpRequest{Method: "POST", Host: "x", Path: "/echo", Body: binary}})
			if err != nil {
				return "", err
			}
			body, err := res.ReadAll()
			return headersOf(res.Head) + " " + sha(body), err
		}},
		{"egress.Describe", func(e *cenv, c wireClients, n int) (string, error) {
			r, err := c.eg.Describe(ctx, &egressv1.DescribeRequest{})
			if err != nil {
				return "", err
			}
			return msg(r), nil
		}},
		{"egress.Fetch", func(e *cenv, c wireClients, n int) (string, error) {
			res, err := c.eg.Fetch(ctx, &egressv1.FetchRequest{Method: "GET", Url: "http://" + e.originAddr + "/p", Allow: []string{e.originAddr}})
			if err != nil {
				return "", err
			}
			body, err := res.ReadAll()
			h := res.Head
			var hs []string
			for _, x := range h.Headers {
				if x.Name == "x-origin" || x.Name == "content-type" {
					hs = append(hs, fmt.Sprintf("%s=%q", x.Name, x.Value))
				}
			}
			sort.Strings(hs)
			return fmt.Sprint(h.Status, hs, sha(body)), err
		}},
	}
}

func TestWireEveryServiceOverEveryProtocol(t *testing.T) {
	e := newCEnv(t)
	calls := wireCalls()
	// the reference answer: Connect, protobuf, HTTP/1.1, no compression
	ref := wireVariant{proto: "connect", respGzip: false}
	hc, _, copts := ref.client()
	want := map[string]string{}
	rc := e.wireClients(hc, copts)
	for i, c := range calls {
		got, err := c.do(e, rc, i)
		if err != nil {
			t.Fatalf("%s: the reference call fails: %v", c.name, err)
		}
		want[c.name] = got
	}

	var mirrored atomic.Bool
	protocolsSeen := map[string]bool{}
	for vi, v := range wireVariants() {
		t.Run(v.name(), func(t *testing.T) {
			hc, sn, copts := v.client()
			cl := e.wireClients(hc, copts)
			for i, c := range calls {
				got, err := c.do(e, cl, 1000*(vi+1)+i)
				if err != nil {
					t.Errorf("%s: %v", c.name, err)
					continue
				}
				if got != want[c.name] {
					t.Errorf("%s differs from the reference\n  want %s\n  got  %s", c.name, trunc(want[c.name]), trunc(got))
				}
			}
			wantMajor := 1
			if v.http2 {
				wantMajor = 2
			}
			if len(sn.protoMajor) != 1 || sn.protoMajor[wantMajor] == 0 {
				t.Errorf("the exchanges ran over HTTP major versions %v, want only %d", sn.protoMajor, wantMajor)
			}
			if v.reqGzip != sn.reqGzipSeen {
				t.Errorf("request gzip asked for: %v, observed on the wire: %v", v.reqGzip, sn.reqGzipSeen)
			}
			switch {
			case v.respGzip && !sn.respGzipSeen:
				t.Errorf("response gzip allowed, but no response was compressed (the compressible ones are over the server's threshold)")
			case !v.respGzip && sn.respGzipSeen && !v.reqGzip:
				t.Errorf("the client accepts only identity, and a response was gzip-compressed")
			case !v.respGzip && sn.respGzipSeen && v.reqGzip:
				// connect-go answers in the encoding of the request when the client's
				// accept list names nothing it has, so a client that sends gzip but
				// cannot read it still gets it. Not edgerun's code, but on its wire.
				mirrored.Store(true)
			}
			ct := strings.Join(sortedKeys(sn.contentTypes), ",")
			protocolsSeen[v.proto+": "+ct] = true
		})
	}
	if mirrored.Load() {
		t.Logf("OBSERVATION: a client that sends gzip and accepts only identity gets gzip back (connect-go mirrors the request encoding)")
	}
	for k := range protocolsSeen {
		t.Logf("content types on the wire: %s", k)
	}
}

func sortedKeys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func trunc(s string) string {
	if len(s) > 400 {
		return s[:400] + "..."
	}
	return s
}

// noResponseCompression tells the server not to compress what it sends back,
// whichever protocol is spoken, while the client can still send gzip.
type noResponseCompression struct{}

func (noResponseCompression) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, r connect.AnyRequest) (connect.AnyResponse, error) {
		r.Header().Set("Accept-Encoding", "identity")
		r.Header().Set("Connect-Accept-Encoding", "identity")
		r.Header().Set("Grpc-Accept-Encoding", "identity")
		return next(ctx, r)
	}
}

func (noResponseCompression) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		h := conn.RequestHeader()
		h.Set("Connect-Accept-Encoding", "identity")
		h.Set("Grpc-Accept-Encoding", "identity")
		h.Set("Accept-Encoding", "identity")
		return conn
	}
}

func (noResponseCompression) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
