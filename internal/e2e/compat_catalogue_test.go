package e2e

// The catalogue: one entry for EVERY RPC of every service in gen/, each with a
// builder for a valid request, against the real component that serves it.
// TestCatalogueCoversEveryRPC fails when a service gains a method nobody listed.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	egressapp "github.com/Olian04/edgerun/internal/egress/app"
)

// cenv is a running system with everything an RPC of any seam needs.
type cenv struct {
	t *testing.T
	s *stack

	orchLog, workerLog, egressLog *logSink

	orch, worker, egress rawClient // clients of the three servers
	originAddr           string
	fid, vid             string
	digest               string
	n                    atomic.Int64
	egressSrv            *egressapp.Server
}

func newCEnv(t *testing.T) *cenv {
	t.Helper()
	e := &cenv{t: t}
	var sink *logSink
	e.s, sink = newCStack(t, nil)
	e.orchLog = sink
	wlog, wsink := newSink()
	e.s.workerLog, e.workerLog = wlog, wsink
	e.s.startWorker(4)

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Origin", "yes")
		fmt.Fprint(w, "from the origin")
	}))
	t.Cleanup(origin.Close)
	e.originAddr = origin.Listener.Addr().String()

	elog, esink := newSink()
	e.egressLog = esink
	eg, err := egressapp.Start(ctx, egressapp.Config{
		Listen: "127.0.0.1:0", EgressToken: egressToken, OrchestratorURL: "http://" + e.s.addr, NetworkToken: networkToken,
		Covers: []string{"127.0.0.0/8"}, AllowNetworks: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}, Log: elog,
	})
	if err != nil {
		t.Fatal(err)
	}
	e.egressSrv = eg
	t.Cleanup(func() { eg.Stop(context.Background()) })

	e.fid = e.s.createFunction("compat")
	e.vid = e.s.uploadVersion(e.fid, e.s.wasm, &commonv1.Limits{MaxResponseBytes: 16 << 20}, true)
	e.digest = edgerun.Digest(e.s.wasm)
	e.s.route("compat.test", "/", e.fid)
	e.s.waitServing("compat.test", "/echo")

	e.orch = rawClient{base: "http://" + e.s.addr}
	var wep string
	eventually(t, "the real worker to be listed", 30*time.Second, func() bool {
		r, err := e.s.ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
		if err != nil {
			return false
		}
		for _, w := range r.Workers {
			if w.Alive && w.Info.GetImplementation().GetName() == "edgerun-worker-wazero" {
				wep = w.Endpoint
			}
		}
		return wep != ""
	})
	e.worker = rawClient{base: wep}
	e.egress = rawClient{base: "http://" + eg.Addr()}
	eventually(t, "the network service to register", 30*time.Second, func() bool {
		r, err := e.s.ctl.ListNetworkServices(ctx, &controlv1.ListNetworkServicesRequest{})
		return err == nil && len(r.Services) > 0
	})
	return e
}

func (e *cenv) uniq(prefix string) string { return fmt.Sprintf("%s%d", prefix, e.n.Add(1)) }

func (e *cenv) logs() map[string]*logSink {
	return map[string]*logSink{"orchestrator": e.orchLog, "worker": e.workerLog, "egress": e.egressLog}
}

func (e *cenv) fn() string { // a fresh function nobody else touches
	e.t.Helper()
	return e.s.createFunction(e.uniq("fn-"))
}

func (e *cenv) httpReq(method, path, body string) *commonv1.HttpRequest {
	return &commonv1.HttpRequest{Method: method, Host: "compat.test", Path: path, Body: []byte(body), RemoteAddr: "192.0.2.1:1234",
		Headers: []*commonv1.Header{{Name: "content-type", Value: []byte("text/x-compat")}}}
}

// rpcCase is one RPC of the catalogue.
type rpcCase struct {
	service, method string
	target          string // "orch", "worker" or "egress"
	token           string
	req             func(e *cenv) proto.Message
	// zero lists the answers the empty request {} may get. Empty means "ok".
	// Anything else, and in particular internal, unknown and unavailable, is a
	// finding.
	zero []connect.Code
	// zeroReason, when not UNSPECIFIED, is the ErrorReason the answer must carry.
	zeroReason commonv1.ErrorReason
	// loose: the response varies between runs (lists of live things) so only
	// its shape is compared, not its content.
	loose bool
	// enum names a field of the request that carries an enum the server must
	// validate (heartbeat states); the test sends unknown values in it.
	enum string
}

func (c rpcCase) proc() string { return "/" + c.service + "/" + c.method }

func (c rpcCase) desc(t testing.TB) protoreflect.MethodDescriptor {
	t.Helper()
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(c.service))
	if err != nil {
		t.Fatalf("%s: %v", c.service, err)
	}
	m := d.(protoreflect.ServiceDescriptor).Methods().ByName(protoreflect.Name(c.method))
	if m == nil {
		t.Fatalf("%s has no method %s", c.service, c.method)
	}
	return m
}

func (c rpcCase) streaming(t testing.TB) bool { return c.desc(t).IsStreamingServer() }

func (c rpcCase) newResp(t testing.TB) proto.Message {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(c.desc(t).Output().FullName())
	if err != nil {
		t.Fatal(err)
	}
	return mt.New().Interface()
}

func (c rpcCase) newReq(t testing.TB) proto.Message {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(c.desc(t).Input().FullName())
	if err != nil {
		t.Fatal(err)
	}
	return mt.New().Interface()
}

func (e *cenv) client(c rpcCase) rawClient {
	var rc rawClient
	switch c.target {
	case "orch":
		rc = e.orch
	case "worker":
		rc = e.worker
	default:
		rc = e.egress
	}
	rc.token = c.token
	return rc
}

var (
	icInvalid  = []connect.Code{connect.CodeInvalidArgument}
	icNotFound = []connect.Code{connect.CodeNotFound}
	icFormOrNF = []connect.Code{connect.CodeInvalidArgument, connect.CodeNotFound}
	icAny      = []connect.Code{codeOK, connect.CodeInvalidArgument, connect.CodeNotFound}
)

// catalogue lists every RPC. The builders create what they refer to afresh each
// time they are called, so every variant of a request starts from the same state.
func catalogue() []rpcCase {
	admin := adminToken
	fs, rs, ps, is := "edgerun.control.v1.FunctionService", "edgerun.control.v1.RouteService", "edgerun.control.v1.PlanService", "edgerun.control.v1.InspectionService"
	ing, ex, fl, ar := "edgerun.ingress.v1.IngressService", "edgerun.worker.v1.ExecutionService", "edgerun.worker.v1.FleetService", "edgerun.worker.v1.ArtifactService"
	eg, rg := "edgerun.egress.v1.EgressService", "edgerun.egress.v1.RegistryService"

	newVersion := func(e *cenv, fid string) *controlv1.FunctionVersion {
		e.t.Helper()
		r, err := e.s.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: []byte(e.uniq("module-")), Abi: "compat-fake"})
		if err != nil {
			e.t.Fatal(err)
		}
		return r.Version
	}
	newRoute := func(e *cenv, fid string) *controlv1.Route {
		e.t.Helper()
		r, err := e.s.ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: e.uniq("r") + ".example", PathPrefix: "/", FunctionId: fid})
		if err != nil {
			e.t.Fatal(err)
		}
		return r.Route
	}
	fakeInfo := func() *workerv1.WorkerInfo {
		return &workerv1.WorkerInfo{Implementation: &commonv1.Implementation{Name: "compat-fake", Version: "1", Protocols: []string{"edgerun.worker.v1"}},
			Abis: []string{"compat-fake"}, MaxConcurrency: 3, Labels: map[string]string{"region": "test"}}
	}
	registerFake := func(e *cenv) string {
		e.t.Helper()
		r, err := edgerun.NewFleet("http://"+e.s.addr, workerToken).Register(ctx, &workerv1.RegisterRequest{Endpoint: "http://127.0.0.1:" + fmt.Sprint(1+e.n.Add(1)), Info: fakeInfo()})
		if err != nil {
			e.t.Fatal(err)
		}
		return r.WorkerId
	}
	svcInfo := func() *egressv1.ServiceInfo {
		return &egressv1.ServiceInfo{Implementation: &commonv1.Implementation{Name: "compat-fake", Version: "1"}, Covers: []string{"*.compat.example"}}
	}
	registerSvc := func(e *cenv) string {
		e.t.Helper()
		r, err := edgerun.NewRegistry("http://"+e.s.addr, networkToken).Register(ctx, &egressv1.RegisterRequest{Endpoint: "http://127.0.0.1:" + fmt.Sprint(1+e.n.Add(1)), Info: svcInfo()})
		if err != nil {
			e.t.Fatal(err)
		}
		return r.ServiceId
	}

	return []rpcCase{
		// control: functions
		{service: fs, method: "CreateFunction", target: "orch", token: admin, zero: icInvalid,
			req: func(e *cenv) proto.Message {
				return &controlv1.CreateFunctionRequest{Name: e.uniq("created-"), Labels: map[string]string{"a": "b", "c": "d"}}
			}},
		{service: fs, method: "GetFunction", target: "orch", token: admin, zero: icNotFound,
			req: func(e *cenv) proto.Message { return &controlv1.GetFunctionRequest{FunctionId: e.fid} }},
		{service: fs, method: "ListFunctions", target: "orch", token: admin, loose: true,
			req: func(e *cenv) proto.Message {
				return &controlv1.ListFunctionsRequest{PageSize: 5, MatchLabels: map[string]string{"x": "y"}, HasLabelKeys: []string{"k"}}
			}},
		{service: fs, method: "DeleteFunction", target: "orch", token: admin, zero: icNotFound,
			req: func(e *cenv) proto.Message { return &controlv1.DeleteFunctionRequest{FunctionId: e.fn()} }},
		{service: fs, method: "CreateVersion", target: "orch", token: admin, zero: icInvalid,
			req: func(e *cenv) proto.Message {
				return &controlv1.CreateVersionRequest{FunctionId: e.fn(), Module: []byte(e.uniq("module-")), Abi: "compat-fake",
					Limits: &commonv1.Limits{MemoryLimitBytes: 64 << 20, MaxResponseBytes: 1 << 20}, Labels: map[string]string{"v": "1"}}
			}},
		{service: fs, method: "ListVersions", target: "orch", token: admin, zero: icAny, loose: true,
			req: func(e *cenv) proto.Message { return &controlv1.ListVersionsRequest{FunctionId: e.fid, PageSize: 3} }},
		{service: fs, method: "ActivateVersion", target: "orch", token: admin, zero: icFormOrNF,
			req: func(e *cenv) proto.Message {
				fid := e.fn()
				return &controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: newVersion(e, fid).Id}
			}},
		{service: fs, method: "SetEnv", target: "orch", token: admin, zero: icInvalid,
			req: func(e *cenv) proto.Message {
				return &controlv1.SetEnvRequest{FunctionId: e.fn(), Entry: &controlv1.EnvEntry{Name: "GREETING", Value: "hi", Secret: true}}
			}},
		{service: fs, method: "DeleteEnv", target: "orch", token: admin, zero: icFormOrNF,
			req: func(e *cenv) proto.Message {
				fid := e.fn()
				e.s.setEnv(fid, "TO_DELETE", "x", false)
				return &controlv1.DeleteEnvRequest{FunctionId: fid, Name: "TO_DELETE"}
			}},
		{service: fs, method: "ListEnv", target: "orch", token: admin, zero: icAny,
			req: func(e *cenv) proto.Message {
				fid := e.fn()
				e.s.setEnv(fid, "A", "1", false)
				return &controlv1.ListEnvRequest{FunctionId: fid}
			}},
		{service: fs, method: "SetEgress", target: "orch", token: admin, zero: icFormOrNF,
			req: func(e *cenv) proto.Message {
				return &controlv1.SetEgressRequest{FunctionId: e.fn(), Allow: []string{"b.example.com", "*.a.example.com:8443"}}
			}},
		{service: fs, method: "SetLabels", target: "orch", token: admin, zero: icFormOrNF,
			req: func(e *cenv) proto.Message {
				return &controlv1.SetLabelsRequest{FunctionId: e.fn(), Labels: map[string]string{"k": "v"}}
			}},
		{service: fs, method: "PatchLabels", target: "orch", token: admin, zero: icFormOrNF,
			req: func(e *cenv) proto.Message {
				return &controlv1.PatchLabelsRequest{FunctionId: e.fn(), Set: map[string]string{"k": "v"}, Remove: []string{"gone"}}
			}},
		// control: routes
		{service: rs, method: "CreateRoute", target: "orch", token: admin, zero: icInvalid,
			req: func(e *cenv) proto.Message {
				return &controlv1.CreateRouteRequest{HostPattern: e.uniq("c") + ".example", PathPrefix: "/api/", FunctionId: e.fn(), Labels: map[string]string{"r": "1"}}
			}},
		{service: rs, method: "ListRoutes", target: "orch", token: admin, loose: true,
			req: func(e *cenv) proto.Message { return &controlv1.ListRoutesRequest{PageSize: 4, FunctionId: e.fid} }},
		{service: rs, method: "DeleteRoute", target: "orch", token: admin, zero: icNotFound,
			req: func(e *cenv) proto.Message { return &controlv1.DeleteRouteRequest{RouteId: newRoute(e, e.fn()).Id} }},
		{service: rs, method: "SetRouteLabels", target: "orch", token: admin, zero: icFormOrNF,
			req: func(e *cenv) proto.Message {
				return &controlv1.SetRouteLabelsRequest{RouteId: newRoute(e, e.fn()).Id, Labels: map[string]string{"k": "v"}}
			}},
		{service: rs, method: "PatchRouteLabels", target: "orch", token: admin, zero: icFormOrNF,
			req: func(e *cenv) proto.Message {
				return &controlv1.PatchRouteLabelsRequest{RouteId: newRoute(e, e.fn()).Id, Set: map[string]string{"k": "v"}}
			}},
		// control: plans
		{service: ps, method: "Validate", target: "orch", token: admin,
			req: func(e *cenv) proto.Message {
				return &controlv1.ValidateRequest{Steps: []*controlv1.Step{
					{Bind: "f", Request: &controlv1.Step_CreateFunction{CreateFunction: &controlv1.CreateFunctionRequest{Name: e.uniq("plan-")}}},
					{Request: &controlv1.Step_CreateRoute{CreateRoute: &controlv1.CreateRouteRequest{HostPattern: e.uniq("p") + ".example", PathPrefix: "/", FunctionId: "$f"}}},
				}}
			}},
		{service: ps, method: "Apply", target: "orch", token: admin,
			req: func(e *cenv) proto.Message {
				return &controlv1.ApplyRequest{Steps: []*controlv1.Step{
					{Bind: "f", Request: &controlv1.Step_CreateFunction{CreateFunction: &controlv1.CreateFunctionRequest{Name: e.uniq("applied-")}}},
					{Request: &controlv1.Step_CreateRoute{CreateRoute: &controlv1.CreateRouteRequest{HostPattern: e.uniq("a") + ".example", PathPrefix: "/", FunctionId: "$f"}}},
				}}
			}},
		// control: inspection
		{service: is, method: "Describe", target: "orch", token: admin,
			req: func(e *cenv) proto.Message { return &controlv1.DescribeRequest{} }},
		{service: is, method: "ListWorkers", target: "orch", token: admin, loose: true,
			req: func(e *cenv) proto.Message { return &controlv1.ListWorkersRequest{} }},
		{service: is, method: "ListInvocations", target: "orch", token: admin, loose: true,
			req: func(e *cenv) proto.Message {
				return &controlv1.ListInvocationsRequest{FunctionId: e.fid, PageSize: 2, RequestId: "nothing-has-this-id"}
			}},
		{service: is, method: "GetInvocation", target: "orch", token: admin, zero: icNotFound, loose: true,
			req: func(e *cenv) proto.Message {
				r := e.s.mustCall(req{host: "compat.test", path: "/echo", body: "for the record"})
				eventually(e.t, "the invocation record", 30*time.Second, func() bool {
					_, err := e.s.ctl.GetInvocation(ctx, &controlv1.GetInvocationRequest{InvocationId: r.head.InvocationId})
					return err == nil
				})
				return &controlv1.GetInvocationRequest{InvocationId: r.head.InvocationId}
			}},
		{service: is, method: "ListNetworkServices", target: "orch", token: admin, loose: true,
			req: func(e *cenv) proto.Message { return &controlv1.ListNetworkServicesRequest{} }},
		// ingress
		{service: ing, method: "Describe", target: "orch", token: ingressToken,
			req: func(e *cenv) proto.Message { return &ingressv1.DescribeRequest{} }},
		{service: ing, method: "Invoke", target: "orch", token: ingressToken, zero: icInvalid,
			req: func(e *cenv) proto.Message {
				return &ingressv1.InvokeRequest{RequestId: e.uniq("req-"), Request: e.httpReq("POST", "/echo", "hello")}
			}},
		// worker: execution (the real worker)
		{service: ex, method: "Describe", target: "worker", token: executionToken,
			req: func(e *cenv) proto.Message { return &workerv1.DescribeRequest{} }},
		{service: ex, method: "Execute", target: "worker", token: executionToken, zero: icInvalid,
			req: func(e *cenv) proto.Message {
				return &workerv1.ExecuteRequest{InvocationId: e.uniq("inv-"), ArtifactDigest: e.digest, Abi: "wagi-1",
					Limits: &commonv1.Limits{MemoryLimitBytes: 64 << 20, MaxResponseBytes: 1 << 20},
					Env:    []*commonv1.EnvVar{{Name: "GREETING", Value: "hi"}}, Request: e.httpReq("POST", "/echo", "hello")}
			}},
		// worker: fleet and artifact (served by the orchestrator)
		{service: fl, method: "Register", target: "orch", token: workerToken, zero: icInvalid,
			req: func(e *cenv) proto.Message {
				return &workerv1.RegisterRequest{Endpoint: "http://127.0.0.1:" + fmt.Sprint(1+e.n.Add(1)), Info: fakeInfo()}
			}},
		{service: fl, method: "Heartbeat", target: "orch", token: workerToken, zero: icInvalid, enum: "state",
			req: func(e *cenv) proto.Message {
				return &workerv1.HeartbeatRequest{WorkerId: registerFake(e), State: workerv1.WorkerState_WORKER_STATE_READY, InFlight: 1}
			}},
		{service: fl, method: "Deregister", target: "orch", token: workerToken,
			req: func(e *cenv) proto.Message { return &workerv1.DeregisterRequest{WorkerId: registerFake(e)} }},
		{service: ar, method: "GetArtifact", target: "orch", token: workerToken, zero: icNotFound, zeroReason: commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND,
			req: func(e *cenv) proto.Message { return &workerv1.GetArtifactRequest{Digest: e.digest} }},
		// egress: registry (served by the orchestrator) and egress (the real network service)
		{service: rg, method: "Register", target: "orch", token: networkToken, zero: icInvalid,
			req: func(e *cenv) proto.Message {
				return &egressv1.RegisterRequest{Endpoint: "http://127.0.0.1:" + fmt.Sprint(1+e.n.Add(1)), Info: svcInfo()}
			}},
		{service: rg, method: "Heartbeat", target: "orch", token: networkToken, zero: icInvalid, enum: "state",
			req: func(e *cenv) proto.Message {
				return &egressv1.HeartbeatRequest{ServiceId: registerSvc(e), State: egressv1.ServiceState_SERVICE_STATE_READY}
			}},
		{service: rg, method: "Deregister", target: "orch", token: networkToken,
			req: func(e *cenv) proto.Message { return &egressv1.DeregisterRequest{ServiceId: registerSvc(e)} }},
		{service: eg, method: "Describe", target: "egress", token: egressToken,
			req: func(e *cenv) proto.Message { return &egressv1.DescribeRequest{} }},
		{service: eg, method: "Fetch", target: "egress", token: egressToken, zero: icInvalid,
			req: func(e *cenv) proto.Message {
				return &egressv1.FetchRequest{Method: "GET", Url: "http://" + e.originAddr + "/p?q=1",
					Headers: []*commonv1.Header{{Name: "x-test", Value: []byte("v")}}, Allow: []string{e.originAddr},
					MaxResponseBytes: 1 << 20, InvocationId: e.uniq("inv-")}
			}},
	}
}

// TestCatalogueCoversEveryRPC fails when a service of gen/ has a method that the
// catalogue (and with it every compatibility test) does not know.
func TestCatalogueCoversEveryRPC(t *testing.T) {
	listed := map[string]bool{}
	for _, c := range catalogue() {
		listed[c.service+"/"+c.method] = true
		c.desc(t) // exists
	}
	total := 0
	files := 0
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if len(fd.Path()) < 8 || fd.Path()[:8] != "edgerun/" {
			return true
		}
		files++
		for i := 0; i < fd.Services().Len(); i++ {
			sd := fd.Services().Get(i)
			for j := 0; j < sd.Methods().Len(); j++ {
				m := sd.Methods().Get(j)
				total++
				if !listed[string(sd.FullName())+"/"+string(m.Name())] {
					t.Errorf("%s/%s is not in the catalogue: add it", sd.FullName(), m.Name())
				}
				if m.IsStreamingClient() {
					t.Errorf("%s/%s is client-streaming; the protocol allows only unary and server-streaming", sd.FullName(), m.Name())
				}
			}
		}
		return true
	})
	if total != len(listed) {
		t.Errorf("the registry has %d RPCs, the catalogue %d", total, len(listed))
	}
	t.Logf("%d RPCs in %d files, all catalogued", total, files)
}

func ctxWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
