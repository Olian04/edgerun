// Package orchestratorconf is the conformance suite for the orchestrator role:
// everything an orchestrator serves, which is the ingress seam
// (edgerun.ingress.v1), the control seam (edgerun.control.v1) and the fleet and
// artifact services workers dial into (edgerun.worker.v1), plus what it does
// as a client of workers (ExecutionService).
//
// The suite is black-box. It starts the orchestrator under test through
// Target.Launch and then plays every other role itself, through the generated
// protocol code alone: it is the frontend (Invoke), the operator (control),
// and a fleet of fake workers it hosts and registers. An orchestrator that
// passes can replace any other that passes, whatever it is written in and
// wherever it keeps its state, without frontends, workers or admin tools
// noticing.
//
// The orchestrator must be dedicated to the suite and start empty: the suite
// counts workers and expects "no worker at all" to be an observable state.
package orchestratorconf

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/gen/edgerun/control/v1/controlv1connect"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
)

const (
	// settle bounds how long a change that is allowed to propagate lazily (a
	// worker joining or going quiet, a record being written) may take to be
	// seen. Changes made through the control seam must be seen at once.
	settle = 15 * time.Second
)

// Env tells the orchestrator under test how to be configured. The suite's
// defaults fill any field left empty in Target.Env.
type Env struct {
	// IngressToken, WorkerToken, AdminToken and NetworkToken are the bearer
	// tokens it must accept, one per scope (frontends, workers, operators,
	// network services); they are distinct.
	IngressToken, WorkerToken, AdminToken, NetworkToken string
	// ExecutionToken is what it must present to workers.
	ExecutionToken string
	// HeartbeatInterval is what it must dictate to workers and to network
	// services, and so one that stays quiet for three of them is no longer alive.
	HeartbeatInterval time.Duration
}

func (e Env) withDefaults() Env {
	if e.IngressToken == "" {
		e.IngressToken = "conformance-ingress-token"
	}
	if e.WorkerToken == "" {
		e.WorkerToken = "conformance-worker-token"
	}
	if e.AdminToken == "" {
		e.AdminToken = "conformance-admin-token"
	}
	if e.NetworkToken == "" {
		e.NetworkToken = "conformance-network-token"
	}
	if e.ExecutionToken == "" {
		e.ExecutionToken = "conformance-execution-token"
	}
	if e.HeartbeatInterval == 0 {
		e.HeartbeatInterval = 200 * time.Millisecond
	}
	return e
}

// Target is the implementation under test, and how to reach the suite.
type Target struct {
	// Launch starts an orchestrator, with empty state, configured per env,
	// and returns its base URL once it is ready to serve. Arranging for it to
	// stop at the end of the test is Launch's job. For an orchestrator that is
	// already running (empty, and used by nothing else), Launch only returns
	// where it is.
	Launch func(t testing.TB, env Env) (url string)
	// Env overrides the suite's defaults for the tokens it configures the
	// orchestrator with.
	Env Env
	// Net says where the suite's fake workers and network services listen, and
	// how the orchestrator reaches them: it calls workers, so they must be
	// reachable from where it runs.
	Net check.Net
}

// Run runs the suite against target.
func Run(t *testing.T, target Target) {
	env := target.Env.withDefaults()
	url := target.Launch(t, env)
	h := newHarness(url, env, target.Net)

	t.Run("Describe", h.describe)
	t.Run("Authentication", h.authentication)
	t.Run("ControlFunctions", h.controlFunctions)
	t.Run("ControlVersions", h.controlVersions)
	t.Run("ControlEnvironment", h.controlEnvironment)
	t.Run("ControlRoutes", h.controlRoutes)
	t.Run("ControlEgress", h.controlEgress)
	t.Run("ControlPagination", h.controlPagination)
	t.Run("ControlLabels", h.controlLabels)
	t.Run("ControlEnvRevision", h.controlEnvRevision)
	t.Run("ControlLabelEditing", h.controlLabelEditing)
	t.Run("ControlFilters", h.controlFilters)
	t.Run("ValidateOnly", h.validateOnly)
	t.Run("Plans", h.plans)
	t.Run("PlansApply", h.plansApply)
	t.Run("Artifacts", h.artifacts)
	t.Run("FleetRegistration", h.fleetRegistration)
	t.Run("FleetLiveness", h.fleetLiveness)
	t.Run("NetworkRegistration", h.networkRegistration)
	t.Run("NetworkLiveness", h.networkLiveness)
	t.Run("NoWorkers", h.noWorkers)
	t.Run("InvokeForwarding", h.invokeForwarding)
	t.Run("InvokeStreaming", h.invokeStreaming)
	t.Run("InvokeValidation", h.invokeValidation)
	t.Run("TraceContext", h.traceContext)
	t.Run("RoutingPrecedence", h.routingPrecedence)
	t.Run("ABIScheduling", h.abiScheduling)
	t.Run("Retries", h.retries)
	t.Run("ErrorPassthrough", h.errorPassthrough)
	t.Run("ReadYourWrites", h.readYourWrites)
	t.Run("NetworkRouting", h.networkRouting)
	t.Run("EgressReachesTheWorker", h.egressReachesTheWorker)
	t.Run("InvocationRecords", h.invocationRecords)
	t.Run("InvocationLookup", h.invocationLookup)
	t.Run("FunctionDeletion", h.functionDeletion)
}

// harness is the suite's side of every seam.
type harness struct {
	url string
	env Env
	net check.Net
	run string // distinguishes this run's names from anything already present

	fn   controlv1connect.FunctionServiceClient
	rt   controlv1connect.RouteServiceClient
	plan controlv1connect.PlanServiceClient
	insp controlv1connect.InspectionServiceClient
	ing  ingressv1connect.IngressServiceClient

	seq atomic.Int64
}

func newHarness(url string, env Env, n check.Net) *harness {
	hc := http.DefaultClient
	admin := check.Bearer(env.AdminToken)
	var run [3]byte
	rand.Read(run[:])
	return &harness{
		url: url, env: env, net: n, run: hex.EncodeToString(run[:]),
		fn:   controlv1connect.NewFunctionServiceClient(hc, url, admin),
		rt:   controlv1connect.NewRouteServiceClient(hc, url, admin),
		plan: controlv1connect.NewPlanServiceClient(hc, url, admin),
		insp: controlv1connect.NewInspectionServiceClient(hc, url, admin),
		ing:  ingressv1connect.NewIngressServiceClient(&http.Client{Transport: budgetTransport{}}, url, check.Bearer(env.IngressToken)),
	}
}

// fleet and artifact return clients for the worker-facing services; token
// is the credential presented ("" for none).
func (h *harness) fleet(token string) workerv1connect.FleetServiceClient {
	return workerv1connect.NewFleetServiceClient(http.DefaultClient, h.url, check.Bearer(token))
}

func (h *harness) artifact(token string) workerv1connect.ArtifactServiceClient {
	return workerv1connect.NewArtifactServiceClient(http.DefaultClient, h.url, check.Bearer(token))
}

// name returns a function name unique to this run and call.
func (h *harness) name(prefix string) string {
	return fmt.Sprintf("%s-%s-%d", prefix, h.run, h.seq.Add(1))
}

// abi returns an ABI name no worker but the one the test registers supports,
// so tests that register workers cannot take each other's requests.
func (h *harness) abi() string { return fmt.Sprintf("c%s-%d", h.run, h.seq.Add(1)) }

// host returns a route host unique to this run and call.
func (h *harness) host() string { return fmt.Sprintf("h%d-%s.conformance.test", h.seq.Add(1), h.run) }

// budgetHeader carries an invocation's wall-clock budget to budgetTransport,
// which puts it on the wire as Connect-Timeout-Ms.
const budgetHeader = "X-Conformance-Budget-Ms"

type budgetTransport struct{}

func (budgetTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if b := r.Header.Get(budgetHeader); b != "" {
		r = r.Clone(r.Context())
		r.Header.Del(budgetHeader)
		r.Header.Set("Connect-Timeout-Ms", b)
	}
	return http.DefaultTransport.RoundTrip(r)
}

func ctxT(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

func sha(b []byte) string {
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:])
}

// module returns distinct bytes for a function; the orchestrator never looks
// inside them, which is the point of the artifact being opaque.
func (h *harness) module(tag string) []byte {
	b := make([]byte, 64)
	rand.Read(b)
	return append([]byte("conformance-module/"+tag+"/"), b...)
}

// --- control helpers ---------------------------------------------------------

func (h *harness) createFunction(t testing.TB, name string) *controlv1.Function {
	t.Helper()
	ctx, cancel := ctxT(settle)
	defer cancel()
	r, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: name}))
	if err != nil {
		t.Fatalf("CreateFunction(%q): %v", name, err)
	}
	return r.Msg.GetFunction()
}

func (h *harness) createVersion(t testing.TB, fnID, abi string, module []byte, limits *commonv1.Limits, activate bool) *controlv1.FunctionVersion {
	t.Helper()
	ctx, cancel := ctxT(60 * time.Second)
	defer cancel()
	r, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{
		FunctionId: fnID, Module: module, Abi: abi, Limits: limits, Activate: activate,
	}))
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	return r.Msg.GetVersion()
}

func (h *harness) createRoute(t testing.TB, host, prefix, fnID string) *controlv1.Route {
	t.Helper()
	ctx, cancel := ctxT(settle)
	defer cancel()
	r, err := h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: prefix, FunctionId: fnID}))
	if err != nil {
		t.Fatalf("CreateRoute(%s%s): %v", host, prefix, err)
	}
	return r.Msg.GetRoute()
}

func (h *harness) setEnv(t testing.TB, fnID, name, value string, secret bool) {
	t.Helper()
	ctx, cancel := ctxT(settle)
	defer cancel()
	if _, err := h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{
		FunctionId: fnID, Entry: &controlv1.EnvEntry{Name: name, Value: value, Secret: secret}})); err != nil {
		t.Fatalf("SetEnv(%s): %v", name, err)
	}
}

// fixture is a deployed function: active version, route, and the facts the
// tests compare against.
type fixture struct {
	fn      *controlv1.Function
	version *controlv1.FunctionVersion
	module  []byte
	abi     string
	host    string
}

func (h *harness) deploy(t testing.TB, abi string) *fixture {
	t.Helper()
	f := &fixture{abi: abi, host: h.host()}
	f.fn = h.createFunction(t, h.name("fn"))
	f.module = h.module(f.fn.GetName())
	f.version = h.createVersion(t, f.fn.GetId(), abi, f.module, nil, true)
	h.createRoute(t, f.host, "/", f.fn.GetId())
	return f
}

// --- invoking ------------------------------------------------------------------

// result is everything one Invoke produced.
type result struct {
	head         *ingressv1.InvokeHead
	body         []byte
	err          error
	invocationID string
}

type call struct {
	host, path, rawQuery, method string
	body                         []byte
	headers                      []*commonv1.Header
	requestID                    string
	traceparent                  string // the call carries this trace context, if set
	budget                       time.Duration
}

func (h *harness) invoke(t testing.TB, c call) result {
	t.Helper()
	if c.method == "" {
		c.method = "GET"
	}
	if c.path == "" {
		c.path = "/"
	}
	if c.budget == 0 {
		c.budget = 30 * time.Second
	}
	// The budget is what the orchestrator is told (Connect-Timeout-Ms, see
	// budgetTransport); the local guard is longer, so that the orchestrator's own
	// typed error arrives before our timer could replace it with a bare context
	// error. Connect clients derive the wire deadline from the context, which
	// would make the two the same value.
	ctx, cancel := ctxT(c.budget + 15*time.Second)
	defer cancel()
	ireq := connect.NewRequest(&ingressv1.InvokeRequest{
		RequestId: c.requestID,
		Request: &commonv1.HttpRequest{
			Method: c.method, Host: c.host, Path: c.path, RawQuery: c.rawQuery, Headers: c.headers,
			Body: c.body, RemoteAddr: "192.0.2.7:4321",
		},
	})
	ireq.Header().Set(budgetHeader, strconv.FormatInt(c.budget.Milliseconds(), 10))
	if c.traceparent != "" {
		ireq.Header().Set("Traceparent", c.traceparent)
	}
	st, err := h.ing.Invoke(ctx, ireq)
	if err != nil {
		return result{err: err}
	}
	defer st.Close()
	var out result
	for st.Receive() {
		switch p := st.Msg().GetPart().(type) {
		case *ingressv1.InvokeResponse_Head:
			if out.head != nil {
				t.Errorf("protocol violation: a second response head")
			}
			out.head = p.Head
			out.invocationID = p.Head.GetInvocationId()
		case *ingressv1.InvokeResponse_Chunk:
			if out.head == nil {
				t.Errorf("protocol violation: a chunk before the head")
			}
			out.body = append(out.body, p.Chunk...)
		default:
			t.Errorf("protocol violation: an empty InvokeResponse")
		}
	}
	out.err = st.Err()
	if out.err == nil && out.head == nil {
		t.Errorf("protocol violation: the stream ended OK without a head")
	}
	return out
}

func (h *harness) mustInvoke(t testing.TB, c call) result {
	t.Helper()
	r := h.invoke(t, c)
	if r.err != nil {
		t.Fatalf("Invoke %s%s: %v", c.host, c.path, r.err)
	}
	return r
}

// served waits until host is answered with a response, which is how a test
// learns that a freshly registered worker has become schedulable.
func (h *harness) served(t testing.TB, host string) {
	t.Helper()
	check.Eventually(t, "the worker to be scheduled for "+host, settle, func() bool {
		return h.invoke(t, call{host: host}).err == nil
	})
}

func bodyOf(r result) string { return string(bytes.TrimSpace(r.body)) }
