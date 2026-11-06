package edgerun_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	"github.com/Olian04/edgerun/conformance/guest"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	egressapp "github.com/Olian04/edgerun/internal/egress/app"
	orchapp "github.com/Olian04/edgerun/internal/orchestrator/app"
	"github.com/Olian04/edgerun/internal/orchestrator/orchtest"
	"github.com/Olian04/edgerun/internal/testpg"
	workerapp "github.com/Olian04/edgerun/internal/worker/app"
)

func TestMain(m *testing.M) { testpg.Main(m) }

// These tests use the library, and only the library, as every role's peer of a
// running system: the orchestrator, worker and network service are the real
// ones, so they prove the library speaks what edgerun speaks.

const (
	ingressToken   = "ingress-token"
	workerToken    = "worker-token"
	adminToken     = "admin-token"
	networkToken   = "network-token"
	executionToken = "execution-token"
	egressToken    = "egress-token"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type system struct {
	url  string
	wasm []byte
}

func start(t *testing.T) system {
	t.Helper()
	srv, err := orchapp.Start(context.Background(), orchapp.Config{
		Listen: "127.0.0.1:0", DatabaseURL: testpg.DSN(t),
		IngressToken: ingressToken, WorkerToken: workerToken, AdminToken: adminToken, NetworkToken: networkToken, ExecutionToken: executionToken,
		SecretKey: orchtest.Key, HeartbeatInterval: 200 * time.Millisecond, RefreshEvery: 100 * time.Millisecond,
		MaxRequestBodyBytes: 4 << 20, ShutdownTimeout: 20 * time.Second, Log: quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop(context.Background()) })
	url := "http://" + srv.Addr()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- workerapp.Run(ctx, workerapp.Config{
			Listen: "127.0.0.1:0", OrchestratorURL: url, WorkerToken: workerToken, ExecutionToken: executionToken,
			MaxConcurrency: 4, DrainTimeout: 10 * time.Second, Log: quiet,
		})
	}()
	t.Cleanup(func() { cancel(); <-done })
	return system{url: url, wasm: guest.Build(t)}
}

func TestAnOperatorAndAFrontendOverTheLibrary(t *testing.T) {
	sys := start(t)
	ctx := context.Background()
	ctl := edgerun.NewControl(sys.url, adminToken)
	ing := edgerun.NewIngress(sys.url, ingressToken)

	info, err := ctl.Describe(ctx, &controlv1.DescribeRequest{})
	if err != nil || info.GetMaxModuleBytes() == 0 {
		t.Fatalf("describe: %v, %v", info, err)
	}

	fn, err := ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	fid := fn.Function.Id
	ver, err := ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: sys.wasm, Abi: "wagi-1", Activate: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: "lib.test", PathPrefix: "/", FunctionId: fid}); err != nil {
		t.Fatal(err)
	}

	t.Run("lists", func(t *testing.T) {
		var names []string
		for f, err := range ctl.AllFunctions(ctx) {
			if err != nil {
				t.Fatal(err)
			}
			names = append(names, f.Name)
		}
		if len(names) != 1 || names[0] != "hello" {
			t.Errorf("functions: %q", names)
		}
		n := 0
		for v, err := range ctl.AllVersions(ctx, fid) {
			if err != nil {
				t.Fatal(err)
			}
			if v.Id != ver.Version.Id {
				t.Errorf("version %s", v.Id)
			}
			n++
		}
		if n != 1 {
			t.Errorf("%d versions", n)
		}
		for r, err := range ctl.AllRoutes(ctx) {
			if err != nil || r.FunctionId != fid {
				t.Errorf("route %v, %v", r, err)
			}
		}
	})

	var invocationID string
	t.Run("a request goes through and comes back", func(t *testing.T) {
		// The route and the worker take a moment to be known. The context
		// outlives the body: cancelling it ends the response.
		deadline := time.Now().Add(20 * time.Second)
		var res *edgerun.Invocation
		var body []byte
		for {
			c, cancel := context.WithTimeout(ctx, 5*time.Second)
			res, err = ing.Invoke(c, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{
				Method: "POST", Host: "lib.test", Path: "/echo", Body: []byte("through the library"),
				Headers: []*commonv1.Header{{Name: "Content-Type", Value: []byte("text/plain")}}}})
			if err == nil {
				body, err = res.ReadAll()
			}
			cancel()
			if err == nil || time.Now().After(deadline) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			t.Fatal(err)
		}
		if res.Status() != 200 || string(body) != "through the library" {
			t.Fatalf("status %d, body %q", res.Status(), body)
		}
		if res.ID == "" {
			t.Error("no invocation id")
		}
		invocationID = res.ID
	})

	t.Run("the invocation is on record", func(t *testing.T) {
		deadline := time.Now().Add(10 * time.Second)
		for {
			for inv, err := range ctl.AllInvocations(ctx, fid) {
				if err != nil {
					t.Fatal(err)
				}
				if inv.Id == invocationID {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("invocation %s never appeared", invocationID)
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	t.Run("a platform failure has a reason", func(t *testing.T) {
		_, err := ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: "unrouted.test", Path: "/"}})
		if !edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND) {
			t.Errorf("a host no route serves: reason %v, %v", edgerun.Reason(err), err)
		}
		_, err = ctl.GetFunction(ctx, &controlv1.GetFunctionRequest{FunctionId: "00000000-0000-0000-0000-000000000000"})
		if connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("a function that does not exist: %v", err)
		}
	})

	t.Run("each token opens its own seam only", func(t *testing.T) {
		if _, err := edgerun.NewControl(sys.url, ingressToken).Describe(ctx, &controlv1.DescribeRequest{}); connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("an ingress token on control: %v", err)
		}
		if _, err := edgerun.NewControl(sys.url, "nonsense").Describe(ctx, &controlv1.DescribeRequest{}); connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("an unknown token: %v", err)
		}
	})
}

func TestAWorkersSideOverTheLibrary(t *testing.T) {
	sys := start(t)
	ctx := context.Background()
	ctl := edgerun.NewControl(sys.url, adminToken)

	fn, _ := ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "w"})
	ver, err := ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fn.Function.Id, Module: sys.wasm, Abi: "wagi-1"})
	if err != nil {
		t.Fatal(err)
	}
	digest := ver.Version.ArtifactDigest

	t.Run("artifacts are fetched and verified", func(t *testing.T) {
		art := edgerun.NewArtifacts(sys.url, workerToken)
		got, err := art.GetArtifact(ctx, digest)
		if err != nil {
			t.Fatal(err)
		}
		if sum := sha256.Sum256(sys.wasm); string(got) != string(sys.wasm) || digest != "sha256:"+hex.EncodeToString(sum[:]) {
			t.Errorf("got %d bytes for %s", len(got), digest)
		}
		_, err = art.GetArtifact(ctx, "sha256:"+strings.Repeat("0", 64))
		if !edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND) {
			t.Errorf("an unknown digest: %v", err)
		}
		if _, err := art.GetArtifact(ctx, "md5:abc"); err == nil {
			t.Error("a digest that is not sha256 must be refused before any call")
		}
		if _, err := edgerun.NewArtifacts(sys.url, adminToken).GetArtifact(ctx, digest); connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("an admin token must not fetch artifacts: %v", err)
		}
	})

	t.Run("a worker of one's own joins, stays, and leaves", func(t *testing.T) {
		fleet := edgerun.NewFleet(sys.url, workerToken)
		reg, err := fleet.Register(ctx, &workerv1.RegisterRequest{
			Endpoint: "http://127.0.0.1:1",
			Info: &workerv1.WorkerInfo{
				Implementation: &commonv1.Implementation{Name: "library-test", Version: "0"},
				Abis:           []string{"wagi-1"}, MaxConcurrency: 1,
			}})
		if err != nil {
			t.Fatal(err)
		}
		listed := func() *controlv1.WorkerStatus {
			r, err := ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range r.Workers {
				if w.Id == reg.WorkerId {
					return w
				}
			}
			return nil
		}
		if w := listed(); w == nil || !w.Alive || w.Endpoint != "http://127.0.0.1:1" {
			t.Fatalf("registered worker: %v", w)
		}
		if hb, err := fleet.Heartbeat(ctx, &workerv1.HeartbeatRequest{WorkerId: reg.WorkerId, State: workerv1.WorkerState_WORKER_STATE_READY}); err != nil || hb.Reregister {
			t.Errorf("heartbeat: %v, %v", hb, err)
		}
		if hb, err := fleet.Heartbeat(ctx, &workerv1.HeartbeatRequest{WorkerId: "00000000-0000-0000-0000-000000000000", State: workerv1.WorkerState_WORKER_STATE_READY}); err != nil || !hb.Reregister {
			t.Errorf("an unknown worker is told to register again: %v, %v", hb, err)
		}
		if _, err := fleet.Deregister(ctx, &workerv1.DeregisterRequest{WorkerId: reg.WorkerId}); err != nil {
			t.Fatal(err)
		}
		if w := listed(); w != nil && w.Alive {
			t.Errorf("a worker that left is still alive: %v", w)
		}
	})

	t.Run("a scheduler of one's own runs a module on a real worker", func(t *testing.T) {
		var endpoint string
		deadline := time.Now().Add(20 * time.Second)
		for endpoint == "" {
			r, err := ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range r.Workers {
				if w.Alive && w.Endpoint != "http://127.0.0.1:1" {
					endpoint = w.Endpoint
				}
			}
			if endpoint == "" {
				if time.Now().After(deadline) {
					t.Fatal("no worker came up")
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
		ex := edgerun.NewExecution(endpoint, executionToken)
		d, err := ex.Describe(ctx, &workerv1.DescribeRequest{})
		if err != nil || len(d.GetInfo().GetAbis()) == 0 {
			t.Fatalf("describe: %v, %v", d, err)
		}
		res, err := ex.Execute(ctx, &workerv1.ExecuteRequest{
			InvocationId: "lib-1", ArtifactDigest: digest, Abi: "wagi-1",
			Request: &commonv1.HttpRequest{Method: "POST", Host: "x.test", Path: "/echo", Body: []byte("run me")},
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.Summary() != nil {
			t.Error("the summary closes the stream: it cannot be known before the body has been read")
		}
		body, err := res.ReadAll()
		if err != nil || string(body) != "run me" || res.Status() != 200 {
			t.Fatalf("status %d, body %q, %v", res.Status(), body, err)
		}
		if res.Summary() == nil || res.Summary().DurationMicros == 0 {
			t.Errorf("summary after the body: %v", res.Summary())
		}
		_, err = ex.Execute(ctx, &workerv1.ExecuteRequest{InvocationId: "lib-2", ArtifactDigest: digest, Abi: "wagi-9",
			Request: &commonv1.HttpRequest{Method: "GET", Path: "/"}})
		if !edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH) {
			t.Errorf("an ABI the worker does not speak: %v", err)
		}
		if _, err := edgerun.NewExecution(endpoint, "wrong").Describe(ctx, &workerv1.DescribeRequest{}); connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("a wrong token: %v", err)
		}
	})
}

func TestANetworkServicesSideOverTheLibrary(t *testing.T) {
	sys := start(t)
	ctx := context.Background()
	ctl := edgerun.NewControl(sys.url, adminToken)

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Origin", "yes")
		w.WriteHeader(418)
		fmt.Fprintf(w, "%s %s", r.Method, r.URL.Path)
	}))
	t.Cleanup(origin.Close)
	host := strings.TrimPrefix(origin.URL, "http://")

	svc, err := egressapp.Start(ctx, egressapp.Config{
		Listen: "127.0.0.1:0", EgressToken: egressToken, OrchestratorURL: sys.url, NetworkToken: networkToken,
		Covers: []string{"127.0.0.0/8"}, AllowNetworks: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}, Log: quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { svc.Stop(context.Background()) })
	eg := edgerun.NewEgress("http://"+svc.Addr(), egressToken)

	t.Run("what it carries is what it said", func(t *testing.T) {
		d, err := eg.Describe(ctx, &egressv1.DescribeRequest{})
		if err != nil || len(d.Covers) != 1 || d.Covers[0] != "127.0.0.0/8" {
			t.Fatalf("describe: %v, %v", d, err)
		}
	})

	t.Run("a request through it", func(t *testing.T) {
		res, err := eg.Fetch(ctx, &egressv1.FetchRequest{Method: "GET", Url: origin.URL + "/thing", Allow: []string{host}})
		if err != nil {
			t.Fatal(err)
		}
		body, err := res.ReadAll()
		if err != nil || res.Status() != 418 || string(body) != "GET /thing" || res.Header().Get("X-Origin") != "yes" {
			t.Errorf("status %d, body %q, headers %v, %v", res.Status(), body, res.Header(), err)
		}
		_, err = eg.Fetch(ctx, &egressv1.FetchRequest{Method: "GET", Url: origin.URL + "/thing", Allow: []string{"elsewhere.test"}})
		if !edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED) {
			t.Errorf("a destination the caller did not permit: %v", err)
		}
	})

	t.Run("a service of one's own registers, stays, and leaves", func(t *testing.T) {
		reg := edgerun.NewRegistry(sys.url, networkToken)
		r, err := reg.Register(ctx, &egressv1.RegisterRequest{
			Endpoint: "http://127.0.0.1:1",
			Info:     &egressv1.ServiceInfo{Implementation: &commonv1.Implementation{Name: "library-test", Version: "0"}, Covers: []string{"*.library.test"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		seen := func() *controlv1.NetworkServiceStatus {
			l, err := ctl.ListNetworkServices(ctx, &controlv1.ListNetworkServicesRequest{})
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range l.Services {
				if s.Id == r.ServiceId {
					return s
				}
			}
			return nil
		}
		if s := seen(); s == nil || !s.Alive || s.Info.Covers[0] != "*.library.test" {
			t.Fatalf("registered service: %v", s)
		}
		if hb, err := reg.Heartbeat(ctx, &egressv1.HeartbeatRequest{ServiceId: r.ServiceId, State: egressv1.ServiceState_SERVICE_STATE_READY}); err != nil || hb.Reregister {
			t.Errorf("heartbeat: %v, %v", hb, err)
		}
		if _, err := reg.Deregister(ctx, &egressv1.DeregisterRequest{ServiceId: r.ServiceId}); err != nil {
			t.Fatal(err)
		}
		if s := seen(); s != nil && s.Alive {
			t.Errorf("a service that left is still alive: %v", s)
		}
		if _, err := edgerun.NewRegistry(sys.url, workerToken).Register(ctx, &egressv1.RegisterRequest{}); connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("a worker token must not register services: %v", err)
		}
	})
}
