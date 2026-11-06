package e2e

// More of part 1(b)/(c): the worker, the network service's registration client,
// the worker's egress client and the CLI RECEIVE answers from scripted peers.

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Olian04/edgerun"
	"github.com/Olian04/edgerun/conformance/guest"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/egress/v1/egressv1connect"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	"github.com/Olian04/edgerun/internal/ctl"
	egressapp "github.com/Olian04/edgerun/internal/egress/app"
	workerapp "github.com/Olian04/edgerun/internal/worker/app"
)

func needBearer(h http.Header, token string) error {
	if h.Get("Authorization") != "Bearer "+token {
		return connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("scripted peer: credentials %q", h.Get("Authorization")))
	}
	return nil
}

// ---------------------------------------------------------------------------
// the worker against a scripted orchestrator (fleet and artifacts)

func TestCompatWorkerAgainstScriptedOrchestrators(t *testing.T) {
	module := guest.Build(t)
	digest := edgerun.Digest(module)
	missing := "sha256:" + strings.Repeat("1", 64)
	broken := "sha256:" + strings.Repeat("2", 64)

	for _, p := range newPersonas(t) {
		t.Run(p.name, func(t *testing.T) {
			srv := newRawServer(t)
			var mu sync.Mutex
			var endpoints []string
			var states []workerv1.WorkerState
			var heartbeats, deregisters int
			srv.unary(workerv1connect.FleetServiceRegisterProcedure, func(_ context.Context, h http.Header, req []byte) ([]byte, error) {
				if err := needBearer(h, workerToken); err != nil {
					return nil, err
				}
				var r workerv1.RegisterRequest
				if err := proto.Unmarshal(req, &r); err != nil || len(r.Info.GetAbis()) == 0 || r.Info.GetMaxConcurrency() != 2 {
					return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("bad registration %v %v", err, &r))
				}
				mu.Lock()
				endpoints = append(endpoints, r.Endpoint)
				n := len(endpoints)
				mu.Unlock()
				return p.dress(&workerv1.RegisterResponse{WorkerId: fmt.Sprintf("w-%d", n), HeartbeatInterval: durationpb.New(100 * time.Millisecond)}), nil
			})
			srv.unary(workerv1connect.FleetServiceHeartbeatProcedure, func(_ context.Context, h http.Header, req []byte) ([]byte, error) {
				if err := needBearer(h, workerToken); err != nil {
					return nil, err
				}
				var r workerv1.HeartbeatRequest
				if err := proto.Unmarshal(req, &r); err != nil {
					return nil, connect.NewError(connect.CodeInvalidArgument, err)
				}
				mu.Lock()
				heartbeats++
				n := heartbeats
				states = append(states, r.State)
				mu.Unlock()
				if n == 3 { // a transient failure, with a reason nobody knows
					return nil, scriptedError(connect.CodeUnavailable, p.info(99, secretMessage, ""), secretMessage)
				}
				return p.dress(&workerv1.HeartbeatResponse{Reregister: n == 5}), nil
			})
			srv.unary(workerv1connect.FleetServiceDeregisterProcedure, func(context.Context, http.Header, []byte) ([]byte, error) {
				mu.Lock()
				deregisters++
				mu.Unlock()
				return p.dress(&workerv1.DeregisterResponse{}), nil
			})
			srv.stream(workerv1connect.ArtifactServiceGetArtifactProcedure, func(_ context.Context, h http.Header, req []byte, send func([]byte) error) error {
				if err := needBearer(h, workerToken); err != nil {
					return err
				}
				var r workerv1.GetArtifactRequest
				if err := proto.Unmarshal(req, &r); err != nil {
					return connect.NewError(connect.CodeInvalidArgument, err)
				}
				switch r.Digest {
				case digest:
					for b := module; len(b) > 0; {
						n := min(len(b), 700<<10)
						if err := send(p.dress(&workerv1.GetArtifactResponse{Chunk: b[:n]})); err != nil {
							return err
						}
						b = b[n:]
					}
					return nil
				case broken:
					return scriptedError(connect.CodeUnknown, p.info(99, secretMessage, ""), secretMessage)
				}
				return scriptedError(connect.CodeNotFound, p.info(int32(commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND), "no such artifact", ""), "no such artifact")
			})

			wlog, wsink := newSink()
			wctx, stop := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- workerapp.Run(wctx, workerapp.Config{Listen: "127.0.0.1:0", OrchestratorURL: srv.URL(), WorkerToken: workerToken,
					ExecutionToken: executionToken, MaxConcurrency: 2, DrainTimeout: 5 * time.Second, Log: wlog})
			}()
			t.Cleanup(func() { stop(); <-done })

			eventually(t, "the worker to register", 30*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(endpoints) > 0 })
			mu.Lock()
			endpoint := endpoints[0]
			mu.Unlock()
			exec := edgerun.NewExecution(endpoint, executionToken)
			run := func(d string) (*edgerun.ExecutionResult, []byte, error) {
				c, cancel := ctxWithTimeout(60 * time.Second)
				defer cancel()
				res, err := exec.Execute(c, &workerv1.ExecuteRequest{InvocationId: "inv-1", ArtifactDigest: d, Abi: "wagi-1",
					Request: &commonv1.HttpRequest{Method: "POST", Host: "x.test", Path: "/echo", Body: []byte("through a peer with unknown fields")}})
				if err != nil {
					return nil, nil, err
				}
				b, err := res.ReadAll()
				return res, b, err
			}

			t.Run("the artifact arrives through responses with unknown fields", func(t *testing.T) {
				res, body, err := run(digest)
				if err != nil || string(body) != "through a peer with unknown fields" || res.Status() != 200 {
					t.Fatalf("%v %q", err, body)
				}
				if res.Summary() == nil || hasUnknown(res.Summary().ProtoReflect()) {
					t.Errorf("summary: %v", res.Summary())
				}
			})
			t.Run("an artifact error is typed whatever else the detail carries", func(t *testing.T) {
				if _, _, err := run(missing); edgerun.Reason(err) != commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND {
					t.Errorf("unknown artifact: %v, want ARTIFACT_NOT_FOUND", err)
				}
				// a reason nobody knows is not "not found": the worker's own fault
				if _, _, err := run(broken); edgerun.Reason(err) != commonv1.ErrorReason_ERROR_REASON_INTERNAL {
					t.Errorf("artifact fetch failing with an unknown reason: %v, want INTERNAL", err)
				}
			})
			t.Run("heartbeats go on through failures, and an answer to re-register is obeyed", func(t *testing.T) {
				eventually(t, "heartbeats and a second registration", 30*time.Second, func() bool {
					mu.Lock()
					defer mu.Unlock()
					return heartbeats >= 9 && len(endpoints) >= 2
				})
				mu.Lock()
				for _, st := range states {
					if st != workerv1.WorkerState_WORKER_STATE_READY {
						t.Errorf("a heartbeat said %s", st)
					}
				}
				mu.Unlock()
			})
			t.Run("it deregisters when stopped", func(t *testing.T) {
				stop()
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("the worker stopped with %v", err)
					}
					done <- nil
				case <-time.After(30 * time.Second):
					t.Fatal("the worker did not stop")
				}
				mu.Lock()
				defer mu.Unlock()
				if deregisters != 1 {
					t.Errorf("%d deregistrations", deregisters)
				}
			})
			if strings.Contains(wsink.String(), unknownCanary) {
				t.Errorf("the worker's log contains the payload of an unknown field")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// a network service against a scripted orchestrator (registry)

func TestCompatNetworkServiceAgainstScriptedOrchestrators(t *testing.T) {
	for _, p := range newPersonas(t) {
		t.Run(p.name, func(t *testing.T) {
			srv := newRawServer(t)
			var mu sync.Mutex
			var registers, heartbeats, deregisters int
			var states []egressv1.ServiceState
			var covers []string
			srv.unary(egressv1connect.RegistryServiceRegisterProcedure, func(_ context.Context, h http.Header, req []byte) ([]byte, error) {
				if err := needBearer(h, networkToken); err != nil {
					return nil, err
				}
				var r egressv1.RegisterRequest
				if err := proto.Unmarshal(req, &r); err != nil {
					return nil, connect.NewError(connect.CodeInvalidArgument, err)
				}
				mu.Lock()
				registers++
				n := registers
				covers = r.Info.GetCovers()
				mu.Unlock()
				if n == 1 { // refused, with a reason nobody knows
					return nil, scriptedError(connect.CodeInvalidArgument, p.info(99, secretMessage, ""), secretMessage)
				}
				return p.dress(&egressv1.RegisterResponse{ServiceId: fmt.Sprintf("s-%d", n), HeartbeatInterval: durationpb.New(100 * time.Millisecond)}), nil
			})
			srv.unary(egressv1connect.RegistryServiceHeartbeatProcedure, func(_ context.Context, h http.Header, req []byte) ([]byte, error) {
				var r egressv1.HeartbeatRequest
				if err := proto.Unmarshal(req, &r); err != nil {
					return nil, connect.NewError(connect.CodeInvalidArgument, err)
				}
				mu.Lock()
				heartbeats++
				n := heartbeats
				states = append(states, r.State)
				mu.Unlock()
				return p.dress(&egressv1.HeartbeatResponse{Reregister: n == 4 && r.State == egressv1.ServiceState_SERVICE_STATE_READY}), nil
			})
			srv.unary(egressv1connect.RegistryServiceDeregisterProcedure, func(context.Context, http.Header, []byte) ([]byte, error) {
				mu.Lock()
				deregisters++
				mu.Unlock()
				return p.dress(&egressv1.DeregisterResponse{}), nil
			})
			elog, esink := newSink()
			eg, err := egressapp.Start(ctx, egressapp.Config{Listen: "127.0.0.1:0", EgressToken: egressToken, OrchestratorURL: srv.URL(),
				NetworkToken: networkToken, Covers: []string{"*.Example.com", "10.0.0.0/8"}, DrainGrace: 50 * time.Millisecond, Log: elog})
			if err != nil {
				t.Fatal(err)
			}
			stopped := false
			t.Cleanup(func() {
				if !stopped {
					eg.Stop(context.Background())
				}
			})
			eventually(t, "a refused registration, a second one, heartbeats and a re-registration", 40*time.Second, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return registers >= 3 && heartbeats >= 7
			})
			mu.Lock()
			if strings.Join(covers, ",") != "*.example.com,10.0.0.0/8" {
				t.Errorf("registered coverage %v, want it canonical", covers)
			}
			mu.Unlock()
			if err := eg.Stop(context.Background()); err != nil {
				t.Errorf("stop: %v", err)
			}
			stopped = true
			mu.Lock()
			if len(states) == 0 || states[len(states)-1] != egressv1.ServiceState_SERVICE_STATE_DRAINING {
				t.Errorf("the last heartbeat reported %v, want DRAINING", states)
			}
			if deregisters != 1 {
				t.Errorf("%d deregistrations", deregisters)
			}
			mu.Unlock()
			if !strings.Contains(esink.String(), "refused") {
				t.Errorf("a refused registration was not reported to the operator")
			}
			if strings.Contains(esink.String(), unknownCanary) {
				t.Errorf("the service's log contains the payload of an unknown field")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// the worker against a scripted network service (egress)

// egressRig is a real orchestrator and worker (wagi-2) whose only network service
// is a scripted one, and a function that fetches through it.
type egressRig struct {
	s         *stack
	workerLog *logSink
	// fetch makes the guest request GET http://scripted.compat.example<path> and
	// returns what it printed: "code=N\n" and the response text.
	fetch func(path string) string
}

func startEgressRig(t *testing.T, install func(srv *rawServer)) *egressRig {
	t.Helper()
	module := guest.BuildNet(t)
	s, _ := newCStack(t, nil)
	wlog, wsink := newSink()
	wctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- workerapp.Run(wctx, workerapp.Config{Listen: "127.0.0.1:0", OrchestratorURL: "http://" + s.addr, WorkerToken: workerToken,
			ExecutionToken: executionToken, EgressToken: egressToken, MaxConcurrency: 2, DrainTimeout: 5 * time.Second, Log: wlog})
	}()
	t.Cleanup(func() { stop(); <-done })

	srv := newRawServer(t)
	install(srv)
	// the service is registered the way a real one registers, and kept alive
	reg := edgerun.NewRegistry("http://"+s.addr, networkToken)
	rr, err := reg.Register(ctx, &egressv1.RegisterRequest{Endpoint: srv.URL(), Info: &egressv1.ServiceInfo{
		Implementation: &commonv1.Implementation{Name: "scripted-egress"}, Covers: []string{"*"}}})
	if err != nil {
		t.Fatal(err)
	}
	hbStop := make(chan struct{})
	t.Cleanup(func() { close(hbStop) })
	go func() {
		tk := time.NewTicker(100 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-hbStop:
				return
			case <-tk.C:
				reg.Heartbeat(ctx, &egressv1.HeartbeatRequest{ServiceId: rr.ServiceId, State: egressv1.ServiceState_SERVICE_STATE_READY})
			}
		}
	}()

	fid := s.createFunction("talker")
	if _, err := s.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: module, Abi: "wagi-2", Activate: true}); err != nil {
		t.Fatal(err)
	}
	s.route("net.test", "/", fid)
	if _, err := s.ctl.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{"scripted.compat.example"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the wagi-2 function to run", 120*time.Second, func() bool {
		return s.call(req{host: "net.test", path: "/noop"}).err == nil
	})
	rig := &egressRig{s: s, workerLog: wsink}
	rig.fetch = func(path string) string {
		r := s.call(req{host: "net.test", path: "/fetch", method: "POST", body: "GET http://scripted.compat.example" + path + "\n\n"})
		if r.err != nil {
			t.Fatalf("%s: %v", path, r.err)
		}
		return string(r.body)
	}
	// the grant exists once the orchestrator has seen the service: until then the guest is told "denied"
	eventually(t, "the scripted network service to be granted", 60*time.Second, func() bool {
		return !strings.HasPrefix(rig.fetch("/ok"), "code=-1\n")
	})
	return rig
}

func codeLine(out string) string {
	first, _, _ := strings.Cut(out, "\n")
	return first
}

func TestCompatWorkerEgressClientAgainstScriptedNetworkServices(t *testing.T) {
	for _, p := range newPersonas(t) {
		t.Run(p.name, func(t *testing.T) {
			chunk := func(s string) []byte {
				return p.dress(&egressv1.FetchResponse{Part: &egressv1.FetchResponse_Chunk{Chunk: []byte(s)}})
			}
			head := func() []byte {
				return p.dress(&egressv1.FetchResponse{Part: &egressv1.FetchResponse_Head{Head: &commonv1.HttpResponseHead{Status: 200,
					Headers: []*commonv1.Header{{Name: "x-r", Value: []byte("1")}}}}})
			}
			rig := startEgressRig(t, func(srv *rawServer) {
				srv.stream(egressv1connect.EgressServiceFetchProcedure, func(_ context.Context, h http.Header, req []byte, send func([]byte) error) error {
					if err := needBearer(h, egressToken); err != nil {
						return err
					}
					var r egressv1.FetchRequest
					if err := proto.Unmarshal(req, &r); err != nil {
						return connect.NewError(connect.CodeInvalidArgument, err)
					}
					u, _ := url.Parse(r.Url)
					extra := func() {
						if p.extra != nil {
							send(p.extra("edgerun.egress.v1.FetchResponse"))
						}
					}
					switch path := u.Path; {
					case path == "/ok":
						send(head())
						send(chunk("A"))
						extra()
						send(chunk("B"))
						return nil
					case path == "/early":
						extra()
						send(head())
						return send(chunk("never seen"))
					case strings.HasPrefix(path, "/err/"):
						f := strings.Split(path, "/")
						reason, _ := strconv.Atoi(f[2])
						code, _ := strconv.Atoi(f[3])
						return scriptedError(connect.Code(code), p.info(int32(reason), secretMessage, ""), secretMessage)
					}
					return connect.NewError(connect.CodeNotFound, nil)
				})
			})

			got := rig.fetch("/ok")
			if !strings.HasPrefix(got, "code=") || strings.HasPrefix(got, "code=-") || !strings.Contains(got, "Status: 200") ||
				!strings.Contains(got, "x-r: 1") || !strings.HasSuffix(got, "AB") {
				t.Errorf("a response with unknown fields and parts: %q", got)
			}
			for _, tc := range []struct {
				path, want, what string
			}{
				{"/err/10/7", "code=-1", "EGRESS_DENIED"},
				{"/err/11/14", "code=-2", "EGRESS_FAILED"},
				{"/err/7/8", "code=-3", "RESPONSE_TOO_LARGE"},
				{"/err/6/3", "code=-4", "REQUEST_TOO_LARGE"},
				{"/err/3/4", "code=-2", "DEADLINE_EXCEEDED"},
				{"/err/9/13", "code=-2", "INTERNAL"},
				{"/err/99/2", "code=-2", "an unknown reason"},
				{"/err/77/8", "code=-2", "an unknown reason with the code of RESPONSE_TOO_LARGE"},
				{"/err/0/3", "code=-4", "no reason, invalid_argument"},
				{"/err/0/14", "code=-2", "no reason, unavailable"},
				{"/early", "code=-2", "a part v1.0 does not know before the head"},
			} {
				if tc.path == "/early" && p.extra == nil {
					continue
				}
				out := rig.fetch(tc.path)
				if codeLine(out) != tc.want {
					t.Errorf("%s: the guest was told %q, want %s", tc.what, codeLine(out), tc.want)
				}
				if strings.Contains(out, secretMessage) {
					t.Errorf("%s: the network service's message reached the guest", tc.what)
				}
			}
			if strings.Contains(rig.workerLog.String(), unknownCanary) {
				t.Errorf("the worker's log contains the payload of an unknown field")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// the CLI against a scripted orchestrator (control)

func TestCompatCLIAgainstScriptedControl(t *testing.T) {
	for _, p := range newPersonas(t) {
		t.Run(p.name, func(t *testing.T) {
			srv := newRawServer(t)
			now := timestamppb.Now()
			info := &workerv1.WorkerInfo{Implementation: &commonv1.Implementation{Name: "w", Version: "1"}, Abis: []string{"wagi-1", "wagi-9"}, MaxConcurrency: 4}
			srv.unary("/edgerun.control.v1.InspectionService/ListWorkers", func(context.Context, http.Header, []byte) ([]byte, error) {
				return p.dress(&controlv1.ListWorkersResponse{Workers: []*controlv1.WorkerStatus{
					{Id: "w1", Endpoint: "http://w1.example:1", Info: info, State: workerv1.WorkerState(99), InFlight: 1, LastHeartbeatTime: now, Alive: true},
					{Id: "w2", Endpoint: "http://w2.example:1", Info: info, State: workerv1.WorkerState_WORKER_STATE_READY, LastHeartbeatTime: now},
				}}), nil
			})
			srv.unary("/edgerun.control.v1.InspectionService/ListNetworkServices", func(context.Context, http.Header, []byte) ([]byte, error) {
				return p.dress(&controlv1.ListNetworkServicesResponse{Services: []*controlv1.NetworkServiceStatus{
					{Id: "s1", Endpoint: "http://s1.example:1", State: egressv1.ServiceState(99), LastHeartbeatTime: now, Alive: true,
						Info: &egressv1.ServiceInfo{Covers: []string{"*"}}},
				}}), nil
			})
			srv.unary("/edgerun.control.v1.InspectionService/ListInvocations", func(context.Context, http.Header, []byte) ([]byte, error) {
				return p.dress(&controlv1.ListInvocationsResponse{Invocations: []*controlv1.Invocation{
					{Id: "i1", RequestId: "r1", FunctionId: "f1", Method: "GET", Path: "/x", Status: 0, ErrorReason: commonv1.ErrorReason(99), StartTime: now, Stderr: []byte("\xff\xfe")},
				}}), nil
			})
			srv.unary("/edgerun.control.v1.FunctionService/ListFunctions", func(context.Context, http.Header, []byte) ([]byte, error) {
				return p.dress(&controlv1.ListFunctionsResponse{Functions: []*controlv1.Function{
					{Id: "f1", Name: "fn", CreateTime: now, Labels: map[string]string{"k": "v"}, EgressAllow: []string{"a.example"}},
				}}), nil
			})
			for _, cmd := range [][]string{{"workers"}, {"networks"}, {"invocations"}, {"functions"}} {
				var out, errb bytes.Buffer
				code := ctl.Run(ctx, append([]string{"--url", srv.URL(), "--token", "t"}, cmd...), func(string) string { return "" }, &out, &errb)
				if code != 0 {
					t.Errorf("%v: exit %d: %s", cmd, code, errb.String())
				}
				if out.Len() == 0 || strings.Contains(out.String()+errb.String(), unknownCanary) || strings.Contains(errb.String(), "panic") {
					t.Errorf("%v: output %q / %q", cmd, out.String(), errb.String())
				}
			}
		})
	}
}
