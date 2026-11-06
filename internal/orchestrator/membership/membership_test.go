package membership_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	membershipv1 "github.com/Olian04/edgerun/gen/edgerun/membership/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/orchestrator/membership"
	"github.com/Olian04/edgerun/internal/orchestrator/orchtest"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
	"github.com/Olian04/edgerun/internal/testpg"
)

func TestMain(m *testing.M) { testpg.Main(m) }

const (
	workerToken  = "worker-secret"
	networkToken = "network-secret"
	adminToken   = "admin-secret"
)

var ctx = context.Background()

type rig struct {
	st      *store.Store
	srv     *httptest.Server
	changes *atomic.Int32
}

func newRig(t *testing.T) *rig {
	t.Helper()
	st := orchtest.Store(t)
	changes := &atomic.Int32{}
	svc := membership.New(membership.Config{Store: st, HeartbeatInterval: 2 * time.Second, OnChange: func() { changes.Add(1) },
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	mux := http.NewServeMux()
	svc.Mount(mux, authn.Verifier{workerToken: membership.WorkerScope, networkToken: membership.NetworkScope, adminToken: membership.AdminScope})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &rig{st: st, changes: changes, srv: srv}
}

func (r *rig) as(token string) *edgerun.Membership {
	return edgerun.NewMembership(r.srv.URL, token, edgerun.WithHTTPClient(r.srv.Client()))
}

func workerInfo() *workerv1.WorkerInfo {
	return &workerv1.WorkerInfo{Abis: []string{"wagi-1"}, MaxConcurrency: 4,
		Implementation: &commonv1.Implementation{Name: "t", Protocols: []string{"edgerun.worker.v1"}}}
}

func serviceInfo() *egressv1.ServiceInfo {
	return &egressv1.ServiceInfo{Covers: []string{"*"}, Implementation: &commonv1.Implementation{Name: "ns", Protocols: []string{"edgerun.egress.v1"}}}
}

func worker(endpoint string) *membershipv1.RegisterRequest {
	return &membershipv1.RegisterRequest{Endpoint: endpoint, Info: &membershipv1.RegisterRequest_Worker{Worker: workerInfo()}}
}

func netService(endpoint string) *membershipv1.RegisterRequest {
	return &membershipv1.RegisterRequest{Endpoint: endpoint, Info: &membershipv1.RegisterRequest_NetworkService{NetworkService: serviceInfo()}}
}

func hb(id string, state membershipv1.MemberState, inFlight uint32) *membershipv1.HeartbeatRequest {
	return &membershipv1.HeartbeatRequest{MemberId: id, State: state, InFlight: inFlight}
}

const ready = membershipv1.MemberState_MEMBER_STATE_READY

func TestRegisterHeartbeatDeregisterAWorker(t *testing.T) {
	r := newRig(t)
	w := r.as(workerToken)
	reg, err := w.Register(ctx, worker("http://10.0.0.1:9000"))
	if err != nil {
		t.Fatal(err)
	}
	if reg.MemberId == "" || reg.HeartbeatInterval.AsDuration() != 2*time.Second {
		t.Errorf("register response = %+v: the orchestrator dictates the interval", reg)
	}
	if r.changes.Load() != 1 {
		t.Error("registering must notify so the invoke path sees the member at once")
	}
	h, err := w.Heartbeat(ctx, hb(reg.MemberId, ready, 2))
	if err != nil || h.Reregister {
		t.Fatalf("heartbeat: %v %+v", err, h)
	}
	if r.changes.Load() != 1 {
		t.Error("heartbeats are frequent and must not trigger refreshes")
	}
	ws, _ := r.st.ReadyWorkers(ctx, time.Minute)
	if len(ws) != 1 || ws[0].InFlight != 2 {
		t.Errorf("ready workers = %+v", ws)
	}
	if _, err := w.Deregister(ctx, &membershipv1.DeregisterRequest{MemberId: reg.MemberId}); err != nil {
		t.Fatal(err)
	}
	if r.changes.Load() != 2 {
		t.Error("deregistering must notify")
	}
	h, _ = w.Heartbeat(ctx, hb(reg.MemberId, ready, 0))
	if !h.Reregister {
		t.Error("a heartbeat from an unknown member must tell it to re-register")
	}
}

func TestRegisterHeartbeatDeregisterANetworkService(t *testing.T) {
	r := newRig(t)
	n := r.as(networkToken)
	reg, err := n.Register(ctx, netService("http://10.0.0.2:9100"))
	if err != nil || reg.MemberId == "" {
		t.Fatalf("%v %+v", err, reg)
	}
	if h, err := n.Heartbeat(ctx, hb(reg.MemberId, ready, 0)); err != nil || h.Reregister {
		t.Fatalf("heartbeat: %v %+v", err, h)
	}
	if _, err := n.Deregister(ctx, &membershipv1.DeregisterRequest{MemberId: reg.MemberId}); err != nil {
		t.Fatal(err)
	}
	if h, _ := n.Heartbeat(ctx, hb(reg.MemberId, ready, 0)); !h.Reregister {
		t.Error("a heartbeat from an unknown network service must tell it to re-register")
	}
}

func TestReregistrationInvalidatesTheOldID(t *testing.T) {
	r := newRig(t)
	w := r.as(workerToken)
	a, _ := w.Register(ctx, worker("http://w:1"))
	b, _ := w.Register(ctx, worker("http://w:1"))
	if a.MemberId == b.MemberId {
		t.Fatal("same endpoint must get a fresh id")
	}
	if h, _ := w.Heartbeat(ctx, hb(a.MemberId, ready, 0)); !h.Reregister {
		t.Error("the replaced member id must be told to re-register")
	}
}

func TestValidationAndErrors(t *testing.T) {
	r := newRig(t)
	w := r.as(workerToken)
	_, err := w.Register(ctx, &membershipv1.RegisterRequest{Endpoint: "not a url", Info: &membershipv1.RegisterRequest_Worker{Worker: workerInfo()}})
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("bad endpoint: %v", err)
	}
	_, err = w.Register(ctx, &membershipv1.RegisterRequest{Endpoint: "http://w:1"})
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("no info: %v", err)
	}
	_, err = w.Register(ctx, &membershipv1.RegisterRequest{Endpoint: "http://w:1", Info: &membershipv1.RegisterRequest_Worker{}})
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("an empty worker: %v", err)
	}
	for _, st := range []membershipv1.MemberState{membershipv1.MemberState_MEMBER_STATE_UNSPECIFIED, 99} {
		_, err = w.Heartbeat(ctx, hb("x", st, 0))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("heartbeat state %v: %v", st, err)
		}
	}
	if _, err := w.Deregister(ctx, &membershipv1.DeregisterRequest{MemberId: "never-existed"}); err != nil {
		t.Errorf("deregister must be idempotent: %v", err)
	}
}

func TestEachScopeDoesOnlyItsOwn(t *testing.T) {
	r := newRig(t)
	for name, tok := range map[string]string{"none": "", "unknown": "nope"} {
		_, err := r.as(tok).Register(ctx, worker("http://w:1"))
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("%s: %v", name, err)
		}
	}
	denied := func(what string, err error) {
		t.Helper()
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("%s: %v, want permission_denied", what, err)
		}
	}
	// A worker cannot register a network service, nor the other way round, nor list; an operator cannot be a member.
	_, err := r.as(workerToken).Register(ctx, netService("http://n:1"))
	denied("a worker registering a network service", err)
	_, err = r.as(networkToken).Register(ctx, worker("http://w:1"))
	denied("a network service registering a worker", err)
	_, err = r.as(adminToken).Register(ctx, worker("http://w:1"))
	denied("an operator registering", err)
	_, err = r.as(adminToken).Heartbeat(ctx, hb("x", ready, 0))
	denied("an operator heartbeating", err)
	_, err = r.as(workerToken).ListMembers(ctx, &membershipv1.ListMembersRequest{})
	denied("a worker listing", err)
	_, err = r.as(networkToken).ListMembers(ctx, &membershipv1.ListMembersRequest{})
	denied("a network service listing", err)

	// Each heartbeats only its own kind: a worker's id means nothing to a network service's heartbeat.
	reg, _ := r.as(workerToken).Register(ctx, worker("http://w:1"))
	if h, err := r.as(networkToken).Heartbeat(ctx, hb(reg.MemberId, ready, 0)); err != nil || !h.Reregister {
		t.Errorf("a network service heartbeating a worker's id: %v %v, want reregister", h, err)
	}
	if _, err := r.as(networkToken).Deregister(ctx, &membershipv1.DeregisterRequest{MemberId: reg.MemberId}); err != nil {
		t.Fatal(err)
	}
	if h, _ := r.as(workerToken).Heartbeat(ctx, hb(reg.MemberId, ready, 0)); h.Reregister {
		t.Error("a network service's deregister removed a worker")
	}
}

func TestListMembersShowsBothKindsOrderedAndFiltered(t *testing.T) {
	r := newRig(t)
	r.as(workerToken).Register(ctx, worker("http://b-worker:1"))
	r.as(workerToken).Register(ctx, worker("http://a-worker:1"))
	r.as(networkToken).Register(ctx, netService("http://c-service:1"))

	all, err := r.as(adminToken).ListMembers(ctx, &membershipv1.ListMembersRequest{})
	if err != nil || len(all.Members) != 3 {
		t.Fatalf("%v %v", all, err)
	}
	var got []string
	for _, m := range all.Members {
		got = append(got, m.Kind.String()+" "+m.Endpoint)
		if !m.Alive || m.State != ready || m.LastHeartbeatTime == nil {
			t.Errorf("%v: alive=%v state=%v", m.Endpoint, m.Alive, m.State)
		}
		switch m.Kind {
		case membershipv1.MemberKind_MEMBER_KIND_WORKER:
			if m.GetWorker() == nil || m.GetNetworkService() != nil {
				t.Errorf("a worker carries its WorkerInfo: %v", m)
			}
		case membershipv1.MemberKind_MEMBER_KIND_NETWORK_SERVICE:
			if m.GetNetworkService() == nil || len(m.GetNetworkService().Covers) == 0 {
				t.Errorf("a network service carries its ServiceInfo: %v", m)
			}
		}
	}
	want := "[MEMBER_KIND_WORKER http://a-worker:1 MEMBER_KIND_WORKER http://b-worker:1 MEMBER_KIND_NETWORK_SERVICE http://c-service:1]"
	if fmt.Sprint(got) != want {
		t.Errorf("order %v, want %s", got, want)
	}
	only, _ := r.as(adminToken).ListMembers(ctx, &membershipv1.ListMembersRequest{Kind: membershipv1.MemberKind_MEMBER_KIND_NETWORK_SERVICE})
	if len(only.Members) != 1 || only.Members[0].Endpoint != "http://c-service:1" {
		t.Errorf("filtered: %v", only.Members)
	}
}
