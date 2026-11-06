package orchsuite

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	membershipv1 "github.com/Olian04/edgerun/gen/edgerun/membership/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

// fakeNetService is a network service written from the protocol alone: it
// registers with the orchestrator under test, says what it carries, and keeps
// heartbeating. The orchestrator never calls it (workers do), so it serves
// nothing.
type fakeNetService struct {
	t        testing.TB
	h        *harness
	endpoint string
	covers   []string

	mu      sync.Mutex
	id      string
	state   membershipv1.MemberState
	paused  bool
	stopped chan struct{}
	done    sync.WaitGroup
}

// netService starts a service that carries covers, registers it, and removes
// it when the test ends.
func (h *harness) netService(t testing.TB, covers ...string) *fakeNetService {
	t.Helper()
	s := &fakeNetService{
		t: t, h: h, covers: covers, state: membershipv1.MemberState_MEMBER_STATE_READY, stopped: make(chan struct{}),
		endpoint: fmt.Sprintf("http://netsvc%d-%s.conformance.test:8090", h.seq.Add(1), h.run),
	}
	interval := s.register()
	s.done.Add(1)
	go s.heartbeat(interval)
	t.Cleanup(s.stop)
	return s
}

func (s *fakeNetService) register() time.Duration {
	ctx, cancel := ctxT(settle)
	defer cancel()
	r, err := s.h.membership(s.h.env.NetworkToken).Register(ctx, connect.NewRequest(&membershipv1.RegisterRequest{
		Endpoint: s.endpoint,
		Info: &membershipv1.RegisterRequest_NetworkService{NetworkService: &egressv1.ServiceInfo{
			Implementation: &commonv1.Implementation{Name: "conformance-network-service", Version: "1", Protocols: []string{"edgerun.egress.v1"}},
			Covers:         s.covers,
		}},
	}))
	if err != nil {
		s.t.Fatalf("Register(%v): %v", s.covers, err)
	}
	s.mu.Lock()
	s.id = r.Msg.GetMemberId()
	s.mu.Unlock()
	if s.id == "" || r.Msg.GetHeartbeatInterval().AsDuration() <= 0 {
		s.t.Fatalf("Register returned id %q and interval %v", s.id, r.Msg.GetHeartbeatInterval().AsDuration())
	}
	return r.Msg.GetHeartbeatInterval().AsDuration()
}

func (s *fakeNetService) heartbeat(every time.Duration) {
	defer s.done.Done()
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-s.stopped:
			return
		case <-tick.C:
		}
		s.mu.Lock()
		id, state, paused := s.id, s.state, s.paused
		s.mu.Unlock()
		if paused {
			continue
		}
		ctx, cancel := ctxT(5 * time.Second)
		r, err := s.h.membership(s.h.env.NetworkToken).Heartbeat(ctx, connect.NewRequest(&membershipv1.HeartbeatRequest{MemberId: id, State: state}))
		cancel()
		if err == nil && r.Msg.GetReregister() {
			s.register()
		}
	}
}

func (s *fakeNetService) stop() {
	select {
	case <-s.stopped:
		return
	default:
	}
	close(s.stopped)
	s.done.Wait()
	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()
	s.mu.Lock()
	id := s.id
	s.mu.Unlock()
	s.h.membership(s.h.env.NetworkToken).Deregister(ctx, connect.NewRequest(&membershipv1.DeregisterRequest{MemberId: id}))
}

func (s *fakeNetService) pause(p bool) { s.mu.Lock(); s.paused = p; s.mu.Unlock() }

func (s *fakeNetService) setState(st membershipv1.MemberState) {
	s.mu.Lock()
	s.state = st
	s.mu.Unlock()
}

// grantsOf describes the egress grants a worker was last told, as
// "entry=service" in the order given.
func grantsOf(gs []*workerv1.EgressGrant) []string {
	var out []string
	for _, g := range gs {
		out = append(out, g.GetAllow()+"="+g.GetService())
	}
	return out
}

// lastGrants returns the grants of the most recent Execute call w received.
func lastGrants(w *fakeWorker) []string {
	calls := w.calls()
	if len(calls) == 0 {
		return nil
	}
	return grantsOf(calls[len(calls)-1].req.GetEgress())
}

// networkRouting (Execution client: the grants the orchestrator puts into Execute; they depend on
// which network services are registered with Membership and on the function's allow list in Catalog):
// each destination a function may reach is given to the network
// service that carries it, the most specific one, and left out if none does.
func (h *harness) networkRouting(t *testing.T) {
	ctx, cancel := ctxT(120 * time.Second)
	defer cancel()
	w, f := h.stack(t, respond("ok"))
	h.served(t, f.host)
	setAllow := func(allow ...string) {
		t.Helper()
		if _, err := h.fn.SetEgress(ctx, connect.NewRequest(&controlv1.SetEgressRequest{FunctionId: f.fn.GetId(), Allow: allow})); err != nil {
			t.Fatal(err)
		}
	}
	// invoke until the grants the worker is told equal want. Changes in the
	// fleet of network services spread lazily, as those of workers do.
	waitGrants := func(what string, want ...string) {
		t.Helper()
		sort.Strings(want)
		var got []string
		check.Eventually(t, what+": the worker to be told "+strings.Join(want, ", "), settle, func() bool {
			h.invoke(t, call{host: f.host})
			got = append([]string(nil), lastGrants(w)...)
			sort.Strings(got)
			return fmt.Sprint(got) == fmt.Sprint(want)
		})
	}

	// A function nobody opened up gets nothing, whatever services exist.
	all := h.netService(t, "*")
	h.invoke(t, call{host: f.host})
	if g := lastGrants(w); len(g) != 0 {
		t.Errorf("a function with no allow list was given %v", g)
	}

	private := h.netService(t, "10.0.0.0/8", "*.corp.test")
	exact := h.netService(t, "api.example.com")
	allow := []string{"api.example.com", "www.example.com", "10.1.2.3:8080", "db.corp.test:5432", "*.cdn.example.com"}
	setAllow(allow...)
	want := []string{
		"api.example.com=" + exact.endpoint, "www.example.com=" + all.endpoint, "10.1.2.3:8080=" + private.endpoint,
		"db.corp.test:5432=" + private.endpoint, "*.cdn.example.com=" + all.endpoint,
	}
	waitGrants("each destination to the most specific service that carries it", want...)

	t.Run("grants follow the allow list's order", func(t *testing.T) {
		got := lastGrants(w)
		var entries []string
		for _, g := range got {
			entries = append(entries, strings.SplitN(g, "=", 2)[0])
		}
		if fmt.Sprint(entries) != "[*.cdn.example.com 10.1.2.3:8080 api.example.com db.corp.test:5432 www.example.com]" {
			t.Errorf("grants %v: the worker takes the first that matches, so they must be in the allow list's canonical order", entries)
		}
	})

	t.Run("a service that leaves hands its destinations on", func(t *testing.T) {
		exact.stop()
		waitGrants("the exact service to be gone",
			"api.example.com="+all.endpoint, "www.example.com="+all.endpoint, "10.1.2.3:8080="+private.endpoint,
			"db.corp.test:5432="+private.endpoint, "*.cdn.example.com="+all.endpoint)
	})

	t.Run("a silent service is given nothing", func(t *testing.T) {
		all.pause(true)
		waitGrants("the catch-all to go quiet", "10.1.2.3:8080="+private.endpoint, "db.corp.test:5432="+private.endpoint)
		all.pause(false)
		waitGrants("the catch-all to return",
			"api.example.com="+all.endpoint, "www.example.com="+all.endpoint, "10.1.2.3:8080="+private.endpoint,
			"db.corp.test:5432="+private.endpoint, "*.cdn.example.com="+all.endpoint)
	})

	t.Run("a draining service is given nothing new", func(t *testing.T) {
		private.setState(membershipv1.MemberState_MEMBER_STATE_DRAINING)
		// The catch-all carries everything, the addresses and names the private
		// service carried included.
		waitGrants("the private service to drain",
			"api.example.com="+all.endpoint, "www.example.com="+all.endpoint, "10.1.2.3:8080="+all.endpoint,
			"db.corp.test:5432="+all.endpoint, "*.cdn.example.com="+all.endpoint)
		private.setState(membershipv1.MemberState_MEMBER_STATE_READY)
		waitGrants("the private service to be ready again",
			"api.example.com="+all.endpoint, "www.example.com="+all.endpoint, "10.1.2.3:8080="+private.endpoint,
			"db.corp.test:5432="+private.endpoint, "*.cdn.example.com="+all.endpoint)
	})

	t.Run("a destination nobody carries is left out", func(t *testing.T) {
		all.stop()
		waitGrants("the catch-all to be gone", "10.1.2.3:8080="+private.endpoint, "db.corp.test:5432="+private.endpoint)
		private.stop()
		waitGrants("every service to be gone")
	})

	t.Run("a service carries a wildcard only if it carries all of it", func(t *testing.T) {
		narrow := h.netService(t, "*.cdn.example.com")
		wide := h.netService(t, "*.example.com")
		setAllow("*.example.com", "*.cdn.example.com", "img.cdn.example.com")
		waitGrants("the narrow and the wide service",
			"*.example.com="+wide.endpoint, "*.cdn.example.com="+narrow.endpoint, "img.cdn.example.com="+narrow.endpoint)
	})

	t.Run("equally specific services share a destination stably", func(t *testing.T) {
		a := h.netService(t, "*.shared.test")
		b := h.netService(t, "*.shared.test")
		var entries []string
		for i := 0; i < 12; i++ {
			entries = append(entries, fmt.Sprintf("h%d.shared.test", i))
		}
		setAllow(entries...)
		check.Eventually(t, "the equal services to carry every entry", settle, func() bool {
			h.invoke(t, call{host: f.host})
			return len(lastGrants(w)) == len(entries)
		})
		first := lastGrants(w)
		for _, g := range first {
			if svc := strings.SplitN(g, "=", 2)[1]; svc != a.endpoint && svc != b.endpoint {
				t.Errorf("grant %s names neither of the equal services", g)
			}
		}
		h.invoke(t, call{host: f.host})
		if again := lastGrants(w); fmt.Sprint(again) != fmt.Sprint(first) {
			t.Errorf("the choice changed between two invocations with nothing changed: %v then %v", first, again)
		}
	})
}
