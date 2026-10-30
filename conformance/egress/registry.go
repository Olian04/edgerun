package egressconf

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/Olian04/edgerun/conformance/internal/check"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/egress/v1/egressv1connect"
)

// registry is the orchestrator side of service registration: the
// RegistryService a network service dials into. It records everything the
// service does, and can be told to misbehave, so that the suite can assert on
// how the service copes.
type registry struct {
	env Env
	srv *check.Server

	mu           sync.Mutex
	registered   []registered // every successful registration, in order
	byEndpoint   map[string]registered
	heartbeats   []heartbeat
	deregistered []string
	attempts     int
	failN        int             // answer the next N registrations with "unavailable"
	forget       map[string]bool // answer the next heartbeat from this id with reregister
	authFailures int
	changed      chan struct{} // closed and replaced on every change
}

type registered struct {
	ID       string
	Endpoint string
	Info     *egressv1.ServiceInfo
}

type heartbeat struct {
	ID    string
	State egressv1.ServiceState
	At    time.Time
}

func newRegistry(t *testing.T, env Env, n check.Net) *registry {
	r := &registry{env: env, byEndpoint: map[string]registered{}, forget: map[string]bool{}, changed: make(chan struct{})}
	mux := http.NewServeMux()
	mux.Handle(egressv1connect.NewRegistryServiceHandler(registryHandler{r}))
	r.srv = n.Serve(t, mux, true)
	return r
}

func (r *registry) update(f func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f()
	close(r.changed)
	r.changed = make(chan struct{})
}

func (r *registry) failRegistrations(n int) { r.update(func() { r.failN = n }) }

// forgetNext makes the next heartbeat from id be answered "register again".
func (r *registry) forgetNext(id string) { r.update(func() { r.forget[id] = true }) }

func norm(endpoint string) string { return strings.TrimSuffix(endpoint, "/") }

func (r *registry) registrationFor(endpoint string) registered {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byEndpoint[norm(endpoint)]
}

func (r *registry) waitFor(t testing.TB, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		r.mu.Lock()
		ok, ch := cond(), r.changed
		r.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-ch:
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
	}
}

// waitRegistered waits for the service reachable at endpoint to register, and
// returns what it declared.
func (r *registry) waitRegistered(t testing.TB, endpoint string) registered {
	t.Helper()
	r.waitFor(t, "the network service at "+endpoint+" to register with the suite's registry (is it configured to register at "+r.srv.URL+"?)", 90*time.Second,
		func() bool { _, ok := r.byEndpoint[norm(endpoint)]; return ok })
	return r.registrationFor(endpoint)
}

func (r *registry) heartbeatsOf(id string) []heartbeat {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []heartbeat
	for _, h := range r.heartbeats {
		if h.ID == id {
			out = append(out, h)
		}
	}
	return out
}

func (r *registry) isDeregistered(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, d := range r.deregistered {
		if d == id {
			return true
		}
	}
	return false
}

func (r *registry) authorised(h http.Header) error {
	if h.Get("Authorization") == "Bearer "+r.env.NetworkToken {
		return nil
	}
	r.update(func() { r.authFailures++ })
	return connect.NewError(connect.CodeUnauthenticated, errors.New("bad network token"))
}

type registryHandler struct{ r *registry }

func (h registryHandler) Register(_ context.Context, req *connect.Request[egressv1.RegisterRequest]) (*connect.Response[egressv1.RegisterResponse], error) {
	if err := h.r.authorised(req.Header()); err != nil {
		return nil, err
	}
	var id string
	var fail bool
	h.r.update(func() {
		h.r.attempts++
		if h.r.failN > 0 {
			h.r.failN--
			fail = true
			return
		}
		id = "service-" + strconv.Itoa(len(h.r.registered)+1)
		rg := registered{ID: id, Endpoint: norm(req.Msg.GetEndpoint()), Info: req.Msg.GetInfo()}
		h.r.registered = append(h.r.registered, rg)
		h.r.byEndpoint[rg.Endpoint] = rg
	})
	if fail {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("the registry is not ready"))
	}
	return connect.NewResponse(&egressv1.RegisterResponse{ServiceId: id, HeartbeatInterval: durationpb.New(h.r.env.HeartbeatInterval)}), nil
}

func (h registryHandler) Heartbeat(_ context.Context, req *connect.Request[egressv1.HeartbeatRequest]) (*connect.Response[egressv1.HeartbeatResponse], error) {
	if err := h.r.authorised(req.Header()); err != nil {
		return nil, err
	}
	var again bool
	h.r.update(func() {
		h.r.heartbeats = append(h.r.heartbeats, heartbeat{req.Msg.GetServiceId(), req.Msg.GetState(), time.Now()})
		if h.r.forget[req.Msg.GetServiceId()] {
			delete(h.r.forget, req.Msg.GetServiceId())
			again = true
		}
	})
	return connect.NewResponse(&egressv1.HeartbeatResponse{Reregister: again}), nil
}

func (h registryHandler) Deregister(_ context.Context, req *connect.Request[egressv1.DeregisterRequest]) (*connect.Response[egressv1.DeregisterResponse], error) {
	if err := h.r.authorised(req.Header()); err != nil {
		return nil, err
	}
	h.r.update(func() { h.r.deregistered = append(h.r.deregistered, req.Msg.GetServiceId()) })
	return connect.NewResponse(&egressv1.DeregisterResponse{}), nil
}
