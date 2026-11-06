// Package fakeorch is the orchestrator a suite plays when it judges a component
// that dials one: a scripted host of Membership (workers and network services
// register with it, heartbeat to it and leave it) and of Artifacts (a worker
// fetches module bytes from it). It records everything the component does so a
// suite can assert on it, and can be told to misbehave (refuse registrations,
// forget a member) to see how the component copes.
//
// It judges nothing itself and takes nothing from any implementation: it is
// written from the protocol alone.
package fakeorch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	membershipv1 "github.com/Olian04/edgerun/gen/edgerun/membership/v1"
	"github.com/Olian04/edgerun/gen/edgerun/membership/v1/membershipv1connect"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
)

// Config says which credentials the fake accepts and what it dictates.
type Config struct {
	// WorkerToken is the scope that may register, heartbeat and deregister
	// workers, and fetch artifacts.
	WorkerToken string
	// NetworkToken is the scope that may register, heartbeat and deregister
	// network services. Empty: network services are not accepted.
	NetworkToken string
	// Interval is the heartbeat interval it dictates.
	Interval time.Duration
}

// Member is one successful registration.
type Member struct {
	ID       string
	Kind     membershipv1.MemberKind
	Endpoint string
	Worker   *workerv1.WorkerInfo // for a worker
	Service  *egressv1.ServiceInfo
}

// Beat is one heartbeat.
type Beat struct {
	ID       string
	State    membershipv1.MemberState
	InFlight uint32
	At       time.Time
}

// Orchestrator is the scripted host. Its recorded state is read with the
// embedded lock held (directly, or inside a WaitFor condition).
type Orchestrator struct {
	sync.Mutex
	Server *check.Server
	cfg    Config

	// Registrations is every successful registration, in order; Known holds
	// the ones that are current (an endpoint registered again replaces the
	// older id; a forgotten member is dropped).
	Registrations []Member
	Known         map[string]Member
	Beats         []Beat
	Deregistered  []string
	// Attempts counts every Register call that got as far as being judged.
	Attempts int
	// AuthFailures counts calls refused for their credentials.
	AuthFailures int
	Blobs        map[string][]byte

	failN   int
	forget  map[string]bool
	changed chan struct{} // closed and replaced on every change
}

// New starts a fake orchestrator. It is the primary server of the suite: its
// port, if n.Listen names one, is fixed.
func New(t testing.TB, n check.Net, cfg Config) *Orchestrator {
	o := &Orchestrator{cfg: cfg, Known: map[string]Member{}, Blobs: map[string][]byte{}, forget: map[string]bool{}, changed: make(chan struct{})}
	mux := http.NewServeMux()
	mux.Handle(membershipv1connect.NewMembershipServiceHandler(membership{o}))
	mux.Handle(workerv1connect.NewArtifactServiceHandler(artifacts{o}))
	o.Server = n.Serve(t, mux, true)
	return o
}

// URL is where the component under test must reach it.
func (o *Orchestrator) URL() string { return o.Server.URL }

// Update runs f under the lock and wakes everyone waiting for a change.
func (o *Orchestrator) Update(f func()) {
	o.Lock()
	defer o.Unlock()
	f()
	close(o.changed)
	o.changed = make(chan struct{})
}

// WaitFor blocks until cond, evaluated under the lock, holds.
func (o *Orchestrator) WaitFor(t testing.TB, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		o.Lock()
		ok, ch := cond(), o.changed
		o.Unlock()
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

// Norm strips what does not tell endpoints apart for a person: the scheme and a
// trailing slash. Endpoints are compared as exact text by the protocol; the
// suite only looks a member up by one of its own, for which this is enough.
func Norm(endpoint string) string {
	return strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"), "/")
}

// MemberAt returns the current registration of the member at endpoint (host:port
// or a URL), or of the first member if endpoint is empty, and whether there is
// one. Call with the lock held.
func (o *Orchestrator) MemberAt(endpoint string, kind membershipv1.MemberKind) (Member, bool) {
	want := Norm(endpoint)
	for i := len(o.Registrations) - 1; i >= 0; i-- {
		m := o.Registrations[i]
		if _, current := o.Known[m.ID]; !current || m.Kind != kind {
			continue
		}
		if want == "" || Norm(m.Endpoint) == want {
			return m, true
		}
	}
	return Member{}, false
}

// BeatsOf returns the heartbeats of id, oldest first. Call with the lock held.
func (o *Orchestrator) BeatsOf(id string) []Beat {
	var out []Beat
	for _, b := range o.Beats {
		if b.ID == id {
			out = append(out, b)
		}
	}
	return out
}

// Left reports whether id has deregistered. Call with the lock held.
func (o *Orchestrator) Left(id string) bool {
	for _, d := range o.Deregistered {
		if d == id {
			return true
		}
	}
	return false
}

// FailRegistrations answers the next n Register calls with "unavailable", as an
// orchestrator that is not up yet does.
func (o *Orchestrator) FailRegistrations(n int) { o.Update(func() { o.failN = n }) }

// ForgetNext answers the next heartbeat from id with reregister, and drops id
// from the members it knows.
func (o *Orchestrator) ForgetNext(id string) { o.Update(func() { o.forget[id] = true }) }

// AddBlob stores bytes under their true digest and returns it.
func (o *Orchestrator) AddBlob(b []byte) string {
	sum := sha256.Sum256(b)
	d := "sha256:" + hex.EncodeToString(sum[:])
	o.SetBlob(d, b)
	return d
}

// SetBlob stores bytes under an arbitrary digest, true or not.
func (o *Orchestrator) SetBlob(digest string, b []byte) { o.Update(func() { o.Blobs[digest] = b }) }

// RemoveBlob forgets a digest.
func (o *Orchestrator) RemoveBlob(digest string) { o.Update(func() { delete(o.Blobs, digest) }) }

// Blob returns the bytes stored under digest.
func (o *Orchestrator) Blob(digest string) []byte {
	o.Lock()
	defer o.Unlock()
	return o.Blobs[digest]
}

func (o *Orchestrator) refuse(code connect.Code, why string) error {
	o.Update(func() { o.AuthFailures++ })
	return connect.NewError(code, errors.New(why))
}

// token returns the bearer token of a call, or "".
func token(h http.Header) string {
	v, _ := strings.CutPrefix(h.Get("Authorization"), "Bearer ")
	return v
}

// scopeOf says which scope a token has: "worker", "network" or "" for none.
func (o *Orchestrator) scopeOf(h http.Header) string {
	tok := token(h)
	switch {
	case tok == "":
		return ""
	case tok == o.cfg.WorkerToken:
		return "worker"
	case o.cfg.NetworkToken != "" && tok == o.cfg.NetworkToken:
		return "network"
	}
	return ""
}

type membership struct{ o *Orchestrator }

func (m membership) Register(_ context.Context, r *connect.Request[membershipv1.RegisterRequest]) (*connect.Response[membershipv1.RegisterResponse], error) {
	o := m.o
	scope := o.scopeOf(r.Header())
	if scope == "" {
		return nil, o.refuse(connect.CodeUnauthenticated, "unknown token")
	}
	mem := Member{Endpoint: r.Msg.GetEndpoint()}
	switch info := r.Msg.GetInfo().(type) {
	case *membershipv1.RegisterRequest_Worker:
		mem.Kind, mem.Worker = membershipv1.MemberKind_MEMBER_KIND_WORKER, info.Worker
		if scope != "worker" {
			return nil, o.refuse(connect.CodePermissionDenied, "a network token may not register a worker")
		}
	case *membershipv1.RegisterRequest_NetworkService:
		mem.Kind, mem.Service = membershipv1.MemberKind_MEMBER_KIND_NETWORK_SERVICE, info.NetworkService
		if scope != "network" {
			return nil, o.refuse(connect.CodePermissionDenied, "a worker token may not register a network service")
		}
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("info is required"))
	}
	var fail bool
	o.Update(func() {
		o.Attempts++
		if o.failN > 0 {
			o.failN--
			fail = true
			return
		}
		for id, k := range o.Known {
			if Norm(k.Endpoint) == Norm(mem.Endpoint) {
				delete(o.Known, id)
			}
		}
		mem.ID = "member-" + strconv.Itoa(len(o.Registrations)+1)
		o.Registrations = append(o.Registrations, mem)
		o.Known[mem.ID] = mem
	})
	if fail {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("the orchestrator is not ready"))
	}
	return connect.NewResponse(&membershipv1.RegisterResponse{MemberId: mem.ID, HeartbeatInterval: durationpb.New(o.cfg.Interval)}), nil
}

func (m membership) Heartbeat(_ context.Context, r *connect.Request[membershipv1.HeartbeatRequest]) (*connect.Response[membershipv1.HeartbeatResponse], error) {
	o := m.o
	if o.scopeOf(r.Header()) == "" {
		return nil, o.refuse(connect.CodeUnauthenticated, "unknown token")
	}
	var again bool
	o.Update(func() {
		id := r.Msg.GetMemberId()
		o.Beats = append(o.Beats, Beat{ID: id, State: r.Msg.GetState(), InFlight: r.Msg.GetInFlight(), At: time.Now()})
		if o.forget[id] {
			delete(o.forget, id)
			delete(o.Known, id)
		}
		_, known := o.Known[id]
		again = !known
	})
	return connect.NewResponse(&membershipv1.HeartbeatResponse{Reregister: again}), nil
}

func (m membership) Deregister(_ context.Context, r *connect.Request[membershipv1.DeregisterRequest]) (*connect.Response[membershipv1.DeregisterResponse], error) {
	o := m.o
	if o.scopeOf(r.Header()) == "" {
		return nil, o.refuse(connect.CodeUnauthenticated, "unknown token")
	}
	o.Update(func() {
		o.Deregistered = append(o.Deregistered, r.Msg.GetMemberId())
		delete(o.Known, r.Msg.GetMemberId())
	})
	return connect.NewResponse(&membershipv1.DeregisterResponse{}), nil
}

func (m membership) ListMembers(context.Context, *connect.Request[membershipv1.ListMembersRequest]) (*connect.Response[membershipv1.ListMembersResponse], error) {
	return nil, connect.NewError(connect.CodePermissionDenied, errors.New("only an operator lists members"))
}

type artifacts struct{ o *Orchestrator }

func (a artifacts) GetArtifact(_ context.Context, r *connect.Request[workerv1.GetArtifactRequest], st *connect.ServerStream[workerv1.GetArtifactResponse]) error {
	if a.o.scopeOf(r.Header()) != "worker" {
		return a.o.refuse(connect.CodeUnauthenticated, "bad worker token")
	}
	a.o.Lock()
	b, ok := a.o.Blobs[r.Msg.GetDigest()]
	a.o.Unlock()
	if !ok {
		return check.Error(commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND, "unknown digest")
	}
	const chunk = 32 << 10
	for len(b) > 0 {
		n := min(chunk, len(b))
		if err := st.Send(&workerv1.GetArtifactResponse{Chunk: b[:n]}); err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}
