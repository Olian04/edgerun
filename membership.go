package edgerun

import (
	"context"

	membershipv1 "github.com/Olian04/edgerun/gen/edgerun/membership/v1"
	"github.com/Olian04/edgerun/gen/edgerun/membership/v1/membershipv1connect"
)

// Membership is the client of an orchestrator's MembershipService: how a worker
// or a network service joins a deployment, stays in it and leaves, and how an
// operator sees who is in it. A member presents a token of its own scope
// (ScopeWorker for a worker, ScopeNetwork for a network service); ListMembers
// needs ScopeAdmin. With it, a worker or a network service in any language can
// offer itself to an orchestrator.
type Membership struct {
	c membershipv1connect.MembershipServiceClient
}

// NewMembership returns a client for the orchestrator at baseURL.
func NewMembership(baseURL, token string, opts ...Option) *Membership {
	h, co := settings(token, opts)
	return &Membership{c: membershipv1connect.NewMembershipServiceClient(h, baseURL, co...)}
}

func (m *Membership) Register(ctx context.Context, req *membershipv1.RegisterRequest) (*membershipv1.RegisterResponse, error) {
	return call(ctx, m.c.Register, req)
}

func (m *Membership) Heartbeat(ctx context.Context, req *membershipv1.HeartbeatRequest) (*membershipv1.HeartbeatResponse, error) {
	return call(ctx, m.c.Heartbeat, req)
}

func (m *Membership) Deregister(ctx context.Context, req *membershipv1.DeregisterRequest) (*membershipv1.DeregisterResponse, error) {
	return call(ctx, m.c.Deregister, req)
}

func (m *Membership) ListMembers(ctx context.Context, req *membershipv1.ListMembersRequest) (*membershipv1.ListMembersResponse, error) {
	return call(ctx, m.c.ListMembers, req)
}
