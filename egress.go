package edgerun

import (
	"context"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/egress/v1/egressv1connect"
)

// Egress is the client of a network service's EgressService: what a worker does
// to let a guest reach the network. It needs a token of ScopeEgress.
type Egress struct {
	c egressv1connect.EgressServiceClient
}

// NewEgress returns a client for the network service at baseURL.
func NewEgress(baseURL, token string, opts ...Option) *Egress {
	h, co := settings(token, opts)
	return &Egress{c: egressv1connect.NewEgressServiceClient(h, baseURL, co...)}
}

func (e *Egress) Describe(ctx context.Context, req *egressv1.DescribeRequest) (*egressv1.DescribeResponse, error) {
	return call(ctx, e.c.Describe, req)
}

// Fetch performs one HTTP request through the service and returns the response
// as soon as its head arrives. The deadline of ctx is the budget for the whole
// exchange. An upstream that answers 4xx or 5xx is a Response, not an error.
func (e *Egress) Fetch(ctx context.Context, req *egressv1.FetchRequest) (*Response, error) {
	st, err := e.c.Fetch(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	res, _, err := openStream(st,
		func(m *egressv1.FetchResponse) *commonv1.HttpResponseHead { return m.GetHead() },
		func(m *egressv1.FetchResponse) []byte { return m.GetChunk() })
	return res, err
}

// Registry is the client of an orchestrator's egress.v1 RegistryService: how a
// network service tells the orchestrator what it carries, and that it is still
// there. It needs a token of ScopeNetwork. With it, a service of your own (in a
// private network, behind a proxy, in another cloud) can join the same
// orchestrators as every other.
type Registry struct {
	c egressv1connect.RegistryServiceClient
}

// NewRegistry returns a client for the orchestrator at baseURL.
func NewRegistry(baseURL, token string, opts ...Option) *Registry {
	h, co := settings(token, opts)
	return &Registry{c: egressv1connect.NewRegistryServiceClient(h, baseURL, co...)}
}

func (r *Registry) Register(ctx context.Context, req *egressv1.RegisterRequest) (*egressv1.RegisterResponse, error) {
	return call(ctx, r.c.Register, req)
}

func (r *Registry) Heartbeat(ctx context.Context, req *egressv1.HeartbeatRequest) (*egressv1.HeartbeatResponse, error) {
	return call(ctx, r.c.Heartbeat, req)
}

func (r *Registry) Deregister(ctx context.Context, req *egressv1.DeregisterRequest) (*egressv1.DeregisterResponse, error) {
	return call(ctx, r.c.Deregister, req)
}
