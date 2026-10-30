package edgerun

import (
	"context"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
)

// Ingress is the client of an orchestrator's ingress.v1 protocol: what a
// frontend does. It needs a token of ScopeIngress. With it, any program can be
// a frontend (a queue consumer, a different protocol) without knowing about
// functions, versions or workers.
type Ingress struct {
	c ingressv1connect.IngressServiceClient
}

// NewIngress returns a client for the orchestrator at baseURL.
func NewIngress(baseURL, token string, opts ...Option) *Ingress {
	h, co := settings(token, opts)
	return &Ingress{c: ingressv1connect.NewIngressServiceClient(h, baseURL, co...)}
}

func (i *Ingress) Describe(ctx context.Context, req *ingressv1.DescribeRequest) (*ingressv1.DescribeResponse, error) {
	return call(ctx, i.c.Describe, req)
}

// Invocation is the response to an Invoke.
type Invocation struct {
	Response
	// ID is the persisted invocation record, for support and tracing.
	ID string
}

// Invoke routes the request to a function and returns its response as soon as
// the head arrives. The deadline of ctx is the invocation's wall-clock budget.
//
// A failure before the head is the returned error. A function that answers 500
// is not an error at all: it is a Response with that status.
func (i *Ingress) Invoke(ctx context.Context, req *ingressv1.InvokeRequest) (*Invocation, error) {
	st, err := i.c.Invoke(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	res, first, err := openStream(st,
		func(m *ingressv1.InvokeResponse) *commonv1.HttpResponseHead { return m.GetHead().GetHttp() },
		func(m *ingressv1.InvokeResponse) []byte { return m.GetChunk() })
	if err != nil {
		return nil, err
	}
	return &Invocation{Response: *res, ID: first.GetHead().GetInvocationId()}, nil
}
