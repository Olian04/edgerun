package edgerun

import (
	"context"
	"iter"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/gen/edgerun/control/v1/controlv1connect"
)

// Control is the client of an orchestrator's control.v1 protocol: everything an
// operator can do. It needs a token of ScopeAdmin.
//
// Every method is one RPC of control.v1 (FunctionService, RouteService and
// InspectionService), taking and returning its messages. The All* methods
// iterate over a paged list.
type Control struct {
	fn   controlv1connect.FunctionServiceClient
	rt   controlv1connect.RouteServiceClient
	insp controlv1connect.InspectionServiceClient
}

// NewControl returns a client for the orchestrator at baseURL.
func NewControl(baseURL, token string, opts ...Option) *Control {
	h, co := settings(token, opts)
	return &Control{
		fn:   controlv1connect.NewFunctionServiceClient(h, baseURL, co...),
		rt:   controlv1connect.NewRouteServiceClient(h, baseURL, co...),
		insp: controlv1connect.NewInspectionServiceClient(h, baseURL, co...),
	}
}

// Functions.

func (c *Control) CreateFunction(ctx context.Context, req *controlv1.CreateFunctionRequest) (*controlv1.CreateFunctionResponse, error) {
	return call(ctx, c.fn.CreateFunction, req)
}

func (c *Control) GetFunction(ctx context.Context, req *controlv1.GetFunctionRequest) (*controlv1.GetFunctionResponse, error) {
	return call(ctx, c.fn.GetFunction, req)
}

func (c *Control) ListFunctions(ctx context.Context, req *controlv1.ListFunctionsRequest) (*controlv1.ListFunctionsResponse, error) {
	return call(ctx, c.fn.ListFunctions, req)
}

func (c *Control) DeleteFunction(ctx context.Context, req *controlv1.DeleteFunctionRequest) (*controlv1.DeleteFunctionResponse, error) {
	return call(ctx, c.fn.DeleteFunction, req)
}

// AllFunctions iterates over every function.
func (c *Control) AllFunctions(ctx context.Context) iter.Seq2[*controlv1.Function, error] {
	return c.AllFunctionsMatching(ctx, nil)
}

// AllFunctionsMatching iterates over the functions that carry every one of the
// labels, with exactly those values.
func (c *Control) AllFunctionsMatching(ctx context.Context, match map[string]string) iter.Seq2[*controlv1.Function, error] {
	return pages(ctx, func(ctx context.Context, token string) ([]*controlv1.Function, string, error) {
		r, err := c.ListFunctions(ctx, &controlv1.ListFunctionsRequest{PageToken: token, MatchLabels: match})
		if err != nil {
			return nil, "", err
		}
		return r.Functions, r.NextPageToken, nil
	})
}

// Versions.

func (c *Control) CreateVersion(ctx context.Context, req *controlv1.CreateVersionRequest) (*controlv1.CreateVersionResponse, error) {
	return call(ctx, c.fn.CreateVersion, req)
}

func (c *Control) ListVersions(ctx context.Context, req *controlv1.ListVersionsRequest) (*controlv1.ListVersionsResponse, error) {
	return call(ctx, c.fn.ListVersions, req)
}

func (c *Control) ActivateVersion(ctx context.Context, req *controlv1.ActivateVersionRequest) (*controlv1.ActivateVersionResponse, error) {
	return call(ctx, c.fn.ActivateVersion, req)
}

// AllVersions iterates over every version of a function, newest first.
func (c *Control) AllVersions(ctx context.Context, functionID string) iter.Seq2[*controlv1.FunctionVersion, error] {
	return c.AllVersionsMatching(ctx, functionID, nil)
}

// AllVersionsMatching is AllVersions restricted to the versions that carry every
// one of the labels, with exactly those values.
func (c *Control) AllVersionsMatching(ctx context.Context, functionID string, match map[string]string) iter.Seq2[*controlv1.FunctionVersion, error] {
	return pages(ctx, func(ctx context.Context, token string) ([]*controlv1.FunctionVersion, string, error) {
		r, err := c.ListVersions(ctx, &controlv1.ListVersionsRequest{FunctionId: functionID, PageToken: token, MatchLabels: match})
		if err != nil {
			return nil, "", err
		}
		return r.Versions, r.NextPageToken, nil
	})
}

// Environment and network policy.

func (c *Control) SetEnv(ctx context.Context, req *controlv1.SetEnvRequest) (*controlv1.SetEnvResponse, error) {
	return call(ctx, c.fn.SetEnv, req)
}

func (c *Control) DeleteEnv(ctx context.Context, req *controlv1.DeleteEnvRequest) (*controlv1.DeleteEnvResponse, error) {
	return call(ctx, c.fn.DeleteEnv, req)
}

func (c *Control) ListEnv(ctx context.Context, req *controlv1.ListEnvRequest) (*controlv1.ListEnvResponse, error) {
	return call(ctx, c.fn.ListEnv, req)
}

func (c *Control) SetLabels(ctx context.Context, req *controlv1.SetLabelsRequest) (*controlv1.SetLabelsResponse, error) {
	return call(ctx, c.fn.SetLabels, req)
}

func (c *Control) SetEgress(ctx context.Context, req *controlv1.SetEgressRequest) (*controlv1.SetEgressResponse, error) {
	return call(ctx, c.fn.SetEgress, req)
}

// Routes.

func (c *Control) CreateRoute(ctx context.Context, req *controlv1.CreateRouteRequest) (*controlv1.CreateRouteResponse, error) {
	return call(ctx, c.rt.CreateRoute, req)
}

func (c *Control) ListRoutes(ctx context.Context, req *controlv1.ListRoutesRequest) (*controlv1.ListRoutesResponse, error) {
	return call(ctx, c.rt.ListRoutes, req)
}

func (c *Control) DeleteRoute(ctx context.Context, req *controlv1.DeleteRouteRequest) (*controlv1.DeleteRouteResponse, error) {
	return call(ctx, c.rt.DeleteRoute, req)
}

// AllRoutes iterates over every route.
func (c *Control) AllRoutes(ctx context.Context) iter.Seq2[*controlv1.Route, error] {
	return c.AllRoutesMatching(ctx, nil)
}

// AllRoutesMatching is AllRoutes restricted to the routes that carry every one
// of the labels, with exactly those values.
func (c *Control) AllRoutesMatching(ctx context.Context, match map[string]string) iter.Seq2[*controlv1.Route, error] {
	return pages(ctx, func(ctx context.Context, token string) ([]*controlv1.Route, string, error) {
		r, err := c.ListRoutes(ctx, &controlv1.ListRoutesRequest{PageToken: token, MatchLabels: match})
		if err != nil {
			return nil, "", err
		}
		return r.Routes, r.NextPageToken, nil
	})
}

// Inspection.

func (c *Control) Describe(ctx context.Context, req *controlv1.DescribeRequest) (*controlv1.DescribeResponse, error) {
	return call(ctx, c.insp.Describe, req)
}

func (c *Control) ListWorkers(ctx context.Context, req *controlv1.ListWorkersRequest) (*controlv1.ListWorkersResponse, error) {
	return call(ctx, c.insp.ListWorkers, req)
}

func (c *Control) ListNetworkServices(ctx context.Context, req *controlv1.ListNetworkServicesRequest) (*controlv1.ListNetworkServicesResponse, error) {
	return call(ctx, c.insp.ListNetworkServices, req)
}

func (c *Control) ListInvocations(ctx context.Context, req *controlv1.ListInvocationsRequest) (*controlv1.ListInvocationsResponse, error) {
	return call(ctx, c.insp.ListInvocations, req)
}

// AllInvocations iterates over invocation records, newest first, of one
// function or (functionID empty) of all.
func (c *Control) AllInvocations(ctx context.Context, functionID string) iter.Seq2[*controlv1.Invocation, error] {
	return pages(ctx, func(ctx context.Context, token string) ([]*controlv1.Invocation, string, error) {
		r, err := c.ListInvocations(ctx, &controlv1.ListInvocationsRequest{FunctionId: functionID, PageToken: token})
		if err != nil {
			return nil, "", err
		}
		return r.Invocations, r.NextPageToken, nil
	})
}
