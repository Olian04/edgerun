package edgerun

import (
	"context"
	"iter"

	"google.golang.org/protobuf/proto"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/gen/edgerun/control/v1/controlv1connect"
)

// Control is the client of an orchestrator's control.v1 protocol: everything an
// operator can do. It needs a token of ScopeAdmin.
//
// Every method is one RPC of control.v1 (FunctionService, RouteService,
// PlanService and InspectionService), taking and returning its messages. The All* methods
// iterate over a paged list.
type Control struct {
	fn   controlv1connect.FunctionServiceClient
	rt   controlv1connect.RouteServiceClient
	plan controlv1connect.PlanServiceClient
	insp controlv1connect.InspectionServiceClient
}

// NewControl returns a client for the orchestrator at baseURL.
func NewControl(baseURL, token string, opts ...Option) *Control {
	h, co := settings(token, opts)
	return &Control{
		fn:   controlv1connect.NewFunctionServiceClient(h, baseURL, co...),
		rt:   controlv1connect.NewRouteServiceClient(h, baseURL, co...),
		plan: controlv1connect.NewPlanServiceClient(h, baseURL, co...),
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
	return c.AllFunctionsWhere(ctx, &controlv1.ListFunctionsRequest{MatchLabels: match})
}

// AllFunctionsWhere iterates over the functions the filters of req select
// (match_labels, has_label_keys, lacks_label_keys, name). Its page_size is used;
// its page_token is not.
func (c *Control) AllFunctionsWhere(ctx context.Context, req *controlv1.ListFunctionsRequest) iter.Seq2[*controlv1.Function, error] {
	return pages(ctx, func(ctx context.Context, token string) ([]*controlv1.Function, string, error) {
		q := proto.Clone(req).(*controlv1.ListFunctionsRequest)
		q.PageToken = token
		r, err := c.ListFunctions(ctx, q)
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
	return c.AllVersionsWhere(ctx, &controlv1.ListVersionsRequest{FunctionId: functionID, MatchLabels: match})
}

// AllVersionsWhere iterates over the versions the filters of req select. Its
// page_size is used; its page_token is not.
func (c *Control) AllVersionsWhere(ctx context.Context, req *controlv1.ListVersionsRequest) iter.Seq2[*controlv1.FunctionVersion, error] {
	return pages(ctx, func(ctx context.Context, token string) ([]*controlv1.FunctionVersion, string, error) {
		q := proto.Clone(req).(*controlv1.ListVersionsRequest)
		q.PageToken = token
		r, err := c.ListVersions(ctx, q)
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

// PatchLabels changes some of a function's labels and leaves the others.
func (c *Control) PatchLabels(ctx context.Context, req *controlv1.PatchLabelsRequest) (*controlv1.PatchLabelsResponse, error) {
	return call(ctx, c.fn.PatchLabels, req)
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
	return c.AllRoutesWhere(ctx, &controlv1.ListRoutesRequest{MatchLabels: match})
}

// AllRoutesWhere iterates over the routes the filters of req select
// (match_labels, has_label_keys, lacks_label_keys, function_id). Its page_size
// is used; its page_token is not.
func (c *Control) AllRoutesWhere(ctx context.Context, req *controlv1.ListRoutesRequest) iter.Seq2[*controlv1.Route, error] {
	return pages(ctx, func(ctx context.Context, token string) ([]*controlv1.Route, string, error) {
		q := proto.Clone(req).(*controlv1.ListRoutesRequest)
		q.PageToken = token
		r, err := c.ListRoutes(ctx, q)
		if err != nil {
			return nil, "", err
		}
		return r.Routes, r.NextPageToken, nil
	})
}

func (c *Control) SetRouteLabels(ctx context.Context, req *controlv1.SetRouteLabelsRequest) (*controlv1.SetRouteLabelsResponse, error) {
	return call(ctx, c.rt.SetRouteLabels, req)
}

// PatchRouteLabels changes some of a route's labels and leaves the others.
func (c *Control) PatchRouteLabels(ctx context.Context, req *controlv1.PatchRouteLabelsRequest) (*controlv1.PatchRouteLabelsResponse, error) {
	return call(ctx, c.rt.PatchRouteLabels, req)
}

// Plans.

// Validate runs a sequence of changes as if each had been applied before the
// next, and keeps nothing: validate_only for a whole plan. The response says
// which step failed, if one did; a plan that is not valid is a response, not an
// error.
func (c *Control) Validate(ctx context.Context, req *controlv1.ValidateRequest) (*controlv1.ValidateResponse, error) {
	return call(ctx, c.plan.Validate, req)
}

// Apply makes the changes of a plan's steps, all or none: they run in one
// transaction, each seeing the ones before it exactly as in Validate, and
// nothing is changed if one fails (the response says which). A plan that is
// refused is a response, not an error.
func (c *Control) Apply(ctx context.Context, req *controlv1.ApplyRequest) (*controlv1.ApplyResponse, error) {
	return call(ctx, c.plan.Apply, req)
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

func (c *Control) GetInvocation(ctx context.Context, req *controlv1.GetInvocationRequest) (*controlv1.GetInvocationResponse, error) {
	return call(ctx, c.insp.GetInvocation, req)
}

func (c *Control) ListInvocations(ctx context.Context, req *controlv1.ListInvocationsRequest) (*controlv1.ListInvocationsResponse, error) {
	return call(ctx, c.insp.ListInvocations, req)
}

// AllInvocations iterates over invocation records, newest first, of one
// function or (functionID empty) of all.
func (c *Control) AllInvocations(ctx context.Context, functionID string) iter.Seq2[*controlv1.Invocation, error] {
	return c.AllInvocationsWhere(ctx, &controlv1.ListInvocationsRequest{FunctionId: functionID})
}

// AllInvocationsWhere iterates over the invocation records the filters of req
// select (function_id, request_id). Its page_size is used; its page_token is not.
func (c *Control) AllInvocationsWhere(ctx context.Context, req *controlv1.ListInvocationsRequest) iter.Seq2[*controlv1.Invocation, error] {
	return pages(ctx, func(ctx context.Context, token string) ([]*controlv1.Invocation, string, error) {
		q := proto.Clone(req).(*controlv1.ListInvocationsRequest)
		q.PageToken = token
		r, err := c.ListInvocations(ctx, q)
		if err != nil {
			return nil, "", err
		}
		return r.Invocations, r.NextPageToken, nil
	})
}
