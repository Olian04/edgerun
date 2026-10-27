// Package admin is the operator UI, built with webui. It is a client of the
// orchestrator's control.v1 protocol and nothing else: it never touches
// PostgreSQL, so it can be replaced without the rest of the system noticing.
package admin

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/sync/errgroup"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/gen/edgerun/control/v1/controlv1connect"
	"github.com/Olian04/edgerun/internal/authn"
)

// maxListed bounds how many rows the UI pulls for a table it lets webui sort
// and filter in memory.
const maxListed = 10000

// Control is the UI's view of the control protocol.
type Control struct {
	fn   controlv1connect.FunctionServiceClient
	rt   controlv1connect.RouteServiceClient
	insp controlv1connect.InspectionServiceClient

	mu       sync.Mutex
	names    map[string]string // function id -> name
	namesAt  time.Time
	versions map[string]int64 // version id -> number (a version's number never changes)
}

// NewControl returns a client for the orchestrator at baseURL.
func NewControl(httpc *http.Client, baseURL, adminToken string) *Control {
	opt := connect.WithInterceptors(authn.Client(adminToken))
	return &Control{
		fn:       controlv1connect.NewFunctionServiceClient(httpc, baseURL, opt),
		rt:       controlv1connect.NewRouteServiceClient(httpc, baseURL, opt),
		insp:     controlv1connect.NewInspectionServiceClient(httpc, baseURL, opt),
		versions: map[string]int64{},
	}
}

// Info describes the orchestrator.
func (c *Control) Info(ctx context.Context) (*controlv1.DescribeResponse, error) {
	r, err := c.insp.Describe(ctx, connect.NewRequest(&controlv1.DescribeRequest{}))
	if err != nil {
		return nil, err
	}
	return r.Msg, nil
}

// Functions lists every function, by name.
func (c *Control) Functions(ctx context.Context) ([]*controlv1.Function, error) {
	var all []*controlv1.Function
	token := ""
	for len(all) < maxListed {
		r, err := c.fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{PageSize: 500, PageToken: token}))
		if err != nil {
			return nil, err
		}
		all = append(all, r.Msg.Functions...)
		if token = r.Msg.NextPageToken; token == "" {
			break
		}
	}
	c.rememberNames(all)
	return all, nil
}

// Function returns one function.
func (c *Control) Function(ctx context.Context, id string) (*controlv1.Function, error) {
	r, err := c.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: id}))
	if err != nil {
		return nil, err
	}
	return r.Msg.Function, nil
}

// Versions lists a function's versions, newest first.
func (c *Control) Versions(ctx context.Context, fid string) ([]*controlv1.FunctionVersion, error) {
	var all []*controlv1.FunctionVersion
	token := ""
	for len(all) < maxListed {
		r, err := c.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: fid, PageSize: 500, PageToken: token}))
		if err != nil {
			return nil, err
		}
		all = append(all, r.Msg.Versions...)
		if token = r.Msg.NextPageToken; token == "" {
			break
		}
	}
	c.mu.Lock()
	for _, v := range all {
		c.versions[v.Id] = v.Number
	}
	c.mu.Unlock()
	return all, nil
}

// ActiveVersionNumbers maps function id to the number of its active version.
// A version's number never changes, so each is looked up once.
func (c *Control) ActiveVersionNumbers(ctx context.Context, fns []*controlv1.Function) map[string]int64 {
	out := map[string]int64{}
	var missing []*controlv1.Function
	c.mu.Lock()
	for _, f := range fns {
		if f.ActiveVersionId == "" {
			continue
		}
		if n, ok := c.versions[f.ActiveVersionId]; ok {
			out[f.Id] = n
		} else {
			missing = append(missing, f)
		}
	}
	c.mu.Unlock()

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, f := range missing {
		g.Go(func() error {
			vs, err := c.Versions(gctx, f.Id)
			if err != nil {
				return nil // a missing number is cosmetic: show a dash, not an error
			}
			for _, v := range vs {
				if v.Id == f.ActiveVersionId {
					mu.Lock()
					out[f.Id] = v.Number
					mu.Unlock()
				}
			}
			return nil
		})
	}
	g.Wait()
	return out
}

// Routes lists every route.
func (c *Control) Routes(ctx context.Context) ([]*controlv1.Route, error) {
	var all []*controlv1.Route
	token := ""
	for len(all) < maxListed {
		r, err := c.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{PageSize: 500, PageToken: token}))
		if err != nil {
			return nil, err
		}
		all = append(all, r.Msg.Routes...)
		if token = r.Msg.NextPageToken; token == "" {
			break
		}
	}
	return all, nil
}

// Env lists a function's environment variables.
func (c *Control) Env(ctx context.Context, fid string) ([]*controlv1.EnvEntry, error) {
	r, err := c.fn.ListEnv(ctx, connect.NewRequest(&controlv1.ListEnvRequest{FunctionId: fid}))
	if err != nil {
		return nil, err
	}
	return r.Msg.Entries, nil
}

// Workers lists the fleet.
func (c *Control) Workers(ctx context.Context) ([]*controlv1.WorkerStatus, error) {
	r, err := c.insp.ListWorkers(ctx, connect.NewRequest(&controlv1.ListWorkersRequest{}))
	if err != nil {
		return nil, err
	}
	ws := r.Msg.Workers
	sort.SliceStable(ws, func(i, j int) bool { return ws[i].Endpoint < ws[j].Endpoint })
	return ws, nil
}

// Invocations returns one page of invocation records, newest first. after is
// the token from the previous page, "" for the first.
func (c *Control) Invocations(ctx context.Context, functionID, after string, limit int) ([]*controlv1.Invocation, string, error) {
	r, err := c.insp.ListInvocations(ctx, connect.NewRequest(&controlv1.ListInvocationsRequest{
		FunctionId: functionID, PageSize: uint32(limit), PageToken: after}))
	if err != nil {
		return nil, "", err
	}
	return r.Msg.Invocations, r.Msg.NextPageToken, nil
}

// FunctionNames maps function ids to names, for tables that show records by
// function. It is cached briefly: names change rarely and a table draws often.
func (c *Control) FunctionNames(ctx context.Context) map[string]string {
	c.mu.Lock()
	if c.names != nil && time.Since(c.namesAt) < 3*time.Second {
		n := c.names
		c.mu.Unlock()
		return n
	}
	c.mu.Unlock()
	if _, err := c.Functions(ctx); err != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.names // possibly stale or nil: a missing name is cosmetic
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.names
}

func (c *Control) rememberNames(fns []*controlv1.Function) {
	n := make(map[string]string, len(fns))
	for _, f := range fns {
		n[f.Id] = f.Name
	}
	c.mu.Lock()
	c.names, c.namesAt = n, time.Now()
	c.mu.Unlock()
}

// --- writes ------------------------------------------------------------------

func (c *Control) CreateFunction(ctx context.Context, name string) (*controlv1.Function, error) {
	r, err := c.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: name}))
	if err != nil {
		return nil, err
	}
	c.invalidateNames()
	return r.Msg.Function, nil
}

func (c *Control) DeleteFunction(ctx context.Context, id string) error {
	_, err := c.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: id}))
	c.invalidateNames()
	return err
}

// Deploy uploads a module as a new version of a function.
func (c *Control) Deploy(ctx context.Context, fid string, module []byte, abi string, limits *commonv1.Limits, activate bool) (*controlv1.FunctionVersion, error) {
	r, err := c.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{
		FunctionId: fid, Module: module, Abi: abi, Limits: limits, Activate: activate}))
	if err != nil {
		return nil, err
	}
	return r.Msg.Version, nil
}

func (c *Control) Activate(ctx context.Context, fid, vid string) error {
	_, err := c.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: vid}))
	return err
}

func (c *Control) SetEnv(ctx context.Context, fid string, e *controlv1.EnvEntry) error {
	_, err := c.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: fid, Entry: e}))
	return err
}

func (c *Control) DeleteEnv(ctx context.Context, fid, name string) error {
	_, err := c.fn.DeleteEnv(ctx, connect.NewRequest(&controlv1.DeleteEnvRequest{FunctionId: fid, Name: name}))
	return err
}

func (c *Control) CreateRoute(ctx context.Context, host, prefix, fid string) (*controlv1.Route, error) {
	r, err := c.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: prefix, FunctionId: fid}))
	if err != nil {
		return nil, err
	}
	return r.Msg.Route, nil
}

func (c *Control) DeleteRoute(ctx context.Context, id string) error {
	_, err := c.rt.DeleteRoute(ctx, connect.NewRequest(&controlv1.DeleteRouteRequest{RouteId: id}))
	return err
}

func (c *Control) invalidateNames() {
	c.mu.Lock()
	c.names = nil
	c.mu.Unlock()
}
