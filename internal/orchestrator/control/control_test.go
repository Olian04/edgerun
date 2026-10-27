package control_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/gen/edgerun/control/v1/controlv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/orchestrator/control"
	"github.com/Olian04/edgerun/internal/orchestrator/migrate"
	"github.com/Olian04/edgerun/internal/orchestrator/orchtest"
	"github.com/Olian04/edgerun/internal/orchestrator/secrets"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
	"github.com/Olian04/edgerun/internal/testpg"
	"github.com/Olian04/edgerun/migrations"
)

func TestMain(m *testing.M) { testpg.Main(m) }

const (
	adminToken  = "admin-secret"
	workerToken = "worker-secret"
)

type rig struct {
	fn    controlv1connect.FunctionServiceClient
	rt    controlv1connect.RouteServiceClient
	insp  controlv1connect.InspectionServiceClient
	pokes *atomic.Int32
	srv   *httptest.Server
}

func newRig(t *testing.T, tweak func(*control.Config)) *rig {
	t.Helper()
	pokes := &atomic.Int32{}
	cfg := control.Config{
		Store: orchtest.Store(t), Poke: func() { pokes.Add(1) }, MaxModuleBytes: 1 << 10,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if tweak != nil {
		tweak(&cfg)
	}
	mux := http.NewServeMux()
	control.New(cfg).Mount(mux, authn.Verifier{adminToken: control.AdminScope, workerToken: "worker"})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &rig{pokes: pokes, srv: srv,
		fn:   controlv1connect.NewFunctionServiceClient(srv.Client(), srv.URL, connect.WithInterceptors(authn.Client(adminToken))),
		rt:   controlv1connect.NewRouteServiceClient(srv.Client(), srv.URL, connect.WithInterceptors(authn.Client(adminToken))),
		insp: controlv1connect.NewInspectionServiceClient(srv.Client(), srv.URL, connect.WithInterceptors(authn.Client(adminToken))),
	}
}

var ctx = context.Background()

func code(err error) connect.Code { return connect.CodeOf(err) }

func TestAuthScopes(t *testing.T) {
	r := newRig(t, nil)
	for name, tok := range map[string]string{"none": "", "unknown": "nope", "worker scope": workerToken} {
		var opts []connect.ClientOption
		if tok != "" {
			opts = append(opts, connect.WithInterceptors(authn.Client(tok)))
		}
		fn := controlv1connect.NewFunctionServiceClient(r.srv.Client(), r.srv.URL, opts...)
		rt := controlv1connect.NewRouteServiceClient(r.srv.Client(), r.srv.URL, opts...)
		insp := controlv1connect.NewInspectionServiceClient(r.srv.Client(), r.srv.URL, opts...)
		want := connect.CodeUnauthenticated
		if name == "worker scope" {
			want = connect.CodePermissionDenied
		}
		_, e1 := fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{}))
		_, e2 := rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{}))
		_, e3 := insp.ListWorkers(ctx, connect.NewRequest(&controlv1.ListWorkersRequest{}))
		for _, e := range []error{e1, e2, e3} {
			if code(e) != want {
				t.Errorf("%s: %v, want %v", name, e, want)
			}
		}
	}
}

func TestManagementLifecycle(t *testing.T) {
	r := newRig(t, nil)
	created, err := r.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: "hello"}))
	if err != nil {
		t.Fatal(err)
	}
	fid := created.Msg.Function.Id

	ver, err := r.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{
		FunctionId: fid, Module: []byte("module-bytes"), Abi: "wagi-1", Limits: &commonv1.Limits{MemoryLimitBytes: 1 << 26}}))
	if err != nil || ver.Msg.Version.Number != 1 {
		t.Fatalf("CreateVersion: %v", err)
	}
	if r.pokes.Load() != 0 {
		t.Error("creating an undeployed version changes nothing the invoke path caches")
	}
	act, err := r.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: ver.Msg.Version.Id}))
	if err != nil || act.Msg.Function.ActiveVersionId != ver.Msg.Version.Id {
		t.Fatalf("Activate: %v", err)
	}

	if _, err := r.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: "*", PathPrefix: "/hello", FunctionId: fid})); err != nil {
		t.Fatal(err)
	}
	rs, _ := r.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{}))
	if len(rs.Msg.Routes) != 1 {
		t.Fatalf("routes = %v", rs.Msg.Routes)
	}

	for _, e := range []*controlv1.EnvEntry{{Name: "PLAIN", Value: "p"}, {Name: "TOKEN", Value: "s3cret", Secret: true}} {
		if _, err := r.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: fid, Entry: e})); err != nil {
			t.Fatal(err)
		}
	}
	env, _ := r.fn.ListEnv(ctx, connect.NewRequest(&controlv1.ListEnvRequest{FunctionId: fid}))
	if len(env.Msg.Entries) != 2 || env.Msg.Entries[1].Value != "" || env.Msg.Entries[0].Value != "p" {
		t.Errorf("env = %v (secrets are write-only)", env.Msg.Entries)
	}
	if _, err := r.fn.DeleteEnv(ctx, connect.NewRequest(&controlv1.DeleteEnvRequest{FunctionId: fid, Name: "PLAIN"})); err != nil {
		t.Fatal(err)
	}

	vs, _ := r.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: fid}))
	fs, _ := r.fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{}))
	got, _ := r.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fid}))
	if len(vs.Msg.Versions) != 1 || len(fs.Msg.Functions) != 1 || got.Msg.Function.Name != "hello" {
		t.Error("reads disagree with writes")
	}

	if _, err := r.rt.DeleteRoute(ctx, connect.NewRequest(&controlv1.DeleteRouteRequest{RouteId: rs.Msg.Routes[0].Id})); err != nil {
		t.Fatal(err)
	}
	if _, err := r.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: fid})); err != nil {
		t.Fatal(err)
	}
	// activate, route create, 2x env set, env delete, route delete, function delete
	if n := r.pokes.Load(); n != 7 {
		t.Errorf("pokes = %d, want one per write the invoke path caches (7)", n)
	}
}

func TestReadsNeverPoke(t *testing.T) {
	r := newRig(t, nil)
	r.fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{}))
	r.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{}))
	r.insp.ListWorkers(ctx, connect.NewRequest(&controlv1.ListWorkersRequest{}))
	r.insp.ListInvocations(ctx, connect.NewRequest(&controlv1.ListInvocationsRequest{}))
	r.insp.Describe(ctx, connect.NewRequest(&controlv1.DescribeRequest{}))
	if r.pokes.Load() != 0 {
		t.Error("reads must not trigger refreshes")
	}
}

func TestErrorCodes(t *testing.T) {
	r := newRig(t, nil)
	c, _ := r.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: "fn"}))
	fid := c.Msg.Function.Id
	zero := "00000000-0000-0000-0000-000000000000"

	_, err := r.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: "fn"}))
	check(t, "duplicate function", err, connect.CodeAlreadyExists)
	_, err = r.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: "Bad Name"}))
	check(t, "invalid name", err, connect.CodeInvalidArgument)
	_, err = r.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: zero}))
	check(t, "unknown function", err, connect.CodeNotFound)
	_, err = r.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: "garbage"}))
	check(t, "garbage id", err, connect.CodeNotFound)

	_, err = r.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: fid, Module: bytes.Repeat([]byte{1}, 1025), Abi: "wagi-1"}))
	check(t, "module over the limit", err, connect.CodeInvalidArgument)
	if err != nil && !strings.Contains(err.Error(), "1025") {
		t.Errorf("the message should say how big it was: %v", err)
	}
	_, err = r.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: fid, Module: []byte("x"), Abi: "Not An ABI!"}))
	check(t, "bad abi", err, connect.CodeInvalidArgument)
	_, err = r.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: zero, Module: []byte("x"), Abi: "wagi-1"}))
	check(t, "version for unknown function", err, connect.CodeNotFound)
	_, err = r.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: zero}))
	check(t, "activate unknown version", err, connect.CodeNotFound)

	_, err = r.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: fid}))
	check(t, "missing entry", err, connect.CodeInvalidArgument)
	_, err = r.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "REQUEST_METHOD", Value: "x"}}))
	check(t, "reserved env name", err, connect.CodeInvalidArgument)

	_, err = r.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: "h.com", PathPrefix: "/", FunctionId: fid}))
	check(t, "first route", err, 0)
	_, err = r.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: "H.com", PathPrefix: "/", FunctionId: fid}))
	check(t, "duplicate route", err, connect.CodeAlreadyExists)
	_, err = r.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: "h.com", PathPrefix: "x", FunctionId: fid}))
	check(t, "bad prefix", err, connect.CodeInvalidArgument)
	_, err = r.rt.DeleteRoute(ctx, connect.NewRequest(&controlv1.DeleteRouteRequest{RouteId: zero}))
	check(t, "unknown route", err, connect.CodeNotFound)
	_, err = r.fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{PageToken: "!!!"}))
	check(t, "forged page token", err, connect.CodeInvalidArgument)
}

func check(t *testing.T, what string, err error, want connect.Code) {
	t.Helper()
	if want == 0 {
		if err != nil {
			t.Errorf("%s: unexpected %v", what, err)
		}
		return
	}
	if code(err) != want {
		t.Errorf("%s: %v, want %v", what, err, want)
	}
}

func TestDescribeReportsTheModuleLimit(t *testing.T) {
	r := newRig(t, func(c *control.Config) { c.MaxModuleBytes = 4096 })
	d, err := r.insp.Describe(ctx, connect.NewRequest(&controlv1.DescribeRequest{}))
	if err != nil || d.Msg.MaxModuleBytes != 4096 || d.Msg.Implementation.Name == "" {
		t.Errorf("%+v %v", d, err)
	}
}

func TestInternalErrorsDoNotLeakDatabaseDetail(t *testing.T) {
	// Close the database pool under a running service: the store then returns
	// an error it does not classify (a pgx "closed pool" error naming the
	// driver), which must reach the client as a generic internal error.
	pool := testpg.New(t)
	if _, err := migrate.Up(ctx, pool, migrations.FS); err != nil {
		t.Fatal(err)
	}
	box, _ := secrets.NewBox(orchtest.Key)
	mux := http.NewServeMux()
	control.New(control.Config{Store: store.New(pool, box), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).
		Mount(mux, authn.Verifier{adminToken: control.AdminScope})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	fn := controlv1connect.NewFunctionServiceClient(srv.Client(), srv.URL, connect.WithInterceptors(authn.Client(adminToken)))

	if _, err := fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{})); err != nil {
		t.Fatalf("sanity: %v", err)
	}
	pool.Close()
	_, err := fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{}))
	if code(err) != connect.CodeInternal {
		t.Fatalf("err = %v, want internal", err)
	}
	if err.Error() != "internal: internal error" {
		t.Errorf("message = %q (want the generic %q; the driver's wording must not reach clients)", err.Error(), "internal: internal error")
	}
}
