package admin

import (
	"context"
	"errors"
	"log/slog"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/internal/admin/auth"
	"github.com/Olian04/webui/pkg/webui"
)

// Page addresses are constants, not the pages' variables, so that pages can
// link to one another without a Go initialization cycle.
const (
	OverviewPath    webui.PageID[webui.NoArgs] = "/"
	FunctionsPath   webui.PageID[webui.NoArgs] = "/function"
	NewFunctionPath webui.PageID[webui.NoArgs] = "/function/new"
	FunctionPath    webui.PageID[FnArgs]       = "/function/{id}"
	DeployPath      webui.PageID[FnArgs]       = "/function/{id}/deploy"
	EnvNewPath      webui.PageID[FnArgs]       = "/function/{id}/env/new"
	SecretNewPath   webui.PageID[FnArgs]       = "/function/{id}/secret/new"
	EgressNewPath   webui.PageID[FnArgs]       = "/function/{id}/network/new"
	RoutesPath      webui.PageID[webui.NoArgs] = "/route"
	NewRoutePath    webui.PageID[RouteNewArgs] = "/route/new"
	WorkersPath     webui.PageID[webui.NoArgs] = "/worker"
	NetworkPath     webui.PageID[webui.NoArgs] = "/network"
	InvocationsPath webui.PageID[webui.NoArgs] = "/invocation"
)

// FnArgs is the argument of every page about one function.
type FnArgs struct {
	ID string `webui:"id"`
}

// RouteNewArgs preselects the function on the new-route form.
type RouteNewArgs struct {
	Function string
}

// ui holds what the page declarations call into.
type ui struct {
	c         *Control
	maxModule int64
	log       *slog.Logger
}

// Config configures the UI.
type Config struct {
	Control *Control
	// MaxModuleBytes bounds an uploaded module; it should match the
	// orchestrator's limit (Control.Info reports it).
	MaxModuleBytes int64
	Log            *slog.Logger
}

// App returns the webui declaration. Compile it with the prefix it is mounted at.
func App(cfg Config) webui.App {
	if cfg.MaxModuleBytes == 0 {
		cfg.MaxModuleBytes = 32 << 20
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	u := &ui{c: cfg.Control, maxModule: cfg.MaxModuleBytes, log: cfg.Log}
	return webui.App{
		Brand: webui.Brand{Name: "edgerun"},
		Pages: webui.Pages{
			u.overviewPage(),
			u.functionsPage(), u.newFunctionPage(), u.functionPage(), u.deployPage(), u.envNewPage(false), u.envNewPage(true), u.egressNewPage(),
			u.routesPage(), u.newRoutePage(),
			u.workersPage(), u.networkPage(), u.invocationsPage(),
		},
		ExternalLinks: []webui.ExternalLink{{Nav: webui.Nav{Label: "Sign out", Icon: "right-from-bracket"}, URL: "/logout"}},
		Logger:        cfg.Log,
	}
}

// --- who may do what -----------------------------------------------------------
//
// An admin may change things; a viewer may look. Controls a viewer ought to know
// exist are drawn disabled with the reason; pages they have no business on are
// refused. The same check authorises the request, so a forged POST fails too.

func mayEdit(ctx context.Context) bool {
	id, ok := auth.FromContext(ctx)
	return ok && id.Role == auth.RoleAdmin
}

// editGuard guards an action.
func editGuard[M any](ctx context.Context, _ M) error {
	if !mayEdit(ctx) {
		return webui.Disabled("Your account is read-only.")
	}
	return nil
}

// editPageGuard guards a page that only exists to change something.
func editPageGuard[A any](ctx context.Context, _ A) error {
	if !mayEdit(ctx) {
		return webui.Forbidden("Your account is read-only.")
	}
	return nil
}

// --- translating control errors for the operator ---------------------------------

// userFixable reports whether an error is something the operator can act on
// (a duplicate name, a bad value) rather than a failure of the system.
func userFixable(err error) bool {
	switch connect.CodeOf(err) {
	case connect.CodeAlreadyExists, connect.CodeInvalidArgument, connect.CodeNotFound,
		connect.CodeFailedPrecondition, connect.CodeResourceExhausted:
		return true
	}
	return false
}

func explain(err error) string {
	var ce *connect.Error
	if errors.As(err, &ce) {
		return ce.Message()
	}
	return err.Error()
}

// failed turns a control error into an outcome: what the operator can fix is
// said to them; anything else is returned, which logs it and shows a generic
// message (internal detail never reaches the page).
func failed(err error) (webui.Outcome, error) {
	if userFixable(err) {
		return webui.Failure(explain(err)), nil
	}
	return webui.Outcome{}, err
}
