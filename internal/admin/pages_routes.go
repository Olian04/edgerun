package admin

import (
	"context"

	"connectrpc.com/connect"

	"github.com/Olian04/webui/pkg/webui"
)

type routeRow struct {
	ID, Host, Prefix, FunctionID, Function string
	Created                                int
}

var (
	routeHost     = webui.String[routeRow]{Label: "Host", Load: func(r routeRow) string { return r.Host }}
	routePrefix   = webui.String[routeRow]{Label: "Path prefix", Load: func(r routeRow) string { return r.Prefix }}
	routeFunction = webui.String[routeRow]{Label: "Function", Load: func(r routeRow) string { return r.Function }}
	routeCreated  = webui.Timestamp[routeRow]{Label: "Created", Load: func(r routeRow) int { return r.Created }, Format: when}
)

// routesTable lists routes, all of them or those of one function. The filter
// reads the function id from the request; it returns "" for every route.
func (u *ui) routesTable(title string, onlyFunction func(context.Context) string) webui.Table[routeRow] {
	return webui.Table[routeRow]{
		Title: title,
		Key:   func(r routeRow) string { return r.ID },
		Rows: func(ctx context.Context) ([]routeRow, error) {
			rs, err := u.c.Routes(ctx)
			if err != nil {
				return nil, err
			}
			only := ""
			if onlyFunction != nil {
				only = onlyFunction(ctx)
			}
			names := u.c.FunctionNames(ctx)
			var rows []routeRow
			for _, r := range rs {
				if only != "" && r.FunctionId != only {
					continue
				}
				name := names[r.FunctionId]
				if name == "" {
					name = shortID(r.FunctionId)
				}
				rows = append(rows, routeRow{ID: r.Id, Host: r.HostPattern, Prefix: r.PathPrefix, FunctionID: r.FunctionId,
					Function: name, Created: int(r.CreateTime.AsTime().Unix())})
			}
			return rows, nil
		},
		Columns: []webui.Accessor[routeRow]{routeHost, routePrefix, routeFunction, routeCreated},
		RowClick: webui.Link[routeRow, FnArgs]{Page: FunctionPath,
			Args: func(_ context.Context, r routeRow) FnArgs { return FnArgs{ID: r.FunctionID} }},
		Actions: []webui.Action[routeRow]{{
			Label: "Delete", Role: webui.RoleDestructive, Guard: editGuard[routeRow],
			Confirm: "Delete this route? Requests it matched will go to the next most specific route, or get a 404.",
			Run: func(ctx context.Context, r routeRow) (webui.Outcome, error) {
				if err := u.c.DeleteRoute(ctx, r.ID); err != nil {
					return failed(err)
				}
				return webui.Success("Route deleted"), nil
			},
		}},
	}
}

func (u *ui) routesPage() webui.Page[webui.NoArgs] {
	return webui.Page[webui.NoArgs]{
		Path: RoutesPath,
		Nav:  webui.Nav{Label: "Routes", Icon: "route"},
		Body: webui.Stack{
			webui.Toolbar[webui.NoArgs]{Left: []webui.ToolbarItem[webui.NoArgs]{
				webui.Link[webui.NoArgs, RouteNewArgs]{Label: "New route", Role: webui.RolePrimary, Page: NewRoutePath,
					Args: func(context.Context, webui.NoArgs) RouteNewArgs { return RouteNewArgs{} }},
			}},
			u.routesTable("Routes", nil),
		},
	}
}

type newRoute struct {
	Host, Prefix string
	Function     []string
}

func (u *ui) newRoutePage() webui.Page[RouteNewArgs] {
	host := webui.String[newRoute]{
		Label: "Host", Placeholder: "api.example.com",
		Help: "An exact host, a wildcard such as *.example.com (which does not match example.com itself), or * for every host.",
		Rules: webui.StringRules{Required: true, MaxLen: 253, Pattern: &webui.PatternRule{
			Expr: `\*|(\*\.)?[a-z0-9]([a-z0-9.\-]*[a-z0-9])?`, Message: "must be a lower-case host, *.suffix, or *"}},
		Load: func(r newRoute) string { return r.Host }, Store: func(r *newRoute, v string) { r.Host = v },
	}
	prefix := webui.String[newRoute]{
		Label: "Path prefix", Placeholder: "/",
		Help:  "Requests whose path is this or below it, on whole segments: /api matches /api and /api/x but not /apiary.",
		Rules: webui.StringRules{Required: true, StartsWith: "/", NotContains: " "},
		Load:  func(r newRoute) string { return r.Prefix }, Store: func(r *newRoute, v string) { r.Prefix = v },
	}
	function := webui.Select[newRoute]{
		Label: "Function", Required: true,
		LoadOptions: func(ctx context.Context) ([]webui.Option, error) {
			fns, err := u.c.Functions(ctx)
			if err != nil {
				return nil, err
			}
			opts := make([]webui.Option, len(fns))
			for i, f := range fns {
				opts[i] = webui.Option{Value: f.Id, Label: f.Name}
			}
			return opts, nil
		},
		Load:  func(r newRoute) []string { return r.Function },
		Store: func(r *newRoute, v []string) { r.Function = v },
	}
	return webui.Page[RouteNewArgs]{
		Path:  NewRoutePath,
		Guard: editPageGuard[RouteNewArgs],
		Body: webui.Form[newRoute]{
			Title: "New route",
			Load: func(ctx context.Context) (newRoute, error) {
				r := newRoute{Prefix: "/"}
				if f := webui.ArgsOf[RouteNewArgs](ctx).Function; f != "" {
					r.Function = []string{f}
				}
				return r, nil
			},
			Fields: []webui.Accessor[newRoute]{host, prefix, function},
			Submit: webui.Action[newRoute]{
				Label: "Create route", Role: webui.RolePrimary, Guard: editGuard[newRoute],
				Run: func(ctx context.Context, r newRoute) (webui.Outcome, error) {
					if len(r.Function) != 1 {
						return webui.Reject(webui.Field[newRoute](function, "choose a function")), nil
					}
					_, err := u.c.CreateRoute(ctx, r.Host, r.Prefix, r.Function[0])
					if err == nil {
						return webui.Success("Route created").Then(webui.Open(ctx, RoutesPath, webui.NoArgs{})), nil
					}
					switch connect.CodeOf(err) {
					case connect.CodeAlreadyExists:
						return webui.Reject(webui.Field[newRoute](prefix, "this host and path are already routed")), nil
					case connect.CodeInvalidArgument:
						return webui.Reject(webui.Field[newRoute](host, explain(err))), nil
					}
					return failed(err)
				},
			},
		},
	}
}
