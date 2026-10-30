package admin

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/internal/egress/policy"
	"github.com/Olian04/edgerun/internal/wagi"
	"github.com/Olian04/webui/pkg/webui"
)

// --- the list ---------------------------------------------------------------------

type fnRow struct {
	ID, Name string
	Active   int64
	Created  int
}

var (
	fnRowName    = webui.String[fnRow]{Label: "Name", Load: func(r fnRow) string { return r.Name }}
	fnRowActive  = webui.Int[fnRow]{Label: "Active version", Load: func(r fnRow) int { return int(r.Active) }, Format: versionLabel}
	fnRowCreated = webui.Timestamp[fnRow]{Label: "Created", Load: func(r fnRow) int { return r.Created }, Format: when}
)

func (u *ui) functionsPage() webui.Page[webui.NoArgs] {
	return webui.Page[webui.NoArgs]{
		Path: FunctionsPath,
		Nav:  webui.Nav{Label: "Functions", Icon: "cube"},
		Body: webui.Stack{
			webui.Toolbar[webui.NoArgs]{Left: []webui.ToolbarItem[webui.NoArgs]{
				webui.Link[webui.NoArgs, webui.NoArgs]{Label: "New function", Role: webui.RolePrimary, Page: NewFunctionPath,
					Args: func(context.Context, webui.NoArgs) webui.NoArgs { return webui.NoArgs{} }},
			}},
			webui.Table[fnRow]{
				Title: "Functions",
				Key:   func(r fnRow) string { return r.ID },
				Rows: func(ctx context.Context) ([]fnRow, error) {
					fns, err := u.c.Functions(ctx)
					if err != nil {
						return nil, err
					}
					numbers := u.c.ActiveVersionNumbers(ctx, fns)
					rows := make([]fnRow, len(fns))
					for i, f := range fns {
						rows[i] = fnRow{ID: f.Id, Name: f.Name, Active: numbers[f.Id], Created: int(f.CreateTime.AsTime().Unix())}
					}
					return rows, nil
				},
				Columns: []webui.Accessor[fnRow]{fnRowName, fnRowActive, fnRowCreated},
				RowClick: webui.Link[fnRow, FnArgs]{Page: FunctionPath,
					Args: func(_ context.Context, r fnRow) FnArgs { return FnArgs{ID: r.ID} }},
			},
		},
	}
}

// --- create -----------------------------------------------------------------------

type newFn struct{ Name string }

var newFnName = webui.String[newFn]{
	Label: "Name", Placeholder: "my-function",
	Help: "Lower-case letters, digits and '-'. This is how the function is referred to; routes decide which requests reach it.",
	Rules: webui.StringRules{Required: true, MaxLen: 63, Pattern: &webui.PatternRule{
		Expr: `[a-z0-9]([a-z0-9\-]*[a-z0-9])?`, Message: "must be lower-case letters, digits and '-', not starting or ending with '-'"}},
	Load:  func(f newFn) string { return f.Name },
	Store: func(f *newFn, v string) { f.Name = v },
}

func (u *ui) newFunctionPage() webui.Page[webui.NoArgs] {
	return webui.Page[webui.NoArgs]{
		Path:  NewFunctionPath,
		Guard: editPageGuard[webui.NoArgs],
		Body: webui.Form[newFn]{
			Title:  "New function",
			Load:   func(context.Context) (newFn, error) { return newFn{}, nil },
			Fields: []webui.Accessor[newFn]{newFnName},
			Submit: webui.Action[newFn]{
				Label: "Create", Role: webui.RolePrimary, Guard: editGuard[newFn],
				Run: func(ctx context.Context, f newFn) (webui.Outcome, error) {
					fn, err := u.c.CreateFunction(ctx, f.Name, nil)
					if err == nil {
						return webui.Success("Created " + fn.Name).Then(webui.Open(ctx, FunctionPath, FnArgs{ID: fn.Id})), nil
					}
					switch connect.CodeOf(err) {
					case connect.CodeAlreadyExists:
						return webui.Reject(webui.Field[newFn](newFnName, "a function with this name already exists")), nil
					case connect.CodeInvalidArgument:
						return webui.Reject(webui.Field[newFn](newFnName, explain(err))), nil
					}
					return failed(err)
				},
			},
		},
	}
}

// --- one function -----------------------------------------------------------------

type fnDetail struct {
	ID, Name string
	Active   int64
	Created  int
	Labels   string
}

var (
	detailName    = webui.String[fnDetail]{Label: "Name", Load: func(d fnDetail) string { return d.Name }}
	detailID      = webui.String[fnDetail]{Label: "ID", Load: func(d fnDetail) string { return d.ID }}
	detailActive  = webui.Int[fnDetail]{Label: "Active version", Load: func(d fnDetail) int { return int(d.Active) }, Format: versionLabel}
	detailCreated = webui.Timestamp[fnDetail]{Label: "Created", Load: func(d fnDetail) int { return d.Created }, Format: when}
	detailLabels  = webui.String[fnDetail]{Label: "Labels", NoSort: true, Load: func(d fnDetail) string { return d.Labels }}
)

func (u *ui) functionPage() webui.Page[FnArgs] {
	return webui.Page[FnArgs]{
		Path: FunctionPath,
		Guard: func(ctx context.Context, a FnArgs) error {
			if _, err := u.c.Function(ctx, a.ID); err != nil {
				if connect.CodeOf(err) == connect.CodeNotFound {
					return webui.NotFound("There is no function "+a.ID+".").
						WithLink("Back to the functions", webui.Open(ctx, FunctionsPath, webui.NoArgs{}))
				}
				return err
			}
			return nil
		},
		Crumb: func(ctx context.Context, a FnArgs) string {
			if f, err := u.c.Function(ctx, a.ID); err == nil {
				return f.Name
			}
			return ""
		},
		Body: webui.Stack{
			webui.Toolbar[FnArgs]{
				Left: []webui.ToolbarItem[FnArgs]{
					webui.Link[FnArgs, FnArgs]{Label: "Deploy a version", Role: webui.RolePrimary, Page: DeployPath, Args: sameArgs},
					webui.Link[FnArgs, FnArgs]{Label: "Add a variable", Page: EnvNewPath, Args: sameArgs},
					webui.Link[FnArgs, FnArgs]{Label: "Add a secret", Page: SecretNewPath, Args: sameArgs},
					webui.Link[FnArgs, FnArgs]{Label: "Allow a destination", Page: EgressNewPath, Args: sameArgs},
					webui.Link[FnArgs, RouteNewArgs]{Label: "Add a route", Page: NewRoutePath,
						Args: func(_ context.Context, a FnArgs) RouteNewArgs { return RouteNewArgs{Function: a.ID} }},
				},
				Right: []webui.ToolbarItem[FnArgs]{
					webui.Action[FnArgs]{
						Label: "Delete function", Role: webui.RoleDestructive, Guard: editGuard[FnArgs],
						Confirm: "Delete this function with its versions, routes and environment? This cannot be undone.",
						Run: func(ctx context.Context, a FnArgs) (webui.Outcome, error) {
							if err := u.c.DeleteFunction(ctx, a.ID); err != nil {
								return failed(err)
							}
							return webui.Success("Function deleted").Then(webui.Open(ctx, FunctionsPath, webui.NoArgs{})), nil
						},
					},
				},
			},
			webui.Form[fnDetail]{
				Title: "Function",
				Load: func(ctx context.Context) (fnDetail, error) {
					id := webui.ArgsOf[FnArgs](ctx).ID
					f, err := u.c.Function(ctx, id)
					if err != nil {
						return fnDetail{}, err
					}
					n := u.c.ActiveVersionNumbers(ctx, []*controlv1.Function{f})[f.Id]
					return fnDetail{ID: f.Id, Name: f.Name, Active: n, Created: int(f.CreateTime.AsTime().Unix()), Labels: labelsText(f.Labels)}, nil
				},
				Fields: []webui.Accessor[fnDetail]{webui.Group[fnDetail]{detailName, detailID}, webui.Group[fnDetail]{detailActive, detailCreated}, detailLabels},
			},
			webui.Tabs{Panels: []webui.Tab{
				{Label: "Versions", Body: u.versionsTable()},
				{Label: "Environment", Body: u.envTable()},
				{Label: "Network", Body: u.egressTable()},
				{Label: "Routes", Body: u.routesTable("Routes to this function", func(ctx context.Context) string { return webui.ArgsOf[FnArgs](ctx).ID })},
				{Label: "Invocations", Body: u.invocationsTable("Invocations", 10, func(ctx context.Context) string { return webui.ArgsOf[FnArgs](ctx).ID })},
			}},
		},
	}
}

func sameArgs(_ context.Context, a FnArgs) FnArgs { return a }

// --- versions ---------------------------------------------------------------------

type verRow struct {
	ID, ABI, Digest        string
	Number                 int64
	Active                 bool
	Size, Memory, Response int
	Created                int
}

var (
	verNumber = webui.Int[verRow]{Label: "Version", Load: func(r verRow) int { return int(r.Number) }, Format: versionLabel}
	verState  = webui.Select[verRow]{Label: "State", Options: []webui.Option{{Value: "active", Label: "Active", Tone: webui.ToneOK}}, NoSort: true,
		Load: func(r verRow) []string {
			if r.Active {
				return []string{"active"}
			}
			return nil
		}}
	verABI      = webui.String[verRow]{Label: "ABI", Load: func(r verRow) string { return r.ABI }}
	verSize     = webui.Int[verRow]{Label: "Size", Load: func(r verRow) int { return r.Size }, Format: humanBytes}
	verMemory   = webui.Int[verRow]{Label: "Memory limit", Load: func(r verRow) int { return r.Memory }, Format: limitLabel}
	verResponse = webui.Int[verRow]{Label: "Response limit", Load: func(r verRow) int { return r.Response }, Format: limitLabel}
	verDigest   = webui.String[verRow]{Label: "Digest", Load: func(r verRow) string { return r.Digest }}
	verCreated  = webui.Timestamp[verRow]{Label: "Uploaded", Load: func(r verRow) int { return r.Created }, Format: when}
)

func (u *ui) versionsTable() webui.Table[verRow] {
	return webui.Table[verRow]{
		Title: "Versions",
		Key:   func(r verRow) string { return r.ID },
		Rows: func(ctx context.Context) ([]verRow, error) {
			fid := webui.ArgsOf[FnArgs](ctx).ID
			fn, err := u.c.Function(ctx, fid)
			if err != nil {
				return nil, err
			}
			vs, err := u.c.Versions(ctx, fid)
			if err != nil {
				return nil, err
			}
			rows := make([]verRow, len(vs))
			for i, v := range vs {
				rows[i] = verRow{ID: v.Id, Number: v.Number, Active: v.Id == fn.ActiveVersionId, ABI: v.Abi,
					Digest: shortDigest(v.ArtifactDigest), Size: int(v.SizeBytes), Created: int(v.CreateTime.AsTime().Unix()),
					Memory: int(v.GetLimits().GetMemoryLimitBytes()), Response: int(v.GetLimits().GetMaxResponseBytes())}
			}
			return rows, nil
		},
		Columns: []webui.Accessor[verRow]{verNumber, verState, verABI, verSize, verMemory, verResponse, verDigest, verCreated},
		Actions: []webui.Action[verRow]{{
			Label: "Activate", Role: webui.RolePrimary,
			Guard: func(ctx context.Context, r verRow) error {
				if err := editGuard(ctx, r); err != nil {
					return err
				}
				if r.Active {
					return webui.Disabled("This version is already live.")
				}
				return nil
			},
			Run: func(ctx context.Context, r verRow) (webui.Outcome, error) {
				if err := u.c.Activate(ctx, webui.ArgsOf[FnArgs](ctx).ID, r.ID); err != nil {
					return failed(err)
				}
				return webui.Success(fmt.Sprintf("Version %d is now live", r.Number)), nil
			},
		}},
	}
}

// --- deploy -----------------------------------------------------------------------

type deployForm struct {
	Module   *webui.Upload
	ABI      string
	MemoryMB int
	RespKB   int
	Activate *bool
}

func (u *ui) deployPage() webui.Page[FnArgs] {
	module := webui.File[deployForm]{
		Label: "Module", Help: "The compiled WebAssembly module.", Required: true,
		Accept: []webui.FileKind{webui.FileAny}, MaxSize: u.maxModule,
		Load:  func(deployForm) *webui.FileInfo { return nil },
		Store: func(d *deployForm, up *webui.Upload) { d.Module = up },
	}
	abi := webui.String[deployForm]{
		Label: "ABI", Help: "The guest ABI the module was built for. Only workers that support it will run it.",
		Rules: webui.StringRules{Required: true, MaxLen: 32, Pattern: &webui.PatternRule{
			Expr: `[a-z0-9][a-z0-9.\-]*`, Message: "must be a short lower-case name such as wagi-1"}},
		Load:  func(d deployForm) string { return d.ABI },
		Store: func(d *deployForm, v string) { d.ABI = v },
	}
	zero := 0
	maxMem, maxResp := 512, 32768
	memory := webui.Int[deployForm]{
		Label: "Memory limit (MiB)", Placeholder: "0", Help: "0 uses the worker's default. Workers round up to a size they support and may clamp.",
		Rules: webui.NumberRules[int]{Min: &zero, Max: &maxMem},
		Load:  func(d deployForm) int { return d.MemoryMB }, Store: func(d *deployForm, v int) { d.MemoryMB = v },
	}
	resp := webui.Int[deployForm]{
		Label: "Response limit (KiB)", Placeholder: "0", Help: "The most a single response may contain, headers included. 0 uses the worker's default.",
		Rules: webui.NumberRules[int]{Min: &zero, Max: &maxResp},
		Load:  func(d deployForm) int { return d.RespKB }, Store: func(d *deployForm, v int) { d.RespKB = v },
	}
	activate := webui.Bool[deployForm]{
		Label: "Make it live", Help: "Yes serves traffic with this version immediately. No uploads it; activate it from the versions list.",
		Load: func(d deployForm) *bool { return d.Activate }, Store: func(d *deployForm, v *bool) { d.Activate = v },
	}
	return webui.Page[FnArgs]{
		Path:  DeployPath,
		Guard: editPageGuard[FnArgs],
		Body: webui.Form[deployForm]{
			Title: "Deploy a version",
			Load: func(context.Context) (deployForm, error) {
				yes := true
				return deployForm{ABI: "wagi-1", Activate: &yes}, nil
			},
			Fields: []webui.Accessor[deployForm]{module, abi, webui.Group[deployForm]{memory, resp}, activate},
			Submit: webui.Action[deployForm]{
				Label: "Upload", Role: webui.RolePrimary, Guard: editGuard[deployForm],
				Run: func(ctx context.Context, d deployForm) (webui.Outcome, error) {
					if d.Module == nil || len(d.Module.Data) == 0 {
						return webui.Reject(webui.Field[deployForm](module, "choose a module to upload")), nil
					}
					// The orchestrator is ABI-agnostic, so it cannot tell a broken upload
					// from a module. The UI knows wagi-1 is WebAssembly, and a file that is
					// not one would go live and fail every request.
					if (d.ABI == wagi.ABI || d.ABI == wagi.ABI2) && !bytes.HasPrefix(d.Module.Data, wasmMagic) {
						return webui.Reject(webui.Field[deployForm](module, "this does not look like a WebAssembly module: it must start with \\0asm")), nil
					}
					fid := webui.ArgsOf[FnArgs](ctx).ID
					live := d.Activate != nil && *d.Activate
					v, err := u.c.Deploy(ctx, fid, d.Module.Data, d.ABI, &commonv1.Limits{
						MemoryLimitBytes: uint64(d.MemoryMB) << 20, MaxResponseBytes: uint64(d.RespKB) << 10}, live)
					if err == nil {
						msg := fmt.Sprintf("Uploaded version %d", v.Number)
						if live {
							msg += " and made it live"
						}
						return webui.Success(msg).Then(webui.Open(ctx, FunctionPath, FnArgs{ID: fid})), nil
					}
					switch connect.CodeOf(err) {
					case connect.CodeInvalidArgument:
						return webui.Reject(webui.Field[deployForm](abi, explain(err))), nil
					}
					return failed(err)
				},
			},
		},
	}
}

// --- environment ------------------------------------------------------------------

type envRow struct {
	Name, Value string
	Secret      bool
}

var (
	envName  = webui.String[envRow]{Label: "Name", Load: func(r envRow) string { return r.Name }}
	envValue = webui.String[envRow]{Label: "Value", NoFilter: true, Load: func(r envRow) string {
		if r.Secret {
			return "••••••••"
		}
		return r.Value
	}}
	envKind = webui.Select[envRow]{Label: "Kind", NoSort: true,
		Options: []webui.Option{{Value: "plain", Label: "Plain"}, {Value: "secret", Label: "Secret", Tone: webui.ToneWarning}},
		Load: func(r envRow) []string {
			if r.Secret {
				return []string{"secret"}
			}
			return []string{"plain"}
		}}
)

func (u *ui) envTable() webui.Table[envRow] {
	return webui.Table[envRow]{
		Title: "Environment",
		Desc:  "Variables the function sees. Secrets are write-only: they are never shown again.",
		Key:   func(r envRow) string { return r.Name },
		Rows: func(ctx context.Context) ([]envRow, error) {
			es, err := u.c.Env(ctx, webui.ArgsOf[FnArgs](ctx).ID)
			if err != nil {
				return nil, err
			}
			rows := make([]envRow, len(es))
			for i, e := range es {
				rows[i] = envRow{Name: e.Name, Value: e.Value, Secret: e.Secret}
			}
			return rows, nil
		},
		Columns: []webui.Accessor[envRow]{envName, envValue, envKind},
		Actions: []webui.Action[envRow]{{
			Label: "Delete", Role: webui.RoleDestructive, Guard: editGuard[envRow],
			Confirm: "Delete this variable?",
			Run: func(ctx context.Context, r envRow) (webui.Outcome, error) {
				if err := u.c.DeleteEnv(ctx, webui.ArgsOf[FnArgs](ctx).ID, r.Name); err != nil {
					return failed(err)
				}
				return webui.Success("Deleted " + r.Name), nil
			},
		}},
	}
}

type envForm struct{ Name, Value string }

// envNewPage is the form for a plain variable, or for a secret, whose value is
// typed into a password field and never shown again.
func (u *ui) envNewPage(secret bool) webui.Page[FnArgs] {
	name := webui.String[envForm]{
		Label: "Name", Placeholder: "MY_VARIABLE",
		Rules: webui.StringRules{Required: true, MaxLen: 128, Pattern: &webui.PatternRule{
			Expr: `[A-Za-z_][A-Za-z0-9_]*`, Message: "must be letters, digits and '_', not starting with a digit"}},
		Load: func(e envForm) string { return e.Name }, Store: func(e *envForm, v string) { e.Name = v },
	}
	value := webui.String[envForm]{
		Label: "Value", Secret: secret, Multiline: !secret,
		Rules: webui.StringRules{MaxLen: 32 << 10},
		Load:  func(e envForm) string { return e.Value }, Store: func(e *envForm, v string) { e.Value = v },
	}
	path, title, verb := EnvNewPath, "Add a variable", "Added "
	if secret {
		path, title, verb = SecretNewPath, "Add a secret", "Stored secret "
		value.Help = "Stored encrypted and never shown again. To change it, add it again under the same name."
	}
	return webui.Page[FnArgs]{
		Path:  path,
		Guard: editPageGuard[FnArgs],
		Body: webui.Form[envForm]{
			Title:  title,
			Load:   func(context.Context) (envForm, error) { return envForm{}, nil },
			Fields: []webui.Accessor[envForm]{name, value},
			Submit: webui.Action[envForm]{
				Label: "Save", Role: webui.RolePrimary, Guard: editGuard[envForm],
				Run: func(ctx context.Context, e envForm) (webui.Outcome, error) {
					fid := webui.ArgsOf[FnArgs](ctx).ID
					err := u.c.SetEnv(ctx, fid, &controlv1.EnvEntry{Name: e.Name, Value: e.Value, Secret: secret})
					if err == nil {
						return webui.Success(verb + e.Name).Then(webui.Open(ctx, FunctionPath, FnArgs{ID: fid})), nil
					}
					switch connect.CodeOf(err) {
					case connect.CodeInvalidArgument:
						return webui.Reject(webui.Field[envForm](name, explain(err))), nil
					}
					return failed(err)
				},
			},
		},
	}
}

// wasmMagic opens every WebAssembly binary.
var wasmMagic = []byte("\x00asm")

// --- network ------------------------------------------------------------------------

type egressRow struct{ Entry, Match, Port, Carrier string }

var (
	egressEntry = webui.String[egressRow]{Label: "Destination", Load: func(r egressRow) string { return r.Entry }}
	egressMatch = webui.String[egressRow]{Label: "Matches", NoSort: true, Load: func(r egressRow) string { return r.Match }}
	egressPort  = webui.String[egressRow]{Label: "Port", NoSort: true, Load: func(r egressRow) string { return r.Port }}
	egressCarry = webui.String[egressRow]{Label: "Carried by", NoSort: true, Load: func(r egressRow) string { return r.Carrier }}
)

func describeRule(entry string) egressRow {
	row := egressRow{Entry: entry, Match: "that host", Port: "80 for http, 443 for https"}
	r, err := policy.ParseRule(entry)
	if err != nil {
		return row
	}
	switch {
	case r.Wildcard:
		row.Match = "any host under it, not the domain itself"
	case net.ParseIP(r.Host) != nil:
		row.Match = "that address"
	}
	if r.Port != 0 {
		row.Port = strconv.Itoa(r.Port)
	}
	return row
}

// carriers names the network services that would carry an allowed destination:
// of those alive and ready whose coverage names all of it, the most specific
// ones (the same choice the orchestrator makes; among equals it spreads the
// load, so several may be named).
func carriers(entry string, services []*controlv1.NetworkServiceStatus) string {
	rule, err := policy.ParseRule(entry)
	if err != nil {
		return "?"
	}
	var best []string
	var bestRank [2]int
	for _, n := range services {
		if !n.Alive || n.State != egressv1.ServiceState_SERVICE_STATE_READY {
			continue
		}
		covers, err := policy.ParseCovers(n.GetInfo().GetCovers())
		if err != nil {
			continue
		}
		c, ok := covers.BestFor(rule)
		if !ok {
			continue
		}
		switch cmp := policy.CompareRank(c.Rank(), bestRank); {
		case len(best) == 0 || cmp > 0:
			best, bestRank = []string{n.Endpoint}, c.Rank()
		case cmp == 0:
			best = append(best, n.Endpoint)
		}
	}
	if len(best) == 0 {
		return "no service: guests are told it is not allowed"
	}
	return strings.Join(best, ", ")
}

func (u *ui) egressTable() webui.Table[egressRow] {
	return webui.Table[egressRow]{
		Title: "Network",
		Desc: "Where this function may send requests. Closed by default: with nothing listed it can reach nothing. " +
			"Only modules built for the wagi-2 ABI can use the network at all.",
		Key: func(r egressRow) string { return r.Entry },
		Rows: func(ctx context.Context) ([]egressRow, error) {
			f, err := u.c.Function(ctx, webui.ArgsOf[FnArgs](ctx).ID)
			if err != nil {
				return nil, err
			}
			services, err := u.c.NetworkServices(ctx)
			if err != nil {
				return nil, err
			}
			rows := make([]egressRow, len(f.EgressAllow))
			for i, e := range f.EgressAllow {
				rows[i] = describeRule(e)
				rows[i].Carrier = carriers(e, services)
			}
			return rows, nil
		},
		Columns: []webui.Accessor[egressRow]{egressEntry, egressMatch, egressPort, egressCarry},
		Actions: []webui.Action[egressRow]{{
			Label: "Remove", Role: webui.RoleDestructive, Guard: editGuard[egressRow],
			Confirm: "Stop allowing this destination?",
			Run: func(ctx context.Context, r egressRow) (webui.Outcome, error) {
				fid := webui.ArgsOf[FnArgs](ctx).ID
				f, err := u.c.Function(ctx, fid)
				if err != nil {
					return failed(err)
				}
				keep := make([]string, 0, len(f.EgressAllow))
				for _, e := range f.EgressAllow {
					if e != r.Entry {
						keep = append(keep, e)
					}
				}
				if _, err := u.c.SetEgress(ctx, fid, keep); err != nil {
					return failed(err)
				}
				return webui.Success("Removed " + r.Entry), nil
			},
		}},
	}
}

type egressForm struct{ Entry string }

func (u *ui) egressNewPage() webui.Page[FnArgs] {
	entry := webui.String[egressForm]{
		Label: "Destination", Placeholder: "api.example.com",
		Help: "A host name (api.example.com), every host under a domain (*.example.com), or an address (192.0.2.7). " +
			"Add :port for a port other than 80 (http) and 443 (https).",
		Rules: webui.StringRules{Required: true, MaxLen: 262},
		Load:  func(e egressForm) string { return e.Entry }, Store: func(e *egressForm, v string) { e.Entry = v },
	}
	return webui.Page[FnArgs]{
		Path:  EgressNewPath,
		Guard: editPageGuard[FnArgs],
		Body: webui.Form[egressForm]{
			Title:  "Allow a destination",
			Load:   func(context.Context) (egressForm, error) { return egressForm{}, nil },
			Fields: []webui.Accessor[egressForm]{entry},
			Submit: webui.Action[egressForm]{
				Label: "Allow", Role: webui.RolePrimary, Guard: editGuard[egressForm],
				Run: func(ctx context.Context, e egressForm) (webui.Outcome, error) {
					fid := webui.ArgsOf[FnArgs](ctx).ID
					f, err := u.c.Function(ctx, fid)
					if err != nil {
						return failed(err)
					}
					_, err = u.c.SetEgress(ctx, fid, append(append([]string{}, f.EgressAllow...), strings.TrimSpace(e.Entry)))
					if err == nil {
						return webui.Success("Allowed " + strings.TrimSpace(e.Entry)).Then(webui.Open(ctx, FunctionPath, FnArgs{ID: fid})), nil
					}
					if connect.CodeOf(err) == connect.CodeInvalidArgument {
						return webui.Reject(webui.Field[egressForm](entry, explain(err))), nil
					}
					return failed(err)
				},
			},
		},
	}
}

// labelsText writes labels as "a=b, c=d", by key, or "none".
func labelsText(m map[string]string) string {
	if len(m) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + m[k]
	}
	return strings.Join(parts, ", ")
}
