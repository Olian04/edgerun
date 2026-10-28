package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// Reconciler brings an orchestrator to the state a Spec describes. It works in
// three steps, like terraform: Plan reads, Validate asks the orchestrator
// whether it would accept every step of the plan (validate_only, nothing is
// kept), and Apply executes the plan. Each run starts from a fresh read of the
// orchestrator, so a run that failed half way needs no recovery logic: the next
// plan is simply smaller.
type Reconciler struct {
	Ctl        *edgerun.Control
	Owner      string // identifies the resources this instance created (label gitops.example.com/owner)
	Prune      bool   // delete functions this instance created that the file no longer declares
	Adopt      bool   // take over functions that exist but carry no owner label
	AllowEmpty bool   // permit Prune with a file that declares no functions
	Getenv     func(string) (string, bool)
	Timeout    time.Duration // per RPC group (see Item.Run)
}

// Module is a function's code as read from the repository.
type Module struct {
	Data   []byte
	Digest string // "sha256:<hex>", the form FunctionVersion.artifact_digest uses
}

func NewModule(data []byte) *Module { return &Module{Data: data, Digest: edgerun.Digest(data)} }

// Phases order the items of a plan across all functions. The order matters for
// a deploy: variables the new version needs exist before it is activated, and
// variables only the old version needed are removed after.
const (
	phCreate = iota
	phEnvDelEarly
	phEnvSet
	phVersion
	phEnvDel
	phLabels
	phEgress
	phRouteDel
	phRouteAdd
	phPrune
)

// nilUUID stands in for the id of a function that does not exist yet when a step
// of its plan is validated: a well-formed id that names nothing.
const nilUUID = "00000000-0000-0000-0000-000000000000"

type Item struct {
	Fn    string // function this belongs to; a failure skips the rest of its items
	Phase int
	Sign  string // "+" create, "~" change, "-" delete
	Text  string
	// Run performs the step, or, when x.vo is set, asks the orchestrator to
	// validate it without doing it.
	Run func(ctx context.Context, x *Exec) error
	// Blind is why this step cannot be validated on its own, if so. validate_only
	// judges one request against the stored state, not against the steps before it.
	Blind string

	Rejected error // the orchestrator's answer to the validate-only call
	Err      error
	Done     bool
	Skipped  bool
}

// Problem is a reason a function (or route) could not be planned: a conflict
// with something this service does not own, an unreadable module, a missing
// secret, a step the orchestrator rejected. It is reported and makes the run
// fail, but does not stop other functions from being reconciled.
type Problem struct {
	Fn  string
	Err error
}

type Plan struct {
	ids      map[string]string // function name -> id, of the functions that exist now
	Items    []*Item
	Problems []Problem
	Notes    []string
	rejected map[string]bool // functions with a step the orchestrator rejected: none of their steps is applied
}

func (p *Plan) Empty() bool { return len(p.Items) == 0 }

func (p *Plan) add(fn string, phase int, sign, text string, run func(context.Context, *Exec) error) *Item {
	it := &Item{Fn: fn, Phase: phase, Sign: sign, Text: text, Run: run}
	p.Items = append(p.Items, it)
	return it
}

func (p *Plan) problem(fn string, format string, a ...any) {
	p.Problems = append(p.Problems, Problem{Fn: fn, Err: fmt.Errorf(format, a...)})
}

// Print writes the plan in diff style.
func (p *Plan) Print(w io.Writer) {
	for _, it := range p.Items {
		blind := ""
		if it.Blind != "" {
			blind = " (not validated ahead: " + it.Blind + ")"
		}
		fmt.Fprintf(w, "%s %s: %s%s\n", it.Sign, label(it.Fn), it.Text, blind)
	}
	for _, pr := range p.Problems {
		fmt.Fprintf(w, "! %s: %v\n", label(pr.Fn), pr.Err)
	}
	for _, n := range p.Notes {
		fmt.Fprintf(w, "? %s\n", n)
	}
}

func label(fn string) string {
	if fn == "" {
		return "-"
	}
	return fn
}

// planner holds one planning run's reads.
type planner struct {
	r      *Reconciler
	plan   *Plan
	spec   *Spec
	mods   map[string]*Module
	modErr map[string]error
	rev    string // the git commit being deployed

	owned       map[string]*controlv1.Function // functions that carry our owner label, by name
	ownedByID   map[string]*controlv1.Function
	ownedRoutes []*controlv1.Route // routes that carry our owner label

	// Read only when something needs them: a name or route the owner filter did not
	// return may still exist, held by somebody else.
	all        map[string]*controlv1.Function // by name
	allByID    map[string]*controlv1.Function
	allRoutes  []*controlv1.Route
	haveAll    bool
	haveRoutes bool

	managed  map[string]bool                // desired functions this instance owns (or is about to)
	existing map[string]*controlv1.Function // managed functions that exist now
	adopted  map[string]bool
	envs     map[string]*envInfo // function id -> env
	err      error               // the orchestrator could not be read
}

type envInfo struct {
	entries map[string]*controlv1.EnvEntry
}

func (pl *planner) env(ctx context.Context, fn *controlv1.Function) (*envInfo, error) {
	if e, ok := pl.envs[fn.Id]; ok {
		return e, nil
	}
	ctx, cancel := context.WithTimeout(ctx, pl.r.Timeout)
	defer cancel()
	res, err := pl.r.Ctl.ListEnv(ctx, &controlv1.ListEnvRequest{FunctionId: fn.Id})
	if err != nil {
		return nil, fmt.Errorf("list env: %w", err)
	}
	e := &envInfo{entries: map[string]*controlv1.EnvEntry{}}
	for _, en := range res.Entries {
		e.entries[en.Name] = en
	}
	pl.envs[fn.Id] = e
	return e, nil
}

// loadAll reads every function. There is no GetFunctionByName and no name
// filter, so finding out whether a name is taken is a scan; it is only done when
// a declared function is not one of ours.
func (pl *planner) loadAll(ctx context.Context) error {
	if pl.haveAll {
		return pl.err
	}
	pl.haveAll = true
	pl.all, pl.allByID = map[string]*controlv1.Function{}, map[string]*controlv1.Function{}
	cctx, cancel := context.WithTimeout(ctx, 2*pl.r.Timeout)
	defer cancel()
	for fn, err := range pl.r.Ctl.AllFunctions(cctx) {
		if err != nil {
			pl.err = fmt.Errorf("list functions: %w", err)
			return pl.err
		}
		pl.all[fn.Name], pl.allByID[fn.Id] = fn, fn
	}
	return nil
}

// loadRoutes reads every route, for the same reason.
func (pl *planner) loadRoutes(ctx context.Context) error {
	if pl.haveRoutes {
		return pl.err
	}
	pl.haveRoutes = true
	cctx, cancel := context.WithTimeout(ctx, 2*pl.r.Timeout)
	defer cancel()
	for rt, err := range pl.r.Ctl.AllRoutes(cctx) {
		if err != nil {
			pl.err = fmt.Errorf("list routes: %w", err)
			return pl.err
		}
		pl.allRoutes = append(pl.allRoutes, rt)
	}
	return nil
}

// ownsFunction: carries our label, or is being adopted in this run.
func (pl *planner) ownsFunction(id string) bool {
	if pl.ownedByID[id] != nil {
		return true
	}
	f := pl.allByID[id]
	return f != nil && pl.adopted[f.Name]
}

func (pl *planner) fnName(ctx context.Context, id string) string {
	if f := pl.ownedByID[id]; f != nil {
		return f.Name
	}
	if pl.loadAll(ctx) == nil {
		if f := pl.allByID[id]; f != nil {
			return f.Name
		}
	}
	return "?"
}

// Plan reads the orchestrator and computes what must change. mods holds the
// module of every function that could be read; modErr the reason for those that
// could not; rev is the git commit they come from. It returns an error only when
// the orchestrator cannot be read.
func (r *Reconciler) Plan(ctx context.Context, spec *Spec, mods map[string]*Module, modErr map[string]error, rev string) (*Plan, error) {
	if r.Owner == "" {
		return nil, errors.New("--owner must not be empty: it is the value of the owner label")
	}
	if r.Prune && len(spec.Functions) == 0 && !r.AllowEmpty {
		return nil, errors.New("refusing to prune: the file declares no functions (pass --allow-empty if that is intended)")
	}
	pl := &planner{r: r, plan: &Plan{ids: map[string]string{}, rejected: map[string]bool{}}, spec: spec, mods: mods, modErr: modErr, rev: rev,
		owned: map[string]*controlv1.Function{}, ownedByID: map[string]*controlv1.Function{},
		managed: map[string]bool{}, existing: map[string]*controlv1.Function{}, adopted: map[string]bool{}, envs: map[string]*envInfo{}}

	// What this instance owns is what carries its label: one filtered list each.
	cctx, cancel := context.WithTimeout(ctx, 2*r.Timeout)
	defer cancel()
	for fn, err := range r.Ctl.AllFunctionsMatching(cctx, ownerLabels(r.Owner)) {
		if err != nil {
			return nil, fmt.Errorf("list functions: %w", err)
		}
		pl.owned[fn.Name], pl.ownedByID[fn.Id] = fn, fn
		pl.plan.ids[fn.Name] = fn.Id
	}
	for rt, err := range r.Ctl.AllRoutesMatching(cctx, ownerLabels(r.Owner)) {
		if err != nil {
			return nil, fmt.Errorf("list routes: %w", err)
		}
		pl.ownedRoutes = append(pl.ownedRoutes, rt)
	}

	for _, d := range spec.Functions {
		pl.function(ctx, d)
	}
	pl.planRoutes(ctx)
	if r.Prune {
		pl.planPrune()
	}
	if pl.err != nil {
		return nil, pl.err
	}
	sort.SliceStable(pl.plan.Items, func(i, j int) bool { return pl.plan.Items[i].Phase < pl.plan.Items[j].Phase })
	return pl.plan, nil
}

func (pl *planner) function(ctx context.Context, d FunctionSpec) {
	p, r := pl.plan, pl.r
	if err := pl.modErr[d.Name]; err != nil {
		p.problem(d.Name, "module: %v", err)
		return
	}
	mod := pl.mods[d.Name]

	secrets := map[string]string{}
	tags := map[string]string{}
	for name, ref := range d.SecretEnv {
		v, ok := r.Getenv(ref)
		if !ok {
			p.problem(d.Name, "secret_env %s: environment variable %s is not set in this process", name, ref)
			return
		}
		if other, dup := tags[nameTag(name)]; dup {
			p.problem(d.Name, "secret_env %s and %s have the same 32-bit name tag in the revision record; rename one", name, other)
			return
		}
		tags[nameTag(name)] = name
		secrets[name] = v
	}

	// Ownership is the owner label, found by the filtered list; a function the
	// filter did not return is looked for among all of them.
	fn := pl.owned[d.Name]
	adopting := false
	if fn == nil {
		if err := pl.loadAll(ctx); err != nil {
			p.problem(d.Name, "%v", err)
			return
		}
		if fn = pl.all[d.Name]; fn != nil {
			if o := fn.Labels[labelOwner]; o != "" {
				p.problem(d.Name, "exists and belongs to another owner (%q); not touching it", o)
				return
			}
			if !r.Adopt {
				p.problem(d.Name, "exists but was not created by this service (it has no %s label); pass --adopt to take it over", labelOwner)
				return
			}
			adopting = true
		}
	}

	var env *envInfo
	var versions []*controlv1.FunctionVersion
	base := map[string]string{} // labels of the function that are not ours
	if fn == nil {
		env = &envInfo{entries: map[string]*controlv1.EnvEntry{}}
		p.add(d.Name, phCreate, "+", "create function", func(ctx context.Context, x *Exec) error {
			// One RPC, and the owner label is part of it: there is no moment at which
			// the function exists and is not ours.
			res, err := x.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: d.Name, Labels: ownerLabels(r.Owner), ValidateOnly: x.vo})
			if err != nil {
				return err
			}
			if !x.vo {
				x.ids[d.Name] = res.Function.Id
			}
			return nil
		})
		pl.managed[d.Name] = true
	} else {
		var err error
		if env, err = pl.env(ctx, fn); err != nil {
			p.problem(d.Name, "%v", err)
			return
		}
		cctx, cancel := context.WithTimeout(ctx, 2*r.Timeout)
		for v, err := range r.Ctl.AllVersions(cctx, fn.Id) {
			if err != nil {
				cancel()
				p.problem(d.Name, "list versions: %v", err)
				return
			}
			versions = append(versions, v)
		}
		cancel()
		base = foreignLabels(fn.Labels)
		p.ids[d.Name] = fn.Id
		pl.existing[d.Name] = fn
		pl.managed[d.Name] = true
		if adopting {
			pl.adopted[d.Name] = true
			nroutes := 0
			if err := pl.loadRoutes(ctx); err == nil {
				for _, rt := range pl.allRoutes {
					if rt.FunctionId == fn.Id {
						nroutes++
					}
				}
			}
			text := fmt.Sprintf("ADOPT existing function it did not create (%d version(s), %d env var(s), %d route(s)); env vars and routes not in the file will be removed", len(versions), len(env.entries), nroutes)
			p.add(d.Name, phCreate, "~", text, func(ctx context.Context, x *Exec) error {
				_, err := x.ctl.SetLabels(ctx, &controlv1.SetLabelsRequest{FunctionId: x.ids[d.Name], Labels: functionLabels(fn.Labels, r.Owner, nil), ValidateOnly: x.vo})
				return err
			})
		}
	}

	pl.planEnv(ctx, d, fn, env, secrets, adopting, base)

	// Version.
	want := toLimits(d.Limits)
	var match *controlv1.FunctionVersion
	for _, v := range versions { // newest first
		if v.ArtifactDigest == mod.Digest && v.Abi == d.ABI && limitsEqual(v.Limits, want) {
			match = v
			break
		}
	}
	switch {
	case match != nil && fn != nil && fn.ActiveVersionId == match.Id:
		// current
	case match != nil:
		var cur string
		if fn.ActiveVersionId == "" {
			cur = "nothing active"
		} else if av := findVersion(versions, fn.ActiveVersionId); av != nil {
			cur = fmt.Sprintf("v%d active", av.Number)
		} else {
			cur = "unknown version active"
		}
		p.add(d.Name, phVersion, "~", fmt.Sprintf("activate existing v%d (%s, %s) instead of %s", match.Number, short(match.ArtifactDigest), match.Abi, cur), func(ctx context.Context, x *Exec) error {
			_, err := x.ctl.ActivateVersion(ctx, &controlv1.ActivateVersionRequest{FunctionId: x.ids[d.Name], VersionId: match.Id, ValidateOnly: x.vo})
			return err
		})
	default:
		p.add(d.Name, phVersion, "+", fmt.Sprintf("upload new version (%s, %d bytes, abi %s) and activate", short(mod.Digest), len(mod.Data), d.ABI), func(ctx context.Context, x *Exec) error {
			// Activate is part of the same RPC. If it is lost on the way back, the next
			// run finds this version by digest and only activates it.
			id, syn := x.fid(d.Name)
			_, err := x.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{
				FunctionId: id, Module: mod.Data, Abi: d.ABI, Limits: want, Activate: true,
				Labels:       map[string]string{labelOwner: r.Owner, labelCommit: pl.rev},
				ValidateOnly: x.vo})
			return x.tol(d.Name, err, syn)
		})
	}

	// Egress.
	var have []string
	if fn != nil {
		have = fn.EgressAllow
	}
	if !slices.Equal(have, d.Egress) && !(len(have) == 0 && len(d.Egress) == 0) {
		p.add(d.Name, phEgress, "~", fmt.Sprintf("egress %v -> %v", have, d.Egress), func(ctx context.Context, x *Exec) error {
			id, syn := x.fid(d.Name)
			_, err := x.ctl.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: id, Allow: d.Egress, ValidateOnly: x.vo})
			return x.tol(d.Name, err, syn)
		})
	}
}

// planEnv plans the plain variables, the secrets and the label record of the
// secrets' revisions.
func (pl *planner) planEnv(ctx context.Context, d FunctionSpec, fn *controlv1.Function, env *envInfo, secrets map[string]string, adopting bool, base map[string]string) {
	p, r := pl.plan, pl.r
	var plain, secret []string
	for n := range d.Env {
		plain = append(plain, n)
	}
	for n := range secrets {
		secret = append(secret, n)
	}
	sort.Strings(plain)
	sort.Strings(secret)

	var stale []string
	for n := range env.entries {
		if _, in := d.Env[n]; in {
			continue
		}
		if _, in := secrets[n]; in {
			continue
		}
		stale = append(stale, n)
	}
	sort.Strings(stale)

	// A function holds at most 64 variables. If the new ones do not fit beside the
	// stale ones, the stale ones go first, and the new ones cannot be validated
	// ahead (each validate-only call sees the stored state, where the slots are
	// still taken).
	fresh := 0
	for _, n := range append(slices.Clone(plain), secret...) {
		if env.entries[n] == nil {
			fresh++
		}
	}
	early := len(stale) > 0 && len(env.entries)+fresh > maxEnvVars
	blind := func(it *Item, a *controlv1.EnvEntry) {
		if early && a == nil {
			it.Blind = "it needs the deletions before it, for the 64-variable limit"
		}
	}

	for _, n := range plain {
		a := env.entries[n]
		var it *Item
		switch {
		case a == nil:
			it = p.add(d.Name, phEnvSet, "+", fmt.Sprintf("env %s=%s", n, show(d.Env[n])), pl.setEnvRun(d.Name, n, d.Env[n], false))
		case a.Secret:
			it = p.add(d.Name, phEnvSet, "~", fmt.Sprintf("env %s: was a secret, now plain %s", n, show(d.Env[n])), pl.setEnvRun(d.Name, n, d.Env[n], false))
		case a.Value != d.Env[n]:
			it = p.add(d.Name, phEnvSet, "~", fmt.Sprintf("env %s: %s -> %s", n, show(a.Value), show(d.Env[n])), pl.setEnvRun(d.Name, n, d.Env[n], false))
		}
		if it != nil {
			blind(it, a)
		}
	}

	// Secrets. Three revisions are in play for each: the one stored now (ListEnv),
	// the one the desired value would have (the answer to a validate-only SetEnv,
	// the only way to learn it: the key is the server's), and the one this service
	// recorded when it last wrote the secret (a label of the function).
	recorded := map[string]string{}
	if fn != nil {
		recorded = decodeSecrets(fn.Labels)
	}
	known := map[string]string{} // secrets that stay as they are: name -> current revision
	willWrite := false
	for _, n := range secret {
		a := env.entries[n]
		var it *Item
		switch {
		case a == nil:
			it = p.add(d.Name, phEnvSet, "+", fmt.Sprintf("secret %s (value not shown)", n), pl.setEnvRun(d.Name, n, secrets[n], true))
		case !a.Secret:
			it = p.add(d.Name, phEnvSet, "~", fmt.Sprintf("env %s: was plain, now secret", n), pl.setEnvRun(d.Name, n, secrets[n], true))
		default:
			if des := pl.revisionOf(ctx, fn.Id, n, secrets[n]); des != "" && des == a.Revision {
				known[n] = a.Revision
				continue
			}
			text := fmt.Sprintf("secret %s differs from the value in the process environment (value not shown)", n)
			if rec, ok := recorded[nameTag(n)]; ok {
				if rec != valueTag(n, a.Revision) {
					text = fmt.Sprintf("secret %s: DRIFT, it was changed in the orchestrator since this service wrote it (revision differs from the recorded one); restoring (value not shown)", n)
				} else {
					text = fmt.Sprintf("secret %s changed in the process environment (value not shown)", n)
				}
			}
			it = p.add(d.Name, phEnvSet, "~", text, pl.setEnvRun(d.Name, n, secrets[n], true))
		}
		blind(it, a)
		willWrite = true
	}

	delPhase := phEnvDel
	if early {
		delPhase = phEnvDelEarly
	}
	for _, n := range stale {
		p.add(d.Name, delPhase, "-", "env "+n+" (not in the file)", func(ctx context.Context, x *Exec) error {
			id, syn := x.fid(d.Name)
			_, err := x.ctl.DeleteEnv(ctx, &controlv1.DeleteEnvRequest{FunctionId: id, Name: n, ValidateOnly: x.vo})
			if connect.CodeOf(err) == connect.CodeNotFound && !x.vo {
				return nil // already gone: the goal state
			}
			return x.tol(d.Name, err, syn)
		})
	}

	// The record of the revisions is written after the secrets, so that it is only
	// ever behind reality: after a crash in between, the next run finds the stored
	// revision equal to the desired one and merely updates the record.
	cur := maps.Clone(fn.GetLabels())
	if cur == nil {
		cur = map[string]string{}
	}
	if adopting {
		cur[labelOwner] = r.Owner
	}
	var needed bool
	switch {
	case willWrite:
		needed = true
	case fn == nil:
		needed = false // nothing to record for a function without secrets
	default:
		revs := map[string]string{}
		for n, rev := range known {
			revs[n] = rev
		}
		needed = !maps.Equal(functionLabels(cur, r.Owner, revs), cur)
	}
	if needed {
		text := fmt.Sprintf("labels: record the revisions of %d secret(s)", len(secret))
		if len(secret) == 0 {
			text = "labels: drop the record of secrets that are no longer declared"
		}
		p.add(d.Name, phLabels, "~", text, func(ctx context.Context, x *Exec) error {
			revs := map[string]string{}
			for _, n := range secret {
				revs[n] = x.revision(d.Name, n, known[n])
			}
			id, syn := x.fid(d.Name)
			_, err := x.ctl.SetLabels(ctx, &controlv1.SetLabelsRequest{FunctionId: id, Labels: functionLabels(base, r.Owner, revs), ValidateOnly: x.vo})
			return x.tol(d.Name, err, syn)
		})
	}
}

// revisionOf asks the orchestrator which revision the value would have, by
// validating a write of it (nothing is written). "" if it cannot say; the
// write is then planned and the validation of the plan reports why.
func (pl *planner) revisionOf(ctx context.Context, fnID, name, value string) string {
	ctx, cancel := context.WithTimeout(ctx, pl.r.Timeout)
	defer cancel()
	res, err := pl.r.Ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: fnID, ValidateOnly: true,
		Entry: &controlv1.EnvEntry{Name: name, Value: value, Secret: true}})
	if err != nil {
		return ""
	}
	return res.GetEntry().GetRevision()
}

func (pl *planner) setEnvRun(fn, name, value string, secret bool) func(context.Context, *Exec) error {
	return func(ctx context.Context, x *Exec) error {
		id, syn := x.fid(fn)
		res, err := x.ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: id, ValidateOnly: x.vo,
			Entry: &controlv1.EnvEntry{Name: name, Value: value, Secret: secret}})
		if err != nil {
			return x.tol(fn, err, syn)
		}
		if secret && res.GetEntry().GetRevision() != "" {
			if x.revs[fn] == nil {
				x.revs[fn] = map[string]string{}
			}
			x.revs[fn][name] = res.Entry.Revision
		}
		return nil
	}
}

// planRoutes. A route is ours when it carries our owner label. Labels cannot be
// edited on a route, so a route that exists without one (made by hand) is not
// ours: if the file wants exactly that route it is replaced by a labelled one,
// otherwise it is somebody else's and a conflict.
func (pl *planner) planRoutes(ctx context.Context) {
	p := pl.plan
	owned := map[string]*controlv1.Route{}
	for _, rt := range pl.ownedRoutes {
		owned[routeKey(rt.HostPattern, rt.PathPrefix)] = rt
	}
	desired := map[string]bool{}
	for _, d := range pl.spec.Routes {
		key := routeKey(d.Host, d.Path)
		desired[key] = true
		if !pl.managed[d.Function] {
			continue // the function is in conflict or failed to plan: leave its routes alone
		}
		target := pl.existing[d.Function] // nil when this run creates it
		add := func(blind string) {
			it := p.add(d.Function, phRouteAdd, "+", fmt.Sprintf("route %s%s", d.Host, d.Path), func(ctx context.Context, x *Exec) error {
				id, syn := x.fid(d.Function)
				_, err := x.ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: d.Host, PathPrefix: d.Path, FunctionId: id,
					Labels: ownerLabels(pl.r.Owner), ValidateOnly: x.vo})
				return x.tol(d.Function, err, syn)
			})
			it.Blind = blind
		}
		del := func(rt *controlv1.Route, why string) {
			rid := rt.Id
			p.add(d.Function, phRouteDel, "-", fmt.Sprintf("route %s%s (%s)", d.Host, d.Path, why), func(ctx context.Context, x *Exec) error {
				return x.deleteRoute(ctx, rid)
			})
		}
		const replaced = "the route it replaces is deleted first"
		if a := owned[key]; a != nil {
			if target != nil && a.FunctionId == target.Id {
				continue // current
			}
			// There is no UpdateRoute: retargeting is delete then create, and in
			// between the route does not exist.
			del(a, "pointed at "+pl.fnName(ctx, a.FunctionId)+"; retargeting")
			add(replaced)
			continue
		}
		// Not one of ours. Is the key taken by somebody else's route?
		if err := pl.loadRoutes(ctx); err != nil {
			p.problem(d.Function, "%v", err)
			continue
		}
		var a *controlv1.Route
		for _, rt := range pl.allRoutes {
			if routeKey(rt.HostPattern, rt.PathPrefix) == key {
				a = rt
			}
		}
		switch {
		case a == nil:
			add("")
		case a.Labels[labelOwner] == "" && pl.ownsFunction(a.FunctionId):
			// Made by hand, onto a function this service owns, at a key the file
			// declares: the file wins. Labels cannot be edited, so the route is
			// replaced by a labelled one (the key is unrouted for an instant).
			del(a, "it has no owner label and labels cannot be edited; replacing it with a labelled one")
			add(replaced)
		default:
			who := "it has no " + labelOwner + " label"
			if o := a.Labels[labelOwner]; o != "" {
				who = "it belongs to owner " + fmt.Sprintf("%q", o)
			}
			p.problem(d.Function, "route %s%s is taken by a route to function %q that this service does not own (%s)", d.Host, d.Path, pl.fnName(ctx, a.FunctionId), who)
		}
	}
	// Our routes that the file no longer has. Only for functions managed in this
	// run: a function the file dropped without --prune keeps its routes.
	for _, rt := range pl.ownedRoutes {
		if desired[routeKey(rt.HostPattern, rt.PathPrefix)] {
			continue
		}
		name := pl.fnName(ctx, rt.FunctionId)
		if !pl.managed[name] {
			continue
		}
		pl.deleteStray(name, rt, "not in the file")
	}
	// An adopted function takes its unlabelled routes with it into the file's
	// description: the adoption was announced as removing what the file lacks.
	if len(pl.adopted) > 0 && pl.loadRoutes(ctx) == nil {
		for _, rt := range pl.allRoutes {
			fn := pl.allByID[rt.FunctionId]
			if fn == nil || !pl.adopted[fn.Name] || rt.Labels[labelOwner] != "" || desired[routeKey(rt.HostPattern, rt.PathPrefix)] {
				continue
			}
			pl.deleteStray(fn.Name, rt, "unlabelled route of an adopted function, not in the file")
		}
	}
}

func (pl *planner) deleteStray(fn string, rt *controlv1.Route, why string) {
	rid := rt.Id
	pl.plan.add(fn, phRouteDel, "-", fmt.Sprintf("route %s%s (%s)", rt.HostPattern, rt.PathPrefix, why), func(ctx context.Context, x *Exec) error {
		return x.deleteRoute(ctx, rid)
	})
}

// planPrune deletes the functions this instance created that the file dropped.
// The filtered list already is that set: no per-function scan.
func (pl *planner) planPrune() {
	declared := map[string]bool{}
	for _, d := range pl.spec.Functions {
		declared[d.Name] = true
	}
	var names []string
	for n := range pl.owned {
		if !declared[n] {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	for _, n := range names {
		id := pl.owned[n].Id
		pl.plan.add(n, phPrune, "-", "delete function (created by this service, no longer in the file; removes its versions, routes and env)", func(ctx context.Context, x *Exec) error {
			_, err := x.ctl.DeleteFunction(ctx, &controlv1.DeleteFunctionRequest{FunctionId: id, ValidateOnly: x.vo})
			if connect.CodeOf(err) == connect.CodeNotFound && !x.vo {
				return nil
			}
			return err
		})
	}
}

// Exec is the state of one Validate or Apply.
type Exec struct {
	ctl  *edgerun.Control
	ids  map[string]string // function name -> id (filled in as functions are created)
	vo   bool              // validate only: every request is sent with validate_only
	revs map[string]map[string]string
	// partial: functions whose later steps were validated without the function.
	partial map[string]bool
}

func newExec(ctl *edgerun.Control, ids map[string]string, vo bool) *Exec {
	return &Exec{ctl: ctl, ids: maps.Clone(ids), vo: vo, revs: map[string]map[string]string{}, partial: map[string]bool{}}
}

// fid is the id to send for a function. A validate-only create has no id to
// give, so a step that follows it in the plan is validated against nilUUID, the
// id of nothing; syn says it is that.
func (x *Exec) fid(fn string) (id string, syn bool) {
	if id := x.ids[fn]; id != "" {
		return id, false
	}
	if x.vo {
		return nilUUID, true
	}
	return "", false
}

// tol accepts not_found for a step validated against nilUUID. The server checks
// the form of a request before it looks the function up, so reaching not_found
// means every check that does not need the function passed. It is the most that
// can be asked of a function that does not exist.
func (x *Exec) tol(fn string, err error, syn bool) error {
	if syn && connect.CodeOf(err) == connect.CodeNotFound {
		x.partial[fn] = true
		return nil
	}
	return err
}

// revision is the revision to record for a secret: the one its write returned,
// or, for a secret that stays as it is, the one it has. A validation that has
// seen neither records a stand-in of the right form.
func (x *Exec) revision(fn, name, current string) string {
	if r := x.revs[fn][name]; r != "" {
		return r
	}
	if current != "" {
		return current
	}
	return "pending-" + name
}

func (x *Exec) deleteRoute(ctx context.Context, id string) error {
	_, err := x.ctl.DeleteRoute(ctx, &controlv1.DeleteRouteRequest{RouteId: id, ValidateOnly: x.vo})
	if connect.CodeOf(err) == connect.CodeNotFound && !x.vo {
		return nil
	}
	return err
}

// Validate asks the orchestrator, for every step of the plan, whether it would
// accept it (validate_only: the real checks against the real state, nothing
// kept). A function with a rejected step is dropped from the apply as a whole, so
// that a rule the orchestrator enforces never leaves a function half deployed.
//
// Each call is judged alone against the stored state: a step cannot see the ones
// before it. For a function that does not exist yet that means the steps after
// its creation are validated as far as the server can without it (see Exec.tol),
// and a few steps cannot be validated at all (Item.Blind).
func (p *Plan) Validate(ctx context.Context, r *Reconciler) {
	x := newExec(r.Ctl, p.ids, true)
	for _, it := range p.Items {
		if it.Blind != "" {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, r.Timeout)
		err := it.Run(cctx, x)
		cancel()
		if err != nil {
			it.Rejected = err
			p.rejected[it.Fn] = true
			p.problem(it.Fn, "rejected by the orchestrator (validate_only): %s: %v", it.Text, err)
		}
	}
	for _, it := range p.Items {
		if x.partial[it.Fn] {
			p.Notes = append(p.Notes, fmt.Sprintf("%s does not exist yet: its steps after the creation were validated as far as the orchestrator can without it", it.Fn))
			delete(x.partial, it.Fn)
		}
	}
}

// Apply executes the plan in phase order. A failed item skips the remaining
// items of the same function (never apply a route or a deletion on top of a
// deploy that did not happen) and the run continues with the others. A function
// with a step that validation rejected is not touched at all.
func (p *Plan) Apply(ctx context.Context, r *Reconciler, w io.Writer) error {
	x := newExec(r.Ctl, p.ids, false)
	failed := map[string]bool{}
	var errs []error
	for _, pr := range p.Problems {
		errs = append(errs, fmt.Errorf("%s: %w", label(pr.Fn), pr.Err))
	}
	for _, it := range p.Items {
		if it.Fn != "" && p.rejected[it.Fn] {
			it.Skipped = true
			fmt.Fprintf(w, "  skipped %s: %s (the orchestrator rejected a step of %s; none of it is applied)\n", label(it.Fn), it.Text, it.Fn)
			continue
		}
		if it.Fn != "" && failed[it.Fn] {
			it.Skipped = true
			fmt.Fprintf(w, "  skipped %s: %s (an earlier step for %s failed)\n", label(it.Fn), it.Text, it.Fn)
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, r.Timeout)
		it.Err = it.Run(cctx, x)
		cancel()
		if it.Err != nil {
			failed[it.Fn] = true
			errs = append(errs, fmt.Errorf("%s: %s: %w", label(it.Fn), it.Text, it.Err))
			fmt.Fprintf(w, "! %s: %s: FAILED: %v\n", label(it.Fn), it.Text, it.Err)
			continue
		}
		it.Done = true
		fmt.Fprintf(w, "%s %s: %s\n", it.Sign, label(it.Fn), it.Text)
	}
	return errors.Join(errs...)
}

// Helpers.

func toLimits(l *LimitsSpec) *commonv1.Limits {
	if l == nil {
		return nil
	}
	return &commonv1.Limits{MemoryLimitBytes: l.MemoryMiB << 20, MaxResponseBytes: l.MaxResponseKiB << 10}
}

func limitsEqual(a, b *commonv1.Limits) bool {
	return a.GetMemoryLimitBytes() == b.GetMemoryLimitBytes() && a.GetMaxResponseBytes() == b.GetMaxResponseBytes()
}

func findVersion(vs []*controlv1.FunctionVersion, id string) *controlv1.FunctionVersion {
	for _, v := range vs {
		if v.Id == id {
			return v
		}
	}
	return nil
}

func short(digest string) string {
	if len(digest) > len("sha256:")+12 {
		return digest[:len("sha256:")+12]
	}
	return digest
}

func show(v string) string {
	if len(v) > 40 {
		v = v[:40] + "..."
	}
	return fmt.Sprintf("%q", strings.ReplaceAll(v, "\n", `\n`))
}
