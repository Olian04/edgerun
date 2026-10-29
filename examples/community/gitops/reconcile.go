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
// whether it would accept the plan (PlanService.Validate: the steps, in the order
// the apply will run them, each seeing the ones before it; nothing is kept), and
// Apply executes the plan. Each run starts from a fresh read of the
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

// Item is one step of a plan: exactly one control request. Validate sends the
// requests as one sequence and Apply sends them one by one, so what was
// validated is what is applied.
type Item struct {
	Fn    string // function this belongs to; a failure skips the rest of its items
	Phase int
	Sign  string // "+" create, "~" change, "-" delete
	Text  string
	// Req builds the request. It is called for a validation and again for the apply:
	// the ids it needs are the real ones (x.fid) or, in a validation, a reference to
	// what an earlier step creates.
	Req func(x *Exec) any
	// IgnoreNotFound: on apply, "not found" means it is already gone, which is the goal.
	IgnoreNotFound bool
	Bytes          int // size of the module this carries, for packing validations

	// Blind is why this step was not validated, if so: only the first 128 steps of a
	// function go into one Validate.
	Blind string

	Rejected error // the orchestrator's answer to the validation of the plan
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
	binds    map[string]string // function name -> the name its create step binds, for the functions this plan creates
	Items    []*Item
	Problems []Problem
	Notes    []string
	rejected map[string]bool // functions with a step the orchestrator rejected: none of their steps is applied
}

func (p *Plan) Empty() bool { return len(p.Items) == 0 }

func (p *Plan) add(fn string, phase int, sign, text string, req func(*Exec) any) *Item {
	it := &Item{Fn: fn, Phase: phase, Sign: sign, Text: text, Req: req}
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

	// Read only when something needs them, and never everything: a function by name
	// or by id, the routes nobody owns.
	byID          map[string]*controlv1.Function // functions looked up by id (nil: there is none)
	unowned       map[string]*controlv1.Route    // routes with no owner label, by route key
	haveUnowned   bool
	adoptedRoutes map[string][]*controlv1.Route // adopted function name -> all its routes

	managed   map[string]bool                // desired functions this instance owns (or is about to)
	existing  map[string]*controlv1.Function // managed functions that exist now
	adopted   map[string]bool
	adoptedID map[string]bool
	envs      map[string]*envInfo // function id -> env
	err       error               // the orchestrator could not be read
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

// functionNamed asks for the one function with this name (ListFunctionsRequest.name):
// whether a name is taken is a lookup, not a scan.
func (pl *planner) functionNamed(ctx context.Context, name string) (*controlv1.Function, error) {
	ctx, cancel := context.WithTimeout(ctx, pl.r.Timeout)
	defer cancel()
	res, err := pl.r.Ctl.ListFunctions(ctx, &controlv1.ListFunctionsRequest{Name: name})
	if err != nil {
		return nil, fmt.Errorf("look up function %s: %w", name, err)
	}
	if len(res.Functions) == 0 {
		return nil, nil
	}
	return res.Functions[0], nil
}

// functionByID is GetFunction, remembered; nil if there is none.
func (pl *planner) functionByID(ctx context.Context, id string) *controlv1.Function {
	if f := pl.ownedByID[id]; f != nil {
		return f
	}
	if f, ok := pl.byID[id]; ok {
		return f
	}
	ctx, cancel := context.WithTimeout(ctx, pl.r.Timeout)
	defer cancel()
	res, err := pl.r.Ctl.GetFunction(ctx, &controlv1.GetFunctionRequest{FunctionId: id})
	var f *controlv1.Function
	switch {
	case err == nil:
		f = res.Function
	case connect.CodeOf(err) != connect.CodeNotFound:
		pl.err = fmt.Errorf("get function %s: %w", id, err)
	}
	pl.byID[id] = f
	return f
}

// loadUnowned reads the routes nobody owns (no owner label at all): has_label_keys
// and lacks_label_keys make "everything nobody owns" one filtered list. They are
// the only routes this service may take over; a route with another owner's label
// is not read, and the orchestrator's refusal to create over it is the report.
func (pl *planner) loadUnowned(ctx context.Context) error {
	if pl.haveUnowned {
		return pl.err
	}
	pl.haveUnowned = true
	cctx, cancel := context.WithTimeout(ctx, 2*pl.r.Timeout)
	defer cancel()
	for rt, err := range pl.r.Ctl.AllRoutesWhere(cctx, &controlv1.ListRoutesRequest{LacksLabelKeys: []string{labelOwner}}) {
		if err != nil {
			pl.err = fmt.Errorf("list routes nobody owns: %w", err)
			return pl.err
		}
		pl.unowned[routeKey(rt.HostPattern, rt.PathPrefix)] = rt
	}
	return nil
}

// ownsFunction: carries our label, or is being adopted in this run.
func (pl *planner) ownsFunction(id string) bool {
	return pl.ownedByID[id] != nil || pl.adoptedID[id]
}

func (pl *planner) fnName(ctx context.Context, id string) string {
	if f := pl.functionByID(ctx, id); f != nil {
		return f.Name
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
	pl := &planner{r: r, plan: &Plan{ids: map[string]string{}, binds: map[string]string{}, rejected: map[string]bool{}}, spec: spec, mods: mods, modErr: modErr, rev: rev,
		owned: map[string]*controlv1.Function{}, ownedByID: map[string]*controlv1.Function{}, byID: map[string]*controlv1.Function{},
		unowned: map[string]*controlv1.Route{}, adoptedRoutes: map[string][]*controlv1.Route{},
		managed: map[string]bool{}, existing: map[string]*controlv1.Function{}, adopted: map[string]bool{}, adoptedID: map[string]bool{},
		envs: map[string]*envInfo{}}

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
	// filter did not return is looked up by its name.
	fn := pl.owned[d.Name]
	adopting := false
	if fn == nil {
		found, err := pl.functionNamed(ctx, d.Name)
		if err != nil {
			p.problem(d.Name, "%v", err)
			return
		}
		if fn = found; fn != nil {
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
	if fn == nil {
		env = &envInfo{entries: map[string]*controlv1.EnvEntry{}}
		p.binds[d.Name] = fmt.Sprintf("f%d", len(p.binds))
		p.add(d.Name, phCreate, "+", "create function", func(x *Exec) any {
			// One RPC, and the owner label is part of it: there is no moment at which
			// the function exists and is not ours.
			return &controlv1.CreateFunctionRequest{Name: d.Name, Labels: ownerLabels(r.Owner)}
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
		p.ids[d.Name] = fn.Id
		pl.existing[d.Name] = fn
		pl.managed[d.Name] = true
		if adopting {
			pl.adopted[d.Name] = true
			pl.adoptedID[fn.Id] = true
			// Its routes, by function (ListRoutesRequest.function_id), not out of all of them.
			cctx, cancel := context.WithTimeout(ctx, 2*r.Timeout)
			for rt, err := range r.Ctl.AllRoutesWhere(cctx, &controlv1.ListRoutesRequest{FunctionId: fn.Id}) {
				if err != nil {
					cancel()
					p.problem(d.Name, "list routes: %v", err)
					return
				}
				pl.adoptedRoutes[d.Name] = append(pl.adoptedRoutes[d.Name], rt)
			}
			cancel()
			text := fmt.Sprintf("ADOPT existing function it did not create (%d version(s), %d env var(s), %d route(s)); env vars and routes not in the file will be removed", len(versions), len(env.entries), len(pl.adoptedRoutes[d.Name]))
			p.add(d.Name, phCreate, "~", text, func(x *Exec) any {
				// Only our own key: the labels other tools put on the function stay, and
				// are not read.
				return &controlv1.PatchLabelsRequest{FunctionId: x.fid(d.Name), Set: ownerLabels(r.Owner)}
			})
		}
	}

	pl.planEnv(ctx, d, fn, env, secrets)

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
		p.add(d.Name, phVersion, "~", fmt.Sprintf("activate existing v%d (%s, %s) instead of %s", match.Number, short(match.ArtifactDigest), match.Abi, cur), func(x *Exec) any {
			return &controlv1.ActivateVersionRequest{FunctionId: x.fid(d.Name), VersionId: match.Id}
		})
	default:
		// Activate is part of the same RPC. If it is lost on the way back, the next
		// run finds this version by digest and only activates it.
		it := p.add(d.Name, phVersion, "+", fmt.Sprintf("upload new version (%s, %d bytes, abi %s) and activate", short(mod.Digest), len(mod.Data), d.ABI), func(x *Exec) any {
			return &controlv1.CreateVersionRequest{
				FunctionId: x.fid(d.Name), Module: mod.Data, Abi: d.ABI, Limits: want, Activate: true,
				Labels: map[string]string{labelOwner: r.Owner, labelCommit: pl.rev}}
		})
		it.Bytes = len(mod.Data)
	}

	// Egress.
	var have []string
	if fn != nil {
		have = fn.EgressAllow
	}
	if !slices.Equal(have, d.Egress) && !(len(have) == 0 && len(d.Egress) == 0) {
		p.add(d.Name, phEgress, "~", fmt.Sprintf("egress %v -> %v", have, d.Egress), func(x *Exec) any {
			return &controlv1.SetEgressRequest{FunctionId: x.fid(d.Name), Allow: d.Egress}
		})
	}
}

// planEnv plans the plain variables, the secrets and the label record of the
// secrets' revisions.
func (pl *planner) planEnv(ctx context.Context, d FunctionSpec, fn *controlv1.Function, env *envInfo, secrets map[string]string) {
	p := pl.plan
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
	// stale ones, the stale ones go first. That is only a choice of order, and the
	// choice is a copy of the limit; whether the order works is Validate's to say:
	// it runs the deletions before the writes, so the slots they free are free.
	fresh := 0
	for _, n := range append(slices.Clone(plain), secret...) {
		if env.entries[n] == nil {
			fresh++
		}
	}
	early := len(stale) > 0 && len(env.entries)+fresh > maxEnvVars

	for _, n := range plain {
		a := env.entries[n]
		switch {
		case a == nil:
			p.add(d.Name, phEnvSet, "+", fmt.Sprintf("env %s=%s", n, show(d.Env[n])), setEnv(d.Name, n, d.Env[n], false))
		case a.Secret:
			p.add(d.Name, phEnvSet, "~", fmt.Sprintf("env %s: was a secret, now plain %s", n, show(d.Env[n])), setEnv(d.Name, n, d.Env[n], false))
		case a.Value != d.Env[n]:
			p.add(d.Name, phEnvSet, "~", fmt.Sprintf("env %s: %s -> %s", n, show(a.Value), show(d.Env[n])), setEnv(d.Name, n, d.Env[n], false))
		}
	}

	// Secrets. Three revisions are in play for each: the one stored now (ListEnv),
	// the one the desired value would have (the answer to a validate-only SetEnv,
	// the only way to learn it: the key is the server's), and the one this service
	// recorded when it last wrote the secret (a label of the function).
	recorded := map[string]string{}
	var desired map[string]string
	if fn != nil {
		recorded = decodeSecrets(fn.Labels)
		var ask []string
		for _, n := range secret {
			if a := env.entries[n]; a != nil && a.Secret {
				ask = append(ask, n)
			}
		}
		desired = pl.revisionsOf(ctx, fn.Id, ask, secrets)
	}
	known := map[string]string{} // secrets that stay as they are: name -> current revision
	willWrite := false
	for _, n := range secret {
		a := env.entries[n]
		switch {
		case a == nil:
			p.add(d.Name, phEnvSet, "+", fmt.Sprintf("secret %s (value not shown)", n), setEnv(d.Name, n, secrets[n], true))
		case !a.Secret:
			p.add(d.Name, phEnvSet, "~", fmt.Sprintf("env %s: was plain, now secret", n), setEnv(d.Name, n, secrets[n], true))
		default:
			if des := desired[n]; des != "" && des == a.Revision {
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
			p.add(d.Name, phEnvSet, "~", text, setEnv(d.Name, n, secrets[n], true))
		}
		willWrite = true
	}

	delPhase := phEnvDel
	if early {
		delPhase = phEnvDelEarly
	}
	for _, n := range stale {
		it := p.add(d.Name, delPhase, "-", "env "+n+" (not in the file)", func(x *Exec) any {
			return &controlv1.DeleteEnvRequest{FunctionId: x.fid(d.Name), Name: n}
		})
		it.IgnoreNotFound = true
	}

	// The record of the revisions is written after the secrets, so that it is only
	// ever behind reality: after a crash in between, the next run finds the stored
	// revision equal to the desired one and merely updates the record. It is a
	// PatchLabels of this service's own keys.
	var needed bool
	switch {
	case willWrite:
		needed = true
	case fn == nil:
		needed = false // nothing to record for a function without secrets
	default:
		want := encodeSecrets(known)
		needed = !maps.Equal(recordLabels(fn.Labels), want)
	}
	if needed {
		text := fmt.Sprintf("labels: record the revisions of %d secret(s)", len(secret))
		if len(secret) == 0 {
			text = "labels: drop the record of secrets that are no longer declared"
		}
		p.add(d.Name, phLabels, "~", text, func(x *Exec) any {
			revs := map[string]string{}
			for _, n := range secret {
				revs[n] = x.revision(d.Name, n, known[n])
			}
			set, remove := recordPatch(revs)
			return &controlv1.PatchLabelsRequest{FunctionId: x.fid(d.Name), Set: set, Remove: remove}
		})
	}
}

// revisionsOf asks the orchestrator which revisions the values would have, by
// validating a write of each (nothing is written): one Validate with a SetEnv step
// per secret, instead of one call per secret. The answer is name -> revision for
// the steps that were accepted; a step that was not (the sequence stops there)
// leaves it and the ones after it out, and the write is then planned, and the
// validation of the plan reports why.
func (pl *planner) revisionsOf(ctx context.Context, fnID string, names []string, values map[string]string) map[string]string {
	out := map[string]string{}
	if len(names) == 0 {
		return out
	}
	steps := make([]*controlv1.Step, len(names))
	for i, n := range names {
		steps[i] = &controlv1.Step{Request: &controlv1.Step_SetEnv{SetEnv: &controlv1.SetEnvRequest{FunctionId: fnID,
			Entry: &controlv1.EnvEntry{Name: n, Value: values[n], Secret: true}}}}
	}
	ctx, cancel := context.WithTimeout(ctx, pl.r.Timeout)
	defer cancel()
	res, err := pl.r.Ctl.Validate(ctx, &controlv1.ValidateRequest{Steps: steps})
	if err != nil {
		return out
	}
	for i, sr := range res.Results {
		if i < len(names) {
			out[names[i]] = sr.GetSetEnv().GetEntry().GetRevision()
		}
	}
	return out
}

func setEnv(fn, name, value string, secret bool) func(*Exec) any {
	return func(x *Exec) any {
		return &controlv1.SetEnvRequest{FunctionId: x.fid(fn), Entry: &controlv1.EnvEntry{Name: name, Value: value, Secret: secret}}
	}
}

// planRoutes. A route is ours when it carries our owner label. A route that exists
// without one (made by hand) is not ours: if it points at one of our functions
// and the file declares its key, the file wins, and the route is labelled with
// PatchRouteLabels if it already points where the file says (it is never
// unrouted), or replaced if it has to be retargeted (there is no UpdateRoute);
// anything else is somebody else's and a conflict.
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
		add := func() {
			p.add(d.Function, phRouteAdd, "+", fmt.Sprintf("route %s%s", d.Host, d.Path), func(x *Exec) any {
				return &controlv1.CreateRouteRequest{HostPattern: d.Host, PathPrefix: d.Path, FunctionId: x.fid(d.Function), Labels: ownerLabels(pl.r.Owner)}
			})
		}
		del := func(rt *controlv1.Route, why string) {
			rid := rt.Id
			it := p.add(d.Function, phRouteDel, "-", fmt.Sprintf("route %s%s (%s)", d.Host, d.Path, why), func(x *Exec) any {
				return &controlv1.DeleteRouteRequest{RouteId: rid}
			})
			it.IgnoreNotFound = true
		}
		if a := owned[key]; a != nil {
			if target != nil && a.FunctionId == target.Id {
				continue // current
			}
			// There is no UpdateRoute: retargeting is delete then create, and in
			// between the route does not exist.
			del(a, "pointed at "+pl.fnName(ctx, a.FunctionId)+"; retargeting")
			add()
			continue
		}
		// Not one of ours. Is the key taken by a route nobody owns?
		if err := pl.loadUnowned(ctx); err != nil {
			p.problem(d.Function, "%v", err)
			continue
		}
		a := pl.unowned[key]
		switch {
		case a == nil:
			add() // free, or held by another owner's route, which the validation reports
		case pl.ownsFunction(a.FunctionId) && target != nil && a.FunctionId == target.Id:
			// Made by hand, onto the function the file wants: it already routes as the
			// file says, so it only needs our label. Nothing is deleted.
			rid := a.Id
			p.add(d.Function, phRouteAdd, "~", fmt.Sprintf("route %s%s (made by hand, already points at %s: labelling it as ours, not recreating it)", d.Host, d.Path, d.Function), func(x *Exec) any {
				return &controlv1.PatchRouteLabelsRequest{RouteId: rid, Set: ownerLabels(pl.r.Owner)}
			})
		case pl.ownsFunction(a.FunctionId):
			// Made by hand, onto another function of ours, at a key the file declares:
			// the file wins. A route cannot be retargeted, so it is replaced by a
			// labelled one (the key is unrouted for an instant).
			del(a, "it has no owner label and points at "+pl.fnName(ctx, a.FunctionId)+", not "+d.Function+"; replacing it with a labelled one")
			add()
		default:
			p.problem(d.Function, "route %s%s is taken by a route to function %q that this service does not own (it has no %s label)", d.Host, d.Path, pl.fnName(ctx, a.FunctionId), labelOwner)
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
	for name, rts := range pl.adoptedRoutes {
		for _, rt := range rts {
			if rt.Labels[labelOwner] != "" || desired[routeKey(rt.HostPattern, rt.PathPrefix)] {
				continue
			}
			pl.deleteStray(name, rt, "unlabelled route of an adopted function, not in the file")
		}
	}
}

func (pl *planner) deleteStray(fn string, rt *controlv1.Route, why string) {
	rid := rt.Id
	it := pl.plan.add(fn, phRouteDel, "-", fmt.Sprintf("route %s%s (%s)", rt.HostPattern, rt.PathPrefix, why), func(x *Exec) any {
		return &controlv1.DeleteRouteRequest{RouteId: rid}
	})
	it.IgnoreNotFound = true
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
		it := pl.plan.add(n, phPrune, "-", "delete function (created by this service, no longer in the file; removes its versions, routes and env)", func(x *Exec) any {
			return &controlv1.DeleteFunctionRequest{FunctionId: id}
		})
		it.IgnoreNotFound = true
	}
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
