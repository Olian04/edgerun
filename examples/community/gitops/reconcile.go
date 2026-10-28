package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// Reconciler brings an orchestrator to the state a Spec describes. It is built
// in two steps, like terraform: Plan only reads, Apply executes the plan. Each
// run starts from a fresh read of the orchestrator, so a run that failed half
// way needs no recovery logic: the next plan is simply smaller.
type Reconciler struct {
	Ctl        *edgerun.Control
	Owner      string // identifies the functions this instance created
	Prune      bool   // delete functions this instance created that the file no longer declares
	Adopt      bool   // take over functions that exist but carry no ownership claim
	AllowEmpty bool   // permit Prune with a file that declares no functions
	FPKey      []byte // key for secret fingerprints
	Getenv     func(string) (string, bool)
	Timeout    time.Duration // per RPC group (see Item.Run)
}

// Module is a function's code as read from the repository.
type Module struct {
	Data   []byte
	Digest string // "sha256:<hex>", the form FunctionVersion.artifact_digest uses
}

func NewModule(data []byte) *Module {
	sum := sha256.Sum256(data)
	return &Module{Data: data, Digest: "sha256:" + hex.EncodeToString(sum[:])}
}

// Phases order the items of a plan across all functions.
const (
	phCreate = iota
	phEnvSet
	phVersion
	phEnvDel
	phMarker
	phEgress
	phRouteDel
	phRouteAdd
	phPrune
)

type Item struct {
	Fn    string // function this belongs to; a failure skips the rest of its items
	Phase int
	Sign  string // "+" create, "~" change, "-" delete
	Text  string
	Run   func(ctx context.Context, x *Exec) error

	Err     error
	Done    bool
	Skipped bool
}

// Problem is a reason a function (or route) could not be planned: a conflict
// with something this service does not own, an unreadable module, a missing
// secret. It is reported and makes the run fail, but does not stop other
// functions from being reconciled.
type Problem struct {
	Fn  string
	Err error
}

type Plan struct {
	ids      map[string]string // function name -> id, of the functions that exist now
	Items    []*Item
	Problems []Problem
	Notes    []string
}

func (p *Plan) Empty() bool { return len(p.Items) == 0 }

func (p *Plan) add(fn string, phase int, sign, text string, run func(context.Context, *Exec) error) {
	p.Items = append(p.Items, &Item{Fn: fn, Phase: phase, Sign: sign, Text: text, Run: run})
}

func (p *Plan) problem(fn string, format string, a ...any) {
	p.Problems = append(p.Problems, Problem{Fn: fn, Err: fmt.Errorf(format, a...)})
}

// Print writes the plan in diff style.
func (p *Plan) Print(w io.Writer) {
	for _, it := range p.Items {
		fmt.Fprintf(w, "%s %s: %s\n", it.Sign, label(it.Fn), it.Text)
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

	byName  map[string]*controlv1.Function
	byID    map[string]*controlv1.Function
	routes  []*controlv1.Route
	envs    map[string]*envInfo // function id -> env, read lazily
	managed map[string]bool     // desired functions this instance owns (or is about to)
	maxMod  uint64
}

type envInfo struct {
	entries map[string]*controlv1.EnvEntry
	marker  marker
	hasMark bool // a well-formed marker exists
}

type ownership int

const (
	unowned ownership = iota
	ours
	foreign
)

func (e *envInfo) ownership(owner string) ownership {
	switch {
	case !e.hasMark:
		return unowned
	case e.marker.Owner == owner:
		return ours
	}
	return foreign
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
	if m := e.entries[markerEnvVar]; m != nil && !m.Secret {
		e.marker, e.hasMark = parseMarker(m.Value)
	}
	pl.envs[fn.Id] = e
	return e, nil
}

// Plan reads the orchestrator and computes what must change. mods holds the
// module of every function that could be read; modErr the reason for those that
// could not. It returns an error only when the orchestrator cannot be read.
func (r *Reconciler) Plan(ctx context.Context, spec *Spec, mods map[string]*Module, modErr map[string]error) (*Plan, error) {
	if r.Prune && len(spec.Functions) == 0 && !r.AllowEmpty {
		return nil, errors.New("refusing to prune: the file declares no functions (pass --allow-empty if that is intended)")
	}
	pl := &planner{r: r, plan: &Plan{ids: map[string]string{}}, spec: spec, mods: mods, modErr: modErr,
		byName: map[string]*controlv1.Function{}, byID: map[string]*controlv1.Function{},
		envs: map[string]*envInfo{}, managed: map[string]bool{}}

	// Describe tells us the module size limit: a dry run can check it.
	{
		cctx, cancel := context.WithTimeout(ctx, r.Timeout)
		d, err := r.Ctl.Describe(cctx, &controlv1.DescribeRequest{})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("describe orchestrator: %w", err)
		}
		pl.maxMod = d.MaxModuleBytes
	}

	// There is no GetFunctionByName: the only way to find a function by name is to
	// read all of them.
	cctx, cancel := context.WithTimeout(ctx, 2*r.Timeout)
	defer cancel()
	for fn, err := range r.Ctl.AllFunctions(cctx) {
		if err != nil {
			return nil, fmt.Errorf("list functions: %w", err)
		}
		pl.byName[fn.Name] = fn
		pl.byID[fn.Id] = fn
		pl.plan.ids[fn.Name] = fn.Id
	}
	for rt, err := range r.Ctl.AllRoutes(cctx) {
		if err != nil {
			return nil, fmt.Errorf("list routes: %w", err)
		}
		pl.routes = append(pl.routes, rt)
	}

	for _, d := range spec.Functions {
		pl.function(ctx, d)
	}
	pl.planRoutes(ctx)
	if r.Prune {
		pl.planPrune(ctx)
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
	if pl.maxMod > 0 && uint64(len(mod.Data)) > pl.maxMod {
		p.problem(d.Name, "module is %d bytes, the orchestrator accepts at most %d", len(mod.Data), pl.maxMod)
		return
	}
	secrets := map[string]string{}
	fps := map[string]string{}
	for name, ref := range d.SecretEnv {
		v, ok := r.Getenv(ref)
		if !ok {
			p.problem(d.Name, "secret_env %s: environment variable %s is not set in this process", name, ref)
			return
		}
		if len(r.FPKey) == 0 {
			p.problem(d.Name, "secret_env needs GITOPS_FINGERPRINT_KEY (secrets are write-only, so changes are detected by a keyed fingerprint)")
			return
		}
		secrets[name] = v
		fps[name] = fingerprint(r.FPKey, r.Owner, d.Name, name, v)
	}
	wantMarker := marker{Owner: r.Owner, Secrets: fps}
	if len(fps) == 0 {
		wantMarker.Secrets = nil
	}

	fn := pl.byName[d.Name]
	var env *envInfo
	var versions []*controlv1.FunctionVersion
	if fn == nil {
		env = &envInfo{entries: map[string]*controlv1.EnvEntry{}}
		p.add(d.Name, phCreate, "+", "create function", func(ctx context.Context, x *Exec) error {
			res, err := x.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: d.Name})
			if err != nil {
				return err
			}
			x.ids[d.Name] = res.Function.Id
			// Claim it. If this fails or we crash right here, the function exists
			// without a claim: the next run recognises it as unfinished (see below).
			return x.setEnv(ctx, d.Name, markerEnvVar, marker{Owner: r.Owner}.encode(), false)
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
		switch env.ownership(r.Owner) {
		case ours:
			pl.managed[d.Name] = true
		case foreign:
			p.problem(d.Name, "exists and belongs to another owner (%q); not touching it", env.marker.Owner)
			return
		case unowned:
			// A previous run may have died between CreateFunction and the claim.
			// A function with nothing in it at all is indistinguishable from that and
			// is safe to take; anything else is somebody's.
			pristine := len(versions) == 0 && len(env.entries) == 0 && len(fn.EgressAllow) == 0 && !pl.hasRoutes(fn.Id)
			switch {
			case pristine:
				p.add(d.Name, phCreate, "~", "claim existing empty function (an earlier run did not finish creating it)", func(ctx context.Context, x *Exec) error {
					return x.setEnv(ctx, d.Name, markerEnvVar, marker{Owner: r.Owner}.encode(), false)
				})
			case r.Adopt:
				p.add(d.Name, phCreate, "~", fmt.Sprintf("ADOPT existing function it did not create (%d version(s), %d env var(s)); anything not in the file will be removed", len(versions), len(env.entries)), func(ctx context.Context, x *Exec) error {
					return x.setEnv(ctx, d.Name, markerEnvVar, marker{Owner: r.Owner}.encode(), false)
				})
			default:
				p.problem(d.Name, "exists but was not created by this service (owner %q); pass --adopt to take it over", r.Owner)
				return
			}
			pl.managed[d.Name] = true
		}
	}

	// Environment.
	var names []string
	for n := range d.Env {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		a := env.entries[n]
		switch {
		case a == nil:
			p.add(d.Name, phEnvSet, "+", fmt.Sprintf("env %s=%s", n, show(d.Env[n])), pl.setEnvRun(d.Name, n, d.Env[n], false))
		case a.Secret:
			p.add(d.Name, phEnvSet, "~", fmt.Sprintf("env %s: was a secret, now plain %s", n, show(d.Env[n])), pl.setEnvRun(d.Name, n, d.Env[n], false))
		case a.Value != d.Env[n]:
			p.add(d.Name, phEnvSet, "~", fmt.Sprintf("env %s: %s -> %s", n, show(a.Value), show(d.Env[n])), pl.setEnvRun(d.Name, n, d.Env[n], false))
		}
	}
	names = names[:0]
	for n := range secrets {
		names = append(names, n)
	}
	sort.Strings(names)
	var old map[string]string
	if env.hasMark && env.marker.Owner == r.Owner {
		old = env.marker.Secrets
	}
	for _, n := range names {
		a := env.entries[n]
		switch {
		case a == nil:
			p.add(d.Name, phEnvSet, "+", fmt.Sprintf("secret %s (value not shown)", n), pl.setEnvRun(d.Name, n, secrets[n], true))
		case !a.Secret:
			p.add(d.Name, phEnvSet, "~", fmt.Sprintf("env %s: was plain, now secret", n), pl.setEnvRun(d.Name, n, secrets[n], true))
		case old[n] != fps[n]:
			p.add(d.Name, phEnvSet, "~", fmt.Sprintf("secret %s changed (fingerprint differs; value not shown)", n), pl.setEnvRun(d.Name, n, secrets[n], true))
		}
	}
	var stale []string
	for n := range env.entries {
		if n == markerEnvVar {
			continue
		}
		if _, p1 := d.Env[n]; p1 {
			continue
		}
		if _, p2 := secrets[n]; p2 {
			continue
		}
		stale = append(stale, n)
	}
	sort.Strings(stale)
	for _, n := range stale {
		n := n
		p.add(d.Name, phEnvDel, "-", "env "+n+" (not in the file)", func(ctx context.Context, x *Exec) error {
			_, err := x.ctl.DeleteEnv(ctx, &controlv1.DeleteEnvRequest{FunctionId: x.ids[d.Name], Name: n})
			if connect.CodeOf(err) == connect.CodeNotFound {
				return nil // already gone: the goal state
			}
			return err
		})
	}
	// Written last so that it is only ever ahead of reality by "nothing": a crash
	// before it re-sets the secrets, which is harmless.
	cur := marker{Owner: r.Owner}.encode() // what the create/claim step leaves behind
	if env.hasMark && env.marker.Owner == r.Owner {
		cur = env.entries[markerEnvVar].Value
	}
	if cur != wantMarker.encode() {
		p.add(d.Name, phMarker, "~", "update ownership/fingerprint record "+markerEnvVar, func(ctx context.Context, x *Exec) error {
			return x.setEnv(ctx, d.Name, markerEnvVar, wantMarker.encode(), false)
		})
	}

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
			_, err := x.ctl.ActivateVersion(ctx, &controlv1.ActivateVersionRequest{FunctionId: x.ids[d.Name], VersionId: match.Id})
			return err
		})
	default:
		p.add(d.Name, phVersion, "+", fmt.Sprintf("upload new version (%s, %d bytes, abi %s) and activate", short(mod.Digest), len(mod.Data), d.ABI), func(ctx context.Context, x *Exec) error {
			// Activate is part of the same RPC. If it is lost on the way back, the next
			// run finds this version by digest and only activates it.
			_, err := x.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{
				FunctionId: x.ids[d.Name], Module: mod.Data, Abi: d.ABI, Limits: want, Activate: true})
			return err
		})
	}

	// Egress.
	var have []string
	if fn != nil {
		have = fn.EgressAllow
	}
	if !slices.Equal(have, d.Egress) && !(len(have) == 0 && len(d.Egress) == 0) {
		p.add(d.Name, phEgress, "~", fmt.Sprintf("egress %v -> %v", have, d.Egress), func(ctx context.Context, x *Exec) error {
			_, err := x.ctl.SetEgress(ctx, &controlv1.SetEgressRequest{FunctionId: x.ids[d.Name], Allow: d.Egress})
			return err
		})
	}
}

func (pl *planner) setEnvRun(fn, name, value string, secret bool) func(context.Context, *Exec) error {
	return func(ctx context.Context, x *Exec) error { return x.setEnv(ctx, fn, name, value, secret) }
}

func (pl *planner) hasRoutes(fnID string) bool {
	for _, rt := range pl.routes {
		if rt.FunctionId == fnID {
			return true
		}
	}
	return false
}

// planRoutes. A route has no owner and no name: its identity is the pair
// (host_pattern, path_prefix) and the only thing that ties it to this service is
// the function it points at. So: routes of functions this service owns are
// ours; routes of anything else are never touched.
func (pl *planner) planRoutes(ctx context.Context) {
	p := pl.plan
	actual := map[string]*controlv1.Route{}
	for _, rt := range pl.routes {
		actual[routeKey(rt.HostPattern, rt.PathPrefix)] = rt
	}
	desired := map[string]bool{}
	for _, d := range pl.spec.Routes {
		d := d
		key := routeKey(d.Host, d.Path)
		desired[key] = true
		if !pl.managed[d.Function] {
			continue // the function is in conflict or failed to plan: leave its routes alone
		}
		target := pl.byName[d.Function] // nil when this run creates it
		a := actual[key]
		add := func() {
			p.add(d.Function, phRouteAdd, "+", fmt.Sprintf("route %s%s", d.Host, d.Path), func(ctx context.Context, x *Exec) error {
				_, err := x.ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{HostPattern: d.Host, PathPrefix: d.Path, FunctionId: x.ids[d.Function]})
				return err
			})
		}
		switch {
		case a == nil:
			add()
		case target != nil && a.FunctionId == target.Id:
			// current
		default:
			// Taken by another function. There is no UpdateRoute: retargeting is delete
			// then create, and in between the route does not exist.
			other := pl.byID[a.FunctionId]
			own := false
			if other != nil {
				if e, err := pl.env(ctx, other); err == nil && e.ownership(pl.r.Owner) == ours {
					own = true
				}
			}
			if !own {
				p.problem(d.Function, "route %s%s is taken by function %q, which this service does not own", d.Host, d.Path, nameOf(other))
				continue
			}
			rid := a.Id
			p.add(d.Function, phRouteDel, "-", fmt.Sprintf("route %s%s (pointed at %s; retargeting)", d.Host, d.Path, other.Name), func(ctx context.Context, x *Exec) error {
				return x.deleteRoute(ctx, rid)
			})
			add()
		}
	}
	for _, rt := range pl.routes {
		if desired[routeKey(rt.HostPattern, rt.PathPrefix)] {
			continue
		}
		fn := pl.byID[rt.FunctionId]
		if fn == nil || !pl.managed[fn.Name] {
			continue
		}
		rid := rt.Id
		p.add(fn.Name, phRouteDel, "-", fmt.Sprintf("route %s%s (not in the file)", rt.HostPattern, rt.PathPrefix), func(ctx context.Context, x *Exec) error {
			return x.deleteRoute(ctx, rid)
		})
	}
}

func (pl *planner) planPrune(ctx context.Context) {
	declared := map[string]bool{}
	for _, d := range pl.spec.Functions {
		declared[d.Name] = true
	}
	var names []string
	for n := range pl.byName {
		if !declared[n] {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	for _, n := range names {
		fn := pl.byName[n]
		// Ownership is an environment variable, so deciding this costs one ListEnv per
		// function of the whole deployment.
		e, err := pl.env(ctx, fn)
		if err != nil {
			pl.plan.problem(n, "%v", err)
			continue
		}
		if e.ownership(pl.r.Owner) != ours {
			continue
		}
		id := fn.Id
		pl.plan.add(n, phPrune, "-", "delete function (created by this service, no longer in the file; removes its versions, routes and env)", func(ctx context.Context, x *Exec) error {
			_, err := x.ctl.DeleteFunction(ctx, &controlv1.DeleteFunctionRequest{FunctionId: id})
			if connect.CodeOf(err) == connect.CodeNotFound {
				return nil
			}
			return err
		})
	}
}

// Exec is the state of one Apply.
type Exec struct {
	ctl *edgerun.Control
	ids map[string]string // function name -> id (filled in as functions are created)
}

func (x *Exec) setEnv(ctx context.Context, fn, name, value string, secret bool) error {
	_, err := x.ctl.SetEnv(ctx, &controlv1.SetEnvRequest{FunctionId: x.ids[fn], Entry: &controlv1.EnvEntry{Name: name, Value: value, Secret: secret}})
	return err
}

func (x *Exec) deleteRoute(ctx context.Context, id string) error {
	_, err := x.ctl.DeleteRoute(ctx, &controlv1.DeleteRouteRequest{RouteId: id})
	if connect.CodeOf(err) == connect.CodeNotFound {
		return nil
	}
	return err
}

// Apply executes the plan in phase order. A failed item skips the remaining
// items of the same function (never apply a route or a deletion on top of a
// deploy that did not happen) and the run continues with the others.
func (p *Plan) Apply(ctx context.Context, r *Reconciler, w io.Writer) error {
	x := &Exec{ctl: r.Ctl, ids: maps.Clone(p.ids)}
	failed := map[string]bool{}
	var errs []error
	for _, pr := range p.Problems {
		errs = append(errs, fmt.Errorf("%s: %w", label(pr.Fn), pr.Err))
	}
	for _, it := range p.Items {
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

func nameOf(f *controlv1.Function) string {
	if f == nil {
		return "?"
	}
	return f.Name
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
