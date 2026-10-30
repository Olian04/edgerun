package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// maxPlanSteps is how many steps one PlanService.Validate takes. It is a number
// of the protocol (proto comment, protocol.md), not something the server reports.
const maxPlanSteps = 128

// planSlack is what a Validate request may carry beyond the largest module the
// orchestrator takes: the server bounds a Validate as it bounds an upload (the
// limit of one module, plus 1 MiB), so a plan of several modules has to be cut.
const planSlack = 1 << 20

// Exec is the state of one Validate or Apply.
type Exec struct {
	ctl   *edgerun.Control
	ids   map[string]string // function name -> id (filled in as functions are created)
	binds map[string]string // function name -> bind name, for the functions the plan creates
	vo    bool              // a validation: ids of what the plan creates are references
	revs  map[string]map[string]string
}

func newExec(ctl *edgerun.Control, ids, binds map[string]string, vo bool) *Exec {
	return &Exec{ctl: ctl, ids: maps.Clone(ids), binds: binds, vo: vo, revs: map[string]map[string]string{}}
}

// fid is the id of a function for a request. In a validation, a function the plan
// creates has no id (nothing a validation creates exists), so the request refers to
// the step that creates it: "$name", which the orchestrator resolves.
func (x *Exec) fid(fn string) string {
	if id := x.ids[fn]; id != "" {
		return id
	}
	if x.vo {
		return "$" + x.binds[fn]
	}
	return ""
}

// revision is the revision to record for a secret: the one its write returned,
// or, for a secret that stays as it is, the one it has. A validation does not feed
// one step's answer to the next (only ids are carried over), so it records a
// stand-in of the right form.
func (x *Exec) revision(fn, name, current string) string {
	if r := x.revs[fn][name]; r != "" {
		return r
	}
	if current != "" {
		return current
	}
	return "pending-" + name
}

// step wraps a request as a step of a plan.
func step(req any, bind string) (*controlv1.Step, error) {
	st := &controlv1.Step{Bind: bind}
	switch q := req.(type) {
	case *controlv1.CreateFunctionRequest:
		st.Request = &controlv1.Step_CreateFunction{CreateFunction: q}
	case *controlv1.DeleteFunctionRequest:
		st.Request = &controlv1.Step_DeleteFunction{DeleteFunction: q}
	case *controlv1.CreateVersionRequest:
		st.Request = &controlv1.Step_CreateVersion{CreateVersion: q}
	case *controlv1.ActivateVersionRequest:
		st.Request = &controlv1.Step_ActivateVersion{ActivateVersion: q}
	case *controlv1.SetEnvRequest:
		st.Request = &controlv1.Step_SetEnv{SetEnv: q}
	case *controlv1.DeleteEnvRequest:
		st.Request = &controlv1.Step_DeleteEnv{DeleteEnv: q}
	case *controlv1.SetEgressRequest:
		st.Request = &controlv1.Step_SetEgress{SetEgress: q}
	case *controlv1.PatchLabelsRequest:
		st.Request = &controlv1.Step_PatchLabels{PatchLabels: q}
	case *controlv1.CreateRouteRequest:
		st.Request = &controlv1.Step_CreateRoute{CreateRoute: q}
	case *controlv1.DeleteRouteRequest:
		st.Request = &controlv1.Step_DeleteRoute{DeleteRoute: q}
	case *controlv1.PatchRouteLabelsRequest:
		st.Request = &controlv1.Step_PatchRouteLabels{PatchRouteLabels: q}
	default:
		return nil, fmt.Errorf("no step for %T", req)
	}
	return st, nil
}

// send makes the call a request stands for.
func (x *Exec) send(ctx context.Context, it *Item, req any) error {
	switch q := req.(type) {
	case *controlv1.CreateFunctionRequest:
		res, err := x.ctl.CreateFunction(ctx, q)
		if err == nil {
			x.ids[it.Fn] = res.Function.Id
		}
		return err
	case *controlv1.DeleteFunctionRequest:
		_, err := x.ctl.DeleteFunction(ctx, q)
		return err
	case *controlv1.CreateVersionRequest:
		_, err := x.ctl.CreateVersion(ctx, q)
		return err
	case *controlv1.ActivateVersionRequest:
		_, err := x.ctl.ActivateVersion(ctx, q)
		return err
	case *controlv1.SetEnvRequest:
		res, err := x.ctl.SetEnv(ctx, q)
		if err == nil && q.Entry.Secret && res.GetEntry().GetRevision() != "" {
			if x.revs[it.Fn] == nil {
				x.revs[it.Fn] = map[string]string{}
			}
			x.revs[it.Fn][q.Entry.Name] = res.Entry.Revision
		}
		return err
	case *controlv1.DeleteEnvRequest:
		_, err := x.ctl.DeleteEnv(ctx, q)
		return err
	case *controlv1.SetEgressRequest:
		_, err := x.ctl.SetEgress(ctx, q)
		return err
	case *controlv1.PatchLabelsRequest:
		_, err := x.ctl.PatchLabels(ctx, q)
		return err
	case *controlv1.CreateRouteRequest:
		_, err := x.ctl.CreateRoute(ctx, q)
		return err
	case *controlv1.DeleteRouteRequest:
		_, err := x.ctl.DeleteRoute(ctx, q)
		return err
	case *controlv1.PatchRouteLabelsRequest:
		_, err := x.ctl.PatchRouteLabels(ctx, q)
		return err
	}
	return fmt.Errorf("no call for %T", req)
}

// chunks splits the functions of the plan into groups that are validated by one
// Validate each: at most 128 steps, and not more bytes of modules than the
// orchestrator takes in one request. Every item of a function lands in the same
// group, in plan order. That is sound because nothing in a plan depends on another
// function: a route is created for its own function, a retarget deletes and
// creates one function's route, the 64-variable limit and the label limit are per
// function. (Two Validates are each judged against the stored state and cannot see
// each other, so a dependence across the cut would be invisible.)
func (p *Plan) chunks(ctx context.Context, r *Reconciler) [][]string {
	var order []string
	steps, bytes := map[string]int{}, map[string]int{}
	total := 0
	for _, it := range p.Items {
		if _, seen := steps[it.Fn]; !seen {
			order = append(order, it.Fn)
		}
		steps[it.Fn]++
		bytes[it.Fn] += it.Bytes
		total += it.Bytes
	}
	limit := int(^uint(0) >> 1)
	if total > planSlack {
		// Only a plan with modules in it needs to know what the server accepts.
		cctx, cancel := context.WithTimeout(ctx, r.Timeout)
		defer cancel()
		if d, err := r.Ctl.Describe(cctx, &controlv1.DescribeRequest{}); err == nil {
			limit = int(d.MaxModuleBytes) + planSlack/2
		}
	}
	var out [][]string
	var cur []string
	cs, cb := 0, 0
	for _, fn := range order {
		if len(cur) > 0 && (cs+steps[fn] > maxPlanSteps || cb+bytes[fn] > limit) {
			out = append(out, cur)
			cur, cs, cb = nil, 0, 0
		}
		cur = append(cur, fn)
		cs += steps[fn]
		cb += bytes[fn]
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

// Validate asks the orchestrator whether it would accept the plan: one
// PlanService.Validate of the steps, in the order the apply will run them, each
// seeing what the ones before it did (a route deleted and created again, variables
// deleted to make room for new ones, a route for a function the plan creates). If a
// step fails, the response says which; the function it belongs to is dropped from
// the apply as a whole, so that a rule the orchestrator enforces never leaves a
// function half deployed, and the rest is validated again without it.
//
// A plan with more than 128 steps for one function, or more modules than fit one
// request, is cut at function boundaries (see chunks); a function with more than 128
// steps of its own has its first 128 validated and the rest marked as not validated.
func (p *Plan) Validate(ctx context.Context, r *Reconciler) {
	if len(p.Items) == 0 {
		return
	}
	x := newExec(r.Ctl, p.ids, p.binds, true)
	for _, group := range p.chunks(ctx, r) {
		in := map[string]bool{}
		for _, fn := range group {
			in[fn] = true
		}
		p.validateGroup(ctx, r, x, in)
	}
}

func (p *Plan) validateGroup(ctx context.Context, r *Reconciler, x *Exec, fns map[string]bool) {
	for {
		var items []*Item
		for _, it := range p.Items {
			if fns[it.Fn] && !p.rejected[it.Fn] {
				items = append(items, it)
				it.Blind = ""
			}
		}
		if len(items) == 0 {
			return
		}
		if len(items) > maxPlanSteps {
			for _, it := range items[maxPlanSteps:] {
				it.Blind = fmt.Sprintf("a function has more steps than the %d one Validate takes", maxPlanSteps)
			}
			items = items[:maxPlanSteps]
		}
		var steps []*controlv1.Step
		for _, it := range items {
			req := it.Req(x)
			bind := ""
			if _, creates := req.(*controlv1.CreateFunctionRequest); creates {
				bind = x.binds[it.Fn]
			}
			st, err := step(req, bind)
			if err != nil {
				p.reject(it, err)
				break
			}
			steps = append(steps, st)
		}
		if len(steps) < len(items) {
			continue // a step that cannot be sent was rejected: go again without its function
		}
		cctx, cancel := context.WithTimeout(ctx, r.Timeout)
		res, err := r.Ctl.Validate(cctx, &controlv1.ValidateRequest{Steps: steps})
		cancel()
		if err != nil {
			// The orchestrator could not judge the plan (not: judged it invalid). What
			// cannot be validated is not applied.
			for _, it := range items {
				if !p.rejected[it.Fn] {
					p.reject(it, err)
				}
			}
			return
		}
		f := res.Failure
		if f == nil {
			return
		}
		if int(f.Step) >= len(items) {
			for _, it := range items {
				p.reject(it, fmt.Errorf("validation failed at step %d: %s: %s", f.Step, f.Code, f.Message))
			}
			return
		}
		p.reject(items[f.Step], fmt.Errorf("%s: %s (step %d of %d)", f.Code, f.Message, f.Step, len(steps)))
	}
}

func (p *Plan) reject(it *Item, err error) {
	it.Rejected = err
	p.rejected[it.Fn] = true
	p.problem(it.Fn, "rejected by the orchestrator (Validate): %s: %v", it.Text, err)
}

// Apply executes the plan in phase order. A failed item skips the remaining
// items of the same function (never apply a route or a deletion on top of a
// deploy that did not happen) and the run continues with the others. A function
// with a step that validation rejected is not touched at all.
func (p *Plan) Apply(ctx context.Context, r *Reconciler, w io.Writer) error {
	x := newExec(r.Ctl, p.ids, p.binds, false)
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
		it.Err = x.send(cctx, it, it.Req(x))
		cancel()
		if it.Err != nil && it.IgnoreNotFound && connect.CodeOf(it.Err) == connect.CodeNotFound {
			it.Err = nil // already gone: the goal state
		}
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
