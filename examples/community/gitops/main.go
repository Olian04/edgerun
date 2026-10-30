// Command edgerun-gitops reconciles an edgerun orchestrator toward a YAML file
// in a git repository.
//
//	edgerun-gitops --repo https://github.com/acme/edge.git --orchestrator http://orch:8080 --once
//
// The admin token is read from EDGERUN_ADMIN_TOKEN (not a flag: argv is
// world-readable). Secret values come from the process environment, named by
// secret_env in the YAML.
//
// Everything the service creates is labelled gitops.example.com/owner=<--owner>
// (functions, versions, routes); that label is how it finds what is its own.
// --dry-run sends the plan the apply would run to the orchestrator as one
// PlanService.Validate (each step seeing the ones before it) and changes nothing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Olian04/edgerun"
)

type Config struct {
	Repo, Branch, File, WorkDir string
	Orchestrator, AdminToken    string
	Owner                       string
	Prune, Adopt, AllowEmpty    bool
	Once, DryRun, ExitCode      bool
	Interval, RPCTimeout        time.Duration
	Getenv                      func(string) (string, bool)
	ClientOptions               []edgerun.Option // for the control client: a proxy, TLS, an interceptor
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	var c Config
	fs := flag.NewFlagSet("edgerun-gitops", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&c.Repo, "repo", "", "git repository URL (https://..., file://...)")
	fs.StringVar(&c.Branch, "branch", "main", "branch to follow")
	fs.StringVar(&c.File, "file", "edgerun.yaml", "path of the YAML file inside the repository")
	fs.StringVar(&c.WorkDir, "workdir", "", "where the bare clone lives (default: a cache directory)")
	fs.StringVar(&c.Orchestrator, "orchestrator", os.Getenv("EDGERUN_URL"), "orchestrator base URL (default $EDGERUN_URL)")
	fs.StringVar(&c.Owner, "owner", "gitops", "identity of this instance, the value of the gitops.example.com/owner label; it only manages (and prunes) what carries it")
	fs.BoolVar(&c.Prune, "prune", false, "delete functions this instance created that the file no longer declares")
	fs.BoolVar(&c.Adopt, "adopt", false, "take over functions that already exist without an owner label (labels them)")
	fs.BoolVar(&c.AllowEmpty, "allow-empty", false, "allow --prune with a file that declares no functions")
	fs.BoolVar(&c.Once, "once", false, "reconcile once and exit")
	fs.BoolVar(&c.DryRun, "dry-run", false, "validate every change with the orchestrator, print what would change, change nothing, exit")
	fs.BoolVar(&c.ExitCode, "detailed-exitcode", false, "with --dry-run: exit 2 if there is a difference")
	fs.DurationVar(&c.Interval, "interval", 30*time.Second, "poll interval")
	fs.DurationVar(&c.RPCTimeout, "rpc-timeout", 30*time.Second, "deadline of each step against the orchestrator")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	c.AdminToken = os.Getenv("EDGERUN_ADMIN_TOKEN")
	c.Getenv = os.LookupEnv
	if c.Repo == "" || c.Orchestrator == "" || c.AdminToken == "" {
		fmt.Fprintln(stderr, "need --repo, --orchestrator (or $EDGERUN_URL) and $EDGERUN_ADMIN_TOKEN")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return Main(ctx, &c, stdout, stderr)
}

// Main runs the mode the config asks for and returns the exit status.
func Main(ctx context.Context, c *Config, stdout, stderr io.Writer) int {
	g := New(c, stdout)
	switch {
	case c.DryRun || c.Once:
		res, err := g.Reconcile(ctx)
		if err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return 1
		}
		if c.DryRun && c.ExitCode && !res.Clean {
			return 2
		}
		return 0
	default:
		fmt.Fprintf(stdout, "polling %s (%s) every %s\n", c.Repo, c.Branch, c.Interval)
		if err := g.Loop(ctx); err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintln(stderr, "error:", err)
			return 1
		}
		return 0
	}
}

type GitOps struct {
	c   *Config
	out io.Writer
	src *Source
	rec *Reconciler

	lastApplied string // commit of the last pass that left nothing to do or applied cleanly
}

func New(c *Config, out io.Writer) *GitOps {
	dir := c.WorkDir
	if dir == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			cache = os.TempDir()
		}
		dir = filepath.Join(cache, "edgerun-gitops", "repo")
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = 30 * time.Second
	}
	if c.Getenv == nil {
		c.Getenv = os.LookupEnv
	}
	return &GitOps{
		c: c, out: out,
		src: &Source{URL: c.Repo, Branch: c.Branch, Dir: dir},
		rec: &Reconciler{
			Ctl:   edgerun.NewControl(c.Orchestrator, c.AdminToken, c.ClientOptions...),
			Owner: c.Owner, Prune: c.Prune, Adopt: c.Adopt, AllowEmpty: c.AllowEmpty,
			Getenv: c.Getenv, Timeout: c.RPCTimeout,
		},
	}
}

// Result is what one pass found and did.
type Result struct {
	Rev     string
	Plan    *Plan
	Clean   bool // nothing differed
	Applied int
}

// Reconcile is one pass: sync the repository, read the YAML and the modules at
// that commit, plan, and (unless --dry-run) apply.
func (g *GitOps) Reconcile(ctx context.Context) (*Result, error) {
	snap, err := g.src.Sync(ctx)
	if err != nil {
		return nil, fmt.Errorf("sync repository: %w", err)
	}
	raw, err := snap.ReadFile(ctx, g.c.File)
	if err != nil {
		return nil, fmt.Errorf("read %s at %s: %w", g.c.File, snap.Rev[:10], err)
	}
	spec, err := ParseSpec(raw)
	if err != nil {
		return nil, fmt.Errorf("%s at %s: %w", g.c.File, snap.Rev[:10], err)
	}
	mods := map[string]*Module{}
	modErr := map[string]error{}
	for _, f := range spec.Functions {
		data, err := snap.ReadFile(ctx, f.Module)
		if err != nil {
			modErr[f.Name] = err
			continue
		}
		mods[f.Name] = NewModule(data)
	}
	plan, err := g.rec.Plan(ctx, spec, mods, modErr, snap.Rev)
	if err != nil {
		return nil, err
	}
	// The same check for a dry run and for an apply: the whole plan goes to the
	// orchestrator as one sequence (PlanService.Validate) before anything is changed.
	plan.Validate(ctx, g.rec)
	res := &Result{Rev: snap.Rev, Plan: plan, Clean: plan.Empty() && len(plan.Problems) == 0}

	kind := "difference"
	if g.lastApplied == snap.Rev && !plan.Empty() {
		kind = "DRIFT (same commit as the last pass, so the orchestrator or a secret in the process environment changed)"
	}
	fmt.Fprintf(g.out, "commit %s: ", snap.Rev[:10])
	switch {
	case res.Clean:
		fmt.Fprintln(g.out, "in sync, nothing to do")
	case g.c.DryRun:
		fmt.Fprintf(g.out, "%d %s(s), %d problem(s); dry run, nothing changed\n", len(plan.Items), kind, len(plan.Problems))
		plan.Print(g.out)
		if len(plan.Problems) > 0 {
			// The apply would fail the same way.
			return res, fmt.Errorf("dry run: %d problem(s); an apply would not succeed", len(plan.Problems))
		}
	default:
		fmt.Fprintf(g.out, "%d %s(s), %d problem(s); applying\n", len(plan.Items), kind, len(plan.Problems))
		for _, n := range plan.Notes {
			fmt.Fprintf(g.out, "? %s\n", n)
		}
		if err := plan.Apply(ctx, g.rec, g.out); err != nil {
			return res, err
		}
		for _, it := range plan.Items {
			if it.Done {
				res.Applied++
			}
		}
		g.lastApplied = snap.Rev
	}
	if res.Clean {
		g.lastApplied = snap.Rev
	}
	return res, nil
}

// Loop reconciles every Interval until ctx ends. A failed pass is logged and
// retried at the next tick: the plan is recomputed from what the orchestrator
// holds, so nothing is lost by failing.
func (g *GitOps) Loop(ctx context.Context) error {
	t := time.NewTicker(g.c.Interval)
	defer t.Stop()
	for {
		if _, err := g.Reconcile(ctx); err != nil && ctx.Err() == nil {
			fmt.Fprintf(g.out, "pass failed: %v\n", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
