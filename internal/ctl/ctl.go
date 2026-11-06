// Package ctl is the command-line client of the orchestrator's control.v1
// protocol. cmd/edgerunctl is a thin main around Run, which takes its streams
// and environment as arguments so it can be tested like any other function.
package ctl

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/internal/admin"
	"github.com/Olian04/edgerun/internal/egress/policy"
)

const usage = `edgerunctl manages an edgerun orchestrator through its control protocol.

usage: edgerunctl [--url URL] [--token TOKEN] <command> [arguments]

  functions [--label K=V]...      list functions, only those with every label given
  create NAME [--label K=V]...    create a function
  delete NAME                     delete a function with its versions, routes and environment
  deploy NAME FILE [flags]        upload FILE as a new version      (--abi, --memory-mib, --max-response-kib, --activate)
  versions NAME                   list a function's versions
  activate NAME VERSION           make version number VERSION live (deploy and rollback)
  routes                          list routes
  route HOST PATH NAME            route requests for HOST and path prefix PATH to a function
  unroute ID                      delete a route
  env NAME                        list a function's environment
  set NAME KEY VALUE [--secret]   set an environment variable
  unset NAME KEY                  delete an environment variable
  labels NAME                     list a function's labels
  label NAME K=V...               set labels on a function (others are kept)
  unlabel NAME K...               remove labels from a function
  egress NAME                     list the network destinations a function may reach (none by default)
  allow NAME DEST...              let a function reach DEST: host, *.domain, or address, with :port if not 80/443
  deny NAME DEST...               stop letting a function reach DEST
  networks                        list network services and what each carries
  workers                         list workers
  invocations [--function NAME] [--limit N]

--url defaults to $EDGERUN_URL and --token to $EDGERUN_ADMIN_TOKEN. --dry-run, before the command, makes any change
a validate-only call: every check is made against the real state and nothing is changed.
`

// Run executes args (without the program name) and returns the exit code.
func Run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	global := flag.NewFlagSet("edgerunctl", flag.ContinueOnError)
	global.SetOutput(io.Discard)
	url := global.String("url", getenv("EDGERUN_URL"), "")
	token := global.String("token", getenv("EDGERUN_ADMIN_TOKEN"), "")
	dryRun := global.Bool("dry-run", false, "")
	if err := global.Parse(args); err != nil {
		fmt.Fprint(stderr, usage)
		return 2
	}
	rest := global.Args()
	if len(rest) == 0 || rest[0] == "help" {
		fmt.Fprint(stdout, usage)
		return 0
	}
	if *url == "" || *token == "" {
		fmt.Fprintln(stderr, "edgerunctl: set --url and --token (or EDGERUN_URL and EDGERUN_ADMIN_TOKEN)")
		return 2
	}
	c := &client{ctx: ctx, c: admin.NewControl(http.DefaultClient, *url, *token), out: stdout}
	c.c.DryRun = *dryRun
	err := c.run(rest[0], rest[1:])
	if err == nil && *dryRun && writes[rest[0]] {
		fmt.Fprintln(stdout, "(dry run: validated only, nothing was changed)")
	}
	if err != nil {
		var usageErr usageError
		if errors.As(err, &usageErr) {
			fmt.Fprintf(stderr, "edgerunctl: %s\n\n%s", err, usage)
			return 2
		}
		fmt.Fprintf(stderr, "edgerunctl: %s\n", explain(err))
		return 1
	}
	return 0
}

// writes are the commands that change something.
var writes = map[string]bool{"create": true, "delete": true, "deploy": true, "activate": true, "route": true, "unroute": true,
	"set": true, "unset": true, "allow": true, "deny": true, "label": true, "unlabel": true}

type usageError string

func (e usageError) Error() string { return string(e) }

// explain shows what the orchestrator said, without Connect's framing.
func explain(err error) string {
	var ce *connect.Error
	if errors.As(err, &ce) {
		return ce.Message()
	}
	return err.Error()
}

type client struct {
	ctx context.Context
	c   *admin.Control
	out io.Writer
}

func (c *client) run(cmd string, args []string) error {
	switch cmd {
	case "functions":
		return c.functions(args)
	case "create":
		return c.create(args)
	case "delete":
		return c.delete(args)
	case "deploy":
		return c.deploy(args)
	case "versions":
		return c.versions(args)
	case "activate":
		return c.activate(args)
	case "routes":
		return c.routes()
	case "route":
		return c.route(args)
	case "unroute":
		return c.unroute(args)
	case "env":
		return c.env(args)
	case "set":
		return c.set(args)
	case "unset":
		return c.unset(args)
	case "labels":
		return c.labels(args)
	case "label":
		return c.label(args, true)
	case "unlabel":
		return c.label(args, false)
	case "egress":
		return c.egress(args)
	case "allow":
		return c.allow(args, true)
	case "deny":
		return c.allow(args, false)
	case "networks":
		return c.networks()
	case "workers":
		return c.workers()
	case "invocations":
		return c.invocations(args)
	}
	return usageError("unknown command " + strconv.Quote(cmd))
}

func (c *client) table() *tabwriter.Writer { return tabwriter.NewWriter(c.out, 0, 4, 2, ' ', 0) }

// function resolves a name to the function.
func (c *client) function(name string) (*controlv1.Function, error) {
	fns, err := c.c.Functions(c.ctx)
	if err != nil {
		return nil, err
	}
	for _, f := range fns {
		if f.Name == name {
			return f, nil
		}
	}
	return nil, fmt.Errorf("no function named %q", name)
}

func wantArgs(args []string, n int, form string) error {
	if len(args) != n {
		return usageError("usage: edgerunctl " + form)
	}
	return nil
}

func when(t interface{ AsTime() time.Time }) string {
	return t.AsTime().UTC().Format("2006-01-02 15:04:05")
}

// labelFlag collects repeated --label K=V flags.
type labelFlag map[string]string

func (l labelFlag) String() string { return fmt.Sprint(map[string]string(l)) }
func (l labelFlag) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok || k == "" {
		return fmt.Errorf("want K=V, got %q", v)
	}
	l[k] = val
	return nil
}

func labelText(m map[string]string) string {
	if len(m) == 0 {
		return "-"
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
	return strings.Join(parts, ",")
}

func (c *client) functions(args []string) error {
	fs := flag.NewFlagSet("functions", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	match := labelFlag{}
	fs.Var(match, "label", "")
	rest, err := parseInterspersed(fs, args)
	if err != nil || len(rest) != 0 {
		return usageError("usage: edgerunctl functions [--label K=V]...")
	}
	fns, err := c.c.FunctionsMatching(c.ctx, match)
	if err != nil {
		return err
	}
	numbers := c.c.ActiveVersionNumbers(c.ctx, fns)
	w := c.table()
	fmt.Fprintln(w, "NAME\tACTIVE\tLABELS\tCREATED\tID")
	for _, f := range fns {
		active := "-"
		if n := numbers[f.Id]; n > 0 {
			active = "v" + strconv.FormatInt(n, 10)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", f.Name, active, labelText(f.Labels), when(f.CreateTime), f.Id)
	}
	return w.Flush()
}

func (c *client) labels(args []string) error {
	if err := wantArgs(args, 1, "labels NAME"); err != nil {
		return err
	}
	f, err := c.function(args[0])
	if err != nil {
		return err
	}
	if len(f.Labels) == 0 {
		fmt.Fprintf(c.out, "%s has no labels\n", f.Name)
		return nil
	}
	keys := make([]string, 0, len(f.Labels))
	for k := range f.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(c.out, "%s=%s\n", k, f.Labels[k])
	}
	return nil
}

// label sets labels (K=V) or, with set false, removes them (K).
func (c *client) label(args []string, set bool) error {
	form := "label NAME K=V..."
	if !set {
		form = "unlabel NAME K..."
	}
	if len(args) < 2 {
		return usageError("usage: edgerunctl " + form)
	}
	f, err := c.function(args[0])
	if err != nil {
		return err
	}
	labels := map[string]string{}
	for k, v := range f.Labels {
		labels[k] = v
	}
	for _, a := range args[1:] {
		if set {
			k, v, ok := strings.Cut(a, "=")
			if !ok || k == "" {
				return usageError("usage: edgerunctl " + form)
			}
			labels[k] = v
		} else {
			delete(labels, a)
		}
	}
	// SetLabels replaces the set; sending the edited one is what keeps the others.
	updated, err := c.c.SetLabels(c.ctx, f.Id, labels)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.out, "%s: %s\n", f.Name, labelText(updated.Labels))
	return nil
}

func (c *client) create(args []string) error {
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	labels := labelFlag{}
	fs.Var(labels, "label", "")
	positional, err := parseInterspersed(fs, args)
	if err != nil {
		return usageError(err.Error())
	}
	if err := wantArgs(positional, 1, "create NAME [--label K=V]..."); err != nil {
		return err
	}
	f, err := c.c.CreateFunction(c.ctx, positional[0], labels)
	if err != nil {
		return err
	}
	if c.c.DryRun {
		fmt.Fprintf(c.out, "would create %s\n", f.Name)
		return nil
	}
	fmt.Fprintf(c.out, "created %s (%s)\n", f.Name, f.Id)
	return nil
}

func (c *client) delete(args []string) error {
	if err := wantArgs(args, 1, "delete NAME"); err != nil {
		return err
	}
	f, err := c.function(args[0])
	if err != nil {
		return err
	}
	if err := c.c.DeleteFunction(c.ctx, f.Id); err != nil {
		return err
	}
	fmt.Fprintf(c.out, "deleted %s\n", f.Name)
	return nil
}

func (c *client) deploy(args []string) error {
	fs := flag.NewFlagSet("deploy", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	abi := fs.String("abi", "wagi-1", "")
	mem := fs.Uint64("memory-mib", 0, "")
	resp := fs.Uint64("max-response-kib", 0, "")
	activate := fs.Bool("activate", false, "")
	positional, err := parseInterspersed(fs, args)
	if err != nil {
		return usageError(err.Error())
	}
	if err := wantArgs(positional, 2, "deploy NAME FILE [--abi A] [--memory-mib N] [--max-response-kib N] [--activate]"); err != nil {
		return err
	}
	module, err := os.ReadFile(positional[1])
	if err != nil {
		return err
	}
	f, err := c.function(positional[0])
	if err != nil {
		return err
	}
	v, err := c.c.Deploy(c.ctx, f.Id, module, *abi, &commonv1.Limits{MemoryLimitBytes: *mem << 20, MaxResponseBytes: *resp << 10}, *activate)
	if err != nil {
		return err
	}
	state := "uploaded"
	if *activate {
		state = "uploaded and live"
	}
	fmt.Fprintf(c.out, "%s: version %d %s (%d bytes, %s)\n", f.Name, v.Number, state, v.SizeBytes, v.ArtifactDigest)
	return nil
}

// parseInterspersed lets flags appear anywhere among the arguments, as users
// expect: "deploy hello hello.wasm --activate" as well as flags first.
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return positional, nil
		}
		positional = append(positional, args[0])
		args = args[1:]
	}
}

func (c *client) versions(args []string) error {
	if err := wantArgs(args, 1, "versions NAME"); err != nil {
		return err
	}
	f, err := c.function(args[0])
	if err != nil {
		return err
	}
	vs, err := c.c.Versions(c.ctx, f.Id)
	if err != nil {
		return err
	}
	w := c.table()
	fmt.Fprintln(w, "VERSION\tSTATE\tABI\tSIZE\tMEMORY\tUPLOADED\tDIGEST")
	for _, v := range vs {
		state := ""
		if v.Id == f.ActiveVersionId {
			state = "live"
		}
		mem := "default"
		if m := v.GetLimits().GetMemoryLimitBytes(); m > 0 {
			mem = strconv.FormatUint(m>>20, 10) + " MiB"
		}
		fmt.Fprintf(w, "v%d\t%s\t%s\t%d\t%s\t%s\t%s\n", v.Number, state, v.Abi, v.SizeBytes, mem, when(v.CreateTime), strings.TrimPrefix(v.ArtifactDigest, "sha256:")[:12])
	}
	return w.Flush()
}

func (c *client) activate(args []string) error {
	if err := wantArgs(args, 2, "activate NAME VERSION"); err != nil {
		return err
	}
	number, err := strconv.ParseInt(strings.TrimPrefix(args[1], "v"), 10, 64)
	if err != nil {
		return usageError("VERSION must be a number such as 2 or v2")
	}
	f, err := c.function(args[0])
	if err != nil {
		return err
	}
	vs, err := c.c.Versions(c.ctx, f.Id)
	if err != nil {
		return err
	}
	for _, v := range vs {
		if v.Number == number {
			if err := c.c.Activate(c.ctx, f.Id, v.Id); err != nil {
				return err
			}
			fmt.Fprintf(c.out, "%s: version %d is live\n", f.Name, number)
			return nil
		}
	}
	return fmt.Errorf("%s has no version %d", f.Name, number)
}

func (c *client) routes() error {
	rs, err := c.c.Routes(c.ctx)
	if err != nil {
		return err
	}
	names := c.c.FunctionNames(c.ctx)
	w := c.table()
	fmt.Fprintln(w, "HOST\tPATH\tFUNCTION\tID")
	for _, r := range rs {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", r.HostPattern, r.PathPrefix, names[r.FunctionId], r.Id)
	}
	return w.Flush()
}

func (c *client) route(args []string) error {
	if err := wantArgs(args, 3, "route HOST PATH NAME"); err != nil {
		return err
	}
	f, err := c.function(args[2])
	if err != nil {
		return err
	}
	r, err := c.c.CreateRoute(c.ctx, args[0], args[1], f.Id)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.out, "routed %s%s to %s (%s)\n", r.HostPattern, r.PathPrefix, f.Name, r.Id)
	return nil
}

func (c *client) unroute(args []string) error {
	if err := wantArgs(args, 1, "unroute ID"); err != nil {
		return err
	}
	if err := c.c.DeleteRoute(c.ctx, args[0]); err != nil {
		return err
	}
	fmt.Fprintln(c.out, "route deleted")
	return nil
}

func (c *client) env(args []string) error {
	if err := wantArgs(args, 1, "env NAME"); err != nil {
		return err
	}
	f, err := c.function(args[0])
	if err != nil {
		return err
	}
	es, err := c.c.Env(c.ctx, f.Id)
	if err != nil {
		return err
	}
	w := c.table()
	fmt.Fprintln(w, "NAME\tVALUE")
	for _, e := range es {
		v := e.Value
		if e.Secret {
			v = "(secret)"
		}
		fmt.Fprintf(w, "%s\t%s\n", e.Name, v)
	}
	return w.Flush()
}

func (c *client) set(args []string) error {
	fs := flag.NewFlagSet("set", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	secret := fs.Bool("secret", false, "")
	positional, err := parseInterspersed(fs, args)
	if err != nil {
		return usageError(err.Error())
	}
	if err := wantArgs(positional, 3, "set NAME KEY VALUE [--secret]"); err != nil {
		return err
	}
	f, err := c.function(positional[0])
	if err != nil {
		return err
	}
	if err := c.c.SetEnv(c.ctx, f.Id, &controlv1.EnvEntry{Name: positional[1], Value: positional[2], Secret: *secret}); err != nil {
		return err
	}
	fmt.Fprintf(c.out, "%s: set %s\n", f.Name, positional[1])
	return nil
}

func (c *client) egress(args []string) error {
	if err := wantArgs(args, 1, "egress NAME"); err != nil {
		return err
	}
	f, err := c.function(args[0])
	if err != nil {
		return err
	}
	if len(f.EgressAllow) == 0 {
		fmt.Fprintf(c.out, "%s may reach nothing\n", f.Name)
		return nil
	}
	for _, e := range f.EgressAllow {
		fmt.Fprintln(c.out, e)
	}
	return nil
}

// allow adds destinations to a function's list, or with add false removes them.
func (c *client) allow(args []string, add bool) error {
	verb := "allow"
	if !add {
		verb = "deny"
	}
	if len(args) < 2 {
		return usageError("usage: edgerunctl " + verb + " NAME DEST...")
	}
	f, err := c.function(args[0])
	if err != nil {
		return err
	}
	list := append([]string{}, f.EgressAllow...)
	if add {
		list = append(list, args[1:]...)
	} else {
		drop := map[string]bool{}
		for _, a := range args[1:] {
			canon, err := policy.Canonical([]string{a})
			if err != nil {
				return usageError(err.Error())
			}
			drop[canon[0]] = true
		}
		kept := list[:0]
		for _, e := range list {
			if !drop[e] {
				kept = append(kept, e)
			}
		}
		list = kept
	}
	updated, err := c.c.SetEgress(c.ctx, f.Id, list)
	if err != nil {
		return err
	}
	if len(updated.EgressAllow) == 0 {
		fmt.Fprintf(c.out, "%s may reach nothing\n", f.Name)
	} else {
		fmt.Fprintf(c.out, "%s may reach: %s\n", f.Name, strings.Join(updated.EgressAllow, ", "))
	}
	return nil
}

func (c *client) networks() error {
	ns, err := c.c.NetworkServices(c.ctx)
	if err != nil {
		return err
	}
	w := c.table()
	fmt.Fprintln(w, "ENDPOINT\tSTATE\tCARRIES")
	for _, n := range ns {
		state := "ready"
		switch {
		case !n.Alive:
			state = "unresponsive"
		case n.State == egressv1.ServiceState_SERVICE_STATE_DRAINING:
			state = "draining"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", n.Endpoint, state, strings.Join(n.GetInfo().GetCovers(), ", "))
	}
	return w.Flush()
}

func (c *client) unset(args []string) error {
	if err := wantArgs(args, 2, "unset NAME KEY"); err != nil {
		return err
	}
	f, err := c.function(args[0])
	if err != nil {
		return err
	}
	if err := c.c.DeleteEnv(c.ctx, f.Id, args[1]); err != nil {
		return err
	}
	fmt.Fprintf(c.out, "%s: unset %s\n", f.Name, args[1])
	return nil
}

func (c *client) workers() error {
	ws, err := c.c.Workers(c.ctx)
	if err != nil {
		return err
	}
	w := c.table()
	fmt.Fprintln(w, "ENDPOINT\tSTATE\tIN FLIGHT\tABIS\tIMPLEMENTATION\tLAST HEARTBEAT")
	for _, x := range ws {
		state := "ready"
		switch {
		case !x.Alive:
			state = "unresponsive"
		case x.State.String() == "WORKER_STATE_DRAINING":
			state = "draining"
		}
		fmt.Fprintf(w, "%s\t%s\t%d/%d\t%s\t%s\t%s\n", x.Endpoint, state, x.InFlight, x.GetInfo().GetMaxConcurrency(),
			strings.Join(x.GetInfo().GetAbis(), ","), x.GetInfo().GetImplementation().GetName(), when(x.LastHeartbeatTime))
	}
	return w.Flush()
}

func (c *client) invocations(args []string) error {
	fs := flag.NewFlagSet("invocations", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fn := fs.String("function", "", "")
	limit := fs.Int("limit", 20, "")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return usageError("usage: edgerunctl invocations [--function NAME] [--limit N]")
	}
	fid := ""
	if *fn != "" {
		f, err := c.function(*fn)
		if err != nil {
			return err
		}
		fid = f.Id
	}
	recs, _, err := c.c.Invocations(c.ctx, fid, "", *limit)
	if err != nil {
		return err
	}
	names := c.c.FunctionNames(c.ctx)
	w := c.table()
	fmt.Fprintln(w, "TIME\tFUNCTION\tREQUEST\tSTATUS\tFAILURE\tDURATION")
	for _, r := range recs {
		failure := "-"
		if r.ErrorReason != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
			failure = strings.ToLower(strings.TrimPrefix(r.ErrorReason.String(), "ERROR_REASON_"))
		}
		fmt.Fprintf(w, "%s\t%s\t%s %s\t%d\t%s\t%s\n", when(r.StartTime), names[r.FunctionId], r.Method, r.Path, r.Status, failure,
			time.Duration(r.DurationMicros)*time.Microsecond)
	}
	return w.Flush()
}
