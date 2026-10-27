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
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/internal/admin"
)

const usage = `edgerunctl manages an edgerun orchestrator through its control protocol.

usage: edgerunctl [--url URL] [--token TOKEN] <command> [arguments]

  functions                       list functions
  create NAME                     create a function
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
  workers                         list workers
  invocations [--function NAME] [--limit N]

--url defaults to $EDGERUN_URL and --token to $EDGERUN_ADMIN_TOKEN.
`

// Run executes args (without the program name) and returns the exit code.
func Run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	global := flag.NewFlagSet("edgerunctl", flag.ContinueOnError)
	global.SetOutput(io.Discard)
	url := global.String("url", getenv("EDGERUN_URL"), "")
	token := global.String("token", getenv("EDGERUN_ADMIN_TOKEN"), "")
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
	if err := c.run(rest[0], rest[1:]); err != nil {
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
		return c.functions()
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

func (c *client) functions() error {
	fns, err := c.c.Functions(c.ctx)
	if err != nil {
		return err
	}
	numbers := c.c.ActiveVersionNumbers(c.ctx, fns)
	w := c.table()
	fmt.Fprintln(w, "NAME\tACTIVE\tCREATED\tID")
	for _, f := range fns {
		active := "-"
		if n := numbers[f.Id]; n > 0 {
			active = "v" + strconv.FormatInt(n, 10)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", f.Name, active, when(f.CreateTime), f.Id)
	}
	return w.Flush()
}

func (c *client) create(args []string) error {
	if err := wantArgs(args, 1, "create NAME"); err != nil {
		return err
	}
	f, err := c.c.CreateFunction(c.ctx, args[0])
	if err != nil {
		return err
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
