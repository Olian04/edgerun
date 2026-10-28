// Command edgerun-trafficmock is a mock traffic ingress for edgerun: it plays
// the frontend's role without real clients, synthesising or replaying HTTP
// traffic into an orchestrator over ingress.v1, so a deployment can be
// load-tested, soaked and regression-tested with no network edge.
//
//	edgerun-trafficmock run    -scenario s.yaml [-expect 'p99<200ms' -expect 'errors<1%'] [-json out.json] [-prom :9100]
//	edgerun-trafficmock listen -addr :8000 [-record trace.jsonl]     (a minimal frontend: real HTTP in, ingress.v1 out)
//	edgerun-trafficmock fake   -addr :18080                          (a pretend orchestrator for developing scenarios)
//
// Exit status: 0 success, 1 an expectation failed, 2 usage or scenario error,
// 3 the run could not be made (orchestrator unreachable, bad token).
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
)

func main() { os.Exit(realMain(os.Args[1:], os.Stdout, os.Stderr)) }

func realMain(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: edgerun-trafficmock run|listen|fake [flags]   (run with a subcommand and -h for its flags)")
		return 2
	}
	switch args[0] {
	case "run":
		return runCmd(args[1:], stdout, stderr)
	case "listen":
		return listenCmd(args[1:], stdout, stderr)
	case "fake":
		return fakeCmd(args[1:])
	}
	fmt.Fprintf(stderr, "unknown command %q\n", args[0])
	return 2
}

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, "; ") }
func (l *listFlag) Set(s string) error { *l = append(*l, s); return nil }

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func newRunID() string {
	var b [3]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// common flags of run and listen
type common struct {
	url, token, httpMode      string
	conns                     int
	jsonOut, prom, reqLogPath string
	expects                   listFlag
	runID                     string
	quiet                     bool
	adminURL, adminToken      string
}

func (c *common) register(fs *flag.FlagSet) {
	fs.StringVar(&c.url, "url", "", "orchestrator base URL (env EDGERUN_ORCHESTRATOR_URL; or target.url of the scenario)")
	fs.StringVar(&c.token, "token", "", "ingress token (env EDGERUN_INGRESS_TOKEN)")
	fs.StringVar(&c.httpMode, "http", "", "1 or 2: HTTP/1.1, or HTTP/2 (h2c with prior knowledge for http://)")
	fs.IntVar(&c.conns, "conns", 0, "cap on connections to the orchestrator (0: unlimited)")
	fs.StringVar(&c.jsonOut, "json", "", "write the JSON report here")
	fs.StringVar(&c.prom, "prom", "", "serve Prometheus metrics on this address while running")
	fs.StringVar(&c.reqLogPath, "requests-log", "", "write one JSON line per request here")
	fs.Var(&c.expects, "expect", "assertion, e.g. 'p99<200ms' or 'errors<1%' (repeatable); a violated one exits 1")
	fs.StringVar(&c.runID, "run-id", "", "prefix of the request ids (default: random)")
	fs.BoolVar(&c.quiet, "quiet", false, "no text summary")
	fs.StringVar(&c.adminURL, "admin-url", "", "control.v1 URL: look up the invocation records of this run (needs -admin-token)")
	fs.StringVar(&c.adminToken, "admin-token", os.Getenv("EDGERUN_ADMIN_TOKEN"), "admin token, for -admin-url")
}

func (c *common) client(sc *Scenario) (*edgerun.Ingress, string, error) {
	url := firstNonEmpty(c.url, sc.Target.URL, os.Getenv("EDGERUN_ORCHESTRATOR_URL"))
	if url == "" {
		return nil, "", errors.New("no orchestrator: set -url, EDGERUN_ORCHESTRATOR_URL or target.url")
	}
	token := firstNonEmpty(c.token, os.Getenv("EDGERUN_INGRESS_TOKEN"))
	if token == "" && sc.Target.TokenEnv != "" {
		token = os.Getenv(sc.Target.TokenEnv)
	}
	mode := firstNonEmpty(c.httpMode, sc.Target.HTTP, "1")
	conns := c.conns
	if conns == 0 {
		conns = sc.Target.Conns
	}
	return edgerun.NewIngress(url, token, edgerun.WithHTTPClient(newHTTPClient(mode, conns))), url, nil
}

func firstNonEmpty(s ...string) string {
	for _, v := range s {
		if v != "" {
			return v
		}
	}
	return ""
}

func runCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	scPath := fs.String("scenario", "", "scenario file (YAML or JSON)")
	dur := fs.Duration("duration", 0, "override load.duration")
	rate := fs.Float64("rate", 0, "override load.rate")
	users := fs.Int("users", 0, "override load.users")
	speed := fs.String("speed", "", "override replay.speed (1, 0.5, 4, max)")
	seed := fs.Uint64("seed", 0, "override the scenario's seed")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *scPath == "" {
		fmt.Fprintln(stderr, "run: -scenario is required")
		return 2
	}
	sc, err := LoadScenario(*scPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if sc.Load != nil {
		if *dur > 0 {
			sc.Load.Duration = *dur
		}
		if *rate > 0 {
			sc.Load.Rate = *rate
		}
		if *users > 0 {
			sc.Load.Users = *users
		}
	}
	if sc.Replay != nil && *speed != "" {
		sc.Replay.Speed = *speed
	}
	if *seed != 0 {
		sc.Seed = *seed
	}
	if err := sc.Validate(); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	ing, url, err := c.client(sc)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	expects := append(append([]string(nil), sc.Expect...), c.expects...)
	runID := firstNonEmpty(c.runID, newRunID())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	lim := NewLimits(ing)
	var warnings []string
	if err := lim.Refresh(ctx); err != nil {
		switch connect.CodeOf(err) {
		case connect.CodeUnauthenticated, connect.CodePermissionDenied, connect.CodeUnavailable:
			fmt.Fprintf(stderr, "cannot reach the orchestrator's ingress.v1 at %s: %v\n", url, err)
			return 3
		}
		warnings = append(warnings, fmt.Sprintf("Describe failed (%v): assuming the protocol's default body limit of 6 MiB", err))
	}
	go lim.Run(ctx, 30*time.Second)

	started := time.Now()
	st := NewStats(started)
	var rl *ReqLog
	if c.reqLogPath != "" {
		if rl, err = NewReqLog(c.reqLogPath); err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		defer rl.Close()
	}
	inv := NewInvoker(ing, lim, st, rl, started)
	var col *Collector
	if c.adminURL != "" {
		col = NewCollector()
		inv.Keep = col.Add
	}
	if c.prom != "" {
		stopProm, err := serveMetrics(c.prom, st, runID, lim, rl)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		defer stopProm()
	}
	runner, err := NewRunner(sc, inv, runID)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	runner.Run(ctx)
	elapsed := time.Since(started)

	mode := "replay"
	if sc.Load != nil {
		mode = sc.Load.Model
	}
	sn := st.Snapshot()
	rep := BuildReport(sc, runID, mode, url, firstNonEmpty(c.httpMode, sc.Target.HTTP, "1"), started, runner.LoadEnd, elapsed, sn, lim)
	rep.Warnings = append(rep.Warnings, warnings...)
	if rl != nil {
		rl.Close()
		if n := rl.Dropped.Load(); n > 0 {
			rep.Warnings = append(rep.Warnings, fmt.Sprintf("the request log fell behind and dropped %d lines", n))
		}
	}
	if sn.Outcomes[OutDropped] > 0 {
		rep.Warnings = append(rep.Warnings, fmt.Sprintf("%d requests were dropped because %d were already in flight: the offered load was not delivered", sn.Outcomes[OutDropped], maxInFlightOf(sc)))
	}
	if sn.Lag.TotalCount() > 0 && sn.Lag.ValueAtQuantile(99) > 5000 {
		rep.Warnings = append(rep.Warnings, fmt.Sprintf("p99 send lag is %.1fms: this machine could not release requests on time, so latencies may be understated", ms(sn.Lag.ValueAtQuantile(99))))
	}
	if col != nil {
		ctl := edgerun.NewControl(c.adminURL, c.adminToken)
		rctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		rep.Invocations, err = ResolveInvocations(rctx, ctl, col, started, 5*time.Second)
		cancel()
		if err != nil {
			rep.Warnings = append(rep.Warnings, "could not read invocation records: "+err.Error())
		}
	}
	rep.Expect = Evaluate(expects, sn, rep)

	if c.jsonOut != "" {
		f, err := os.Create(c.jsonOut)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 3
		}
		rep.JSON(f)
		f.Close()
	}
	if !c.quiet {
		rep.Text(stdout)
	}
	for _, e := range rep.Expect {
		if e.Error != "" {
			fmt.Fprintf(stderr, "expectation %q: %s\n", e.Expr, e.Error)
			return 2
		}
	}
	for _, e := range rep.Expect {
		if !e.Pass {
			return 1
		}
	}
	return 0
}

func maxInFlightOf(sc *Scenario) int {
	if sc.Load != nil {
		return sc.Load.MaxInFlight
	}
	return 10000
}

func serveMetrics(addr string, st *Stats, runID string, lim *Limits, rl *ReqLog) (func(), error) {
	mux := http.NewServeMux()
	var dropped func() int64
	if rl != nil {
		dropped = rl.Dropped.Load
	}
	mux.Handle("/metrics", metricsHandler(st, runID, lim, dropped))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ln, err := netListen(addr)
	if err != nil {
		return nil, err
	}
	go srv.Serve(ln)
	return func() { srv.Close() }, nil
}
