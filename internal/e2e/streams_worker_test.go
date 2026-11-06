package e2e

// The worker as a consumer of two streams: Fetch (from a network service) and
// GetArtifact (from the orchestrator). The producers are scripted; the
// "orchestrator" is a scripted peer too (it answers Fleet and Artifact), so the
// worker is driven the way the orchestrator drives it: Execute.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	workerapp "github.com/Olian04/edgerun/internal/worker/app"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ---- a scripted orchestrator for a worker: Fleet and Artifact --------------------

type stArtifactOrch struct {
	srv *stServer
	// serve plays the GetArtifact stream of a digest.
	serve atomic.Pointer[func(o *stOut, digest string)]

	mu       sync.Mutex
	fetches  map[string]int
	registry int
}

type stFleetHandler struct {
	workerv1connect.UnimplementedFleetServiceHandler
	o *stArtifactOrch
}

func (h stFleetHandler) Register(context.Context, *connect.Request[workerv1.RegisterRequest]) (*connect.Response[workerv1.RegisterResponse], error) {
	h.o.mu.Lock()
	h.o.registry++
	h.o.mu.Unlock()
	return connect.NewResponse(&workerv1.RegisterResponse{WorkerId: "scripted-1", HeartbeatInterval: durationpb.New(200 * time.Millisecond)}), nil
}
func (stFleetHandler) Heartbeat(context.Context, *connect.Request[workerv1.HeartbeatRequest]) (*connect.Response[workerv1.HeartbeatResponse], error) {
	return connect.NewResponse(&workerv1.HeartbeatResponse{}), nil
}
func (stFleetHandler) Deregister(context.Context, *connect.Request[workerv1.DeregisterRequest]) (*connect.Response[workerv1.DeregisterResponse], error) {
	return connect.NewResponse(&workerv1.DeregisterResponse{}), nil
}

func stNewArtifactOrch(t testing.TB) *stArtifactOrch {
	o := &stArtifactOrch{fetches: map[string]int{}}
	mux := http.NewServeMux()
	path, h := workerv1connect.NewFleetServiceHandler(stFleetHandler{o: o})
	mux.Handle(path, h)
	mux.HandleFunc(workerv1connect.ArtifactServiceGetArtifactProcedure, func(w http.ResponseWriter, r *http.Request) {
		var req workerv1.GetArtifactRequest
		if err := stReadRequest(r, &req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		o.mu.Lock()
		o.fetches[req.GetDigest()]++
		o.mu.Unlock()
		out := stNewOut(w, r)
		if f := o.serve.Load(); f != nil {
			(*f)(out, req.GetDigest())
			return
		}
		out.end(connect.CodeNotFound, "no such artifact", &commonv1.ErrorInfo{Reason: commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND, Message: "x"})
	})
	o.srv = stServe(t, mux)
	return o
}

func (o *stArtifactOrch) fetched(digest string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.fetches[digest]
}

// stStartWorker runs a real worker against orchestratorURL and returns the URL of its ExecutionService.
func stStartWorker(t testing.TB, orchestratorURL string, egress bool, slots uint32) (execURL string) {
	t.Helper()
	addr := stFreePort(t)
	cfg := workerapp.Config{
		Listen: addr, OrchestratorURL: orchestratorURL, WorkerToken: workerToken, ExecutionToken: executionToken,
		MaxConcurrency: slots, DrainTimeout: 5 * time.Second, Log: quiet,
	}
	if egress {
		cfg.EgressToken = egressToken
	}
	wctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- workerapp.Run(wctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("the worker did not exit")
		}
	})
	// serving?
	url := "http://" + addr
	exec := edgerun.NewExecution(url, executionToken)
	eventually2(t, "the worker to serve", 30*time.Second, func() bool {
		_, err := exec.Describe(context.Background(), &workerv1.DescribeRequest{})
		return err == nil
	})
	return url
}

func eventually2(t testing.TB, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

// ---- Fetch: the worker consumes a network service's stream -----------------------------

const stWorkerBudget = 800 * time.Millisecond

type stGuestObs struct {
	execErr error
	code    int
	resp    string
	elapsed time.Duration
}

// stRunNetGuest runs the network guest on the worker: it fetches `GET http://x.test/page`
// through the service at serviceURL, and answers with the code it was given.
func stRunNetGuest(exec *edgerun.Execution, digest, serviceURL, id string, budget time.Duration) stGuestObs {
	cctx, cancel := context.WithTimeout(context.WithValue(context.Background(), budgetKey{}, budget), 30*time.Second)
	defer cancel()
	start := time.Now()
	res, err := exec.Execute(cctx, &workerv1.ExecuteRequest{
		InvocationId: id, ArtifactDigest: digest, Abi: "wagi-2",
		Request: &commonv1.HttpRequest{Method: "POST", Host: "net.test", Path: "/fetch", Body: []byte("GET http://x.test/page\n\n")},
		Egress:  []*workerv1.EgressGrant{{Allow: "x.test", Service: serviceURL}},
	})
	if err != nil {
		return stGuestObs{execErr: err, elapsed: time.Since(start)}
	}
	body, err := res.ReadAll()
	o := stGuestObs{elapsed: time.Since(start), execErr: err}
	if err == nil {
		var code int
		rest := string(body)
		if _, serr := fmt.Sscanf(rest, "code=%d\n", &code); serr != nil {
			o.execErr = fmt.Errorf("the guest said %q", body)
			return o
		}
		o.code = code
		o.resp = rest[strings.Index(rest, "\n")+1:]
	}
	return o
}

// stFetchCode is the guest-visible code for what ends a stream with the given terminal.
func stFetchCodes(e stExpect) []int {
	codes := []int{-2}
	// (a consumer that has not noticed a violation reports the error that ends the stream)
	switch e.term {
	case "ERR:EGRESS_DENIED":
		codes = append(codes, -1)
	case "ERR:TOO_LARGE":
		codes = append(codes, -3)
	case "ERR:invalid_argument":
		codes = append(codes, -4)
	}
	return codes
}

func stCheckFetch(e stExpect, o stGuestObs) (id, msg string) {
	bad := func(id, format string, a ...any) (string, string) { return id, fmt.Sprintf(format, a...) }
	if o.elapsed > stWorkerBudget+1500*time.Millisecond {
		return bad("WK-HANG", "took %v with a %v budget", o.elapsed, stWorkerBudget)
	}
	if o.execErr != nil {
		// the guest was blocked in a stalled fetch when the budget ran out: the invocation fails by its deadline
		if e.stalled && edgerun.Reason(o.execErr) == commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED {
			return "", ""
		}
		return bad("WK-EXEC", "the invocation itself failed: %v", o.execErr)
	}
	switch e.kind {
	case stOK, stEither:
		if o.code >= 0 {
			parts := strings.SplitN(o.resp, "\n\n", 2)
			if len(parts) != 2 || !strings.HasPrefix(parts[0], "Status: 200") || parts[1] != string(e.body) {
				return bad("WK-CORRUPT", "the guest got %q, want a 200 with %q", o.resp, e.body)
			}
			return "", ""
		}
		if e.kind == stOK {
			return bad("WK-REJECTED", "a valid stream was refused: code %d", o.code)
		}
		return "", ""
	}
	if o.code >= 0 {
		id := "WK-HIDDEN"
		if e.badHead {
			id = "D-WK-FETCH-BAD-STATUS"
		}
		return bad(id, "%s: the guest was told it succeeded (code %d, %q)", e.violationOr(), o.code, o.resp)
	}
	ok := false
	for _, c := range stFetchCodes(e) {
		ok = ok || c == o.code
	}
	if !ok {
		return bad("WK-CODE", "%s: the guest got code %d, want one of %v", e.violationOr(), o.code, stFetchCodes(e))
	}
	return "", ""
}

func stFetchSequences() []stSeq {
	syms := []string{stH, stHB, stC, stE, stN}
	terms := []string{stCLEAN, stTRUNC, stRESET, "ERR:EGRESS_DENIED", "ERR:EGRESS_FAILED", "ERR:TOO_LARGE", "ERR:INTERNAL", "ERR:NO_CAPACITY", "ERR:unavailable", "ERR:invalid_argument"}
	depth := 2
	if stRaceBuild {
		depth = 1
	}
	seqs := stEnumerate(syms, depth, terms, []string{stSTALL}, 1)
	// a few deeper prefixes with the terminals that matter most
	deeper := stEnumerate([]string{stH, stC, stN, stHB}, 3, []string{stCLEAN, stRESET, "ERR:EGRESS_FAILED"}, nil, 0)
	for _, s := range deeper {
		if len(s) == 4 {
			seqs = append(seqs, s)
		}
	}
	return append(seqs, stSample(syms, append(terms, stSTALL), 60, 20240603)...)
}

// The worker hands the guest a response only for a valid Fetch stream, and tells
// it the destination failed for every violation and failure, whatever the
// service does; the worker stays whole.
type stFetchRig struct {
	exec    *edgerun.Execution
	digest  string
	svc     *stServer
	current atomic.Pointer[stSeq]
	calls   stCounter
	custom  atomic.Pointer[func(*stOut)]
}

// stNewFetchRig starts a real worker whose orchestrator is scripted (it serves the
// network guest) and a scripted network service.
func stNewFetchRig(t *testing.T) *stFetchRig {
	rig := &stFetchRig{}
	orch := stNewArtifactOrch(t)
	module := stBuildNet(t)
	rig.digest = edgerun.Digest(module)
	clean := func(o *stOut, d string) {
		if d == rig.digest {
			o.msg(stMust(&workerv1.GetArtifactResponse{Chunk: module}))
			o.end(0, "", nil)
		}
	}
	orch.serve.Store(&clean)
	svcMux := http.NewServeMux()
	svcMux.HandleFunc("/edgerun.egress.v1.EgressService/Fetch", func(w http.ResponseWriter, r *http.Request) {
		var req egressv1.FetchRequest
		if err := stReadRequest(r, &req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		rig.calls.add()
		out := stNewOut(w, r)
		if c := rig.custom.Load(); c != nil {
			(*c)(out)
			return
		}
		stPlay(out, stFetchCodec, *rig.current.Load())
	})
	rig.svc = stServe(t, svcMux)
	execURL := stStartWorker(t, orch.srv.URL, true, 4)
	rig.exec = edgerun.NewExecution(execURL, executionToken, edgerun.WithHTTPClient(&http.Client{Transport: budgetTransport{}}))
	return rig
}

// setCustom replaces the scripted play with f (nil restores it).
func (r *stFetchRig) setCustom(f func(*stOut)) {
	if f == nil {
		r.custom.Store(nil)
		return
	}
	r.custom.Store(&f)
}

func (r *stFetchRig) run(seq stSeq, id string, budget time.Duration) stGuestObs {
	r.current.Store(&seq)
	return stRunNetGuest(r.exec, r.digest, r.svc.URL, id, budget)
}

// D-WK-FETCH-BAD-STATUS: the head of a Fetch is "HTTP status code, 100-599
// excluding 1xx" (common.proto). A network service that sends another status
// (700 here) has its head handed to the guest as a successful fetch
// ("Status: 700"): the worker validates neither the status nor, therefore, what a
// guest's own response may not contain (a guest's Status line must be 200-599;
// a frontend answers 502 for the same fault one hop up).
func TestStreams_KnownDefect_FetchBadStatus(t *testing.T) {
	stSkipKnown(t, "D-WK-FETCH-BAD-STATUS", "a network service's head with status 700 reaches the guest as a successful fetch")
	rig := stNewFetchRig(t)
	eventually2(t, "the network guest to run", 60*time.Second, func() bool {
		o := rig.run(stSeq{stH, stCLEAN}, "warm", 5*time.Second)
		return o.execErr == nil && o.code >= 0
	})
	o := rig.run(stSeq{stHB, stC, stCLEAN}, "bad", 5*time.Second)
	if o.execErr != nil || o.code >= 0 {
		t.Fatalf("a head with status 700 should fail the fetch (-2): code %d, %v, response %q", o.code, o.execErr, o.resp)
	}
}

func TestStreams_Fetch_Worker(t *testing.T) {
	rig := stNewFetchRig(t)
	exec, digest, svc := rig.exec, rig.digest, rig.svc
	normal := stSeq{stH, stC, stCLEAN}
	run := func(seq stSeq, id string) stGuestObs {
		return rig.run(seq, id, stWorkerBudget+1200*time.Millisecond)
	}
	runBudget := func(seq stSeq, id string, budget time.Duration) stGuestObs {
		return rig.run(seq, id, budget)
	}
	_, _, _ = exec, digest, svc
	// the guest runs, and compiles once
	eventually2(t, "the network guest to run", 60*time.Second, func() bool {
		o := run(normal, "warm")
		return o.execErr == nil && o.code >= 0
	})
	for i := 0; i < 3; i++ {
		run(normal, "warm")
	}
	base := stSnapshot()
	t.Logf("baseline: %v", base)

	var f stFindings
	seqs := stFetchSequences()
	start := time.Now()
	for i, seq := range seqs {
		exps := stAnalyze(false, seq)
		budget := stWorkerBudget + 1200*time.Millisecond
		if seq.term() == stSTALL {
			budget = stWorkerBudget
		}
		o := runBudget(seq, fmt.Sprintf("seq-%d", i), budget)
		if id, msg := stCheckAny(exps, func(e stExpect) (string, string) { return stCheckFetch(e, o) }); msg != "" {
			f.add(id, seq.String(), msg)
		}
		if true {
			if n := run(normal, "normal"); n.execErr != nil || n.code < 0 || !strings.HasSuffix(n.resp, string(stChunk(1))) {
				f.add("WK-DEAD", seq.String(), fmt.Sprintf("the next, normal request: %v code %d %q", n.execErr, n.code, n.resp))
			}
		}
		if cur, ok := stSettle(base, 8, 6, 3*time.Second); !ok {
			f.add("WK-LEAK-RES", seq.String(), fmt.Sprintf("did not return to the baseline (%v): now %v", base, cur))
			base = cur
		}
	}
	t.Logf("%d sequences in %v", len(seqs), time.Since(start).Round(time.Millisecond))
	if cur, ok := stSettle(base, 8, 6, 5*time.Second); !ok {
		f.add("WK-LEAK-RES", "(end)", fmt.Sprintf("after all sequences: %v, baseline %v", cur, base))
	}
	f.report(t, stFetchKnown)
	stFetchBounds(t, rig)
}

var stFetchKnown = map[string]string{
	"D-WK-FETCH-BAD-STATUS": "the worker passes a network service's head with a status outside 200-599 to the guest as a successful fetch",
}
