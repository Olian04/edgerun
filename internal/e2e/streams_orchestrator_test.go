package e2e

// Execute seam, consumer = the real orchestrator (on its real PostgreSQL). The
// producers are two scripted workers, written from the protocol; the client is
// a raw ingress.v1 client, so that what the orchestrator emits is seen message
// by message, with no library in between to tidy it.
//
// Oracle, per sequence of symbols (see streams_grammar_test.go):
//   - valid: head, the bytes intact, clean end; the head carries an invocation id;
//   - violation or failure: the stream ends in a typed platform error (never a
//     clean end) that carries the id of the invocation record; what was
//     delivered before it is a prefix of what the worker sent;
//   - a stream that stops ends by its deadline;
//   - at most once: the function is offered to one worker, and to another only
//     when the first answered NO_CAPACITY before anything else;
//   - exactly one invocation record per invocation, with the error_reason and
//     the status the client saw;
//   - the orchestrator serves the next request, and returns to its baseline
//     of goroutines and sockets.

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
)

// ---- a scripted worker, registered with the orchestrator ------------------------

// stFakeWorker serves ExecutionService by script and keeps itself registered.
type stFakeWorker struct {
	t   testing.TB
	srv *stServer

	// plan chooses what an Execute does; hook, if set, runs first (after the
	// call is counted) and may block.
	plan atomic.Pointer[func(*workerv1.ExecuteRequest) stSeq]
	hook atomic.Pointer[func(*workerv1.ExecuteRequest, *http.Request)]

	mu     sync.Mutex
	counts map[string]int // by x-seq header
	total  int
	got    []*workerv1.ExecuteRequest

	fleet *stFleetMember
}

type stDescribeWorker struct {
	workerv1connect.UnimplementedExecutionServiceHandler
	info *workerv1.WorkerInfo
}

func (d stDescribeWorker) Describe(context.Context, *connect.Request[workerv1.DescribeRequest]) (*connect.Response[workerv1.DescribeResponse], error) {
	return connect.NewResponse(&workerv1.DescribeResponse{Info: d.info}), nil
}

func stWorkerInfo(max uint32, abis ...string) *workerv1.WorkerInfo {
	return &workerv1.WorkerInfo{
		Implementation: &commonv1.Implementation{Name: "scripted-worker", Version: "1", Protocols: []string{"edgerun.worker.v1"}},
		Abis:           abis, MaxConcurrency: max}
}

func stHeaderOf(r *workerv1.ExecuteRequest, name string) string {
	for _, h := range r.GetRequest().GetHeaders() {
		if h.GetName() == name {
			return string(h.GetValue())
		}
	}
	return ""
}

// stNewFakeWorker starts a scripted worker. Registration is the caller's (it
// may go through a proxy).
func stNewFakeWorker(t testing.TB) *stFakeWorker {
	w := &stFakeWorker{t: t, counts: map[string]int{}}
	mux := http.NewServeMux()
	path, h := workerv1connect.NewExecutionServiceHandler(stDescribeWorker{info: stWorkerInfo(64, "wagi-1")})
	mux.Handle(path, h)
	mux.HandleFunc(workerv1connect.ExecutionServiceExecuteProcedure, func(rw http.ResponseWriter, r *http.Request) {
		var req workerv1.ExecuteRequest
		if err := stReadRequest(r, &req); err != nil {
			http.Error(rw, err.Error(), 400)
			return
		}
		w.mu.Lock()
		w.total++
		w.counts[stHeaderOf(&req, "x-seq")]++
		w.got = append(w.got, &req)
		w.mu.Unlock()
		if hk := w.hook.Load(); hk != nil {
			(*hk)(&req, r)
		}
		seq := stSeq{stH, stC, stS, stCLEAN}
		if p := w.plan.Load(); p != nil {
			seq = (*p)(&req)
		}
		out := stNewOut(rw, r)
		stPlay(out, stExecuteCodec, seq)
	})
	w.srv = stServe(t, mux)
	return w
}

func (w *stFakeWorker) count(seq string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.counts[seq]
}

func (w *stFakeWorker) calls() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.total
}

// stFleetMember registers an endpoint with an orchestrator and heartbeats until stopped.
type stFleetMember struct {
	t        testing.TB
	fleet    *edgerun.Fleet
	endpoint string
	info     *workerv1.WorkerInfo
	every    time.Duration
	inFlight func() uint32

	mu       sync.Mutex
	id       string
	paused   bool
	registry int // times registered
	hbErrs   int
	hbOK     int
	stop     chan struct{}
	done     chan struct{}
}

func stJoinFleet(t testing.TB, orchURL, endpoint string, info *workerv1.WorkerInfo, inFlight func() uint32) *stFleetMember {
	m := &stFleetMember{t: t, fleet: edgerun.NewFleet(orchURL, workerToken), endpoint: endpoint, info: info,
		every: 100 * time.Millisecond, inFlight: inFlight, stop: make(chan struct{}), done: make(chan struct{})}
	if err := m.register(); err != nil {
		t.Fatalf("register %s: %v", endpoint, err)
	}
	go m.run()
	t.Cleanup(m.Stop)
	return m
}

func (m *stFleetMember) register() error {
	ctx, cancel := stCtx(5 * time.Second)
	defer cancel()
	r, err := m.fleet.Register(ctx, &workerv1.RegisterRequest{Endpoint: m.endpoint, Info: m.info})
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.id = r.WorkerId
	m.registry++
	m.mu.Unlock()
	return nil
}

func (m *stFleetMember) run() {
	defer close(m.done)
	tick := time.NewTicker(m.every)
	defer tick.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-tick.C:
		}
		m.mu.Lock()
		id, paused := m.id, m.paused
		m.mu.Unlock()
		if paused {
			continue
		}
		var n uint32
		if m.inFlight != nil {
			n = m.inFlight()
		}
		ctx, cancel := stCtx(2 * time.Second)
		r, err := m.fleet.Heartbeat(ctx, &workerv1.HeartbeatRequest{WorkerId: id, State: workerv1.WorkerState_WORKER_STATE_READY, InFlight: n})
		cancel()
		m.mu.Lock()
		if err != nil {
			m.hbErrs++
		} else {
			m.hbOK++
		}
		m.mu.Unlock()
		if err == nil && r.Reregister {
			m.register()
		}
	}
}

func (m *stFleetMember) Stop() {
	select {
	case <-m.stop:
		return
	default:
	}
	close(m.stop)
	<-m.done
	ctx, cancel := stCtx(2 * time.Second)
	defer cancel()
	m.mu.Lock()
	id := m.id
	m.mu.Unlock()
	m.fleet.Deregister(ctx, &workerv1.DeregisterRequest{WorkerId: id})
}

func (m *stFleetMember) ID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.id
}

func (m *stFleetMember) stats() (registered, hbOK, hbErrs int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.registry, m.hbOK, m.hbErrs
}

// ---- the raw ingress client -------------------------------------------------------

// stInvokeObs is what a frontend sees of one Invoke, message by message.
type stInvokeObs struct {
	head    *ingressv1.InvokeHead
	body    []byte
	msgs    int
	nopart  int // messages with no part set
	err     error
	elapsed time.Duration
}

type stIngress struct {
	c ingressv1connect.IngressServiceClient
}

func stNewIngress(url string) *stIngress {
	hc := &http.Client{Transport: budgetTransport{}}
	return &stIngress{c: ingressv1connect.NewIngressServiceClient(&stBearer{hc, ingressToken}, url)}
}

// stBearer adds the credential.
type stBearer struct {
	hc    *http.Client
	token string
}

func (b *stBearer) Do(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.hc.Do(r)
}

// invoke calls Invoke with a budget for the orchestrator (the test's own timer is long).
func (i *stIngress) invoke(host, path, requestID, seqHeader string, budget time.Duration) stInvokeObs {
	cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if budget > 0 {
		cctx = context.WithValue(cctx, budgetKey{}, budget)
	}
	start := time.Now()
	st, err := i.c.Invoke(cctx, connect.NewRequest(&ingressv1.InvokeRequest{
		RequestId: requestID,
		Request: &commonv1.HttpRequest{Method: "GET", Host: host, Path: path,
			Headers: []*commonv1.Header{{Name: "x-seq", Value: []byte(seqHeader)}}},
	}))
	var o stInvokeObs
	if err != nil {
		o.err = err
		o.elapsed = time.Since(start)
		return o
	}
	defer st.Close()
	for st.Receive() {
		m := st.Msg()
		o.msgs++
		switch p := m.Part.(type) {
		case *ingressv1.InvokeResponse_Head:
			if o.head != nil {
				o.err = fmt.Errorf("the orchestrator sent a second head")
			}
			o.head = p.Head
		case *ingressv1.InvokeResponse_Chunk:
			o.body = append(o.body, p.Chunk...)
		default:
			o.nopart++
		}
	}
	if o.err == nil {
		o.err = st.Err()
	}
	o.elapsed = time.Since(start)
	return o
}

// ---- the check ------------------------------------------------------------------------

const stOrchBudget = 700 * time.Millisecond

func stInfoOf(err error) *commonv1.ErrorInfo { return edgerun.Info(err) }

// stCheckOrchestrator judges what the orchestrator did with a sequence against one verdict.
func stCheckOrchestrator(e stExpect, o stInvokeObs) (id, msg string) {
	bad := func(id, format string, a ...any) (string, string) { return id, fmt.Sprintf(format, a...) }
	if o.elapsed > stOrchBudget+1500*time.Millisecond {
		return bad("OR-HANG", "took %v with a %v budget", o.elapsed, stOrchBudget)
	}
	if o.nopart > 0 {
		return bad("OR-NOPART", "the orchestrator sent %d messages with no part set", o.nopart)
	}
	switch e.kind {
	case stOK, stEither:
		if o.err == nil {
			if o.head == nil || o.head.GetHttp().GetStatus() != 200 || o.head.GetInvocationId() == "" {
				return bad("OR-HEAD", "a clean stream without a proper head: %v", o.head)
			}
			if string(o.body) != string(e.body) {
				return bad("OR-CORRUPT", "body %q, want %q", o.body, e.body)
			}
			return "", ""
		}
		if e.kind == stOK {
			return bad("OR-REJECTED", "a valid stream failed: %v", o.err)
		}
		return "", ""
	}
	// a violation, or a failure: it must end in a typed error, never in a clean end
	if o.err == nil {
		id := "OR-HIDDEN"
		if e.violation == "data after the summary" || e.violation == "a second summary" {
			id = "D-OR-LATE-DATA"
		}
		return bad(id, "%s: the orchestrator ended the stream cleanly (head %v, body %q)", e.violationOr(), o.head != nil, o.body)
	}
	info := stInfoOf(o.err)
	if info == nil || info.GetReason() == commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
		return bad("OR-UNTYPED", "%s: the error carries no reason: %v", e.violationOr(), o.err)
	}
	if !slices.Contains(e.reasons, info.GetReason()) {
		return bad("OR-REASON", "%s: reason %v, want one of %v", e.violationOr(), info.GetReason(), e.reasons)
	}
	if info.GetInvocationId() == "" {
		return bad("OR-NO-INVOCATION-ID", "%s: a failure after the request was routed carries no invocation id", e.violationOr())
	}
	if !stPrefix(o.body, e.sent) {
		return bad("OR-CORRUPT", "%s: delivered %q, which is not a prefix of what was sent (%q)", e.violationOr(), o.body, e.sent)
	}
	if e.kind == stFailBefore && o.head != nil {
		return bad("OR-HEAD-EARLY", "%s: a head was delivered for a stream that has none", e.violationOr())
	}
	if e.kind == stFailAfter && o.head == nil {
		return bad("OR-HEAD-LATE", "%s: no head was delivered, though the worker sent one", e.violationOr())
	}
	return "", ""
}

func stPrefix(got, sent []byte) bool {
	return len(got) <= len(sent) && string(sent[:len(got)]) == string(got)
}

func stOrchestratorSequences() []stSeq {
	syms := []string{stH, stC, stE, stN, stS}
	terms := []string{stCLEAN, stTRUNC, stRESET,
		"ERR:INTERNAL", "ERR:NO_CAPACITY", "ERR:TRAPPED", "ERR:DEADLINE", "ERR:unavailable", "ERR:internal"}
	seqs := stEnumerate(syms, 3, terms, []string{stSTALL}, 2)
	return append(seqs, stSample(syms, append(slices.Clone(terms), stSTALL), 200, 20240602)...)
}

// The orchestrator accepts valid worker streams and ends every violation and
// failure in a typed error, whatever the worker does.
func TestStreams_Execute_Orchestrator(t *testing.T) {
	s := newStack(t)
	w1, w2 := stNewFakeWorker(t), stNewFakeWorker(t)
	stJoinFleet(t, "http://"+s.addr, w1.srv.URL, stWorkerInfo(64, "wagi-1"), nil)
	stJoinFleet(t, "http://"+s.addr, w2.srv.URL, stWorkerInfo(64, "wagi-1"), nil)
	fid := s.createFunction("grammar")
	s.uploadVersion(fid, []byte("\x00asm-not-run-by-a-scripted-worker"), nil, true)
	s.route("grammar.test", "/", fid)

	var current atomic.Pointer[stSeq]
	plan := func(*workerv1.ExecuteRequest) stSeq { return *current.Load() }
	w1.plan.Store(&plan)
	w2.plan.Store(&plan)
	ing := stNewIngress("http://" + s.addr)

	normal := stSeq{stH, stC, stS, stCLEAN}
	run := func(seq stSeq, reqID string) stInvokeObs {
		current.Store(&seq)
		return ing.invoke("grammar.test", "/g", reqID, reqID, stOrchBudget)
	}
	// both workers registered, and routing known
	eventually(t, "the scripted workers to be schedulable", 30*time.Second, func() bool {
		o := run(normal, "warmup")
		return o.err == nil && s.aliveWorkers() == 2
	})
	for i := 0; i < 5; i++ {
		run(normal, "warmup")
	}
	// The orchestrator keeps a pool of idle keep-alive connections to its workers (by design, see the library's
	// default client), whose size depends on how many calls overlapped: the slack below allows for a handful of
	// them, while a real leak grows with the 1600 sequences and is far beyond it.
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	base := stSnapshot()
	t.Logf("baseline: %v", base)

	var f stFindings
	type verdict struct {
		reason commonv1.ErrorReason
		status uint32
	}
	seen := map[string]verdict{}
	seqs := stOrchestratorSequences()
	start := time.Now()
	for i, seq := range seqs {
		reqID := fmt.Sprintf("seq-%d", i)
		exps := stAnalyze(true, seq)
		o := run(seq, reqID)
		if id, msg := stCheckAny(exps, func(e stExpect) (string, string) { return stCheckOrchestrator(e, o) }); msg != "" {
			f.add(id, seq.String(), msg)
		}
		v := verdict{}
		if o.err != nil {
			v.reason = edgerun.Reason(o.err)
		}
		if o.head != nil {
			v.status = o.head.GetHttp().GetStatus()
		}
		seen[reqID] = v

		// at most once: offered to one worker; to a second only after a NO_CAPACITY that came first
		calls := w1.count(reqID) + w2.count(reqID)
		want := []int{1}
		if seq.term() == "ERR:NO_CAPACITY" && !slices.ContainsFunc(seq.prefix(), func(s string) bool { return s != stN }) {
			want = []int{2}
			if slices.Contains(seq.prefix(), stN) {
				want = []int{1, 2} // the no-part message first: the orchestrator may or may not call it a violation
			}
		}
		if !slices.Contains(want, calls) {
			f.add("OR-RETRY", seq.String(), fmt.Sprintf("the function was offered to %d workers, want %v", calls, want))
		}

		if n := run(normal, fmt.Sprintf("normal-%d", i)); n.err != nil || string(n.body) != string(stChunk(1)) {
			f.add("OR-DEAD", seq.String(), fmt.Sprintf("the next, normal request: %v %q", n.err, n.body))
		}
		http.DefaultTransport.(*http.Transport).CloseIdleConnections()
		if cur, ok := stSettle(base, 24, 24, 3*time.Second); !ok {
			f.add("OR-LEAK-RES", seq.String(), fmt.Sprintf("did not return to the baseline (%v): now %v", base, cur))
			base = cur
		}
	}
	t.Logf("%d sequences in %v", len(seqs), time.Since(start).Round(time.Millisecond))

	// exactly one record per invocation, saying what the client saw
	want := len(seqs) * 2
	var recs []*controlv1.Invocation
	eventually(t, "every invocation to be recorded", 30*time.Second, func() bool {
		recs = recs[:0]
		tok := ""
		for {
			r, err := s.ctl.ListInvocations(ctx, &controlv1.ListInvocationsRequest{FunctionId: fid, PageSize: 500, PageToken: tok})
			if err != nil {
				return false
			}
			for _, rec := range r.Invocations {
				if rec.RequestId != "warmup" {
					recs = append(recs, rec)
				}
			}
			if tok = r.NextPageToken; tok == "" {
				break
			}
		}
		return len(recs) >= want
	})
	byReq := map[string][]*controlv1.Invocation{}
	for _, r := range recs {
		byReq[r.RequestId] = append(byReq[r.RequestId], r)
	}
	for reqID, v := range seen {
		rs := byReq[reqID]
		if len(rs) != 1 {
			f.add("OR-RECORDS", reqID, fmt.Sprintf("%d invocation records for %s, want exactly 1", len(rs), reqID))
			continue
		}
		if rs[0].ErrorReason != v.reason || rs[0].Status != v.status {
			f.add("OR-RECORD-WRONG", reqID, fmt.Sprintf("record says status %d reason %v; the client saw status %d reason %v", rs[0].Status, rs[0].ErrorReason, v.status, v.reason))
		}
	}
	if len(recs) != want {
		f.add("OR-RECORDS", "(total)", fmt.Sprintf("%d records for %d invocations", len(recs), want))
	}
	if cur, ok := stSettle(base, 24, 24, 5*time.Second); !ok {
		f.add("OR-LEAK-RES", "(end)", fmt.Sprintf("after all sequences: %v, baseline %v", cur, base))
	}
	f.report(t, stOrchestratorKnown)
}

var stOrchestratorKnown = map[string]string{
	"D-OR-LATE-DATA": "the orchestrator forwards chunks that follow a worker's summary, and accepts a second summary, then ends the stream as a success",
}

// D-OR-LATE-DATA: the Execute stream is "exactly one head, zero or more chunks,
// then exactly one summary". The orchestrator records the summary when it sees
// one but keeps reading: a worker that sends H,S,C,CLEAN has its late chunk
// delivered to the client and the invocation recorded as a success; H,S,S is
// accepted too.
func TestStreams_KnownDefect_ExecuteDataAfterSummary(t *testing.T) {
	stSkipKnown(t, "D-OR-LATE-DATA", "the orchestrator delivers chunks after the worker's summary and ends the stream cleanly")
	s := newStack(t)
	w := stNewFakeWorker(t)
	stJoinFleet(t, "http://"+s.addr, w.srv.URL, stWorkerInfo(8, "wagi-1"), nil)
	fid := s.createFunction("late")
	s.uploadVersion(fid, []byte("\x00asm-not-run"), nil, true)
	s.route("late.test", "/", fid)
	seq := stSeq{stH, stC, stS, stC, stCLEAN}
	plan := func(*workerv1.ExecuteRequest) stSeq { return seq }
	w.plan.Store(&plan)
	ing := stNewIngress("http://" + s.addr)
	eventually(t, "the worker to be schedulable", 30*time.Second, func() bool { return s.aliveWorkers() == 1 })
	var o stInvokeObs
	eventually(t, "a routed answer", 30*time.Second, func() bool {
		o = ing.invoke("late.test", "/x", "late", "late", time.Second)
		return o.head != nil || edgerun.Info(o.err) != nil
	})
	if o.err == nil {
		t.Fatalf("a stream H,C,S,C ended cleanly and the client got %q after the worker's summary", o.body)
	}
}
