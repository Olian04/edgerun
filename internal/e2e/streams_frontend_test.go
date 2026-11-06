package e2e

// Invoke seam, consumer = the real frontend. The producer is a scripted
// orchestrator (stPlay); the client is a plain HTTP client. Oracle: a valid
// stream is a 200 with the bytes intact; a stream that fails before the head is
// the documented status; a stream that fails after the head aborts the client's
// connection (never a complete response); a stalled stream ends by the
// deadline; and the frontend still serves, and leaks nothing.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"connectrpc.com/connect"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
	frontendapp "github.com/Olian04/edgerun/internal/frontend/app"
)

const stFrontendBudget = 600 * time.Millisecond

// stFakeOrch is an orchestrator written from the protocol: Describe is answered,
// Invoke plays whatever sequence is set.
type stFakeOrch struct {
	srv   *stServer
	plan  atomic.Pointer[stSeq]
	calls stCounter
}

type stDescribeOnly struct {
	ingressv1connect.UnimplementedIngressServiceHandler
}

func (stDescribeOnly) Describe(context.Context, *connect.Request[ingressv1.DescribeRequest]) (*connect.Response[ingressv1.DescribeResponse], error) {
	return connect.NewResponse(&ingressv1.DescribeResponse{
		Implementation:      &commonv1.Implementation{Name: "scripted-orchestrator", Protocols: []string{"edgerun.ingress.v1"}},
		MaxRequestBodyBytes: 1 << 20}), nil
}

func stNewFakeOrch(t testing.TB) *stFakeOrch {
	o := &stFakeOrch{}
	mux := http.NewServeMux()
	path, h := ingressv1connect.NewIngressServiceHandler(stDescribeOnly{})
	mux.Handle(path, h)
	mux.HandleFunc(ingressv1connect.IngressServiceInvokeProcedure, func(w http.ResponseWriter, r *http.Request) {
		var req ingressv1.InvokeRequest
		if err := stReadRequest(r, &req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		o.calls.add()
		seq := o.plan.Load()
		out := stNewOut(w, r)
		stPlay(out, stInvokeCodec, *seq)
	})
	o.srv = stServe(t, mux)
	return o
}

type stHTTPObs struct {
	status       int
	header       http.Header
	body         []byte
	transportErr error // no response at all
	bodyErr      error // a response whose body did not complete
	elapsed      time.Duration
}

func (o stHTTPObs) aborted() bool { return o.transportErr != nil || o.bodyErr != nil }

func stDo(client *http.Client, url, reqID string) stHTTPObs {
	start := time.Now()
	req, _ := http.NewRequest("GET", url+"/grammar", nil)
	req.Header.Set("X-Request-Id", reqID)
	resp, err := client.Do(req)
	if err != nil {
		return stHTTPObs{transportErr: err, elapsed: time.Since(start)}
	}
	defer resp.Body.Close()
	b, berr := io.ReadAll(resp.Body)
	return stHTTPObs{status: resp.StatusCode, header: resp.Header, body: b, bodyErr: berr, elapsed: time.Since(start)}
}

// stFrontendStatus is the documented status for what the frontend is told.
func stFrontendStatuses(e stExpect) []int {
	if e.violation != "" {
		if e.badHead && e.kind == stFailBefore {
			return []int{502}
		}
		if e.violation == "a stream that ends before the head" {
			return []int{502}
		}
		return []int{500, 502}
	}
	switch t := e.term; {
	case t == stSTALL:
		return []int{504}
	case t == stRESET, t == stTRUNC:
		return []int{500, 502, 503}
	}
	switch e.term {
	case "ERR:NO_CAPACITY", "ERR:unavailable":
		return []int{503}
	case "ERR:DEADLINE":
		return []int{504}
	case "ERR:TRAPPED", "ERR:ARTIFACT_NOT_FOUND", "ERR:TOO_LARGE":
		return []int{502}
	case "ERR:NOT_FOUND":
		return []int{404}
	case "ERR:resource_exhausted":
		return []int{413}
	}
	// INTERNAL, an unknown reason (EGRESS_*, 999), cancelled while the client is
	// still waiting, invalid_argument: "INTERNAL, unknown -> 500"
	return []int{500}
}

func stCheckFrontend(seq stSeq, e stExpect, o stHTTPObs, reqID string) (id, msg string) {
	bad := func(id, format string, a ...any) (string, string) { return id, fmt.Sprintf(format, a...) }
	if o.elapsed > stFrontendBudget+1500*time.Millisecond {
		return bad("FE-HANG", "took %v with a %v budget", o.elapsed, stFrontendBudget)
	}
	accepted := !o.aborted() && o.status >= 200 && o.status < 300
	switch e.kind {
	case stOK, stEither:
		if accepted {
			if o.status != 200 || !bytes.Equal(o.body, e.body) {
				return bad("FE-CORRUPT", "status %d, body %q, want 200 %q", o.status, o.body, e.body)
			}
			if o.header.Get("x-grammar") != "yes" || o.header.Get("X-Request-Id") != reqID {
				return bad("FE-HEADERS", "headers of a valid response: %v", o.header)
			}
			return "", ""
		}
		if e.kind == stOK {
			return bad("FE-REJECTED", "a valid stream was not served: status %d, transport %v, body %v", o.status, o.transportErr, o.bodyErr)
		}
		return "", "" // ambiguous: a failure is acceptable
	case stFailAfter:
		if !o.aborted() {
			// A success that hides the violation (or a failure that is not an abort).
			if o.status >= 200 && o.status < 300 {
				return bad("FE-HIDDEN", "%s: the client got a complete %d response with %q", e.violationOr(), o.status, o.body)
			}
			return bad("FE-AFTERHEAD-STATUS", "%s: failure after the head answered as a complete %d response", e.violationOr(), o.status)
		}
		return "", ""
	case stFailBefore:
		if o.aborted() && e.badHead {
			return "", "" // the 502 was out (or about to be) and the stream then failed
		}
		if o.aborted() {
			return bad("FE-ABORT-BEFORE-HEAD", "%s: the connection was aborted (%v %v) instead of answering with a status", e.violationOr(), o.transportErr, o.bodyErr)
		}
		if o.status >= 200 && o.status < 300 {
			return bad("FE-HIDDEN", "%s: the client got a %d with %q", e.violationOr(), o.status, o.body)
		}
		if want := stFrontendStatuses(e); !slices.Contains(want, o.status) {
			if e.violation == "" && (e.term == "ERR:EGRESS_FAILED" || e.term == "ERR:EGRESS_DENIED") {
				return bad("D-FE-UNMAPPED-REASON", "a typed reason the table has no row for (%s) is classified by its Connect code (%d), not 500 as 'INTERNAL, unknown' says", e.term, o.status)
			}
			return bad("FE-STATUS", "%s: status %d, want one of %v", e.violationOr(), o.status, want)
		}
		if o.status == 503 && o.header.Get("Retry-After") != "1" {
			return bad("FE-RETRY-AFTER", "503 without Retry-After: 1 (%q)", o.header.Get("Retry-After"))
		}
		if o.header.Get("X-Request-Id") != reqID {
			return bad("FE-REQID", "an error response without the request id: %q", o.header.Get("X-Request-Id"))
		}
		if bytes.Contains(o.body, []byte(stSecret)) {
			return bad("FE-LEAK", "the operator's message reached the client: %q", o.body)
		}
		return "", ""
	}
	return "", ""
}

func (e stExpect) violationOr() string {
	if e.violation != "" {
		return e.violation
	}
	return "the stream's error (" + e.term + ")"
}

func stFrontendSequences() []stSeq {
	syms := []string{stH, stHB, stC, stE, stN}
	terms := []string{stCLEAN, stTRUNC, stRESET,
		"ERR:NO_CAPACITY", "ERR:DEADLINE", "ERR:TRAPPED", "ERR:NOT_FOUND", "ERR:INTERNAL", "ERR:EGRESS_FAILED", "ERR:EGRESS_DENIED", "ERR:R999",
		"ERR:unavailable", "ERR:internal", "ERR:resource_exhausted", "ERR:cancelled"}
	seqs := stEnumerate(syms, 3, terms, []string{stSTALL}, 2)
	return append(seqs, stSample(syms, append(slices.Clone(terms), stSTALL), 250, 20240601)...)
}

// The frontend accepts valid streams and answers every violation and every
// failure as the protocol says, whatever the producer does, and stays whole.
func TestStreams_Invoke_Frontend(t *testing.T) {
	orch := stNewFakeOrch(t)
	fe, err := frontendapp.Start(context.Background(), frontendapp.Config{
		Listen: "127.0.0.1:0", OrchestratorURL: orch.srv.URL, IngressToken: ingressToken,
		RequestTimeout: stFrontendBudget, LimitRefresh: time.Hour, ShutdownTimeout: 2 * time.Second, Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fe.Stop(context.Background()) })
	url := "http://" + fe.Addr()
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 10 * time.Second}

	normal := stSeq{stH, stC, stCLEAN}
	run := func(seq stSeq, reqID string) stHTTPObs {
		orch.plan.Store(&seq)
		return stDo(client, url, reqID)
	}
	// warm up, and wait until the frontend has asked for its limit
	for i := 0; i < 3; i++ {
		run(normal, "warmup")
	}
	base, _ := stSettle(stSnapshot(), 1000, 1000, 100*time.Millisecond)
	base = stSnapshot()

	var f stFindings
	seqs := stFrontendSequences()
	start := time.Now()
	for i, seq := range seqs {
		exps := stAnalyze(false, seq)
		reqID := fmt.Sprintf("seq-%d", i)
		o := run(seq, reqID)
		if id, msg := stCheckAny(exps, func(e stExpect) (string, string) { return stCheckFrontend(seq, e, o, reqID) }); msg != "" {
			f.add(id, seq.String(), msg)
		}
		// still serving
		n := run(normal, "normal")
		if n.aborted() || n.status != 200 || string(n.body) != string(stChunk(1)) {
			f.add("FE-DEAD", seq.String(), fmt.Sprintf("the next, normal request: status %d %v %v", n.status, n.transportErr, n.bodyErr))
		}
		if cur, ok := stSettle(base, 6, 4, 3*time.Second); !ok {
			f.add("FE-LEAK-RES", seq.String(), fmt.Sprintf("did not return to the baseline (%v): now %v", base, cur))
			base = cur
		}
	}
	t.Logf("%d sequences in %v", len(seqs), time.Since(start).Round(time.Millisecond))
	if cur, ok := stSettle(base, 6, 4, 5*time.Second); !ok {
		f.add("FE-LEAK-RES", "(end)", fmt.Sprintf("after all sequences: %v, baseline %v", cur, base))
	}
	f.report(t, stFrontendKnown)
	_ = errors.Is
	_ = proto.Marshal
}

var stFrontendKnown = map[string]string{
	"D-FE-UNMAPPED-REASON": "edgerun.FrontendStatus: a typed reason with no row in the table (EGRESS_FAILED, EGRESS_DENIED, an unknown number) falls through to the Connect code: 503+Retry-After / 502 instead of 500",
}

// D-FE-UNMAPPED-REASON: protocol.md's table ends with "INTERNAL, unknown -> 500". An
// invocation that fails with a reason the frontend has no row for (the EGRESS_*
// reasons "occur on the egress seam only", but a producer may send them) is
// answered by the canonical Connect code of that reason, so EGRESS_FAILED
// (unavailable) becomes a 503 with Retry-After: 1, which tells clients and load
// balancers to retry something the table says is a 500.
func TestStreams_KnownDefect_FrontendUnmappedReason(t *testing.T) {
	stSkipKnown(t, "D-FE-UNMAPPED-REASON", "a typed EGRESS_FAILED from the orchestrator is a 503 + Retry-After, the table says unknown reasons are 500")
	orch := stNewFakeOrch(t)
	fe, err := frontendapp.Start(context.Background(), frontendapp.Config{
		Listen: "127.0.0.1:0", OrchestratorURL: orch.srv.URL, IngressToken: ingressToken,
		RequestTimeout: stFrontendBudget, LimitRefresh: time.Hour, Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fe.Stop(context.Background()) })
	seq := stSeq{"ERR:EGRESS_FAILED"}
	orch.plan.Store(&seq)
	o := stDo(&http.Client{Timeout: 5 * time.Second}, "http://"+fe.Addr(), "x")
	if o.status != 500 {
		t.Fatalf("status %d (Retry-After %q), want 500", o.status, o.header.Get("Retry-After"))
	}
}
