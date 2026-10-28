package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

// These tests drive the tool against a real orchestrator and worker, with the
// guest of testdata/guest deployed, and check the report against what is known
// to be true: how many requests were released, what the function answered, what
// the orchestrator recorded.

// shared is one stack reused by the tests that need no special worker.
func sharedStack(t *testing.T) *Stack {
	t.Helper()
	s := newStack(t, StackOpts{Concurrency: 64})
	s.Deploy(t, "mock", mockHost, env.guest)
	warm(t, s, mockHost)
	return s
}

func TestConstantRateStatusesAndCounts(t *testing.T) {
	s := sharedStack(t)
	code, rep, out := runOn(t, s, `
name: constant
defaults: {host: mock.test, budget: 5s}
seed: 7
requests:
  - {name: ok, weight: 3, path: /status/200}
  - {name: unavailable, weight: 1, path: /status/503}
load: {model: constant, rate: 40, duration: 3s}
`, "-run-id", "const1", "-expect", "errors==0", "-expect", "status.503<60%")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	tt := rep.Totals
	// The schedule is deterministic: arrivals at k/40 s for k < 120.
	if tt.Sent != 120 || tt.Attempted != 120 || tt.OK != 120 {
		t.Errorf("sent/attempted/ok = %d/%d/%d, want 120/120/120", tt.Sent, tt.Attempted, tt.OK)
	}
	if tt.PlatformErrors != 0 || tt.StreamFailures != 0 || len(rep.Reasons) != 0 {
		t.Errorf("a function answering 503 must not be an error: %+v %+v", tt, rep.Reasons)
	}
	if rep.Status["200"]+rep.Status["503"] != 120 {
		t.Errorf("statuses %v", rep.Status)
	}
	if got := rep.Templates["unavailable"].Sent; got != rep.Status["503"] {
		t.Errorf("template unavailable sent %d but 503s %d", got, rep.Status["503"])
	}
	between(t, "503 share", float64(rep.Status["503"]), 15, 50) // expect 30 of 120
	between(t, "rps_sent", tt.RPSSent, 38, 42)

	// Ground truth from the orchestrator, counted independently of the tool.
	n, workers := countRecords(t, s, "tm-const1-")
	if n != 120 {
		t.Errorf("the orchestrator recorded %d invocations for this run, want 120", n)
	}
	if len(workers) != 1 {
		t.Errorf("one worker, got %v", workers)
	}
	inv := rep.Invocations
	if inv == nil || inv.Found != 120 || inv.Expected != 120 || inv.IDMismatch+inv.StatusMismatch+inv.ReasonMismatch != 0 {
		t.Errorf("invocation lookup: %+v", inv)
	}
	if rep.Latency["ttfb"].Count != 120 || rep.Latency["total"].P50 <= 0 {
		t.Errorf("latency: %+v", rep.Latency)
	}
	if rep.Latency["total"].P50 < rep.Latency["ttfb"].P50 {
		t.Errorf("total (%v) cannot be shorter than the time to the head (%v)", rep.Latency["total"].P50, rep.Latency["ttfb"].P50)
	}
}

func countRecords(t testing.TB, s *Stack, prefix string) (int, map[string]int) {
	t.Helper()
	time.Sleep(1500 * time.Millisecond) // "Records appear shortly after the invocation"
	n, workers := 0, map[string]int{}
	for rec, err := range s.Ctl.AllInvocations(context.Background(), "") {
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(rec.RequestId, prefix) {
			n++
			workers[rec.WorkerId]++
		}
	}
	return n, workers
}

func TestPlatformErrorsAreMappedLikeAFrontend(t *testing.T) {
	s := sharedStack(t)
	code, rep, out := runOn(t, s, `
name: errors
defaults: {host: mock.test, budget: 1500ms}
requests:
  - {name: missing, host: nowhere.test, path: /status/200}
  - {name: crash, path: /trap}
  - {name: spin, path: /spin}
  - {name: fine, path: /status/200}
load: {model: constant, rate: 8, duration: 2s}
`, "-run-id", "err1")
	t.Log("\n" + out)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	tt := rep.Totals
	if tt.Sent != 16 {
		t.Fatalf("sent %d, want 16", tt.Sent)
	}
	want := map[string]int{"missing": 404, "crash": 502, "spin": 504, "fine": 200}
	got := map[string]struct {
		reason string
		status int
	}{"FUNCTION_NOT_FOUND": {"missing", 404}, "FUNCTION_TRAPPED": {"crash", 502}, "DEADLINE_EXCEEDED": {"spin", 504}}
	for reason, w := range got {
		rs, ok := rep.Reasons[reason]
		if !ok || int64(rs.Count) != rep.Templates[w.reason].Sent || rs.HTTPStatus != w.status {
			t.Errorf("%s: %+v (template %s sent %d), want status %d for all of them", reason, rs, w.reason, rep.Templates[w.reason].Sent, w.status)
		}
		if rep.Status[fmt.Sprint(w.status)] < rs.Count {
			t.Errorf("status %d count %d is less than %s count %d", w.status, rep.Status[fmt.Sprint(w.status)], reason, rs.Count)
		}
	}
	_ = want
	if rep.Reasons["DEADLINE_EXCEEDED"].Count > 0 {
		// The spinning requests ran for their whole budget.
		atLeast(t, "error latency p50 (ms)", rep.Latency["errors"].P99, 1400)
		t.Logf("DEADLINE_EXCEEDED typed by the orchestrator: %d, by the local timer: %d", rep.DeadlineSource.Orchestrator, rep.DeadlineSource.LocalTimer)
	}
	if inv := rep.Invocations; inv != nil && len(inv.LocalDeadlineRecordedAs) > 0 {
		// An observation, not a promise of the protocol: when the caller's timer wins the race
		// the orchestrator can record the cancellation it saw, not the deadline.
		t.Logf("deadline requests that ended on this tool's timer are recorded as: %v", inv.LocalDeadlineRecordedAs)
	}
	// FUNCTION_NOT_FOUND leaves no invocation record; the others do.
	inv := rep.Invocations
	if inv == nil || inv.Found != inv.Expected || inv.ReasonMismatch != 0 {
		t.Errorf("invocation records: %+v", inv)
	}
	if want := int(tt.Sent - rep.Templates["missing"].Sent); inv != nil && inv.Expected != want {
		t.Errorf("expected records for the %d requests that were routed (not-found have none), got %d", want, inv.Expected)
	}
	if rep.Totals.OK != rep.Templates["fine"].OK || rep.Totals.OK == 0 {
		t.Errorf("ok %d, fine ok %d", rep.Totals.OK, rep.Templates["fine"].OK)
	}
}

func TestNoCapacityUnderASpike(t *testing.T) {
	s := newStack(t, StackOpts{Concurrency: 2})
	s.Deploy(t, "mock", mockHost, env.guest)
	warm(t, s, mockHost)
	code, rep, out := runOn(t, s, `
name: spike
defaults: {host: mock.test, budget: 5s}
requests: [{path: /sleep/600}]
load:
  model: spike
  duration: 1s
  bursts: [{at: 100ms, count: 20}]
`, "-run-id", "cap1")
	t.Log("\n" + out)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	nc := rep.Reasons["NO_CAPACITY"]
	if rep.Totals.Sent != 20 || rep.Totals.OK+nc.Count != 20 {
		t.Errorf("sent %d, ok %d, NO_CAPACITY %d: every request is one or the other", rep.Totals.Sent, rep.Totals.OK, nc.Count)
	}
	// Two slots, nothing queues: at most a few can succeed in the 600ms the first two hold their slots.
	between(t, "ok", float64(rep.Totals.OK), 2, 5)
	if nc.HTTPStatus != 503 || rep.Status["503"] != nc.Count {
		t.Errorf("NO_CAPACITY must read as 503: %+v, status %v", nc, rep.Status)
	}
	for _, e := range rep.Samples.Errors {
		if e.Reason == "NO_CAPACITY" && !e.RetryAfter {
			t.Errorf("NO_CAPACITY maps to 503 with Retry-After: 1: %+v", e)
		}
	}
	// A refusal comes back at once; a success took 600ms.
	atLeast(t, "ok latency p50", rep.Latency["total"].P50, 590)
	if rep.Latency["errors"].P99 > 500 {
		t.Errorf("NO_CAPACITY should be immediate, p99 was %vms", rep.Latency["errors"].P99)
	}
	if inv := rep.Invocations; inv == nil || inv.Found != 20 {
		t.Errorf("NO_CAPACITY invocations are recorded too: %+v", inv)
	}
	// What a client can learn from the refusal itself: no retry hint beyond the reason.
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.Ing.Invoke(context.Background(), &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: mockHost, Path: "/sleep/300"}})
			if err == nil {
				r.ReadAll()
				return
			}
			var ce *connect.Error
			if errors.As(err, &ce) && edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY) {
				t.Logf("NO_CAPACITY: code %v, message %q, error metadata %v, %d detail(s), info %v", ce.Code(), ce.Message(), ce.Meta(), len(ce.Details()), edgerun.Info(err))
			}
		}()
	}
	wg.Wait()
}

func TestRamp(t *testing.T) {
	s := sharedStack(t)
	code, rep, _ := runOn(t, s, `
name: ramp
defaults: {host: mock.test, budget: 5s}
requests: [{path: /status/200}]
load: {model: ramp, from: 10, to: 70, duration: 4s}
`)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	// integral of 10..70 over 4 s = 160 requests
	between(t, "sent", float64(rep.Totals.Sent), 150, 170)
	tl := rep.Timeline
	if len(tl) < 4 {
		t.Fatalf("timeline %+v", tl)
	}
	first, last := tl[0].Sent, tl[3].Sent
	if last < 3*first {
		t.Errorf("the rate should climb: first second %d, fourth %d (%+v)", first, last, tl)
	}
	if rep.Totals.PlatformErrors != 0 {
		t.Errorf("errors: %+v", rep.Reasons)
	}
}

func TestSpikeAndBurst(t *testing.T) {
	s := sharedStack(t)
	code, rep, _ := runOn(t, s, `
name: spike
defaults: {host: mock.test, budget: 5s}
requests: [{path: /status/200}]
load:
  model: spike
  rate: 5
  duration: 4s
  spikes: [{at: 2s, duration: 1s, rate: 100}]
  bursts: [{at: 500ms, count: 40}]
`)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	// 3 s at 5/s + 1 s at 100/s + a burst of 40
	between(t, "sent", float64(rep.Totals.Sent), 15+100+40-5, 15+100+40+8)
	tl := rep.Timeline
	if len(tl) < 4 || tl[2].Sent < 90 || tl[0].Sent < 40 || tl[1].Sent > 10 {
		t.Errorf("timeline should show the burst in second 0 and the spike in second 2: %+v", tl)
	}
	if rep.Totals.PlatformErrors != 0 {
		t.Errorf("errors: %+v", rep.Reasons)
	}
}

func TestClosedLoop(t *testing.T) {
	s := sharedStack(t)
	code, rep, _ := runOn(t, s, `
name: closed
defaults: {host: mock.test, budget: 5s}
requests: [{path: /sleep/100}]
load: {model: closed, users: 4, think: 100ms, duration: 3s}
`)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	// one cycle is >= 200 ms, so a user makes at most 15 requests in 3 s
	between(t, "sent", float64(rep.Totals.Sent), 40, 60)
	if rep.Totals.PeakInFlight > 4 {
		t.Errorf("a closed loop of 4 users had %d requests in flight", rep.Totals.PeakInFlight)
	}
	atLeast(t, "p50 latency", rep.Latency["total"].P50, 100)
	if rep.Totals.Dropped != 0 || rep.Totals.PlatformErrors != 0 {
		t.Errorf("%+v", rep.Totals)
	}
}

func TestReplay(t *testing.T) {
	s := sharedStack(t)
	var trace strings.Builder
	echoBytes := 0
	for i := 0; i < 30; i++ {
		at := float64(i) / 10 // 3 s of trace
		switch i % 3 {
		case 0:
			fmt.Fprintf(&trace, `{"t": %v, "path": "/status/%d"}`+"\n", at, 200+i%5)
		case 1:
			body := strings.Repeat("a", 100+i)
			echoBytes += len(body)
			fmt.Fprintf(&trace, `{"t": "%dms", "method": "POST", "path": "/echo", "body": %q}`+"\n", int(at*1000), body)
		case 2:
			echoBytes += 2048
			fmt.Fprintf(&trace, `{"t": %v, "method": "POST", "path": "/echo", "body_size": 2048, "headers": {"content-type": "application/octet-stream"}}`+"\n", at)
		}
	}
	tracePath := writeFile(t, "trace.jsonl", trace.String())
	scen := fmt.Sprintf("name: replay\ndefaults: {host: mock.test, budget: 5s}\nreplay: {file: %s}\n", tracePath)
	for _, c := range []struct {
		speed      string
		minD, maxD float64
	}{{"1", 2.8, 4.5}, {"3", 0.9, 2.5}, {"max", 0, 2.5}} {
		t.Run("speed "+c.speed, func(t *testing.T) {
			code, rep, out := runOn(t, s, scen, "-speed", c.speed)
			if code != 0 {
				t.Fatalf("exit %d\n%s", code, out)
			}
			if rep.Totals.Sent != 30 || rep.Totals.OK != 30 {
				t.Errorf("sent %d ok %d, want 30", rep.Totals.Sent, rep.Totals.OK)
			}
			// The /echo entries return exactly what was sent; /status/N returns "status N\n".
			if want := int64(echoBytes + 10*len("status 200\n")); rep.Totals.BytesReceived != want {
				t.Errorf("received %d bytes, want %d", rep.Totals.BytesReceived, want)
			}
			between(t, "load seconds", rep.LoadSeconds, c.minD, c.maxD)
			if inv := rep.Invocations; inv == nil || inv.Found != 30 {
				t.Errorf("records: %+v", inv)
			}
		})
	}
}

func TestRequestFidelity(t *testing.T) {
	s := sharedStack(t)
	// What the function sees is what a frontend would have sent: lower-cased
	// headers, a request id, the client's address in x-forwarded-for, the exact body.
	sc := &Scenario{Name: "f", Load: &LoadSpec{Model: "constant", Rate: 1, Duration: time.Second}, Defaults: Defaults{Host: mockHost, Budget: 5 * time.Second}, Clients: 4,
		Requests: []TemplateSp{{Name: "h", Method: "POST", Path: "/headers?a=1&b=%20x", Headers: map[string]StrOrSeq{
			"X-Custom": {"one", "two"}, "Connection": {"close"}, "X-Forwarded-For": {"6.6.6.6"}, "Content-Type": {"text/plain"}},
			Body: &BodySpec{Inline: ptr("hello")}}}}
	if err := sc.Validate(); err != nil {
		t.Fatal(err)
	}
	c, err := sc.compile()
	if err != nil {
		t.Fatal(err)
	}
	p := c.tmpls[0].build(nil, 7, "fid", sc.Clients)
	lim := NewLimits(s.Ing)
	st := NewStats(time.Now())
	iv := NewInvoker(s.Ing, lim, st, nil, time.Now())
	cap := &capture{}
	p.Sink = cap
	res := iv.Do(context.Background(), p)
	if res.Outcome != OutOK || res.Status != 200 {
		t.Fatalf("%+v", res)
	}
	body := cap.buf.String()
	for _, want := range []string{
		"HTTP_X_REQUEST_ID=tm-fid-7", "HTTP_X_CUSTOM=one, two", "HTTP_X_FORWARDED_PROTO=http", "HTTP_X_FORWARDED_HOST=mock.test",
		"CONTENT_TYPE=text/plain", "CONTENT_LENGTH=5", "REQUEST_METHOD=POST", "PATH_INFO=/headers", "QUERY_STRING=a=1&b=%20x", "body_len=5",
		"HTTP_X_FORWARDED_FOR=10.0.0.", // seq 7 % 4 clients = 3: an address of ours
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the function did not see %q in:\n%s", want, body)
		}
	}
	for _, bad := range []string{"6.6.6.6", "HTTP_CONNECTION"} {
		if strings.Contains(body, bad) {
			t.Errorf("%q must not reach the function:\n%s", bad, body)
		}
	}
	if res.InvocationID == "" {
		t.Error("no invocation id")
	}
	// The invocation record carries the request id and the invocation id of the head.
	time.Sleep(1500 * time.Millisecond)
	found := false
	for rec, err := range s.Ctl.AllInvocations(context.Background(), "") {
		if err != nil {
			t.Fatal(err)
		}
		if rec.RequestId == "tm-fid-7" {
			found = true
			if rec.Id != res.InvocationID {
				t.Errorf("record id %q, head said %q", rec.Id, res.InvocationID)
			}
			if rec.Status != 200 || rec.Path != "/headers" || rec.Method != "POST" {
				t.Errorf("record %+v", rec)
			}
		}
	}
	if !found {
		t.Error("no invocation record for the request id")
	}
}

func ptr[T any](v T) *T { return &v }

type capture struct {
	buf  strings.Builder
	head int
}

func (c *capture) Head(inv *edgerun.Invocation) error { c.head = inv.Status(); return nil }
func (c *capture) Write(p []byte) (int, error)        { return c.buf.Write(p) }

// The orchestrator's limit (6 MiB here) is read from Describe: a frontend
// answers 413 itself above it, and what the orchestrator does with a body that
// is sent anyway depends on how far over it is.
func TestBodyLimitFromDescribe(t *testing.T) {
	s := sharedStack(t)
	var code int
	var rep *Report
	var out string
	code, rep, out = runOn(t, s, `
name: bodies
defaults: {host: mock.test, budget: 15s}
requests:
  - {name: at_limit, path: /status/200, method: POST, body: {size: 6MiB, kind: zeros}}
  - {name: over_refused_locally, path: /status/200, method: POST, body: {size: 6300KiB, kind: zeros}}
  - {name: over_sent_anyway, path: /status/200, method: POST, allow_oversize: true, body: {size: 6300KiB, kind: zeros}}
  - {name: far_over_sent_anyway, path: /status/200, method: POST, allow_oversize: true, body: {size: 8MiB, kind: zeros}}
load: {model: closed, users: 1, iterations: 24}
seed: 1
`, "-run-id", "big1")
	t.Log("\n" + out)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	if rep.Orchestrator.MaxRequestBody != 6<<20 || !rep.Orchestrator.LimitFromDescribe {
		t.Errorf("limit: %+v", rep.Orchestrator)
	}
	// Four iterations of one user pick templates at random: look at what each saw.
	for name, ts := range rep.Templates {
		t.Logf("%s: sent %d ok %d errors %d", name, ts.Sent, ts.OK, ts.Errors)
	}
	if got := rep.Totals.Rejected; got != rep.Templates["over_refused_locally"].Sent {
		t.Errorf("rejected locally %d, want %d", got, rep.Templates["over_refused_locally"].Sent)
	}
	if rep.Templates["over_refused_locally"].Sent > 0 {
		if rep.Reasons["LOCAL_REQUEST_TOO_LARGE"].HTTPStatus != 413 {
			t.Errorf("%+v", rep.Reasons)
		}
	}
	if n := rep.Templates["over_sent_anyway"].Sent; n > 0 && rep.Reasons["REQUEST_TOO_LARGE"].Count != n {
		t.Errorf("a body just over the limit is refused by the orchestrator with REQUEST_TOO_LARGE: %+v", rep.Reasons)
	}
	if n := rep.Templates["far_over_sent_anyway"].Sent; n > 0 && rep.Reasons["UNTYPED_RESOURCE_EXHAUSTED"].Count != n {
		t.Errorf("a body far over the limit is refused by the transport, untyped: %+v", rep.Reasons)
	}
	if rep.Templates["at_limit"].Sent > 0 && rep.Templates["at_limit"].OK != rep.Templates["at_limit"].Sent {
		t.Errorf("a body of exactly the limit must be accepted: %+v", rep.Templates["at_limit"])
	}
}

// What "time to the head" means with this stack: the worker buffers a guest's
// whole response before it streams it, so the head arrives when the function
// has finished, not when it first wrote.
func TestTimeToHeadIsTheFunctionRunTime(t *testing.T) {
	s := sharedStack(t)
	code, rep, out := runOn(t, s, `
name: ttfb
defaults: {host: mock.test, budget: 10s}
requests:
  - {name: slow_small, path: /sleep/400}
  - {name: fast_big, path: /bytes/4000000}
load: {model: closed, users: 1, iterations: 6}
seed: 2
`)
	t.Log("\n" + out)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	if rep.Templates["slow_small"].Sent == 0 || rep.Templates["fast_big"].Sent == 0 {
		t.Skip("the random mix did not draw both")
	}
	ls := rep.Latency
	t.Logf("ttfb p50 %.1fms, p99 %.1fms; total p50 %.1fms, p99 %.1fms", ls["ttfb"].P50, ls["ttfb"].P99, ls["total"].P50, ls["total"].P99)
	atLeast(t, "ttfb p99 (>= the 400ms sleep)", ls["ttfb"].P99, 395)
	if rep.Totals.BytesReceived < 4_000_000*rep.Templates["fast_big"].OK {
		t.Errorf("bytes %d", rep.Totals.BytesReceived)
	}
}

// The example function of the repository, deployed as it is, through a
// scenario that needs no knowledge of this module's guest.
func TestHelloExample(t *testing.T) {
	s := newStack(t, StackOpts{})
	s.Deploy(t, "hello", "hello.test", env.hello)
	warm(t, s, "hello.test")
	code, rep, out := runOn(t, s, `
defaults: {host: hello.test}
requests: [{paths: ["/", "/?name=a"], headers: {x-test: "1"}}]
load: {model: constant, rate: 30, duration: 2s}
`, "-expect", "errors==0")
	if code != 0 || rep.Totals.OK != 60 {
		t.Fatalf("%d %+v\n%s", code, rep.Totals, out)
	}
}

// Observations about the orchestrator that matter to a load generator, logged
// rather than asserted where they are not the protocol's promise.
func TestOrchestratorObservations(t *testing.T) {
	s := sharedStack(t)
	// 1. Is the response compressed on the wire (Connect's default)?
	rt := &recordingTransport{rt: http.DefaultTransport}
	ing := edgerun.NewIngress(s.URL, ingressToken, edgerun.WithHTTPClient(&http.Client{Transport: rt}))
	for i := 0; i < 5; i++ {
		r, err := ing.Invoke(context.Background(), &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: mockHost, Path: "/bytes/100000"}})
		if err != nil {
			t.Fatal(err)
		}
		r.ReadAll()
	}
	t.Logf("client advertises Connect-Accept-Encoding: %v; orchestrator answered gzip on %d of %d calls", rt.accept.Load(), rt.gzipped.Load(), rt.total.Load())

	// 2. How long until an invocation record can be read back, and what does the lookup cost?
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("lag-%d", i)
		r, err := s.Ing.Invoke(context.Background(), &ingressv1.InvokeRequest{RequestId: id, Request: &commonv1.HttpRequest{Method: "GET", Host: mockHost, Path: "/status/200"}})
		if err != nil {
			t.Fatal(err)
		}
		r.ReadAll()
		t0 := time.Now()
		for time.Since(t0) < 10*time.Second {
			found := false
			for rec, err := range s.Ctl.AllInvocations(context.Background(), "") {
				if err != nil {
					t.Fatal(err)
				}
				if rec.RequestId == id {
					found = true
				}
				break // the newest page is enough: the record is the newest
			}
			if found {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Logf("invocation record visible %v after the response ended", time.Since(t0).Round(time.Millisecond))
	}
	// 3. A frontend-style request id is echoed nowhere in the response (head has only the invocation id).
	r, _ := s.Ing.Invoke(context.Background(), &ingressv1.InvokeRequest{RequestId: "echo-me", Request: &commonv1.HttpRequest{Method: "GET", Host: mockHost, Path: "/status/200"}})
	t.Logf("head headers: %v, invocation id %q", r.Header(), r.ID)
	r.ReadAll()
}
