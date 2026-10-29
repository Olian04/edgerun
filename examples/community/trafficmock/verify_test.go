package main

import (
	"bytes"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// feed gives the check a response in chunks of the given sizes (the last chunk
// takes the rest).
func feed(c *echoCheck, resp []byte, chunk int) {
	for len(resp) > 0 {
		n := min(chunk, len(resp))
		c.Write(resp[:n])
		resp = resp[n:]
	}
}

func TestEchoCheckExactAndMismatches(t *testing.T) {
	body := append(stampOf("tm-a-1", "a"), bytes.Repeat([]byte("0123456789"), 500)...)
	flip := func(i int) []byte {
		b := append([]byte(nil), body...)
		b[i] ^= 1
		return b
	}
	other := append(stampOf("tm-b-9", "b"), body[len(stampOf("tm-a-1", "a")):]...)
	for _, chunk := range []int{1, 7, 64, 4999, 1 << 20} {
		for name, c := range map[string]struct {
			resp []byte
			want string // "" = verified; else a part of the detail
		}{
			"exact":              {body, ""},
			"first byte":         {flip(0), "differs at byte 0 of"},
			"middle byte":        {flip(2500), "differs at byte 2500 of"},
			"last byte":          {flip(len(body) - 1), "differs at byte " + itoa(len(body)-1) + " of"},
			"truncated":          {body[:len(body)-1], "truncated: " + itoa(len(body)-1) + " of " + itoa(len(body))},
			"extra byte":         {append(append([]byte(nil), body...), 'x'), "differs at byte " + itoa(len(body)) + " of"},
			"empty":              {nil, "empty body"},
			"someone else's":     {other, "carries the stamp of request tm-b-9 (run b)"},
			"another's, shorter": {other[:100], "carries the stamp of request tm-b-9 (run b)"},
		} {
			ec := newEchoCheck(verifyEcho, "tm-a-1", body)
			feed(ec, c.resp, chunk)
			got := ec.finish("tm-a-1")
			if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
				t.Errorf("chunk %d, %s: %q, want %q", chunk, name, got, c.want)
			}
		}
	}
}

func TestEchoCheckWithID(t *testing.T) {
	body := append(stampOf("tm-a-1", "a"), "padding"...)
	good := append([]byte("tm-a-1\n"), body...)
	for _, chunk := range []int{1, 3, 1 << 20} {
		ec := newEchoCheck(verifyEchoWithID, "tm-a-1", body)
		feed(ec, good, chunk)
		if got := ec.finish("tm-a-1"); got != "" {
			t.Errorf("chunk %d: %s", chunk, got)
		}
		// The right body with someone else's id in front: the id line is wrong.
		ec = newEchoCheck(verifyEchoWithID, "tm-a-1", body)
		feed(ec, append([]byte("tm-b-1\n"), body...), chunk)
		if got := ec.finish("tm-a-1"); !strings.Contains(got, "differs at byte 3 of") {
			t.Errorf("chunk %d: %q", chunk, got)
		}
		// The plain echo of the body is not the id-prefixed echo.
		ec = newEchoCheck(verifyEchoWithID, "tm-a-1", body)
		feed(ec, body, chunk)
		if got := ec.finish("tm-a-1"); got == "" {
			t.Errorf("chunk %d: a response without the id line verified", chunk)
		}
	}
}

func TestEchoCheckEmptyBodyIsAValidEcho(t *testing.T) {
	ec := newEchoCheck(verifyEcho, "x", nil)
	if got := ec.finish("x"); got != "" {
		t.Errorf("an empty body echoed as an empty body: %q", got)
	}
	ec = newEchoCheck(verifyEcho, "x", nil)
	ec.Write([]byte("z"))
	if got := ec.finish("x"); got == "" {
		t.Error("a byte where nothing was sent verified")
	}
}

func compileOne(t *testing.T, y string) (*Scenario, *tmpl) {
	t.Helper()
	sc, err := LoadScenario(writeFile(t, "s.yaml", y))
	if err != nil {
		t.Fatal(err)
	}
	c, err := sc.compile()
	if err != nil {
		t.Fatal(err)
	}
	return sc, c.tmpls[0]
}

func TestStampedBodies(t *testing.T) {
	sc, tp := compileOne(t, `
seed: 3
defaults: {host: a.test}
requests:
  - {name: s, method: POST, path: /echo, verify: echo, body: {stamp: true, size_min: 10, size_max: 5000}}
load: {model: constant, rate: 1, duration: 1s}
`)
	rng := rand.New(rand.NewPCG(sc.Seed, 1))
	seen := map[string]bool{}
	padding := map[string]bool{}
	sizes := map[int]bool{}
	for seq := uint64(1); seq <= 400; seq++ {
		p := tp.build(rng, seq, "run1", 8)
		body := p.Req.Request.Body
		stamp := stampOf(requestID("run1", seq), "run1")
		if p.Req.RequestId != requestID("run1", seq) || !bytes.HasPrefix(body, stamp) {
			t.Fatalf("seq %d: body starts %q", seq, body[:min(len(body), 60)])
		}
		if string(stamp) != "tm1 tm-run1-"+itoa(int(seq))+" run1\n" {
			t.Fatalf("stamp format: %q", stamp)
		}
		pad := body[len(stamp):]
		if len(pad) < 10 || len(pad) > 5000 {
			t.Fatalf("padding %d bytes, want 10..5000", len(pad))
		}
		sizes[len(pad)] = true
		if seen[string(body)] {
			t.Fatalf("seq %d: a body that was sent before", seq)
		}
		seen[string(body)] = true
		padding[string(pad[:min(len(pad), 16)])] = true
		if p.Verify != verifyEcho {
			t.Fatal("verify mode is not carried to the request")
		}
	}
	if len(sizes) < 100 || len(padding) < 20 {
		t.Errorf("%d distinct padding sizes and %d distinct padding starts in 400 requests: not varied", len(sizes), len(padding))
	}
	// Reproducible: the same seed makes the same bodies.
	r1, r2 := rand.New(rand.NewPCG(9, 1)), rand.New(rand.NewPCG(9, 1))
	for seq := uint64(1); seq < 20; seq++ {
		if !bytes.Equal(tp.build(r1, seq, "r", 8).Req.Request.Body, tp.build(r2, seq, "r", 8).Req.Request.Body) {
			t.Fatal("the same seed made different bodies")
		}
	}
}

func TestStampOnly(t *testing.T) {
	_, tp := compileOne(t, `
defaults: {host: a.test}
requests: [{path: /echo, verify: echo-with-id, body: {stamp: true}}]
load: {model: constant, rate: 1, duration: 1s}
`)
	p := tp.build(rand.New(rand.NewPCG(1, 1)), 5, "q", 1)
	if string(p.Req.Request.Body) != "tm1 tm-q-5 q\n" {
		t.Errorf("%q", p.Req.Request.Body)
	}
	if p.Verify != verifyEchoWithID {
		t.Error(p.Verify)
	}
}

func TestVerifyScenarioErrors(t *testing.T) {
	for name, c := range map[string]struct{ body, want string }{
		"verify needs a stamp":  {`{path: /echo, verify: echo, body: {size: 100}}`, "needs body.stamp"},
		"verify without a body": {`{path: /echo, verify: echo}`, "needs body.stamp"},
		"unknown mode":          {`{path: /echo, verify: same, body: {stamp: true}}`, "echo or echo-with-id"},
		"stamp with inline":     {`{path: /echo, body: {stamp: true, inline: hi}}`, "stamp goes with size"},
		"stamp with file":       {`{path: /echo, body: {stamp: true, file: x}}`, "stamp goes with size"},
		"stamp, bad kind":       {`{path: /echo, body: {stamp: true, size: 5, kind: pi}}`, "body.kind"},
		"stamp, min above max":  {`{path: /echo, body: {stamp: true, size_min: 9, size_max: 5}}`, "size_min > size_max"},
	} {
		sc, err := LoadScenario(writeFile(t, "s.yaml", "defaults: {host: a}\nrequests: ["+c.body+"]\nload: {model: constant, rate: 1, duration: 1s}\n"))
		if err == nil {
			_, err = sc.compile()
		}
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want it to mention %q", name, err, c.want)
		}
	}
}

func echoScenario(extra string) string {
	return `
name: echo
seed: 4
defaults: {host: mock.test, budget: 5s}
requests:
  - {name: echo, path: /echo, method: POST, ` + extra + `, body: {stamp: true, size_min: 0, size_max: 20KiB}}
load: {model: closed, users: 8, iterations: 25}
`
}

func TestVerifyAgainstAnEchoingFake(t *testing.T) {
	for name, c := range map[string]struct {
		path, extra string
	}{
		"echo":         {"/echo", "verify: echo, verify_header: X-Echo-Request-Id"},
		"echo-with-id": {"/echo-with-id", "verify: echo-with-id, verify_header: x-echo-request-id"},
	} {
		t.Run(name, func(t *testing.T) {
			url, _ := startFake(t, newFake())
			sc := strings.Replace(echoScenario(c.extra), "path: /echo,", "path: "+c.path+",", 1)
			code, rep, out := tm(t, "-scenario", writeFile(t, "s.yaml", sc), "-url", url, "-token", ingressToken, "-run-id", "ok1",
				"-expect", "echo_mismatches==0", "-expect", "echo_checked==200", "-expect", "echo_body_mismatches==0", "-expect", "echo_header_mismatches==0")
			if code != 0 {
				t.Fatalf("exit %d\n%s", code, out)
			}
			if rep.Echo == nil || rep.Echo.Checked != 200 || rep.Echo.Mismatches != 0 || len(rep.Samples.Mismatches) != 0 || len(rep.Warnings) != 0 {
				t.Errorf("%+v %v", rep.Echo, rep.Warnings)
			}
			if !strings.Contains(out, "response verification: 200 checked, 0 mismatches") {
				t.Errorf("the text report says nothing about verification:\n%s", out)
			}
		})
	}
}

// misdelivering is a fake that, serialised, answers each request with the body
// of the previous one: what an orchestrator or a frontend that mixed up
// responses would do.
func TestVerifyCatchesAnotherRequestsResponse(t *testing.T) {
	f := newFake()
	var mu sync.Mutex
	var prev []byte
	f.Corrupt = func(id string, body []byte) []byte {
		mu.Lock()
		defer mu.Unlock()
		out := prev
		if out == nil {
			out = body
		}
		prev = body
		return out
	}
	url, _ := startFake(t, f)
	logPath := t.TempDir() + "/req.jsonl"
	code, rep, out := tm(t, "-scenario", writeFile(t, "s.yaml", echoScenario("verify: echo")), "-url", url, "-token", ingressToken, "-run-id", "mix",
		"-requests-log", logPath, "-expect", "echo_mismatches==0", "-expect", "echo_checked==200", "-expect", "echo_mismatches==199")
	if code != 1 {
		t.Fatalf("exit %d, want 1 (the first expectation fails)\n%s", code, out)
	}
	if rep.Echo.Checked != 200 || rep.Echo.Mismatches != 199 || rep.Echo.BodyMismatches != 199 || rep.Echo.HdrMismatches != 0 {
		t.Errorf("%+v", rep.Echo)
	}
	// 3 expectations: ==0 fails, ==200 passes, ==199 passes.
	if len(rep.Expect) != 3 || rep.Expect[0].Pass || !rep.Expect[1].Pass || !rep.Expect[2].Pass {
		t.Errorf("%+v", rep.Expect)
	}
	if rep.Totals.OK != 200 || rep.Totals.PlatformErrors != 0 {
		t.Errorf("a misdelivery is not a platform error and the status was fine: %+v", rep.Totals)
	}
	// The sample is bounded, says whose request it is, and says what came back instead.
	ms := rep.Samples.Mismatches
	if len(ms) != sampleErr {
		t.Fatalf("%d samples, want the bound %d", len(ms), sampleErr)
	}
	for _, m := range ms {
		if !strings.HasPrefix(m.RequestID, "tm-mix-") || m.Verify != VerifyBody || m.InvocationID == "" ||
			!strings.Contains(m.VerifyDetail, "carries the stamp of request tm-mix-") || m.VerifyDetail == "" {
			t.Fatalf("%+v", m)
		}
		if strings.Contains(m.VerifyDetail, "carries the stamp of request "+m.RequestID+" ") {
			t.Fatalf("a mismatch that carries its own stamp: %+v", m)
		}
	}
	if !strings.Contains(out, "response verification: 200 checked, 199 mismatches (body 199, header 0)") || !strings.Contains(out, "NOT the response to their request") ||
		!strings.Contains(out, "FAIL  echo_mismatches==0") {
		t.Errorf("text report:\n%s", out)
	}
	// The request log says per request.
	n, bad := 0, 0
	for _, l := range strings.Split(strings.TrimSpace(string(mustRead(t, logPath))), "\n") {
		n++
		if strings.Contains(l, `"verify":"body_mismatch"`) {
			bad++
		} else if !strings.Contains(l, `"verify":"ok"`) {
			t.Errorf("line without a verdict: %s", l)
		}
	}
	if n != 200 || bad != 199 {
		t.Errorf("request log: %d lines, %d mismatches", n, bad)
	}
}

func TestVerifyCatchesTruncationFlipsAndHeaders(t *testing.T) {
	for name, c := range map[string]struct {
		corrupt  func(string, []byte) []byte
		extra    string
		kind     string
		inDetail string
	}{
		"truncated": {func(_ string, b []byte) []byte { return b[:len(b)/2] }, "verify: echo", "body", "truncated"},
		"one byte": {func(_ string, b []byte) []byte {
			o := append([]byte(nil), b...)
			o[len(o)-1] ^= 0xff
			return o
		}, "verify: echo", "body", "differs at byte"},
		"padding appended": {func(_ string, b []byte) []byte { return append(append([]byte(nil), b...), "x"...) }, "verify: echo", "body", "differs at byte"},
		"nothing":          {func(_ string, b []byte) []byte { return nil }, "verify: echo", "body", "empty body"},
		"header absent":    {nil, "verify: echo, verify_header: x-no-such", "header", "header x-no-such absent"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake()
			f.Corrupt = c.corrupt
			url, _ := startFake(t, f)
			// the first request has no padding at all (size_min 0): the bytes are all stamp
			code, rep, out := tm(t, "-scenario", writeFile(t, "s.yaml", echoScenario(c.extra)), "-url", url, "-token", ingressToken, "-expect", "echo_mismatches==0")
			if code != 1 || rep.Echo.Mismatches == 0 {
				t.Fatalf("exit %d, %+v\n%s", code, rep.Echo, out)
			}
			if c.kind == "body" && (rep.Echo.BodyMismatches != rep.Echo.Mismatches || rep.Echo.HdrMismatches != 0) ||
				c.kind == "header" && (rep.Echo.HdrMismatches != rep.Echo.Mismatches || rep.Echo.BodyMismatches != 0) {
				t.Errorf("%s: %+v", c.kind, rep.Echo)
			}
			if d := rep.Samples.Mismatches[0].VerifyDetail; !strings.Contains(d, c.inDetail) {
				t.Errorf("detail %q does not mention %q", d, c.inDetail)
			}
		})
	}
}

func TestVerifyHeaderWrongValueAndBoth(t *testing.T) {
	// A response head with the right header name and another request's id, and a wrong body.
	f := newFake()
	f.Corrupt = func(id string, b []byte) []byte { return append([]byte("zz"), b...) }
	url, _ := startFake(t, f)
	_, rep, _ := tm(t, "-scenario", writeFile(t, "s.yaml", echoScenario("verify: echo, verify_header: x-echo-request-id")), "-url", url, "-token", ingressToken)
	if rep.Echo.Mismatches != 200 || rep.Echo.BodyMismatches != 200 || rep.Echo.HdrMismatches != 0 {
		t.Errorf("%+v", rep.Echo)
	}
	// checkHeader itself: wrong value, duplicated, case-insensitive name.
	for name, c := range map[string]struct {
		hs   []*commonv1.Header
		want string
	}{
		"right":      {[]*commonv1.Header{{Name: "X-Echo", Value: "id1"}}, ""},
		"wrong":      {[]*commonv1.Header{{Name: "x-echo", Value: "id2"}}, `header x-echo is ["id2"], want "id1"`},
		"duplicated": {[]*commonv1.Header{{Name: "x-echo", Value: "id1"}, {Name: "x-echo", Value: "id1"}}, "want"},
		"absent":     {[]*commonv1.Header{{Name: "other", Value: "id1"}}, "absent"},
	} {
		if got := checkHeader(c.hs, "x-echo", "id1"); !strings.Contains(got, c.want) || (c.want == "") != (got == "") {
			t.Errorf("%s: %q, want %q", name, got, c.want)
		}
	}
}

func TestVerifyIsConcurrencySafe(t *testing.T) {
	// Many users at once, one fake that swaps pairs of responses: the counts
	// must be exact and -race clean.
	f := newFake()
	var mu sync.Mutex
	var held []byte
	f.Corrupt = func(id string, b []byte) []byte {
		mu.Lock()
		defer mu.Unlock()
		if held == nil {
			held = b
			return b // the first of a pair is answered right
		}
		out := held
		held = nil
		return out // the second gets the first's body
	}
	url, _ := startFake(t, f)
	sc := strings.Replace(echoScenario("verify: echo"), "users: 8, iterations: 25", "users: 32, iterations: 20", 1)
	_, rep, _ := tm(t, "-scenario", writeFile(t, "s.yaml", sc), "-url", url, "-token", ingressToken)
	if rep.Echo.Checked != 640 || rep.Echo.Mismatches != 320 {
		t.Errorf("%+v, want 640 checked and every second one (320) wrong", rep.Echo)
	}
}

func TestVerifyMetricsAndExpectErrors(t *testing.T) {
	// Prometheus output carries the counters even at zero.
	st := NewStats(time.Now())
	lim := NewLimits(nil)
	rec := httptest.NewRecorder()
	st.Record(&Result{Seq: 1, Outcome: OutOK, Status: 200, FuncStatus: 200, Verify: VerifyOK})
	st.Record(&Result{Seq: 2, Outcome: OutOK, Status: 200, FuncStatus: 200, Verify: VerifyBodyHdr, VerifyDetail: "x"})
	st.Record(&Result{Seq: 3, Outcome: OutOK, Status: 200, FuncStatus: 200, Verify: VerifyHeader})
	st.Record(&Result{Seq: 4, Outcome: OutOK, Status: 200, FuncStatus: 200})
	metricsHandler(st, "r1", lim, nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Result().Body)
	for _, want := range []string{
		`trafficmock_echo_checked_total{run_id="r1"} 3`,
		`trafficmock_echo_mismatches_total{run_id="r1",kind="body"} 1`,
		`trafficmock_echo_mismatches_total{run_id="r1",kind="header"} 2`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics lack %q", want)
		}
	}
	sn := st.Snapshot()
	if sn.EchoChecked != 3 || sn.EchoMismatches != 2 || len(sn.Mismatched) != 2 {
		t.Errorf("%d %d %d", sn.EchoChecked, sn.EchoMismatches, len(sn.Mismatched))
	}
	rep := &Report{}
	for expr, want := range map[string]bool{
		"echo_mismatches==2": true, "echo_mismatches==0": false, "echo_checked>=3": true,
		"echo_body_mismatches==1": true, "echo_header_mismatches<2": false, "echo_mismatches.count==2": true,
	} {
		ex := Evaluate([]string{expr}, sn, rep)
		if ex[0].Error != "" || ex[0].Pass != want {
			t.Errorf("%s: %+v, want pass=%v", expr, ex[0], want)
		}
	}
}

// An orchestrator that says "cancelled" when the caller cancelled nothing (a
// SIGKILLed worker produced this against the real stack: connect-go reports
// "connection reset by peer" as CodeCanceled) is a platform error, not a
// cancellation by the caller, and not an answer to be left unwritten.
func TestCancelledByPeerIsAPlatformError(t *testing.T) {
	url, _ := startFake(t, newFake())
	code, rep, out := tm(t, "-scenario", writeFile(t, "s.yaml", `
defaults: {host: mock.test, budget: 3s}
requests: [{path: /canceled}]
load: {model: closed, users: 2, iterations: 5}
`), "-url", url, "-token", ingressToken, "-expect", "canceled==0", "-expect", "status.500==100%")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if rep.Totals.PlatformErrors != 10 || rep.Totals.Canceled != 0 || rep.Reasons[ReasonCanceledByPeer].Count != 10 || rep.Reasons[ReasonCanceledByPeer].HTTPStatus != 500 {
		t.Errorf("%+v %+v", rep.Totals, rep.Reasons)
	}
}
