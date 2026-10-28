package main

import (
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestScenarioYAMLAndJSONAreTheSame(t *testing.T) {
	y := writeFile(t, "a.yaml", `
name: x
defaults: {host: a.test, budget: 3s}
requests:
  - {name: r, weight: 2, method: post, paths: [/a, "/b?c=1"], headers: {X-A: [1, 2], X-B: q}, body: {size: 4KiB, kind: text}}
load: {model: ramp, from: 1, to: 9, duration: 10s}
`)
	j := writeFile(t, "a.json", `{"name":"x","defaults":{"host":"a.test","budget":"3s"},"requests":[{"name":"r","weight":2,"method":"post","paths":["/a","/b?c=1"],"headers":{"X-A":["1","2"],"X-B":"q"},"body":{"size":"4KiB","kind":"text"}}],"load":{"model":"ramp","from":1,"to":9,"duration":"10s"}}`)
	a, err := LoadScenario(y)
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadScenario(j)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := a.compile()
	cb, _ := b.compile()
	ta, tb := ca.tmpls[0], cb.tmpls[0]
	if ta.method != "POST" || len(ta.paths) != 2 || ta.paths[1] != (pathQuery{"/b", "c=1"}) || len(ta.body) != 4096 || len(ta.static) != 3 {
		t.Errorf("%+v", ta)
	}
	if string(ta.body) != string(tb.body) || ta.budget != 3*time.Second || tb.budget != 3*time.Second || len(tb.static) != 3 {
		t.Errorf("json: %+v", tb)
	}
}

func TestScenarioErrors(t *testing.T) {
	for name, c := range map[string]struct{ in, want string }{
		"unknown field":  {"defaults: {host: a}\nrequests: [{path: /}]\nload: {model: constant, rate: 1, duration: 1s}\nbogus: 1\n", "bogus"},
		"no load":        {"requests: [{path: /}]\n", "exactly one of load"},
		"both":           {"requests: [{path: /}]\nload: {model: constant, rate: 1, duration: 1s}\nreplay: {file: x}\n", "exactly one of load"},
		"no host":        {"requests: [{path: /}]\nload: {model: constant, rate: 1, duration: 1s}\n", "no host"},
		"bad model":      {"load: {model: x}\nrequests: [{path: /}]\n", "load.model"},
		"bad path":       {"defaults: {host: a}\nrequests: [{path: x}]\nload: {model: constant, rate: 1, duration: 1s}\n", "must start with /"},
		"bad size":       {"defaults: {host: a}\nrequests: [{path: /, body: {size: lots}}]\nload: {model: constant, rate: 1, duration: 1s}\n", "bad size"},
		"two bodies":     {"defaults: {host: a}\nrequests: [{path: /, body: {inline: a, size: 5}}]\nload: {model: constant, rate: 1, duration: 1s}\n", "exactly one"},
		"integer budget": {"defaults: {host: a, budget: 5}\nrequests: [{path: /}]\nload: {model: constant, rate: 1, duration: 1s}\n", "Duration"},
		"closed no stop": {"defaults: {host: a}\nrequests: [{path: /}]\nload: {model: closed, users: 2}\n", "duration or iterations"},
	} {
		sc, err := LoadScenario(writeFile(t, "s.yaml", c.in))
		if err == nil {
			_, err = sc.compile()
		}
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want it to mention %q", name, err, c.want)
		}
	}
}

func TestSizes(t *testing.T) {
	for in, want := range map[string]int64{"10": 10, "1KiB": 1024, "6MiB": 6 << 20, "1.5MiB": 1572864, "2MB": 2e6, "3 B": 3} {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("%q: %d %v", in, got, err)
		}
	}
}

func TestTemplateChoiceIsWeightedAndReproducible(t *testing.T) {
	sc := &Scenario{Defaults: Defaults{Host: "a"}, Seed: 5, Load: &LoadSpec{Model: "constant", Rate: 1, Duration: time.Second},
		Requests: []TemplateSp{{Name: "heavy", Weight: 9, Path: "/h"}, {Name: "light", Weight: 1, Path: "/l"}}}
	if err := sc.Validate(); err != nil {
		t.Fatal(err)
	}
	c, _ := sc.compile()
	count := func() map[string]int {
		rng := rand.New(rand.NewPCG(5, 1))
		m := map[string]int{}
		for range 10000 {
			m[c.pick(rng).name]++
		}
		return m
	}
	a, b := count(), count()
	if a["heavy"] != b["heavy"] {
		t.Error("the same seed must give the same mix")
	}
	between(t, "heavy share", float64(a["heavy"])/10000, 0.88, 0.92)
}

func TestGeneratedBodiesAreCutFromOneBuffer(t *testing.T) {
	sc := &Scenario{Defaults: Defaults{Host: "a"}, Load: &LoadSpec{Model: "constant", Rate: 1, Duration: time.Second},
		Requests: []TemplateSp{{Path: "/", Body: &BodySpec{SizeMin: 10, SizeMax: 1000}}}}
	sc.Validate()
	c, err := sc.compile()
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(1, 2))
	sizes := map[int]bool{}
	for i := range 200 {
		p := c.tmpls[0].build(rng, uint64(i), "r", 8)
		n := len(p.Req.Request.Body)
		if n < 10 || n > 1000 {
			t.Fatalf("size %d", n)
		}
		sizes[n] = true
	}
	if len(sizes) < 50 {
		t.Errorf("sizes should vary: %d distinct", len(sizes))
	}
}

func TestFilterHeadersLikeAFrontend(t *testing.T) {
	h := http.Header{}
	h.Add("X-One", "a")
	h.Add("X-One", "b")
	h.Set("Connection", "close")
	h.Set("Transfer-Encoding", "chunked")
	h.Set("X-Forwarded-For", "1.2.3.4")
	h.Set("Host", "ignored")
	got := filterHeaders(h, false)
	if len(got) != 2 || got[0].Name != "x-one" || got[0].Value != "a" || got[1].Value != "b" {
		t.Errorf("%v", got)
	}
	if got := filterHeaders(h, true); len(got) != 3 {
		t.Errorf("trusting a proxy keeps x-forwarded-for: %v", got)
	}
}

func TestTraceOffsets(t *testing.T) {
	for in, want := range map[string]time.Duration{`0.25`: 250 * time.Millisecond, `"1.5s"`: 1500 * time.Millisecond, `3`: 3 * time.Second, ``: 0} {
		e := TraceEntry{}
		e.T = []byte(in)
		if got, err := e.offset(); err != nil || got != want {
			t.Errorf("%q: %v %v", in, got, err)
		}
	}
}

func TestRateModels(t *testing.T) {
	l := &LoadSpec{Model: "ramp", From: 10, To: 110, Duration: 10 * time.Second}
	if r := rateAt(l, 5*time.Second); r != 60 {
		t.Errorf("ramp midpoint %v", r)
	}
	l = &LoadSpec{Model: "spike", Rate: 5, Spikes: []Spike{{At: time.Second, Duration: time.Second, Rate: 50}}}
	if rateAt(l, 500*time.Millisecond) != 5 || rateAt(l, 1500*time.Millisecond) != 50 || rateAt(l, 2*time.Second) != 5 {
		t.Error("spike window")
	}
}

func snapshotOf(results ...*Result) *Snapshot {
	st := NewStats(time.Now())
	for i, r := range results {
		r.Seq = uint64(i + 1)
		st.Released(time.Now())
		st.Record(r)
	}
	return st.Snapshot()
}

func TestExpectationEvaluation(t *testing.T) {
	var rs []*Result
	for i := 0; i < 100; i++ {
		rs = append(rs, &Result{Outcome: OutOK, Status: 200, FuncStatus: 200, Template: "t", TTFB: time.Duration(i+1) * time.Millisecond, Total: time.Duration(i+1) * 2 * time.Millisecond})
	}
	rs = append(rs, &Result{Outcome: OutPlatformError, Status: 503, Reason: "NO_CAPACITY", Template: "t", Total: time.Millisecond})
	rs = append(rs, &Result{Outcome: OutOK, Status: 503, FuncStatus: 503, Template: "t", TTFB: time.Millisecond, Total: time.Millisecond})
	sn := snapshotOf(rs...)
	rep := &Report{}
	rep.Totals.RPSCompleted = 50
	for expr, want := range map[string]bool{
		"p50<=102ms":                  true, // total p50 of 1..100 *2ms
		"p50<101ms":                   true,
		"p99>=198ms":                  true,
		"ttfb.p99<101ms":              true,
		"ttfb.p99<90ms":               false,
		"max<=201ms":                  true,
		"errors<0.5%":                 false, // 1 of 102
		"errors<=1%":                  true,
		"errors.count==1":             true,
		"status.503.count==2":         true,
		"status.5xx>1%":               true,
		"status.2xx>90%":              true,
		"reason.NO_CAPACITY.count==1": true,
		"reason.FUNCTION_TRAPPED==0":  true,
		"rps>=50":                     true,
		"rps<50":                      false,
		"sent==102":                   true,
		"all.p50<=102ms":              true,
		"p999>199ms":                  true,
		"total.mean>100ms":            true,
	} {
		ex := Evaluate([]string{expr}, sn, rep)[0]
		if ex.Error != "" || ex.Pass != want {
			t.Errorf("%s: pass=%v (%v, %s) err=%q, want %v", expr, ex.Pass, ex.Value, ex.Unit, ex.Error, want)
		}
	}
}

func TestStatsAccounting(t *testing.T) {
	sn := snapshotOf(
		&Result{Outcome: OutOK, Status: 200, FuncStatus: 200, Template: "a", Total: time.Millisecond, TTFB: time.Millisecond, Bytes: 10},
		&Result{Outcome: OutOK, Status: 500, FuncStatus: 500, Template: "a", Total: time.Millisecond, TTFB: time.Millisecond},
		&Result{Outcome: OutPlatformError, Status: 504, Reason: "DEADLINE_EXCEEDED", LocalTimer: true, Template: "b", Total: time.Second},
		&Result{Outcome: OutStreamFailure, Status: 0, Reason: "INTERNAL", FuncStatus: 200, Template: "b", TTFB: time.Millisecond, Total: time.Second, Bytes: 5},
		&Result{Outcome: OutRejected, Status: 413, Reason: "LOCAL_REQUEST_TOO_LARGE", Template: "b"},
		&Result{Outcome: OutDropped, Template: "b"},
	)
	if sn.Attempted() != 4 || sn.ErrorCount() != 2 || sn.Outcomes[OutRejected] != 1 || sn.Outcomes[OutDropped] != 1 {
		t.Errorf("%+v", sn.Outcomes)
	}
	if sn.Status[500] != 1 || sn.Status[504] != 1 || sn.Status[0] != 1 || sn.Status[413] != 1 || sn.FuncStatus[500] != 1 {
		t.Errorf("status %v func %v", sn.Status, sn.FuncStatus)
	}
	if sn.DeadlineLocal != 1 || sn.DeadlineTyped != 0 || sn.Reasons["STREAM:INTERNAL"] != 1 || sn.Bytes != 15 {
		t.Errorf("%+v %v %d", sn.DeadlineLocal, sn.Reasons, sn.Bytes)
	}
	if sn.TTFB.TotalCount() != 3 || sn.Total.TotalCount() != 2 || sn.ErrLat.TotalCount() != 2 {
		t.Errorf("histograms: ttfb %d total %d err %d", sn.TTFB.TotalCount(), sn.Total.TotalCount(), sn.ErrLat.TotalCount())
	}
}

func TestScenarioFilesInRepoLoad(t *testing.T) {
	files, _ := filepath.Glob("scenarios/*.yaml")
	if len(files) == 0 {
		t.Skip("no scenarios")
	}
	for _, f := range files {
		if _, err := os.Stat(f); err != nil {
			t.Fatal(err)
		}
		sc, err := LoadScenario(f)
		if err != nil {
			t.Errorf("%s: %v", f, err)
			continue
		}
		if sc.Load != nil {
			if _, err := sc.compile(); err != nil {
				t.Errorf("%s: %v", f, err)
			}
		}
	}
}

func TestArrivalSchedules(t *testing.T) {
	count := func(l *LoadSpec) (int, []time.Duration) {
		g := newArrivals(l)
		var at []time.Duration
		for {
			d, ok := g.next()
			if !ok {
				return len(at), at
			}
			at = append(at, d)
		}
	}
	// constant: exactly rate*duration, however the division rounds
	for _, c := range []struct {
		rate float64
		d    time.Duration
		want int
	}{{30, 2 * time.Second, 60}, {40, 3 * time.Second, 120}, {7, 10 * time.Second, 70}, {1000, time.Second, 1000}, {3, 1500 * time.Millisecond, 5}} {
		if n, _ := count(&LoadSpec{Model: "constant", Rate: c.rate, Duration: c.d}); n != c.want {
			t.Errorf("constant %v for %v: %d arrivals, want %d", c.rate, c.d, n, c.want)
		}
	}
	// ramp 10 -> 70 over 4 s: 160 requests, climbing
	n, at := count(&LoadSpec{Model: "ramp", From: 10, To: 70, Duration: 4 * time.Second})
	between(t, "ramp arrivals", float64(n), 155, 165)
	first, last := 0, 0
	for _, a := range at {
		if a < time.Second {
			first++
		}
		if a >= 3*time.Second {
			last++
		}
	}
	if last < 3*first {
		t.Errorf("ramp: %d in the first second, %d in the last", first, last)
	}
	// a spike that starts in the middle of a slow interval must still start on time
	n, at = count(&LoadSpec{Model: "spike", Rate: 5, Duration: 4 * time.Second, Spikes: []Spike{{At: 2 * time.Second, Duration: time.Second, Rate: 100}}})
	inWindow := 0
	for _, a := range at {
		if a >= 2*time.Second && a < 3*time.Second {
			inWindow++
		}
	}
	if inWindow < 99 || inWindow > 101 || n < 113 || n > 118 {
		t.Errorf("spike: %d in the window, %d in all", inWindow, n)
	}
	// bursts only: no arrivals from the rate
	if n, _ := count(&LoadSpec{Model: "spike", Duration: time.Second, Bursts: []Burst{{At: 0, Count: 5}}}); n != 0 {
		t.Errorf("no base rate, got %d", n)
	}
}
