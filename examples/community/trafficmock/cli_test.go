package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExpectationsAndExitCodes(t *testing.T) {
	url, _ := startFake(t, newFake())
	scen := writeFile(t, "s.yaml", `
defaults: {host: mock.test, budget: 2s}
requests:
  - {weight: 9, path: /status/200}
  - {weight: 1, path: /trap}
seed: 3
load: {model: constant, rate: 100, duration: 1s}
`)
	base := []string{"-scenario", scen, "-url", url, "-token", ingressToken}
	cases := []struct {
		name string
		args []string
		code int
	}{
		{"passes", []string{"-expect", "p99<1s", "-expect", "errors<30%", "-expect", "status.2xx>=70%", "-expect", "reason.FUNCTION_TRAPPED.count>0", "-expect", "sent==100"}, 0},
		{"latency violated", []string{"-expect", "p99<1us"}, 1},
		{"errors violated", []string{"-expect", "errors<1%"}, 1},
		{"one of two violated", []string{"-expect", "p50<1s", "-expect", "errors<1%"}, 1},
		{"ttfb", []string{"-expect", "ttfb.p99<1us"}, 1},
		{"bad expression", []string{"-expect", "p99 is small"}, 2},
		{"bad unit", []string{"-expect", "p99<5"}, 2},
		{"unknown metric", []string{"-expect", "wat<1"}, 2},
		{"from the scenario", nil, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, rep, out := tm(t, append(append([]string{}, base...), c.args...)...)
			if code != c.code {
				t.Errorf("exit %d, want %d\n%s", code, c.code, out)
			}
			if code != 2 && rep == nil {
				t.Error("no report")
			}
		})
	}
	// The scenario file may carry the expectations itself.
	code, _, _ := tm(t, "-scenario", writeFile(t, "s2.yaml", `
defaults: {host: mock.test}
requests: [{path: /status/200}]
load: {model: constant, rate: 50, duration: 500ms}
expect: ["p99<1us"]
`), "-url", url, "-token", ingressToken)
	if code != 1 {
		t.Errorf("an expectation in the scenario file: exit %d, want 1", code)
	}
}

func TestBinaryExitStatus(t *testing.T) {
	requireStack(t)
	// the real executable: usage errors, an unreachable orchestrator, a wrong token
	run := func(args ...string) (int, string) {
		cmd := exec.Command(env.tmBin, args...)
		var b bytes.Buffer
		cmd.Stdout, cmd.Stderr = &b, &b
		err := cmd.Run()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
		return code, b.String()
	}
	if code, out := run(); code != 2 {
		t.Errorf("no args: %d %s", code, out)
	}
	scen := writeFile(t, "s.yaml", "defaults: {host: a.test}\nrequests: [{path: /}]\nload: {model: constant, rate: 5, duration: 1s}\n")
	if code, out := run("run", "-scenario", scen, "-url", "http://127.0.0.1:1", "-token", "x"); code != 3 {
		t.Errorf("unreachable: %d %s", code, out)
	}
	url, _ := startFake(t, newFake())
	if code, out := run("run", "-scenario", scen, "-url", url, "-token", "wrong"); code != 3 || !strings.Contains(out, "unauthenticated") {
		t.Errorf("wrong token: %d %s", code, out)
	}
	if code, out := run("run", "-scenario", scen, "-url", url, "-token", ingressToken, "-expect", "errors<1%", "-quiet"); code != 0 {
		t.Errorf("ok: %d %s", code, out)
	}
	if code, _ := run("run", "-scenario", scen, "-url", url, "-token", ingressToken, "-expect", "p99<1us", "-quiet"); code != 1 {
		t.Errorf("violated: %d", code)
	}
	bad := writeFile(t, "bad.yaml", "load: {model: warp}\n")
	if code, out := run("run", "-scenario", bad, "-url", url); code != 2 {
		t.Errorf("bad scenario: %d %s", code, out)
	}
}

func TestPrometheusEndpointWhileRunning(t *testing.T) {
	url, _ := startFake(t, newFake())
	port := freePort(t)
	addr := "127.0.0.1:" + itoa(port)
	var wg sync.WaitGroup
	var mid, last string
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(700 * time.Millisecond)
		mid = scrape(t, addr)
	}()
	code, rep, _ := tm(t, "-scenario", writeFile(t, "s.yaml", `
defaults: {host: mock.test, budget: 2s}
requests:
  - {weight: 1, path: /status/200}
  - {weight: 1, path: /status/500}
  - {weight: 1, path: /trap}
  - {weight: 1, host: missing.test, path: /}
  - {weight: 1, path: /midfail/10}
load: {model: constant, rate: 100, duration: 1500ms}
`), "-url", url, "-token", ingressToken, "-prom", addr)
	wg.Wait()
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	_ = last
	for _, want := range []string{
		`trafficmock_requests_total{run_id=`, `outcome="ok"`, `trafficmock_responses_total{`, `status="404"`, `status="502"`, `status="500"`,
		`trafficmock_platform_errors_total{`, `reason="FUNCTION_NOT_FOUND",http_status="404"`, `reason="FUNCTION_TRAPPED"`, `reason="STREAM:INTERNAL"`,
		`trafficmock_ttfb_seconds_bucket{`, `le="+Inf"`, `trafficmock_total_seconds_count`, `trafficmock_in_flight`,
	} {
		if !strings.Contains(mid, want) {
			t.Errorf("metrics (mid-run) lack %q", want)
		}
	}
	if !strings.Contains(mid, "trafficmock_requests_released_total") || strings.Contains(mid, `released_total{run_id="`+rep.RunID+`"} 0`) {
		t.Errorf("a scrape in the middle of the run should already show requests:\n%s", mid)
	}
}

func scrape(t testing.TB, addr string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://"+addr+"/metrics", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("scrape: %v", err)
		return ""
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return string(b)
}

func TestRequestLogAndSamples(t *testing.T) {
	url, _ := startFake(t, newFake())
	logPath := t.TempDir() + "/req.jsonl"
	code, rep, _ := tm(t, "-scenario", writeFile(t, "s.yaml", `
defaults: {host: mock.test}
requests: [{weight: 4, path: /status/200}, {weight: 1, path: /trap}]
load: {model: constant, rate: 100, duration: 1s}
`), "-url", url, "-token", ingressToken, "-requests-log", logPath, "-run-id", "log1")
	if code != 0 {
		t.Fatal(code)
	}
	b := mustRead(t, logPath)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 100 {
		t.Fatalf("%d lines, want 100", len(lines))
	}
	if !strings.Contains(lines[0], `"request_id":"tm-log1-`) {
		t.Errorf("request ids should be correlated: %s", lines[0])
	}
	if len(rep.Samples.Errors) == 0 || rep.Samples.Errors[0].Reason != "FUNCTION_TRAPPED" || rep.Samples.Errors[0].RequestID == "" {
		t.Errorf("error samples: %+v", rep.Samples.Errors)
	}
	if len(rep.Samples.Slowest) == 0 || rep.Samples.Slowest[0].InvocationID == "" {
		t.Errorf("slowest samples should keep invocation ids: %+v", rep.Samples.Slowest)
	}
}
