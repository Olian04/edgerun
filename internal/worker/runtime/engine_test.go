package runtime

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Olian04/edgerun/conformance/guest"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

const digest = "sha256:test"

type fixture struct {
	e    *Engine
	wasm []byte
}

// The engine is shared by the tests: compiling the guest is the slow part
// (under -race, seconds), and sharing it also exercises the compiled-module
// cache the way a long-running worker does.
var shared struct {
	once sync.Once
	f    *fixture
}

func TestMain(m *testing.M) {
	code := m.Run()
	if shared.f != nil {
		shared.f.e.Close(context.Background())
	}
	os.Exit(code)
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	shared.once.Do(func() {
		e, err := New(Config{})
		if err != nil {
			t.Fatal(err)
		}
		shared.f = &fixture{e: e, wasm: guest.Build(t)}
	})
	if shared.f == nil {
		t.Fatal("shared fixture failed to initialise")
	}
	return shared.f
}

func (f *fixture) run(t *testing.T, path, query string, body string, limits *commonv1.Limits, timeout time.Duration) (*Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return f.e.Run(ctx, Invocation{
		Digest: digest, Wasm: f.wasm, Limits: limits,
		Request: &commonv1.HttpRequest{
			Method: "POST", Host: "h", Path: path, RawQuery: query, Body: []byte(body),
			Headers: []*commonv1.Header{{Name: "content-type", Value: "text/x-test"}},
		},
		Env: []*commonv1.EnvVar{{Name: "MY_VAR", Value: "v1"}},
	})
}

func wantReason(t *testing.T, err error, want commonv1.ErrorReason) *Error {
	t.Helper()
	var re *Error
	if !errors.As(err, &re) {
		t.Fatalf("err = %v (%T), want *Error with %v", err, err, want)
	}
	if re.Reason != want {
		t.Fatalf("reason = %v (%s), want %v", re.Reason, re.Msg, want)
	}
	return re
}

func TestEcho(t *testing.T) {
	f := newFixture(t)
	res, err := f.run(t, "/echo", "", "hello", nil, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if res.Head.Status != 200 || string(res.Body) != "hello" {
		t.Errorf("status %d body %q", res.Head.Status, res.Body)
	}
	if got := res.Head.Headers[0]; got.Name != "content-type" || got.Value != "text/x-test" {
		t.Errorf("content-type header = %v", got)
	}
	if res.PeakMemoryBytes == 0 || res.Duration == 0 {
		t.Errorf("measurements missing: peak=%d dur=%v", res.PeakMemoryBytes, res.Duration)
	}
}

func TestEnvAndStatusAndHeaders(t *testing.T) {
	f := newFixture(t)
	res, err := f.run(t, "/env", "a=b", "xyz", nil, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"MY_VAR=v1", "REQUEST_METHOD=POST", "PATH_INFO=/env", "QUERY_STRING=a=b", "CONTENT_LENGTH=3", "CONTENT_TYPE=text/x-test", "SERVER_NAME=h"} {
		if !strings.Contains(string(res.Body), want+"\n") {
			t.Errorf("env missing %q in:\n%s", want, res.Body)
		}
	}

	res, err = f.run(t, "/status", "code=418", "", nil, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if res.Head.Status != 418 {
		t.Errorf("status = %d", res.Head.Status)
	}
	var cookies []string
	for _, h := range res.Head.Headers {
		if h.Name == "set-cookie" {
			cookies = append(cookies, h.Value)
		}
	}
	if strings.Join(cookies, ",") != "a=1,b=2" {
		t.Errorf("duplicate headers not preserved in order: %v", cookies)
	}
}

func TestFunctionErrorStatusIsNotAPlatformError(t *testing.T) {
	f := newFixture(t)
	res, err := f.run(t, "/status", "code=500", "", nil, 30*time.Second)
	if err != nil || res.Head.Status != 500 {
		t.Fatalf("a function answering 500 must be a successful invocation: res=%v err=%v", res, err)
	}
}

func TestStderrCaptured(t *testing.T) {
	f := newFixture(t)
	res, err := f.run(t, "/stderr", "", "", nil, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Stderr) != "log line one\nlog line two\n" || res.StderrTruncated {
		t.Errorf("stderr = %q truncated=%v", res.Stderr, res.StderrTruncated)
	}
}

func TestTraps(t *testing.T) {
	f := newFixture(t)
	for _, path := range []string{"/exit", "/panic"} {
		_, err := f.run(t, path, "code=3", "", nil, 30*time.Second)
		re := wantReason(t, err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED)
		if path == "/exit" && !strings.Contains(re.Msg, "exiting on request") {
			t.Errorf("trap message should carry the stderr tail, got %q", re.Msg)
		}
	}
}

func TestDeadline(t *testing.T) {
	f := newFixture(t)
	// Warm the compile cache first so the deadline measures execution.
	if _, err := f.run(t, "/echo", "", "", nil, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err := f.run(t, "/spin", "", "", nil, 300*time.Millisecond)
	wantReason(t, err, commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED)
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("a spinning guest was not interrupted promptly: %v", d)
	}
}

func TestSleepingGuestIsInterrupted(t *testing.T) {
	f := newFixture(t)
	if _, err := f.run(t, "/echo", "", "", nil, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err := f.run(t, "/sleep", "ms=20000", "", nil, 300*time.Millisecond)
	wantReason(t, err, commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED)
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("a sleeping guest was not interrupted promptly: %v", d)
	}
}

func TestGuestSeesRealRandomnessAndTime(t *testing.T) {
	f := newFixture(t)
	a, err := f.run(t, "/random", "", "", nil, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.run(t, "/random", "", "", nil, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(strings.TrimSpace(string(a.Body))) != 32 || string(a.Body) == string(b.Body) {
		t.Errorf("entropy must differ between invocations (wazero's default source is deterministic): %q vs %q", a.Body, b.Body)
	}

	res, err := f.run(t, "/time", "", "", nil, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got, perr := strconv.ParseInt(strings.TrimSpace(string(res.Body)), 10, 64)
	if perr != nil || absDiff(got, time.Now().Unix()) > 5 {
		t.Errorf("guest clock = %q, want within 5s of now (wazero's default clock is fake)", res.Body)
	}
}

func absDiff(a, b int64) int64 {
	if a < b {
		return b - a
	}
	return a - b
}

func TestSleepActuallySleeps(t *testing.T) {
	f := newFixture(t)
	start := time.Now()
	if _, err := f.run(t, "/sleep", "ms=400", "", nil, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 400*time.Millisecond {
		t.Errorf("guest slept %v, want >= 400ms (wazero's default sleep is a no-op)", d)
	}
}

func TestMemoryLimitIsPerInvocation(t *testing.T) {
	f := newFixture(t)
	small := &commonv1.Limits{MemoryLimitBytes: 64 << 20}
	large := &commonv1.Limits{MemoryLimitBytes: 512 << 20}

	// 200 MiB does not fit in 64 MiB...
	_, err := f.run(t, "/memhog", "mb=200", "", small, 60*time.Second)
	wantReason(t, err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED)
	// ...but the same module, same worker, fits in 512 MiB.
	res, err := f.run(t, "/memhog", "mb=200", "", large, 60*time.Second)
	if err != nil {
		t.Fatalf("large limit: %v", err)
	}
	if res.PeakMemoryBytes < 200<<20 {
		t.Errorf("peak = %d, want >= 200MiB", res.PeakMemoryBytes)
	}
}

func TestResponseTooLarge(t *testing.T) {
	f := newFixture(t)
	start := time.Now()
	_, err := f.run(t, "/big", "bytes=1048576", "", &commonv1.Limits{MaxResponseBytes: 64 << 10}, 30*time.Second)
	wantReason(t, err, commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE)
	_ = start

	res, err := f.run(t, "/big", "bytes=32768", "", &commonv1.Limits{MaxResponseBytes: 64 << 10}, 30*time.Second)
	if err != nil || len(res.Body) < 32768 {
		t.Fatalf("under the cap must succeed: len=%d err=%v", len(res.Body), err)
	}
}

func TestMalformedOutputIsABIViolation(t *testing.T) {
	f := newFixture(t)
	for _, path := range []string{"/noseparator", "/empty"} {
		_, err := f.run(t, path, "", "", nil, 30*time.Second)
		wantReason(t, err, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
	}
}

func TestABIValidation(t *testing.T) {
	e, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	run := func(digest string, wasm []byte) error {
		_, err := e.Run(context.Background(), Invocation{Digest: digest, Wasm: wasm,
			Request: &commonv1.HttpRequest{Method: "GET", Path: "/"}})
		return err
	}

	// (module (import "env" "f" (func)) (func (export "_start")))
	importsEnv := []byte{0, 'a', 's', 'm', 1, 0, 0, 0,
		1, 4, 1, 0x60, 0, 0,
		2, 9, 1, 3, 'e', 'n', 'v', 1, 'f', 0, 0,
		3, 2, 1, 0,
		7, 10, 1, 6, '_', 's', 't', 'a', 'r', 't', 0, 1,
		10, 4, 1, 2, 0, 0x0b}
	re := wantReason(t, run("sha256:imports-env", importsEnv), commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
	if !strings.Contains(re.Msg, "env.f") {
		t.Errorf("message should name the offending import: %q", re.Msg)
	}

	// (module): valid, but not a command.
	noStart := []byte{0, 'a', 's', 'm', 1, 0, 0, 0}
	re = wantReason(t, run("sha256:no-start", noStart), commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
	if !strings.Contains(re.Msg, "_start") {
		t.Errorf("message should mention _start: %q", re.Msg)
	}

	wantReason(t, run("sha256:garbage", []byte("definitely not wasm")), commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
}

func TestConcurrentRunsShareOneCompile(t *testing.T) {
	f := newFixture(t)
	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	start := time.Now()
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := f.run(t, "/echo", "", "x", nil, 60*time.Second)
			if err == nil && string(res.Body) != "x" {
				err = errors.New("wrong body")
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	t.Logf("%d concurrent first-time runs took %v", n, time.Since(start))
}

func TestBucketSize(t *testing.T) {
	e, _ := New(Config{})
	defer e.Close(context.Background())
	for _, tc := range []struct{ in, want uint64 }{
		{0, 128 * mib}, {1, 16 * mib}, {16 * mib, 16 * mib}, {16*mib + 1, 32 * mib},
		{100 * mib, 128 * mib}, {512 * mib, 512 * mib}, {10 << 30, 512 * mib},
	} {
		if got := e.bucketSize(tc.in); got != tc.want {
			t.Errorf("bucketSize(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestTailBuffer(t *testing.T) {
	tb := &tailBuffer{max: 5}
	tb.Write([]byte("abc"))
	if string(tb.Bytes()) != "abc" || tb.truncated {
		t.Fatalf("%q %v", tb.Bytes(), tb.truncated)
	}
	tb.Write([]byte("defg"))
	if string(tb.Bytes()) != "cdefg" || !tb.truncated {
		t.Fatalf("%q %v", tb.Bytes(), tb.truncated)
	}
	tb.Write([]byte("0123456789"))
	if string(tb.Bytes()) != "56789" {
		t.Fatalf("%q", tb.Bytes())
	}
}
