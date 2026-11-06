package e2e

// Part 1(b) and 1(c), the other direction: the real components RECEIVE answers
// from scripted peers. A peer is dressed three ways: plain (the control, so that
// a failure is the dressing's), with unknown fields of every wire type at every
// level, and as a v1.1 peer built from the mutated descriptors, which also adds
// a member to the stream `part` oneof and an ErrorReason value v1.0 never heard of.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	feapp "github.com/Olian04/edgerun/internal/frontend/app"
)

type persona struct {
	name string
	// dress encodes a v1.0 message the way this peer would send it.
	dress func(proto.Message) []byte
	// info is an ErrorInfo detail with the given reason number.
	info func(reason int32, msg, invocationID string) *anypb.Any
	// extra is a message of the named stream type carrying a `part` this reader
	// has never heard of; nil for the plain peer.
	extra func(streamType string) []byte
}

func newPersonas(t testing.TB) []persona {
	v := buildV11(t)
	return []persona{
		{name: "plain",
			dress: func(m proto.Message) []byte { return mustMarshal(t, m) },
			info:  func(r int32, msg, inv string) *anypb.Any { return errorInfoAny(r, msg, inv, nil) }},
		{name: "unknown-fields",
			dress: func(m proto.Message) []byte { return mustMarshal(t, withUnknown(m)) },
			info:  func(r int32, msg, inv string) *anypb.Any { return errorInfoAny(r, msg, inv, unknownBlob()) },
			extra: func(string) []byte {
				// a oneof member nobody declared: field 77, a nested message
				return protowire.AppendBytes(protowire.AppendTag(nil, 77, protowire.BytesType), append([]byte{0x0a, 0x03}, "abc"...))
			}},
		{name: "v1.1",
			dress: func(m proto.Message) []byte { return v.upgrade(t, m) },
			info: func(r int32, msg, inv string) *anypb.Any {
				return &anypb.Any{TypeUrl: "type.googleapis.com/edgerun.common.v1.ErrorInfo", Value: v.errorInfo(t, r, msg, inv)}
			},
			extra: func(stream string) []byte { return v.newPart(t, stream) }},
	}
}

const scriptInvocationID = "00000000-0000-4000-8000-0000000000aa"

// ---------------------------------------------------------------------------
// the frontend against a scripted orchestrator

func startFrontendAgainst(t *testing.T, orchURL string, timeout time.Duration) (base string, log *logSink) {
	t.Helper()
	l, sink := newSink()
	fe, err := feapp.Start(context.Background(), feapp.Config{
		Listen: "127.0.0.1:0", OrchestratorURL: orchURL, IngressToken: ingressToken,
		RequestTimeout: timeout, LimitRefresh: 50 * time.Millisecond, Log: l,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fe.Stop(context.Background()) })
	return "http://" + fe.Addr(), sink
}

func httpGet(t *testing.T, base, host, path, body string) (*http.Response, []byte, error) {
	t.Helper()
	method := "GET"
	if body != "" {
		method = "POST"
	}
	r, _ := http.NewRequest(method, base+path, strings.NewReader(body))
	r.Host = host
	cl := &http.Client{Timeout: 20 * time.Second}
	resp, err := cl.Do(r)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	b, rerr := io.ReadAll(resp.Body)
	return resp, b, rerr
}

// frontendExpectation is docs/protocol.md "Frontend mapping of platform errors".
var frontendTable = []struct {
	reason commonv1.ErrorReason
	status int
	retry  bool
}{
	{commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND, 404, false},
	{commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, 503, true},
	{commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, 504, false},
	{commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, 502, false},
	{commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH, 502, false},
	{commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE, 413, false},
	{commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE, 502, false},
	{commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND, 502, false},
	{commonv1.ErrorReason_ERROR_REASON_INTERNAL, 500, false},
	// the egress reasons occur on the egress seam only; the frontend has no
	// mapping for them, and an unmapped reason is a 500
	{commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED, 500, false},
	{commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED, 500, false},
}

// scriptedIngress serves the ingress seam as the persona's peer would.
func scriptedIngress(t *testing.T, p persona, maxBody uint64) *rawServer {
	srv := newRawServer(t)
	srv.unary(ingressv1connect.IngressServiceDescribeProcedure, func(context.Context, http.Header, []byte) ([]byte, error) {
		return p.dress(&ingressv1.DescribeResponse{
			Implementation:      &commonv1.Implementation{Name: "scripted", Version: "9", Protocols: []string{"edgerun.ingress.v1", "edgerun.ingress.v2"}},
			MaxRequestBodyBytes: maxBody}), nil
	})
	head := func() []byte {
		return p.dress(&ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Head{Head: &ingressv1.InvokeHead{InvocationId: scriptInvocationID,
			Http: &commonv1.HttpResponseHead{Status: 201, Headers: []*commonv1.Header{
				{Name: "x-a", Value: []byte("1")}, {Name: "set-cookie", Value: []byte("a=1")}, {Name: "set-cookie", Value: []byte("b=2")},
				{Name: "content-type", Value: []byte("text/x-scripted")}}}}}})
	}
	chunk := func(s string) []byte {
		return p.dress(&ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Chunk{Chunk: []byte(s)}})
	}
	srv.stream(ingressv1connect.IngressServiceInvokeProcedure, func(ctx context.Context, h http.Header, req []byte, send func([]byte) error) error {
		var ir ingressv1.InvokeRequest
		if err := proto.Unmarshal(req, &ir); err != nil {
			return connect.NewError(connect.CodeInvalidArgument, err)
		}
		path := ir.GetRequest().GetPath()
		extra := func() {
			if p.extra != nil {
				send(p.extra("edgerun.ingress.v1.InvokeResponse"))
			}
		}
		switch {
		case path == "/ok":
			send(head())
			send(chunk("hello "))
			extra()
			send(chunk("world"))
			return nil
		case path == "/late":
			send(head())
			send(chunk("hello"))
			extra()
			return nil
		case path == "/early":
			extra()
			send(head())
			send(chunk("never seen"))
			return nil
		case path == "/headless":
			return nil
		case strings.HasPrefix(path, "/err/"), strings.HasPrefix(path, "/mid/"):
			f := strings.Split(path, "/") // "", err|mid, reason, code
			reason, _ := strconv.Atoi(f[2])
			code, _ := strconv.Atoi(f[3])
			if f[1] == "mid" {
				send(head())
				send(chunk("partial"))
			}
			return scriptedError(connect.Code(code), p.info(int32(reason), secretMessage, scriptInvocationID), secretMessage)
		}
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("script has no %s", path))
	})
	return srv
}

func TestCompatFrontendAgainstScriptedOrchestrators(t *testing.T) {
	for _, p := range newPersonas(t) {
		t.Run(p.name, func(t *testing.T) {
			srv := scriptedIngress(t, p, 1000)
			base, fl := startFrontendAgainst(t, srv.URL(), 5*time.Second)

			// the limit from Describe, whose response is dressed, is the one enforced
			eventually(t, "the frontend to learn the limit", 20*time.Second, func() bool {
				resp, _, err := httpGet(t, base, "x.test", "/ok", strings.Repeat("x", 1001))
				return err == nil && resp.StatusCode == 413
			})
			calls := srv.count(ingressv1connect.IngressServiceInvokeProcedure)
			if resp, _, err := httpGet(t, base, "x.test", "/ok", strings.Repeat("x", 1001)); err != nil || resp.StatusCode != 413 {
				t.Errorf("a body over the limit: %v %v", resp, err)
			}
			if srv.count(ingressv1connect.IngressServiceInvokeProcedure) != calls {
				t.Errorf("the frontend called Invoke for a body over the limit")
			}

			t.Run("a response with unknown parts is relayed as it is", func(t *testing.T) {
				resp, body, err := httpGet(t, base, "x.test", "/ok", "")
				if err != nil || resp.StatusCode != 201 || string(body) != "hello world" {
					t.Fatalf("%v %v %q", resp, err, body)
				}
				if resp.Header.Get("X-A") != "1" || resp.Header.Get("Content-Type") != "text/x-scripted" ||
					strings.Join(resp.Header.Values("Set-Cookie"), "|") != "a=1|b=2" {
					t.Errorf("headers: %v", resp.Header)
				}
				if resp.Header.Get("X-Edgerun-Invocation-Id") != scriptInvocationID || resp.Header.Get("X-Request-Id") == "" {
					t.Errorf("ids: %v", resp.Header)
				}
				for k := range resp.Header {
					if strings.Contains(strings.ToLower(k), "v11") || strings.Contains(strings.ToLower(k), "unknown") {
						t.Errorf("a header named after an unknown field: %s", k)
					}
				}
				if strings.Contains(string(body), unknownCanary) {
					t.Errorf("an unknown field leaked into the body")
				}
				resp, body, err = httpGet(t, base, "x.test", "/late", "")
				if err != nil || resp.StatusCode != 201 || string(body) != "hello" {
					t.Errorf("a part after the last chunk: %v %v %q", resp, err, body)
				}
			})

			if p.extra != nil {
				t.Run("a part v1.0 does not know before the head cannot be told from no head", func(t *testing.T) {
					// docs: "A stream that ends cleanly without a head is 502". They say
					// nothing about a stream that opens with something new, so this
					// records what happens: a v1.1 server must not put a new part in
					// front of the head (it would break every v1.0 reader).
					resp, _, err := httpGet(t, base, "x.test", "/early", "")
					if err != nil || resp.StatusCode != 502 {
						t.Errorf("%v %v, want 502", resp, err)
					}
				})
			}
			t.Run("a stream without a head is a 502", func(t *testing.T) {
				resp, _, err := httpGet(t, base, "x.test", "/headless", "")
				if err != nil || resp.StatusCode != 502 {
					t.Errorf("%v %v", resp, err)
				}
			})

			t.Run("every reason maps by the table whatever the code says", func(t *testing.T) {
				for _, row := range frontendTable {
					for _, code := range []connect.Code{connect.CodeUnknown, connect.CodeInternal, connect.CodeAborted} {
						path := fmt.Sprintf("/err/%d/%d", row.reason, code)
						resp, body, err := httpGet(t, base, "x.test", path, "")
						if err != nil {
							t.Errorf("%s: %v", path, err)
							continue
						}
						if resp.StatusCode != row.status {
							t.Errorf("reason %s with code %s: %d, want %d", row.reason, code, resp.StatusCode, row.status)
						}
						if (resp.Header.Get("Retry-After") != "") != row.retry {
							t.Errorf("reason %s: Retry-After %q", row.reason, resp.Header.Get("Retry-After"))
						}
						if strings.Contains(string(body), secretMessage) || strings.Contains(string(body), unknownCanary) {
							t.Errorf("reason %s: the body shows the operator's message: %q", row.reason, body)
						}
						if resp.Header.Get("X-Edgerun-Invocation-Id") != scriptInvocationID {
							t.Errorf("reason %s: X-Edgerun-Invocation-Id %q", row.reason, resp.Header.Get("X-Edgerun-Invocation-Id"))
						}
					}
				}
			})

			t.Run("an unknown reason is a 500 when the code is no hint", func(t *testing.T) {
				for _, reason := range []int{99, 77, 13, 1000, -5} { // 12 is CANCELLED now
					for _, code := range []connect.Code{connect.CodeUnknown, connect.CodeInternal} {
						path := fmt.Sprintf("/err/%d/%d", reason, code)
						resp, body, err := httpGet(t, base, "x.test", path, "")
						if err != nil || resp.StatusCode != 500 {
							t.Errorf("reason %d code %s: %v %v, want 500 (docs: INTERNAL, unknown -> 500)", reason, code, resp, err)
							continue
						}
						if strings.Contains(string(body), secretMessage) {
							t.Errorf("reason %d: the body shows the operator's message", reason)
						}
						if resp.Header.Get("X-Edgerun-Invocation-Id") != scriptInvocationID {
							t.Errorf("reason %d: X-Edgerun-Invocation-Id %q", reason, resp.Header.Get("X-Edgerun-Invocation-Id"))
						}
					}
				}
			})

			t.Run("an unknown reason fails the stream after the head by aborting the connection", func(t *testing.T) {
				for _, reason := range []int{99, int(commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED)} {
					resp, body, err := httpGet(t, base, "x.test", fmt.Sprintf("/mid/%d/13", reason), "")
					if err == nil {
						t.Errorf("reason %d: the client saw a complete response: %v %q", reason, resp.Status, body)
					}
				}
			})

			if t.Failed() {
				return
			}
			if strings.Contains(fl.String(), unknownCanary) {
				t.Errorf("the frontend's log contains the payload of an unknown field")
			}
		})
	}
}

// DEFECT: an unknown reason is classified by the Connect code, though the docs
// say a peer branches on the reason and the table says "INTERNAL, unknown -> 500".
func TestCompatKnownDefectUnknownReasonIsClassifiedByCode(t *testing.T) {
	knownDefect(t, "COMPAT-1", "a frontend maps an ErrorInfo with an unknown reason by the Connect code (resource_exhausted -> 413, unavailable -> 503, deadline_exceeded -> 504) instead of 500")
	for _, p := range newPersonas(t) {
		t.Run(p.name, func(t *testing.T) {
			srv := scriptedIngress(t, p, 0)
			base, _ := startFrontendAgainst(t, srv.URL(), 5*time.Second)
			for _, code := range []connect.Code{connect.CodeResourceExhausted, connect.CodeUnavailable, connect.CodeDeadlineExceeded,
				connect.CodeUnauthenticated, connect.CodePermissionDenied, connect.CodeInvalidArgument, connect.CodeNotFound} {
				resp, _, err := httpGet(t, base, "x.test", fmt.Sprintf("/err/99/%d", code), "")
				if err != nil || resp.StatusCode != 500 {
					t.Errorf("unknown reason 99 with code %s: %v %v, want 500", code, resp, err)
				}
			}
			// the two egress reasons have no mapping either ("a function never fails
			// an invocation with them, so the frontend has no mapping for them"); an
			// orchestrator that lets one through, with its canonical code, gets the
			// code's mapping
			for _, r := range []commonv1.ErrorReason{commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED, commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED} {
				code := connect.CodeUnavailable
				if r == commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED {
					code = connect.CodePermissionDenied
				}
				resp, _, err := httpGet(t, base, "x.test", fmt.Sprintf("/err/%d/%d", r, code), "")
				if err != nil || resp.StatusCode != 500 {
					t.Errorf("%s with its canonical code %s: %v %v, want 500", r, code, resp, err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// the orchestrator against a scripted worker

type fakeWorker struct {
	srv  *rawServer
	id   atomic.Value
	stop chan struct{}
	once sync.Once
}

// startFakeWorker registers a scripted worker (ExecutionService on srv) with the
// orchestrator and keeps it alive.
func startFakeWorker(t *testing.T, s *stack, srv *rawServer, abi string, maxConcurrency uint32) *fakeWorker {
	t.Helper()
	return startFakeWorkerAt(t, s, srv, srv.URL(), abi, maxConcurrency)
}

// startFakeWorkerAt registers endpoint (which may be a proxy in front of srv) as
// the address of the scripted worker.
func startFakeWorkerAt(t *testing.T, s *stack, srv *rawServer, endpoint, abi string, maxConcurrency uint32) *fakeWorker {
	t.Helper()
	fleet := edgerun.NewFleet("http://"+s.addr, workerToken)
	info := &workerv1.WorkerInfo{Implementation: &commonv1.Implementation{Name: "scripted-worker", Version: "9", Protocols: []string{"edgerun.worker.v1"}},
		Abis: []string{abi}, MaxConcurrency: maxConcurrency}
	fw := &fakeWorker{srv: srv, stop: make(chan struct{})}
	reg := func() {
		r, err := fleet.Register(context.Background(), &workerv1.RegisterRequest{Endpoint: endpoint, Info: info})
		if err != nil {
			t.Errorf("register: %v", err)
			return
		}
		fw.id.Store(r.WorkerId)
	}
	reg()
	go func() {
		tk := time.NewTicker(100 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-fw.stop:
				return
			case <-tk.C:
				id, _ := fw.id.Load().(string)
				if r, err := fleet.Heartbeat(context.Background(), &workerv1.HeartbeatRequest{WorkerId: id, State: workerv1.WorkerState_WORKER_STATE_READY}); err == nil && r.Reregister {
					reg()
				}
			}
		}
	}()
	t.Cleanup(fw.close)
	return fw
}

func (fw *fakeWorker) close() {
	fw.once.Do(func() {
		close(fw.stop)
		id, _ := fw.id.Load().(string)
		_ = id
	})
}

func (fw *fakeWorker) workerID() string { id, _ := fw.id.Load().(string); return id }

func scriptedWorker(t *testing.T, p persona, authHeaders *sync.Map) *rawServer {
	srv := newRawServer(t)
	head := func() []byte {
		return p.dress(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Head{Head: &commonv1.HttpResponseHead{Status: 201,
			Headers: []*commonv1.Header{{Name: "x-a", Value: []byte("1")}, {Name: "content-type", Value: []byte("text/x-scripted")}}}}})
	}
	chunk := func(s string) []byte {
		return p.dress(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Chunk{Chunk: []byte(s)}})
	}
	summary := func() []byte {
		return p.dress(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Summary{Summary: &workerv1.ExecutionSummary{
			DurationMicros: 12345, PeakMemoryBytes: 6789, Stderr: []byte("log line\n\xff\xfe"), StderrTruncated: true}}})
	}
	srv.stream(workerv1connect.ExecutionServiceExecuteProcedure, func(ctx context.Context, h http.Header, req []byte, send func([]byte) error) error {
		if authHeaders != nil {
			authHeaders.Store("last", h.Get("Authorization"))
		}
		var er workerv1.ExecuteRequest
		if err := proto.Unmarshal(req, &er); err != nil {
			return connect.NewError(connect.CodeInvalidArgument, err)
		}
		path := er.GetRequest().GetPath()
		extra := func() {
			if p.extra != nil {
				send(p.extra("edgerun.worker.v1.ExecuteResponse"))
			}
		}
		switch {
		case path == "/ok":
			send(head())
			send(chunk("hello "))
			extra()
			send(chunk("world"))
			extra()
			return send(summary())
		case path == "/late":
			send(head())
			send(chunk("hello"))
			send(summary())
			extra()
			return nil
		case path == "/early":
			extra()
			send(head())
			send(chunk("never seen"))
			return send(summary())
		case path == "/nosummary":
			send(head())
			return send(chunk("no summary follows"))
		case strings.HasPrefix(path, "/err/"):
			f := strings.Split(path, "/") // "", err, reason, code
			reason, _ := strconv.Atoi(f[2])
			code, _ := strconv.Atoi(f[3])
			return scriptedError(connect.Code(code), p.info(int32(reason), secretMessage+" from the worker", ""), secretMessage+" from the worker")
		}
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("script has no %s", path))
	})
	return srv
}

func TestCompatOrchestratorAgainstScriptedWorkers(t *testing.T) {
	for _, p := range newPersonas(t) {
		t.Run(p.name, func(t *testing.T) {
			s, orchLog := newCStack(t, nil)
			auth := &sync.Map{}
			srv := scriptedWorker(t, p, auth)
			startFakeWorker(t, s, srv, "compat-fake", 8)
			fid := s.createFunction("scripted")
			if _, err := s.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: []byte("not run"), Abi: "compat-fake", Activate: true}); err != nil {
				t.Fatal(err)
			}
			s.route("fake.test", "/", fid)
			base, fl := startFrontendAgainst(t, "http://"+s.addr, 5*time.Second)
			eventually(t, "the scripted worker to serve", 30*time.Second, func() bool {
				resp, _, err := httpGet(t, base, "fake.test", "/ok", "")
				return err == nil && resp.StatusCode == 201
			})

			t.Run("a response with unknown fields and parts reaches the client intact", func(t *testing.T) {
				resp, body, err := httpGet(t, base, "fake.test", "/ok", "")
				if err != nil || resp.StatusCode != 201 || string(body) != "hello world" || resp.Header.Get("X-A") != "1" {
					t.Fatalf("%v %v %q", resp, err, body)
				}
				if strings.Contains(string(body), unknownCanary) {
					t.Errorf("an unknown field leaked into the body")
				}
				id := resp.Header.Get("X-Edgerun-Invocation-Id")
				eventually(t, "the record", 20*time.Second, func() bool {
					_, err := s.ctl.GetInvocation(ctx, &controlv1.GetInvocationRequest{InvocationId: id})
					return err == nil
				})
				r, _ := s.ctl.GetInvocation(ctx, &controlv1.GetInvocationRequest{InvocationId: id})
				inv := r.Invocation
				if inv.Status != 201 || inv.DurationMicros != 12345 || inv.PeakMemoryBytes != 6789 || string(inv.Stderr) != "log line\n\xff\xfe" ||
					inv.ErrorReason != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED || inv.WorkerId == "" {
					t.Errorf("the record lost what the summary said: %+v", inv)
				}
				if hasUnknown(inv.ProtoReflect()) {
					t.Errorf("the record, as the control API returns it, carries unknown fields")
				}
				if got, _ := auth.Load("last"); got != "Bearer "+executionToken {
					t.Errorf("the worker was called with credentials %q", got)
				}
			})

			t.Run("a part after the summary is ignored", func(t *testing.T) {
				resp, body, err := httpGet(t, base, "fake.test", "/late", "")
				if err != nil || resp.StatusCode != 201 || string(body) != "hello" {
					t.Errorf("%v %v %q", resp, err, body)
				}
			})

			if p.extra != nil {
				t.Run("a part v1.0 does not know before the head fails the invocation (INTERNAL)", func(t *testing.T) {
					// Not covered by the docs: a v1.1 worker that opens its stream with a new
					// part (progress, early hints) makes every v1.0 orchestrator answer 500.
					resp, _, err := httpGet(t, base, "fake.test", "/early", "")
					if err != nil || resp.StatusCode != 500 {
						t.Errorf("%v %v, want 500", resp, err)
					}
				})
			}

			t.Run("a stream with no summary is a failed invocation", func(t *testing.T) {
				resp, _, err := httpGet(t, base, "fake.test", "/nosummary", "")
				if err == nil {
					t.Errorf("the client saw a complete response (%v) although the worker never said it had finished", resp.Status)
				}
			})

			t.Run("errors of every reason, known and not, reach the frontend as the table says", func(t *testing.T) {
				for _, row := range frontendTable {
					if row.reason == commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED || row.reason == commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED {
						continue // see TestCompatKnownDefectUnknownReasonIsClassifiedByCode
					}
					path := fmt.Sprintf("/err/%d/%d", row.reason, 2)
					resp, body, err := httpGet(t, base, "fake.test", path, "")
					if err != nil {
						t.Errorf("%s: %v", path, err)
						continue
					}
					// a worker's reasons are the orchestrator's to translate; the egress
					// ones are not a function's failure and come out as internal
					want := row.status
					if row.reason == commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY {
						want = 503
					}
					if resp.StatusCode != want {
						t.Errorf("worker reason %s: the client got %d, want %d", row.reason, resp.StatusCode, want)
					}
					if strings.Contains(string(body), secretMessage) {
						t.Errorf("worker reason %s: the worker's message reached the client", row.reason)
					}
				}
				for _, reason := range []int{99, 77, 1000} {
					path := fmt.Sprintf("/err/%d/%d", reason, 2)
					resp, body, err := httpGet(t, base, "fake.test", path, "")
					if err != nil || resp.StatusCode != 500 {
						t.Errorf("unknown worker reason %d: %v %v, want 500", reason, resp, err)
						continue
					}
					if strings.Contains(string(body), secretMessage) {
						t.Errorf("the worker's message reached the client")
					}
					id := resp.Header.Get("X-Edgerun-Invocation-Id")
					if id == "" {
						t.Errorf("unknown worker reason %d: no invocation id for a failure after routing", reason)
						continue
					}
					eventually(t, "the record of the failure", 20*time.Second, func() bool {
						_, err := s.ctl.GetInvocation(ctx, &controlv1.GetInvocationRequest{InvocationId: id})
						return err == nil
					})
					r, _ := s.ctl.GetInvocation(ctx, &controlv1.GetInvocationRequest{InvocationId: id})
					t.Logf("unknown worker reason %d is recorded as error_reason=%d", reason, r.Invocation.ErrorReason)
				}
			})

			if t.Failed() {
				return
			}
			for name, sink := range map[string]*logSink{"orchestrator": orchLog, "frontend": fl} {
				if strings.Contains(sink.String(), unknownCanary) {
					t.Errorf("the %s's log contains the payload of an unknown field", name)
				}
			}
		})
	}
}
