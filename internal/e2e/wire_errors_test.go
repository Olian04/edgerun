package e2e

// Part 2: what an error looks like on every wire, and the two ways of saying
// "this call has a budget" (Connect-Timeout-Ms, grpc-timeout).

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

func rawPost(t *testing.T, url, contentType, token string, body []byte, hdr map[string]string) (*http.Response, []byte) {
	t.Helper()
	r, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", contentType)
	r.Header.Set("Connect-Protocol-Version", "1")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

type jsonError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details []struct {
		Type  string `json:"type"`
		Value string `json:"value"`
		Debug any    `json:"debug"`
	} `json:"details"`
}

// errorInfoOf decodes the ErrorInfo of a Connect JSON error: the Any is
// {"type": the message name without the type.googleapis.com/ prefix, "value":
// base64 of the protobuf}.
func errorInfoOf(t *testing.T, je jsonError) *commonv1.ErrorInfo {
	t.Helper()
	for _, d := range je.Details {
		if d.Type != "edgerun.common.v1.ErrorInfo" {
			continue
		}
		raw, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(d.Value, "="))
		if err != nil {
			t.Fatalf("detail value is not base64: %v", err)
		}
		var info commonv1.ErrorInfo
		if err := proto.Unmarshal(raw, &info); err != nil {
			t.Fatalf("detail value is not an ErrorInfo: %v", err)
		}
		return &info
	}
	return nil
}

// envelopes splits a Connect stream body into (flags, payload) pairs.
func envelopes(t *testing.T, b []byte) (msgs [][]byte, end []byte) {
	t.Helper()
	for len(b) > 0 {
		if len(b) < 5 {
			t.Fatalf("a truncated envelope: %d bytes left", len(b))
		}
		flags, n := b[0], binary.BigEndian.Uint32(b[1:5])
		if uint32(len(b)-5) < n {
			t.Fatalf("an envelope says %d bytes, %d follow", n, len(b)-5)
		}
		payload := b[5 : 5+n]
		b = b[5+n:]
		if flags&2 != 0 {
			end = payload
		} else {
			msgs = append(msgs, payload)
		}
	}
	return
}

func envelope(payload []byte) []byte {
	out := make([]byte, 5, 5+len(payload))
	binary.BigEndian.PutUint32(out[1:], uint32(len(payload)))
	return append(out, payload...)
}

func TestWireConnectJSONErrors(t *testing.T) {
	e := newCEnv(t)

	t.Run("a unary error is a JSON body with code and message, and the matching HTTP status", func(t *testing.T) {
		for _, tc := range []struct {
			name, proc, token, body string
			status                  int
			code                    string
		}{
			{"not found", "edgerun.control.v1.FunctionService/GetFunction", adminToken, `{"functionId":"00000000-0000-4000-8000-000000000000"}`, 404, "not_found"},
			{"invalid argument", "edgerun.control.v1.FunctionService/CreateFunction", adminToken, `{"name":"NOT A NAME"}`, 400, "invalid_argument"},
			{"no credentials", "edgerun.control.v1.FunctionService/ListFunctions", "", `{}`, 401, "unauthenticated"},
			{"a token nobody knows", "edgerun.control.v1.FunctionService/ListFunctions", "no-such-token", `{}`, 401, "unauthenticated"},
			{"a token of another scope", "edgerun.control.v1.FunctionService/ListFunctions", ingressToken, `{}`, 403, "permission_denied"},
			{"a worker with the orchestrator's token", "edgerun.worker.v1.ExecutionService/Describe", workerToken, `{}`, 401, "unauthenticated"},
			{"malformed JSON", "edgerun.control.v1.FunctionService/GetFunction", adminToken, `{"functionId":`, 400, "invalid_argument"},
			{"an unknown JSON field is ignored", "edgerun.control.v1.FunctionService/GetFunction", adminToken, `{"functionId":"x","v11Field":{"a":[1,2]}}`, 404, "not_found"},
		} {
			base := e.orch.base
			if strings.Contains(tc.proc, "ExecutionService") {
				base = e.worker.base
			}
			resp, body := rawPost(t, base+"/"+tc.proc, "application/json", tc.token, []byte(tc.body), nil)
			var je jsonError
			if err := json.Unmarshal(body, &je); err != nil {
				t.Errorf("%s: the error body is not JSON: %q", tc.name, body)
				continue
			}
			if resp.StatusCode != tc.status || je.Code != tc.code || je.Message == "" {
				t.Errorf("%s: HTTP %d %+v, want %d %s with a message", tc.name, resp.StatusCode, je, tc.status, tc.code)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Errorf("%s: Content-Type %q", tc.name, ct)
			}
		}
	})

	t.Run("a streaming error ends the stream with the ErrorInfo as an Any with its type URL", func(t *testing.T) {
		abi := "no-such-abi"
		for _, tc := range []struct {
			name, base, proc, token string
			req                     map[string]any
			code                    string
			reason                  commonv1.ErrorReason
		}{
			{"Invoke of an unrouted host", e.orch.base, "edgerun.ingress.v1.IngressService/Invoke", ingressToken,
				map[string]any{"requestId": "wire", "request": map[string]any{"method": "GET", "host": "nowhere.test", "path": "/"}},
				"not_found", commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND},
			{"Execute of an ABI the worker lacks", e.worker.base, "edgerun.worker.v1.ExecutionService/Execute", executionToken,
				map[string]any{"invocationId": "wire", "artifactDigest": e.digest, "abi": abi, "request": map[string]any{"method": "GET", "host": "x", "path": "/"}},
				"failed_precondition", commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH},
			{"GetArtifact of an unknown digest", e.orch.base, "edgerun.worker.v1.ArtifactService/GetArtifact", workerToken,
				map[string]any{"digest": "sha256:" + strings.Repeat("0", 64)},
				"not_found", commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND},
			{"Fetch of a destination nobody allowed", e.egress.base, "edgerun.egress.v1.EgressService/Fetch", egressToken,
				map[string]any{"method": "GET", "url": "http://" + e.originAddr + "/", "allow": []string{"example.com"}},
				"permission_denied", commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED},
		} {
			payload, _ := json.Marshal(tc.req)
			resp, body := rawPost(t, tc.base+"/"+tc.proc, "application/connect+json", tc.token, envelope(payload), nil)
			if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/connect+json") {
				t.Errorf("%s: HTTP %d %s: a stream that fails after it started is 200", tc.name, resp.StatusCode, resp.Header.Get("Content-Type"))
			}
			msgs, end := envelopes(t, body)
			if len(msgs) != 0 || end == nil {
				t.Errorf("%s: %d messages and an end of %q", tc.name, len(msgs), end)
				continue
			}
			var es struct {
				Error    jsonError           `json:"error"`
				Metadata map[string][]string `json:"metadata"`
			}
			if err := json.Unmarshal(end, &es); err != nil {
				t.Errorf("%s: the end-of-stream message is not JSON: %q", tc.name, end)
				continue
			}
			if es.Error.Code != tc.code || es.Error.Message == "" {
				t.Errorf("%s: error %+v, want code %s with a message", tc.name, es.Error, tc.code)
			}
			info := errorInfoOf(t, es.Error)
			if info == nil || info.Reason != tc.reason || info.Message == "" {
				t.Errorf("%s: ErrorInfo %v in details %+v, want reason %s", tc.name, info, es.Error.Details, tc.reason)
			}
		}
	})

	t.Run("the same errors, with the same reasons, over every protocol", func(t *testing.T) {
		abi := "no-such-abi"
		for _, v := range wireVariants() {
			hc, _, opts := v.client()
			c := e.wireClients(hc, opts)
			_, err := c.ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: "nowhere.test", Path: "/"}})
			check := func(what string, err error, code connect.Code, reason commonv1.ErrorReason) {
				if connect.CodeOf(err) != code || edgerun.Reason(err) != reason {
					t.Errorf("%s: %s: %v (%s), want %s with %s", v.name(), what, err, edgerun.Reason(err), code, reason)
				}
			}
			check("Invoke", err, connect.CodeNotFound, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)
			_, err = c.exec.Execute(ctx, &workerv1.ExecuteRequest{ArtifactDigest: e.digest, Abi: abi, Request: &commonv1.HttpRequest{Method: "GET", Host: "x", Path: "/"}})
			check("Execute", err, connect.CodeFailedPrecondition, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
			_, err = c.art.GetArtifact(ctx, "sha256:"+strings.Repeat("0", 64))
			check("GetArtifact", err, connect.CodeNotFound, commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND)
			_, err = c.eg.Fetch(ctx, &egressv1.FetchRequest{Method: "GET", Url: "http://" + e.originAddr + "/", Allow: []string{"example.com"}})
			check("Fetch", err, connect.CodePermissionDenied, commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED)
			_, err = c.ctl.GetFunction(ctx, &controlv1.GetFunctionRequest{FunctionId: "nope"})
			check("GetFunction", err, connect.CodeNotFound, commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED)
			// the invocation id of a failure after routing survives every wire
			f := e.fn()
			if _, err := e.s.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: f, Module: []byte("x"), Abi: "no-worker-has-this", Activate: true}); err != nil {
				t.Fatal(err)
			}
			name := e.uniq("abi") + ".test"
			e.s.route(name, "/", f)
			eventually(t, "the route", 20*time.Second, func() bool {
				_, err := c.ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: name, Path: "/"}})
				return edgerun.Reason(err) == commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH
			})
			_, err = c.ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: name, Path: "/"}})
			check("Invoke of a function no worker can run", err, connect.CodeFailedPrecondition, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH)
			if edgerun.InvocationID(err) == "" {
				t.Errorf("%s: the failure after routing carries no invocation id", v.name())
			}
		}
	})
}

// A budget travels as Connect-Timeout-Ms (Connect) or grpc-timeout (gRPC,
// gRPC-Web), whatever the client's own context says. Invoke and Execute honour
// both: the guest is stopped, and the slot is free.
func TestWireDeadlinesInEveryForm(t *testing.T) {
	e := newCEnv(t)
	sleeping := &commonv1.HttpRequest{Method: "GET", Host: "compat.test", Path: "/sleep", RawQuery: "ms=30000"}
	forms := []struct {
		name   string
		opts   []connect.ClientOption
		header map[string]string
	}{
		{"Connect-Timeout-Ms over Connect", nil, map[string]string{"Connect-Timeout-Ms": "500"}},
		{"grpc-timeout over gRPC", []connect.ClientOption{connect.WithGRPC()}, map[string]string{"Grpc-Timeout": "500m"}},
		{"grpc-timeout over gRPC-Web", []connect.ClientOption{connect.WithGRPCWeb()}, map[string]string{"Grpc-Timeout": "500m"}},
		{"grpc-timeout in seconds over gRPC-Web", []connect.ClientOption{connect.WithGRPCWeb()}, map[string]string{"Grpc-Timeout": "1S"}},
	}
	for _, f := range forms {
		for _, tg := range []string{"Invoke", "Execute"} {
			t.Run(f.name+"/"+tg, func(t *testing.T) {
				hc := h2cClient()
				opts := append([]connect.ClientOption{connect.WithInterceptors(headerInjector(f.header))}, f.opts...)
				c := e.wireClients(hc, opts)
				cctx, cancel := context.WithCancel(context.Background()) // no deadline of its own: the header is the budget
				defer cancel()
				watchdog := time.AfterFunc(25*time.Second, cancel)
				defer watchdog.Stop()
				start := time.Now()
				var err error
				if tg == "Invoke" {
					var res *edgerun.Invocation
					if res, err = c.ing.Invoke(cctx, &ingressv1.InvokeRequest{RequestId: "wire", Request: sleeping}); err == nil {
						_, err = res.ReadAll()
					}
				} else {
					var res *edgerun.ExecutionResult
					if res, err = c.exec.Execute(cctx, &workerv1.ExecuteRequest{InvocationId: "wire", ArtifactDigest: e.digest, Abi: "wagi-1", Request: sleeping}); err == nil {
						_, err = res.ReadAll()
					}
				}
				elapsed := time.Since(start)
				if edgerun.Reason(err) != commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED || connect.CodeOf(err) != connect.CodeDeadlineExceeded {
					t.Fatalf("after %v: %v (%s), want DEADLINE_EXCEEDED", elapsed, err, edgerun.Reason(err))
				}
				if elapsed < 300*time.Millisecond || elapsed > 6*time.Second {
					t.Errorf("a budget of 0.5-1 s was honoured after %v", elapsed)
				}
			})
		}
	}

	t.Run("the guests were stopped and their slots are free", func(t *testing.T) {
		eventually(t, "no invocation in flight on the worker", 30*time.Second, func() bool {
			ws, err := e.s.ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
			if err != nil {
				return false
			}
			for _, w := range ws.Workers {
				if w.Info.GetImplementation().GetName() == "edgerun-worker-wazero" {
					return w.InFlight == 0
				}
			}
			return false
		})
		for i := 0; i < 4; i++ { // the worker has 4 slots
			if r := e.s.call(req{host: "compat.test", path: "/echo", body: "free"}); r.err != nil {
				t.Fatalf("slot %d: %v", i, r.err)
			}
		}
	})

	t.Run("a budget that is not a number is refused or ignored, never a 5xx or a hang", func(t *testing.T) {
		quick := mustMarshal(t, &ingressv1.InvokeRequest{Request: e.httpReq("GET", "/echo", "")})
		for _, bad := range []string{"abc", "-1", "99999999999", "1.5", "", " 5"} {
			rc := rawClient{base: e.orch.base, token: ingressToken, hc: http1Client()}
			cctx, cancel := ctxWithTimeout(20 * time.Second)
			start := time.Now()
			_, err := rc.stream(cctx, "/edgerun.ingress.v1.IngressService/Invoke", quick, http.Header{"Connect-Timeout-Ms": {bad}})
			cancel()
			t.Logf("Connect-Timeout-Ms: %q -> %v (%v)", bad, err, time.Since(start).Round(time.Millisecond))
			if time.Since(start) > 10*time.Second {
				t.Errorf("Connect-Timeout-Ms %q: it took %v", bad, time.Since(start))
			}
			if err != nil && connect.CodeOf(err) != connect.CodeInvalidArgument && connect.CodeOf(err) != connect.CodeDeadlineExceeded {
				t.Errorf("Connect-Timeout-Ms %q: %v", bad, err)
			}
		}
	})
	_ = fmt.Sprint
}

func nilIfEmpty(s string) []string {
	if s == "" {
		return nil
	}
	return []string{s}
}

// headerInjector sets headers on every call, after the client has set its own.
type headerInjector map[string]string

func (h headerInjector) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, r connect.AnyRequest) (connect.AnyResponse, error) {
		for k, v := range h {
			r.Header().Set(k, v)
		}
		return next(ctx, r)
	}
}

func (h headerInjector) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		for k, v := range h {
			conn.RequestHeader().Set(k, v)
		}
		return conn
	}
}

func (h headerInjector) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
