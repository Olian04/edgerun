package e2e

// Part 1, the types: can every field carry what the docs say it carries, and
// what happens to the values a peer can send that its Go type does not hold
// (uint64 above MaxInt64, uint32 above MaxInt32, text that is not UTF-8).

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	egressapp "github.com/Olian04/edgerun/internal/egress/app"
)

// numericFields is every integer or enum field of the protocol and who can put a
// value into it. A field that is added without a line here fails
// TestCompatEveryNumericFieldIsAccountedFor, so that nobody adds a uint64 without
// asking what a Go int64 does with it.
var numericFields = map[string]string{
	"edgerun.common.v1.Limits.memory_limit_bytes":                "client (CreateVersion) -> orchestrator -> worker: TestCompatLimitsBeyondInt64",
	"edgerun.common.v1.Limits.max_response_bytes":                "client (CreateVersion) -> orchestrator -> worker: TestCompatLimitsBeyondInt64",
	"edgerun.common.v1.HttpResponseHead.status":                  "worker -> orchestrator -> frontend, egress -> worker: TestCompatNumbersFromWorkers, TestCompatNumbersFromNetworkServices",
	"edgerun.common.v1.ErrorInfo.reason":                         "any peer: compat_peers*_test.go",
	"edgerun.ingress.v1.DescribeResponse.max_request_body_bytes": "orchestrator -> frontend: TestCompatFrontendLimitBeyondInt64",
	"edgerun.worker.v1.ExecutionSummary.duration_micros":         "worker -> orchestrator: TestCompatNumbersFromWorkers",
	"edgerun.worker.v1.ExecutionSummary.peak_memory_bytes":       "worker -> orchestrator: TestCompatNumbersFromWorkers",
	"edgerun.worker.v1.WorkerInfo.max_concurrency":               "worker -> orchestrator: TestCompatNumbersFromWorkerRegistration",
	"edgerun.worker.v1.HeartbeatRequest.state":                   "worker -> orchestrator: TestCompatEveryRPCToleratesUnknownFieldsEnumsAndZeroValues",
	"edgerun.worker.v1.HeartbeatRequest.in_flight":               "worker -> orchestrator: TestCompatNumbersFromWorkerRegistration",
	"edgerun.egress.v1.HeartbeatRequest.state":                   "network service -> orchestrator: TestCompatEveryRPCToleratesUnknownFieldsEnumsAndZeroValues",
	"edgerun.egress.v1.DescribeResponse.max_request_body_bytes":  "output of a network service; nothing in the reference implementation reads it (no peer calls Describe)",
	"edgerun.egress.v1.DescribeResponse.max_response_body_bytes": "output of a network service; nothing in the reference implementation reads it (no peer calls Describe)",
	"edgerun.egress.v1.FetchRequest.max_response_bytes":          "worker -> network service: TestCompatNumbersFromNetworkServices",
	"edgerun.control.v1.ListFunctionsRequest.page_size":          "client -> orchestrator: TestCompatPageSizesBeyondInt32",
	"edgerun.control.v1.ListVersionsRequest.page_size":           "client -> orchestrator: TestCompatPageSizesBeyondInt32",
	"edgerun.control.v1.ListRoutesRequest.page_size":             "client -> orchestrator: TestCompatPageSizesBeyondInt32",
	"edgerun.control.v1.ListInvocationsRequest.page_size":        "client -> orchestrator: TestCompatPageSizesBeyondInt32",
	"edgerun.control.v1.DescribeResponse.max_module_bytes":       "output of the orchestrator; read by the CLI and the admin UI to refuse an upload early",
	"edgerun.control.v1.StepFailure.step":                        "output of the orchestrator",
	"edgerun.control.v1.FunctionVersion.number":                  "output of the orchestrator",
	"edgerun.control.v1.FunctionVersion.size_bytes":              "output of the orchestrator",
	"edgerun.control.v1.WorkerStatus.state":                      "output of the orchestrator, from a validated heartbeat",
	"edgerun.control.v1.WorkerStatus.in_flight":                  "output: what the worker's heartbeat said, TestCompatNumbersFromWorkerRegistration",
	"edgerun.control.v1.NetworkServiceStatus.state":              "output of the orchestrator, from a validated heartbeat",
	"edgerun.control.v1.Invocation.status":                       "output: what the worker's head said, TestCompatNumbersFromWorkers",
	"edgerun.control.v1.Invocation.error_reason":                 "output: what the worker's ErrorInfo said (unknown values included), TestCompatOrchestratorAgainstScriptedWorkers",
	"edgerun.control.v1.Invocation.duration_micros":              "output: what the worker's summary said, TestCompatNumbersFromWorkers",
	"edgerun.control.v1.Invocation.peak_memory_bytes":            "output: what the worker's summary said, TestCompatNumbersFromWorkers",
}

func TestCompatEveryNumericFieldIsAccountedFor(t *testing.T) {
	found := map[string]bool{}
	var walk func(md protoreflect.MessageDescriptor)
	walk = func(md protoreflect.MessageDescriptor) {
		for i := 0; i < md.Fields().Len(); i++ {
			fd := md.Fields().Get(i)
			switch fd.Kind() {
			case protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.Int32Kind, protoreflect.Int64Kind,
				protoreflect.Sint32Kind, protoreflect.Sint64Kind, protoreflect.Fixed32Kind, protoreflect.Fixed64Kind,
				protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind, protoreflect.EnumKind:
				found[string(fd.FullName())] = true
			}
		}
		for i := 0; i < md.Messages().Len(); i++ {
			walk(md.Messages().Get(i))
		}
	}
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if strings.HasPrefix(fd.Path(), "edgerun/") {
			for i := 0; i < fd.Messages().Len(); i++ {
				walk(fd.Messages().Get(i))
			}
		}
		return true
	})
	for name := range found {
		if _, ok := numericFields[name]; !ok {
			t.Errorf("%s is a numeric field nobody has asked questions about: add it to numericFields, with its test", name)
		}
	}
	for name := range numericFields {
		if !found[name] {
			t.Errorf("numericFields lists %s, which no longer exists", name)
		}
	}
	t.Logf("%d numeric fields", len(found))
}

// ErrorInfo.message is text: the tail of a guest's stderr that goes into it is made valid UTF-8 (see
// TestCompatTrappedGuestWithStderrThatIsNotUTF8), so the reason, which peers branch on, is never lost with it.

// Text a peer sends in a string field that is not UTF-8 is refused as a
// malformed request (invalid_argument), whatever the field and the RPC; never a
// 5xx, never accepted.
func TestCompatInvalidUTF8InAStringFieldIsInvalidArgument(t *testing.T) {
	e := newCEnv(t)
	checked := 0
	for _, c := range catalogue() {
		t.Run(c.name(), func(t *testing.T) {
			e.t, e.s.t = t, t
			md := c.newReq(t).ProtoReflect().Descriptor()
			for i := 0; i < md.Fields().Len(); i++ {
				fd := md.Fields().Get(i)
				if fd.Kind() != protoreflect.StringKind || fd.IsList() || fd.IsMap() {
					continue
				}
				b := mustMarshal(t, c.req(e))
				b = protowire.AppendTag(b, fd.Number(), protowire.BytesType)
				b = protowire.AppendBytes(b, []byte("ok\xff\xfe\xc3("))
				o := e.do(c, b)
				checked++
				if o.code != connect.CodeInvalidArgument {
					t.Errorf("%s with %s not UTF-8: %s, want invalid_argument", c.name(), fd.Name(), o)
				}
			}
		})
	}
	// and one level down: the strings of the HTTP message inside an Invoke
	t.Run("nested", func(t *testing.T) {
		e.t, e.s.t = t, t
		for _, tc := range []struct {
			name string
			num  protowire.Number
		}{{"method", 1}, {"host", 2}, {"path", 3}, {"raw_query", 4}, {"remote_addr", 7}} {
			r := e.httpReq("GET", "/echo", "")
			hb := mustMarshal(t, r)
			hb = protowire.AppendTag(hb, tc.num, protowire.BytesType)
			hb = protowire.AppendBytes(hb, []byte("/a\xffb"))
			inv := protowire.AppendTag(nil, 1, protowire.BytesType)
			inv = protowire.AppendBytes(inv, hb)
			o := e.do(catalogue()[findCase("IngressService", "Invoke")], inv)
			if o.code != connect.CodeInvalidArgument {
				t.Errorf("Invoke with HttpRequest.%s not UTF-8: %s, want invalid_argument", tc.name, o)
			}
		}
	})
	t.Logf("%d string fields checked", checked)
}

func findCase(service, method string) int {
	for i, c := range catalogue() {
		if strings.HasSuffix(c.service, service) && c.method == method {
			return i
		}
	}
	panic("no such case")
}

// A guest that traps leaves the tail of its stderr in the error message, which
// is a string.
func TestCompatTrappedGuestWithStderrThatIsNotUTF8(t *testing.T) {
	s, _ := newCStack(t, nil)
	s.startWorker(2)
	web := s.startFrontend(20 * time.Second)

	smoke := s.createFunction("smoke")
	s.uploadVersion(smoke, wasmModule([]byte("Content-Type: text/plain\n\nhand assembled"), []byte("stderr is fine\n"), 0), nil, true)
	s.route("smoke.test", "/", smoke)
	s.waitServing("smoke.test", "/")
	if r := web.do("GET", "smoke.test", "/", ""); r.status != 200 || r.body != "hand assembled" {
		t.Fatalf("the hand-assembled guest does not work: %d %q", r.status, r.body)
	}

	cases := []struct {
		name, host string
		stderr     []byte
	}{
		{"stderr of plain text", "text.test", []byte("a perfectly valid\nstderr tail\n")},
		{"stderr that is binary", "binary.test", []byte("trap: \xff\xfe\x00\x80 not text\n")},
		{"stderr cut in the middle of a character by the 4 KiB tail", "cut.test", append(append([]byte("x"), []byte(strings.Repeat("é", 3000))...), 'y')},
	}
	for _, tc := range cases {
		fid := s.createFunction(strings.TrimSuffix(tc.host, ".test"))
		s.uploadVersion(fid, wasmModule(nil, tc.stderr, 1), nil, true)
		s.route(tc.host, "/", fid)
		s.waitServingAs(tc.host, "/", func(r result) bool {
			return reason(r.err) != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED || r.err == nil
		})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := s.call(req{host: tc.host, path: "/"})
			isUTF8 := strings.ToValidUTF8(string(tc.stderr), "") == string(tc.stderr)
			switch tc.host {
			case "cut.test":
				isUTF8 = false // after the tail is cut
			}
			got := reason(r.err)
			if got != commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED {
				if isUTF8 {
					t.Fatalf("a trapping guest with plain stderr: %v, want FUNCTION_TRAPPED", r.err)
				}
				knownDefect(t, "COMPAT-2", fmt.Sprintf("a guest that traps with stderr that is not valid UTF-8 is reported as %s instead of FUNCTION_TRAPPED (the message is a string, so the ErrorInfo cannot be encoded and is dropped); a frontend answers %d instead of 502", got, 500))
				t.Errorf("reason %s (%v), want FUNCTION_TRAPPED", got, r.err)
			}
			if got := web.do("GET", tc.host, "/", "").status; got != 502 {
				if isUTF8 {
					t.Errorf("HTTP status %d, want 502", got)
				} else {
					knownDefect(t, "COMPAT-2", "HTTP status of a trapping guest whose stderr is not UTF-8")
					t.Errorf("HTTP status %d, want 502", got)
				}
			}
		})
	}
}

// waitServingAs waits until a call to host+path is answered in a way ok accepts
// (the stack has to compile the module first).
func (s *stack) waitServingAs(host, path string, ok func(result) bool) {
	s.t.Helper()
	eventually(s.t, "the stack to answer "+host+path, 120*time.Second, func() bool {
		return ok(s.call(req{host: host, path: path}))
	})
}

// ---------------------------------------------------------------------------
// numbers from a scripted worker

func dbRow(t *testing.T, dsn, query string, args []any, dest ...any) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if err := conn.QueryRow(ctx, query, args...).Scan(dest...); err != nil {
		t.Fatal(err)
	}
}

func TestCompatNumbersFromWorkers(t *testing.T) {
	s, _ := newCStack(t, nil)
	srv := newRawServer(t)
	p := newPersonas(t)[0]
	srv.stream(workerv1connect.ExecutionServiceExecuteProcedure, func(_ context.Context, _ http.Header, req []byte, send func([]byte) error) error {
		var er workerv1.ExecuteRequest
		if err := proto.Unmarshal(req, &er); err != nil {
			return err
		}
		var status uint32
		var dur, peak uint64
		if _, err := fmt.Sscanf(er.GetRequest().GetPath(), "/n/%d/%d/%d", &status, &dur, &peak); err != nil {
			return connect.NewError(connect.CodeInvalidArgument, err)
		}
		send(p.dress(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Head{Head: &commonv1.HttpResponseHead{Status: status}}}))
		send(p.dress(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Chunk{Chunk: []byte("x")}}))
		return send(p.dress(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Summary{Summary: &workerv1.ExecutionSummary{DurationMicros: dur, PeakMemoryBytes: peak}}}))
	})
	startFakeWorker(t, s, srv, "compat-fake", 8)
	fid := s.createFunction("numbers")
	if _, err := s.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: []byte("not run"), Abi: "compat-fake", Activate: true}); err != nil {
		t.Fatal(err)
	}
	s.route("n.test", "/", fid)
	base, _ := startFrontendAgainst(t, "http://"+s.addr, 10*time.Second)

	type tc struct {
		status    uint32
		dur, peak uint64
	}
	var cases []tc
	for _, st := range []uint32{0, 99, 199, 200, 599, 600, 1000, math.MaxInt32, math.MaxInt32 + 1, math.MaxUint32} {
		cases = append(cases, tc{st, 1, 1})
	}
	for _, v := range []uint64{math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64} {
		cases = append(cases, tc{200, v, v})
	}
	ids := map[tc]string{}
	for _, c := range cases {
		path := fmt.Sprintf("/n/%d/%d/%d", c.status, c.dur, c.peak)
		resp, _, err := httpGet(t, base, "n.test", path, "")
		if err != nil {
			t.Errorf("%s: %v", path, err)
			continue
		}
		want := int(c.status)
		if c.status < 200 || c.status > 599 {
			want = 502 // docs: "The head's status must be 200-599; anything else is a platform fault and is answered 502"
		}
		if resp.StatusCode != want {
			t.Errorf("the worker's head says status %d: the frontend answered %d, want %d", c.status, resp.StatusCode, want)
		}
		ids[c] = resp.Header.Get("X-Edgerun-Invocation-Id")
	}
	var negative []string
	for c, id := range ids {
		eventually(t, "the record", 20*time.Second, func() bool {
			_, err := s.ctl.GetInvocation(ctx, &controlv1.GetInvocationRequest{InvocationId: id})
			return err == nil
		})
		r, _ := s.ctl.GetInvocation(ctx, &controlv1.GetInvocationRequest{InvocationId: id})
		inv := r.Invocation
		// A value the column cannot hold is clamped to the largest it can, never wrapped.
		wantStatus, wantDur, wantPeak := min(c.status, uint32(math.MaxInt32)), min(c.dur, uint64(math.MaxInt64)), min(c.peak, uint64(math.MaxInt64))
		if inv.Status != wantStatus || inv.DurationMicros != wantDur || inv.PeakMemoryBytes != wantPeak {
			t.Errorf("the worker said status=%d duration=%d peak=%d; the record says status=%d duration=%d peak=%d",
				c.status, c.dur, c.peak, inv.Status, inv.DurationMicros, inv.PeakMemoryBytes)
		}
		var st int32
		var du, pk int64
		dbRow(t, s.dsn, `SELECT status, duration_micros, peak_memory_bytes FROM invocations WHERE id = $1::uuid`, []any{id}, &st, &du, &pk)
		if st < 0 || du < 0 || pk < 0 {
			negative = append(negative, fmt.Sprintf("sent status=%d duration=%d peak=%d, stored status=%d duration_micros=%d peak_memory_bytes=%d", c.status, c.dur, c.peak, st, du, pk))
		}
	}
	t.Run("the database holds what was sent, not its two's complement", func(t *testing.T) {
		sort.Strings(negative)
		if len(negative) > 0 {
			knownDefect(t, "COMPAT-3", "uint64/uint32 values above MaxInt64/MaxInt32 from a worker's head or summary are stored as negative numbers (int64(v), int32(v) in store.InsertInvocations); the API reads them back wrapped again, so only SQL sees it")
			t.Errorf("%d records hold negative numbers:\n%s", len(negative), strings.Join(negative, "\n"))
		}
	})
}

// ---------------------------------------------------------------------------
// numbers in registration and heartbeats

func TestCompatNumbersFromWorkerRegistration(t *testing.T) {
	s, orchLog := newCStack(t, nil)
	fleet := edgerun.NewFleet("http://"+s.addr, workerToken)
	n := 0
	register := func(mc uint32) (string, error) {
		n++
		r, err := fleet.Register(ctx, &workerv1.RegisterRequest{Endpoint: fmt.Sprintf("http://127.0.0.1:%d", 20000+n),
			Info: &workerv1.WorkerInfo{Abis: []string{"compat-fake"}, MaxConcurrency: mc}})
		if err != nil {
			return "", err
		}
		return r.WorkerId, nil
	}
	var bad []string
	for _, mc := range []uint32{1, math.MaxInt32, math.MaxInt32 + 1, math.MaxUint32} {
		id, err := register(mc)
		code := codeOK
		if err != nil {
			code = connect.CodeOf(err)
		}
		switch code {
		case codeOK:
			ws, _ := s.ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
			found := false
			for _, w := range ws.Workers {
				if w.Id == id {
					found = true
					if w.Info.MaxConcurrency != mc { // the document the worker sent is kept as sent
						t.Errorf("max_concurrency %d is listed as %d", mc, w.Info.MaxConcurrency)
					}
				}
			}
			if !found {
				t.Errorf("max_concurrency %d: registered but not listed", mc)
			}
		case connect.CodeInvalidArgument:
		default:
			bad = append(bad, fmt.Sprintf("max_concurrency=%d: %v", mc, err))
		}

	}
	t.Run("a max_concurrency the database cannot hold is refused, not an internal error", func(t *testing.T) {
		if len(bad) > 0 {
			knownDefect(t, "COMPAT-4", "Register with max_concurrency >= 2^31 (a positive uint32) fails with an internal error: int32(v) is negative and violates the CHECK (max_concurrency > 0); docs: anything but a positive value is invalid_argument, and this one is positive")
			t.Errorf("%s", strings.Join(bad, "\n"))
			if !strings.Contains(orchLog.String(), "store error") {
				t.Errorf("and it was not even logged")
			}
		}
	})

	id, err := register(4)
	if err != nil {
		t.Fatal(err)
	}
	var negative []string
	for _, inflight := range []uint32{0, math.MaxInt32, math.MaxInt32 + 1, math.MaxUint32} {
		r, err := fleet.Heartbeat(ctx, &workerv1.HeartbeatRequest{WorkerId: id, State: workerv1.WorkerState_WORKER_STATE_READY, InFlight: inflight})
		if err != nil || r.Reregister {
			t.Errorf("in_flight=%d: %v %v", inflight, r, err)
			continue
		}
		ws, _ := s.ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
		for _, w := range ws.Workers {
			if w.Id == id && w.InFlight != min(inflight, uint32(math.MaxInt32)) { // clamped to what the column holds
				t.Errorf("in_flight %d is listed as %d", inflight, w.InFlight)
			}
		}
		var stored int32
		dbRow(t, s.dsn, `SELECT in_flight FROM workers WHERE id = $1::uuid`, []any{id}, &stored)
		if stored < 0 {
			negative = append(negative, fmt.Sprintf("sent in_flight=%d, stored %d", inflight, stored))
		}
	}
	t.Run("the database holds the in_flight that was sent", func(t *testing.T) {
		if len(negative) > 0 {
			knownDefect(t, "COMPAT-3", "in_flight above MaxInt32 is stored as a negative number (int32(v))")
			t.Errorf("%s", strings.Join(negative, "\n"))
		}
	})
}

// ---------------------------------------------------------------------------
// page sizes

func TestCompatPageSizesBeyondInt32(t *testing.T) {
	s, _ := newCStack(t, nil)
	var fid string
	for i := 0; i < 3; i++ {
		fid = s.createFunction(fmt.Sprintf("paged-%d", i))
		s.uploadVersion(fid, []byte(fmt.Sprintf("m%d", i)), nil, false)
		s.route(fmt.Sprintf("paged%d.test", i), "/", fid)
	}
	for _, size := range []uint32{1, 2, math.MaxInt32, math.MaxInt32 + 1, math.MaxUint32} {
		want := int(min(size, 3))
		fs, err := s.ctl.ListFunctions(ctx, &controlv1.ListFunctionsRequest{PageSize: size})
		if err != nil || len(fs.Functions) != want {
			t.Errorf("ListFunctions page_size=%d: %d functions, %v (want %d)", size, len(fs.GetFunctions()), err, want)
		}
		rs, err := s.ctl.ListRoutes(ctx, &controlv1.ListRoutesRequest{PageSize: size})
		if err != nil || len(rs.Routes) != want {
			t.Errorf("ListRoutes page_size=%d: %d routes, %v (want %d)", size, len(rs.GetRoutes()), err, want)
		}
		if _, err := s.ctl.ListVersions(ctx, &controlv1.ListVersionsRequest{FunctionId: fid, PageSize: size}); err != nil {
			t.Errorf("ListVersions page_size=%d: %v", size, err)
		}
		if _, err := s.ctl.ListInvocations(ctx, &controlv1.ListInvocationsRequest{PageSize: size}); err != nil {
			t.Errorf("ListInvocations page_size=%d: %v", size, err)
		}
	}
}

// ---------------------------------------------------------------------------
// limits

func TestCompatLimitsBeyondInt64(t *testing.T) {
	s, _ := newCStack(t, nil)
	s.startWorker(2)
	fid := s.createFunction("limited")
	for _, l := range []*commonv1.Limits{
		{MemoryLimitBytes: math.MaxInt64 + 1}, {MaxResponseBytes: math.MaxInt64 + 1},
		{MemoryLimitBytes: math.MaxUint64, MaxResponseBytes: math.MaxUint64},
	} {
		_, err := s.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: s.wasm, Abi: "wagi-1", Limits: l})
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("limits %v: %v, want invalid_argument", l, err)
		}
	}
	big := &commonv1.Limits{MemoryLimitBytes: math.MaxInt64, MaxResponseBytes: math.MaxInt64}
	v, err := s.ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{FunctionId: fid, Module: s.wasm, Abi: "wagi-1", Limits: big, Activate: true})
	if err != nil {
		t.Fatalf("limits at MaxInt64: %v", err)
	}
	if v.Version.Limits.GetMemoryLimitBytes() != math.MaxInt64 || v.Version.Limits.GetMaxResponseBytes() != math.MaxInt64 {
		t.Errorf("limits come back as %v", v.Version.Limits)
	}
	s.route("limited.test", "/", fid)
	s.waitServingAs("limited.test", "/echo", func(r result) bool { return r.err == nil })
	// a worker clamps what it is asked for: the guest runs
	r := s.mustCall(req{host: "limited.test", path: "/echo", method: "POST", body: "within limits"})
	if string(r.body) != "within limits" {
		t.Errorf("%q", r.body)
	}
	// and so does a worker that is asked directly, by a peer that is not our orchestrator
	ws, _ := s.ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
	exec := edgerun.NewExecution(ws.Workers[0].Endpoint, executionToken)
	for _, l := range []*commonv1.Limits{{MemoryLimitBytes: math.MaxUint64, MaxResponseBytes: math.MaxUint64}, {MemoryLimitBytes: math.MaxInt64 + 1}} {
		res, err := exec.Execute(ctx, &workerv1.ExecuteRequest{InvocationId: "i", ArtifactDigest: edgerun.Digest(s.wasm), Abi: "wagi-1", Limits: l,
			Request: &commonv1.HttpRequest{Method: "POST", Host: "x", Path: "/echo", Body: []byte("direct")}})
		if err != nil {
			t.Errorf("Execute with limits %v: %v", l, err)
			continue
		}
		if b, err := res.ReadAll(); err != nil || string(b) != "direct" {
			t.Errorf("Execute with limits %v: %q %v", l, b, err)
		}
	}
}

// ---------------------------------------------------------------------------
// the frontend's limit

func TestCompatFrontendLimitBeyondInt64(t *testing.T) {
	p := newPersonas(t)[0]
	for _, limit := range []uint64{math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			srv := scriptedIngress(t, p, limit)
			base, _ := startFrontendAgainst(t, srv.URL(), 5*time.Second)
			eventually(t, "the frontend to have asked twice", 20*time.Second, func() bool {
				return srv.count(ingressv1connect.IngressServiceDescribeProcedure) >= 3
			})
			resp, body, err := httpGet(t, base, "x.test", "/ok", "ten bytes!")
			if err != nil || resp.StatusCode != 201 || string(body) != "hello world" {
				if limit > math.MaxInt64 {
					knownDefect(t, "COMPAT-5", "an orchestrator that reports max_request_body_bytes above MaxInt64 (2^64-1 is the natural spelling of \"no limit\") makes the frontend answer 413 to every request with a body: http.MaxBytesReader(w, r, int64(limit)) treats the negative number as 0")
				}
				t.Errorf("max_request_body_bytes=%d, a 10 byte POST: %v %v %q", limit, resp, err, body)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// numbers sent to and from a network service

func TestCompatNumbersFromNetworkServices(t *testing.T) {
	s, _ := newCStack(t, nil)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "from the origin") }))
	t.Cleanup(origin.Close)
	addr := origin.Listener.Addr().String()
	eg, err := egressapp.Start(ctx, egressapp.Config{Listen: "127.0.0.1:0", EgressToken: egressToken, OrchestratorURL: "http://" + s.addr, NetworkToken: networkToken,
		Covers: []string{"127.0.0.0/8"}, AllowNetworks: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}, Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eg.Stop(context.Background()) })
	client := edgerun.NewEgress("http://"+eg.Addr(), egressToken)
	for _, limit := range []uint64{0, 1 << 20, math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64} {
		res, err := client.Fetch(ctx, &egressv1.FetchRequest{Method: "GET", Url: "http://" + addr + "/", Allow: []string{addr}, MaxResponseBytes: limit})
		if err != nil {
			t.Errorf("max_response_bytes=%d: %v", limit, err)
			continue
		}
		if b, err := res.ReadAll(); err != nil || string(b) != "from the origin" {
			t.Errorf("max_response_bytes=%d: %q %v", limit, b, err)
		}
	}
	_, err = client.Fetch(ctx, &egressv1.FetchRequest{Method: "GET", Url: "http://" + addr + "/", Allow: []string{addr}, MaxResponseBytes: 3})
	if !edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE) {
		t.Errorf("max_response_bytes=3: %v, want RESPONSE_TOO_LARGE", err)
	}

	// what a worker does with the status a network service puts in its head
	p := newPersonas(t)[0]
	var mu sync.Mutex
	status := uint32(200)
	rig := startEgressRig(t, func(srv *rawServer) {
		srv.stream("/edgerun.egress.v1.EgressService/Fetch", func(_ context.Context, _ http.Header, _ []byte, send func([]byte) error) error {
			mu.Lock()
			st := status
			mu.Unlock()
			send(p.dress(&egressv1.FetchResponse{Part: &egressv1.FetchResponse_Head{Head: &commonv1.HttpResponseHead{Status: st}}}))
			return send(p.dress(&egressv1.FetchResponse{Part: &egressv1.FetchResponse_Chunk{Chunk: []byte("body")}}))
		})
	})
	for _, st := range []uint32{0, 99, 600, math.MaxUint32} {
		mu.Lock()
		status = st
		mu.Unlock()
		out := rig.fetch("/x")
		line := ""
		for _, l := range strings.Split(out, "\n") {
			if strings.HasPrefix(l, "Status:") {
				line = l
			}
		}
		// The docs bound the size of what a service returns and say a worker does not
		// trust it to keep to it; they say nothing about the status. The guest is
		// handed whatever the service said.
		t.Logf("a network service answering with status %d: the guest is told code=%q, %q", st, codeLine(out), line)
		if !strings.HasPrefix(out, "code=") {
			t.Errorf("status %d: %q", st, out)
		}
	}
	_ = fmt.Sprint
}
