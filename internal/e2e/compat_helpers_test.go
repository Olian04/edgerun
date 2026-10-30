package e2e

// Helpers shared by the compat_* and wire_* tests: a stack whose components log
// into buffers, a raw (bytes in, bytes out) Connect client and server so that a
// test can say exactly what is on the wire, and a hand-assembled WebAssembly
// guest for the cases the shared test guest has no path for.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/Olian04/edgerun"
	"github.com/Olian04/edgerun/conformance/guest"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	orchapp "github.com/Olian04/edgerun/internal/orchestrator/app"
	"github.com/Olian04/edgerun/internal/testpg"
)

const (
	// unknownCanary is carried in every unknown field the tests send or script.
	// It must never be found in a log or in a response.
	unknownCanary = "UNKNOWN-FIELD-CANARY-31337"
	// secretMessage is the operator-facing text of scripted errors. The frontend
	// must never show it to a client.
	secretMessage = "OPERATOR-ONLY-MESSAGE-4711"
)

// knownDefect skips a test that exposes a real defect, unless the environment
// asks to run them.
func knownDefect(t *testing.T, id, what string) {
	t.Helper()
	if os.Getenv("EDGERUN_E2E_KNOWN_DEFECTS") == "" {
		t.Skip("KNOWN DEFECT " + id + ": " + what)
	}
}

// ---------------------------------------------------------------------------
// logs

type logSink struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logSink) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logSink) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func newSink() (*slog.Logger, *logSink) {
	s := &logSink{}
	return slog.New(slog.NewTextHandler(s, &slog.HandlerOptions{Level: slog.LevelDebug})), s
}

// ---------------------------------------------------------------------------
// a stack whose orchestrator logs into a buffer

// newCStack is newStack with the orchestrator's log captured and its
// configuration adjustable.
func newCStack(t *testing.T, adjust func(*orchapp.Config)) (*stack, *logSink) {
	t.Helper()
	s := &stack{t: t, dsn: testpg.DSN(t), wasm: guest.Build(t)}
	log, sink := newSink()
	cfg := orchConfig(s.dsn, "127.0.0.1:0")
	cfg.Log = log
	if adjust != nil {
		adjust(&cfg)
	}
	srv, err := orchapp.Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.orch, s.addr = srv, srv.Addr()
	url := "http://" + s.addr
	s.ctl = edgerun.NewControl(url, adminToken)
	s.ingress = edgerun.NewIngress(url, ingressToken, edgerun.WithHTTPClient(&http.Client{Transport: budgetTransport{}}))
	t.Cleanup(func() { s.orch.Stop(context.Background()) })
	return s, sink
}

// ---------------------------------------------------------------------------
// raw Connect: bytes in, bytes out

// rawMsg is a message the codec does not look inside.
type rawMsg struct{ b []byte }

// passCodec is the "proto" codec that hands the bytes through untouched, so a
// test controls every byte of a message (unknown fields, v1.1 descriptors).
type passCodec struct{}

func (passCodec) Name() string { return "proto" }
func (passCodec) Marshal(v any) ([]byte, error) {
	m, ok := v.(*rawMsg)
	if !ok {
		return nil, fmt.Errorf("passCodec: %T", v)
	}
	return m.b, nil
}
func (passCodec) Unmarshal(b []byte, v any) error {
	m, ok := v.(*rawMsg)
	if !ok {
		return fmt.Errorf("passCodec: %T", v)
	}
	m.b = append([]byte(nil), b...)
	return nil
}

// rawClient calls any procedure of a server with whatever bytes it is given.
type rawClient struct {
	hc    connect.HTTPClient
	base  string
	token string
	opts  []connect.ClientOption
}

func (c rawClient) client(proc string) *connect.Client[rawMsg, rawMsg] {
	opts := append([]connect.ClientOption{connect.WithCodec(passCodec{})}, c.opts...)
	hc := c.hc
	if hc == nil {
		hc = http.DefaultClient
	}
	return connect.NewClient[rawMsg, rawMsg](hc, c.base+proc, opts...)
}

func (c rawClient) header(extra http.Header) http.Header {
	h := http.Header{}
	if c.token != "" {
		h.Set("Authorization", "Bearer "+c.token)
	}
	for k, v := range extra {
		h[k] = v
	}
	return h
}

// unary sends req to proc and returns the response bytes.
func (c rawClient) unary(ctx context.Context, proc string, req []byte, extra http.Header) ([]byte, error) {
	r := connect.NewRequest(&rawMsg{b: req})
	for k, v := range c.header(extra) {
		r.Header()[k] = v
	}
	res, err := c.client(proc).CallUnary(ctx, r)
	if err != nil {
		return nil, err
	}
	return res.Msg.b, nil
}

// stream sends req to a server-streaming proc and returns every message. The
// error is the stream's (before or after messages).
func (c rawClient) stream(ctx context.Context, proc string, req []byte, extra http.Header) ([][]byte, error) {
	r := connect.NewRequest(&rawMsg{b: req})
	for k, v := range c.header(extra) {
		r.Header()[k] = v
	}
	st, err := c.client(proc).CallServerStream(ctx, r)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	var out [][]byte
	for st.Receive() {
		out = append(out, append([]byte(nil), st.Msg().b...))
	}
	return out, st.Err()
}

// rawServer is a scripted peer: any procedure, any bytes.
type rawServer struct {
	t   *testing.T
	srv *httptest.Server
	mux *http.ServeMux

	mu    sync.Mutex
	calls map[string]int
	last  map[string][]byte
}

type unaryFn func(ctx context.Context, h http.Header, req []byte) ([]byte, error)
type streamFn func(ctx context.Context, h http.Header, req []byte, send func([]byte) error) error

func newRawServer(t *testing.T) *rawServer {
	t.Helper()
	s := &rawServer{t: t, mux: http.NewServeMux(), calls: map[string]int{}, last: map[string][]byte{}}
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	s.srv = httptest.NewServer(s.mux)
	t.Cleanup(s.srv.Close)
	return s
}

func (s *rawServer) URL() string { return s.srv.URL }

func (s *rawServer) note(proc string, req []byte) {
	s.mu.Lock()
	s.calls[proc]++
	s.last[proc] = append([]byte(nil), req...)
	s.mu.Unlock()
}

func (s *rawServer) count(proc string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[proc]
}

func (s *rawServer) lastReq(proc string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last[proc]
}

func (s *rawServer) unary(proc string, fn unaryFn) {
	s.mux.Handle(proc, connect.NewUnaryHandler(proc,
		func(ctx context.Context, r *connect.Request[rawMsg]) (*connect.Response[rawMsg], error) {
			s.note(proc, r.Msg.b)
			b, err := fn(ctx, r.Header(), r.Msg.b)
			if err != nil {
				return nil, err
			}
			return connect.NewResponse(&rawMsg{b: b}), nil
		}, connect.WithCodec(passCodec{})))
}

func (s *rawServer) stream(proc string, fn streamFn) {
	s.mux.Handle(proc, connect.NewServerStreamHandler(proc,
		func(ctx context.Context, r *connect.Request[rawMsg], st *connect.ServerStream[rawMsg]) error {
			s.note(proc, r.Msg.b)
			return fn(ctx, r.Header(), r.Msg.b, func(b []byte) error { return st.Send(&rawMsg{b: b}) })
		}, connect.WithCodec(passCodec{})))
}

// ---------------------------------------------------------------------------
// bytes of unknown fields

// unknownBlob is a run of fields no v1.0 message declares: every wire type, low
// and high numbers, the highest legal number, a nested message and a group.
// Field numbers 1-14 are not used, because some message may declare one.
func unknownBlob() []byte {
	var b []byte
	b = protowire.AppendTag(b, 15, protowire.VarintType)
	b = protowire.AppendVarint(b, 1<<40+7)
	b = protowire.AppendTag(b, 16, protowire.BytesType)
	b = protowire.AppendString(b, unknownCanary)
	b = protowire.AppendTag(b, 100, protowire.Fixed32Type)
	b = protowire.AppendFixed32(b, 0xdeadbeef)
	b = protowire.AppendTag(b, 1000, protowire.VarintType)
	b = protowire.AppendVarint(b, ^uint64(0)) // a negative int64 / the largest uint64
	b = protowire.AppendTag(b, 1001, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, 0xcafef00dcafef00d)
	b = protowire.AppendTag(b, 1002, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte(unknownCanary+"-bytes\x00\xff\xfe"))
	// a nested message: {1: varint, 2: string, 3: nested {1: varint}}
	var inner []byte
	inner = protowire.AppendTag(inner, 1, protowire.VarintType)
	inner = protowire.AppendVarint(inner, 42)
	inner = protowire.AppendTag(inner, 2, protowire.BytesType)
	inner = protowire.AppendString(inner, unknownCanary+"-nested")
	var deeper []byte
	deeper = protowire.AppendTag(deeper, 1, protowire.VarintType)
	deeper = protowire.AppendVarint(deeper, 7)
	inner = protowire.AppendTag(inner, 3, protowire.BytesType)
	inner = protowire.AppendBytes(inner, deeper)
	b = protowire.AppendTag(b, 1003, protowire.BytesType)
	b = protowire.AppendBytes(b, inner)
	// a group (wire types 3 and 4), which proto3 does not use but still parses
	var grp []byte
	grp = protowire.AppendTag(grp, 1, protowire.VarintType)
	grp = protowire.AppendVarint(grp, 9)
	b = protowire.AppendTag(b, 1004, protowire.StartGroupType)
	b = protowire.AppendGroup(b, 1004, grp)
	b = protowire.AppendTag(b, 2047, protowire.VarintType)
	b = protowire.AppendVarint(b, 1)
	b = protowire.AppendTag(b, 2048, protowire.BytesType)
	b = protowire.AppendString(b, "")
	b = protowire.AppendTag(b, protowire.MaxValidNumber, protowire.VarintType)
	b = protowire.AppendVarint(b, 5)
	return b
}

// withUnknown returns a copy of m with unknownBlob in it and in every message
// nested in it (singular, repeated, map values).
func withUnknown(m proto.Message) proto.Message {
	c := proto.Clone(m)
	addUnknown(c.ProtoReflect())
	return c
}

func addUnknown(m protoreflect.Message) {
	m.SetUnknown(append(m.GetUnknown(), unknownBlob()...))
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Message() != nil:
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				addUnknown(l.Get(i).Message())
			}
		case fd.IsMap() && fd.MapValue().Message() != nil:
			v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
				addUnknown(mv.Message())
				return true
			})
		case fd.Message() != nil && !fd.IsList() && !fd.IsMap():
			addUnknown(v.Message())
		}
		return true
	})
}

// hasUnknown reports whether m or anything nested in it carries unknown fields.
func hasUnknown(m protoreflect.Message) bool {
	if len(m.GetUnknown()) > 0 {
		return true
	}
	found := false
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Message() != nil:
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				found = found || hasUnknown(l.Get(i).Message())
			}
		case fd.IsMap() && fd.MapValue().Message() != nil:
			v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
				found = found || hasUnknown(mv.Message())
				return !found
			})
		case fd.Message() != nil && !fd.IsList() && !fd.IsMap():
			found = found || hasUnknown(v.Message())
		}
		return !found
	})
	return found
}

// errorInfoAny is an ErrorInfo detail carrying exactly the given bytes after the
// known fields (use it to script unknown reasons and unknown fields).
func errorInfoAny(reason int32, message, invocationID string, extra []byte) *anypb.Any {
	info := &commonv1.ErrorInfo{Reason: commonv1.ErrorReason(reason), Message: message, InvocationId: invocationID}
	b, _ := proto.Marshal(info)
	return &anypb.Any{TypeUrl: "type.googleapis.com/edgerun.common.v1.ErrorInfo", Value: append(b, extra...)}
}

// scriptedError is a Connect error with a code and an ErrorInfo of exactly this
// shape.
func scriptedError(code connect.Code, detail *anypb.Any, msg string) error {
	err := connect.NewError(code, errors.New(msg))
	if detail != nil {
		if d, derr := connect.NewErrorDetail(detail); derr == nil {
			err.AddDetail(d)
		}
	}
	return err
}

// ---------------------------------------------------------------------------
// normalising responses

var (
	uuidRe = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	timeRe = regexp.MustCompile(`"\d{4}-\d\d-\d\dT[0-9:.]+Z"`)
	numRe  = regexp.MustCompile(`"(durationMicros|peakMemoryBytes|inFlight|port)":\s*"?\d+"?`)
	revRe  = regexp.MustCompile(`"revision":"[0-9a-f]+"`)
	portRe = regexp.MustCompile(`127\.0\.0\.1:\d+`)
	// names the tests number ("created-7", "r12.example") and digests of those
	nameRe   = regexp.MustCompile(`"([a-z]{1,8}-?)\d{1,5}(\.example)?"`)
	digestRe = regexp.MustCompile(`sha256:[0-9a-f]{64}`)
)

// normalize renders a message as JSON with the values that legitimately differ
// between two runs (ids, times, measurements) blanked.
func normalize(m proto.Message) string {
	b, err := protojson.Marshal(m)
	if err != nil {
		return "unmarshalable: " + err.Error()
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return "unparsable: " + err.Error()
	}
	sortHeaders(v)
	b, _ = json.Marshal(v) // sorted keys, no insignificant whitespace
	s := string(b)
	s = uuidRe.ReplaceAllString(s, "UUID")
	s = timeRe.ReplaceAllString(s, `"TIME"`)
	s = numRe.ReplaceAllString(s, `"${1}":0`)
	s = revRe.ReplaceAllString(s, `"revision":"REV"`)
	s = portRe.ReplaceAllString(s, "ADDR")
	s = nameRe.ReplaceAllString(s, `"$1N$2"`)
	s = digestRe.ReplaceAllString(s, "DIGEST")
	return s
}

// sortHeaders orders the "headers" arrays: a server that builds them from a Go
// map does not promise an order between different names (HTTP does not either).
func sortHeaders(v any) {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			if k == "headers" {
				if arr, ok := e.([]any); ok {
					sort.SliceStable(arr, func(i, j int) bool { return fmt.Sprint(arr[i]) < fmt.Sprint(arr[j]) })
				}
			}
			sortHeaders(e)
		}
	case []any:
		for _, e := range x {
			sortHeaders(e)
		}
	}
}

// ---------------------------------------------------------------------------
// a hand-assembled wagi-1 guest

// wasmModule assembles a WASI command that writes stderr to fd 2, stdout to fd
// 1, then exits with code. It imports nothing but wasi_snapshot_preview1, so it
// is a valid wagi-1 module without any toolchain. Used for the cases the shared
// test guest has no path for (stderr that is not UTF-8).
func wasmModule(stdout, stderr []byte, exit int) []byte {
	leb := func(v uint32) []byte {
		var b []byte
		for {
			c := byte(v & 0x7f)
			v >>= 7
			if v != 0 {
				b = append(b, c|0x80)
			} else {
				return append(b, c)
			}
		}
	}
	sleb := func(v int32) []byte {
		var b []byte
		for {
			c := byte(v & 0x7f)
			v >>= 7
			if (v == 0 && c&0x40 == 0) || (v == -1 && c&0x40 != 0) {
				return append(b, c)
			}
			b = append(b, c|0x80)
		}
	}
	section := func(id byte, body []byte) []byte {
		return append(append([]byte{id}, leb(uint32(len(body)))...), body...)
	}
	vec := func(items ...[]byte) []byte {
		b := leb(uint32(len(items)))
		for _, it := range items {
			b = append(b, it...)
		}
		return b
	}
	name := func(s string) []byte { return append(leb(uint32(len(s))), s...) }

	// memory layout: iovecs at 0 (two of them), nwritten at 32, data from 64
	errOff := uint32(64)
	outOff := errOff + uint32(len(stderr))
	iov := make([]byte, 32)
	binary.LittleEndian.PutUint32(iov[0:], errOff)
	binary.LittleEndian.PutUint32(iov[4:], uint32(len(stderr)))
	binary.LittleEndian.PutUint32(iov[8:], outOff)
	binary.LittleEndian.PutUint32(iov[12:], uint32(len(stdout)))

	types := section(1, vec(
		append([]byte{0x60}, append(vec([]byte{0x7f}, []byte{0x7f}, []byte{0x7f}, []byte{0x7f}), vec([]byte{0x7f})...)...), // fd_write
		append([]byte{0x60}, append(vec([]byte{0x7f}), vec()...)...),                                                       // proc_exit
		append([]byte{0x60}, append(vec(), vec()...)...),                                                                   // _start
	))
	imports := section(2, vec(
		append(append(name("wasi_snapshot_preview1"), name("fd_write")...), 0x00, 0x00),
		append(append(name("wasi_snapshot_preview1"), name("proc_exit")...), 0x00, 0x01),
	))
	funcs := section(3, vec(leb(2)))
	mem := section(5, vec([]byte{0x00, 0x01}))
	exports := section(7, vec(
		append(append(name("memory"), 0x02), leb(0)...),
		append(append(name("_start"), 0x00), leb(2)...),
	))
	call := func(fd int32, iovAt int32, nwrittenAt int32) []byte {
		var c []byte
		c = append(c, 0x41)
		c = append(c, sleb(fd)...)
		c = append(c, 0x41)
		c = append(c, sleb(iovAt)...)
		c = append(c, 0x41)
		c = append(c, sleb(1)...)
		c = append(c, 0x41)
		c = append(c, sleb(nwrittenAt)...)
		c = append(c, 0x10, 0x00, 0x1a) // call fd_write; drop
		return c
	}
	var body []byte
	body = append(body, 0x00) // no locals
	body = append(body, call(2, 0, 32)...)
	body = append(body, call(1, 8, 32)...)
	body = append(body, 0x41)
	body = append(body, sleb(int32(exit))...)
	body = append(body, 0x10, 0x01, 0x0b) // call proc_exit; end
	code := section(10, vec(append(leb(uint32(len(body))), body...)))
	data := append([]byte{0x00, 0x41}, sleb(0)...)
	data = append(data, 0x0b)
	data = append(data, leb(uint32(len(iov)+0))...)
	data = append(data, iov...)
	data2 := append([]byte{0x00, 0x41}, sleb(int32(errOff))...)
	data2 = append(data2, 0x0b)
	payload := append(append([]byte{}, stderr...), stdout...)
	data2 = append(data2, leb(uint32(len(payload)))...)
	data2 = append(data2, payload...)
	datas := section(11, vec(data, data2))

	out := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}
	for _, s := range [][]byte{types, imports, funcs, mem, exports, code, datas} {
		out = append(out, s...)
	}
	return out
}

// codeOK stands for "no error" where a connect.Code is expected (Connect has no
// such code: a call that succeeds has no error at all).
const codeOK connect.Code = 0

func contains(s, sub string) bool { return strings.Contains(s, sub) }
