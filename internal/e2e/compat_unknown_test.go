package e2e

// Part 1(a): every RPC of every service, against the real component that
// serves it, receives its request with unknown fields (every wire type, at
// every nesting level, before and after the known fields), with unknown enum
// values, with every optional field absent and with every field written out at
// its zero value. Each must be tolerated exactly as the clean request is.

import (
	"bytes"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// outcome is what a call came to.
type outcome struct {
	code   connect.Code // CodeOK (0) if it succeeded
	reason commonv1.ErrorReason
	err    error
	msgs   []string // the response messages, normalised
	raw    [][]byte
	// unknown is set if a response message carried unknown fields.
	unknown bool
}

func (o outcome) String() string {
	if o.err != nil {
		return o.code.String() + "/" + o.reason.String()
	}
	return "ok"
}

func (e *cenv) do(c rpcCase, req []byte) outcome {
	e.t.Helper()
	cctx, cancel := ctxWithTimeout(60e9)
	defer cancel()
	rc := e.client(c)
	var raw [][]byte
	var err error
	if c.streaming(e.t) {
		raw, err = rc.stream(cctx, c.proc(), req, nil)
	} else {
		var b []byte
		b, err = rc.unary(cctx, c.proc(), req, nil)
		if err == nil {
			raw = [][]byte{b}
		}
	}
	o := outcome{err: err, raw: raw}
	if err != nil {
		o.code, o.reason = connect.CodeOf(err), edgerun.Reason(err)
	}
	for _, b := range raw {
		m := c.newResp(e.t)
		if perr := proto.Unmarshal(b, m); perr != nil {
			e.t.Errorf("%s: the response does not parse: %v", c.proc(), perr)
			continue
		}
		o.unknown = o.unknown || hasUnknown(m.ProtoReflect())
		o.msgs = append(o.msgs, normalize(m))
	}
	return o
}

func mustMarshal(t testing.TB, m proto.Message) []byte {
	t.Helper()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// explicitZeros is the wire encoding of every singular, non-oneof field of m's
// type that m does not set, written out at its zero value (a message as an empty
// message): what an implementation that always writes its fields sends.
func explicitZeros(m proto.Message) []byte {
	var b []byte
	md := m.ProtoReflect().Descriptor()
	for i := 0; i < md.Fields().Len(); i++ {
		fd := md.Fields().Get(i)
		if fd.ContainingOneof() != nil || fd.IsList() || fd.IsMap() || m.ProtoReflect().Has(fd) {
			continue
		}
		n := fd.Number()
		switch fd.Kind() {
		case protoreflect.BoolKind, protoreflect.EnumKind, protoreflect.Int32Kind, protoreflect.Int64Kind,
			protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.Sint32Kind, protoreflect.Sint64Kind:
			b = protowire.AppendTag(b, n, protowire.VarintType)
			b = protowire.AppendVarint(b, 0)
		case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind, protoreflect.FloatKind:
			b = protowire.AppendTag(b, n, protowire.Fixed32Type)
			b = protowire.AppendFixed32(b, 0)
		case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind, protoreflect.DoubleKind:
			b = protowire.AppendTag(b, n, protowire.Fixed64Type)
			b = protowire.AppendFixed64(b, 0)
		default: // string, bytes, message
			b = protowire.AppendTag(b, n, protowire.BytesType)
			b = protowire.AppendBytes(b, nil)
		}
	}
	return b
}

// emptyMessages is a request of c's type in which every message-typed field is
// present and empty, and nothing else is set.
func emptyMessages(t testing.TB, c rpcCase) proto.Message {
	m := c.newReq(t)
	r := m.ProtoReflect()
	fields := r.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.Message() != nil && !fd.IsList() && !fd.IsMap() {
			r.Mutable(fd)
		}
	}
	return m
}

func (c rpcCase) name() string {
	return c.service[strings.LastIndex(c.service, ".")+1:] + "." + c.method
}

func okOutcome(c rpcCase, want []connect.Code) bool {
	if len(want) == 0 {
		return true
	}
	for _, w := range want {
		if w == codeOK {
			return true
		}
	}
	return false
}

func TestCompatEveryRPCToleratesUnknownFieldsEnumsAndZeroValues(t *testing.T) {
	e := newCEnv(t)
	empties := map[string]string{}

	for _, c := range catalogue() {
		t.Run(c.name(), func(t *testing.T) {
			e.t, e.s.t = t, t
			clean := e.do(c, mustMarshal(t, c.req(e)))
			if clean.err != nil {
				t.Fatalf("the clean request itself fails: %v", clean.err)
			}
			if clean.unknown {
				t.Errorf("a response carries unknown fields")
			}

			same := func(what string, got outcome) {
				t.Helper()
				if got.err != nil {
					t.Errorf("%s: %v (the clean request succeeds)", what, got.err)
					return
				}
				if got.unknown {
					t.Errorf("%s: the response carries unknown fields (they must not be echoed)", what)
				}
				if len(got.msgs) != len(clean.msgs) {
					t.Errorf("%s: %d response messages, the clean request got %d", what, len(got.msgs), len(clean.msgs))
					return
				}
				if !c.loose {
					for i := range got.msgs {
						if got.msgs[i] != clean.msgs[i] {
							t.Errorf("%s: response %d differs from the clean one\n  clean: %s\n  got:   %s", what, i, clean.msgs[i], got.msgs[i])
						}
					}
				}
				for _, r := range got.raw {
					if bytes.Contains(r, []byte(unknownCanary)) {
						t.Errorf("%s: the response contains the unknown field's payload", what)
					}
				}
			}

			// unknown fields of every wire type, at every level, after the known fields
			same("unknown fields after the known ones", e.do(c, mustMarshal(t, withUnknown(c.req(e)))))
			// and before them (the order of fields on the wire is not part of the message)
			same("unknown fields before the known ones", e.do(c, append(unknownBlob(), mustMarshal(t, c.req(e))...)))
			// every unset field written out at its zero value
			req := c.req(e)
			same("every unset field written as its zero value", e.do(c, append(mustMarshal(t, req), explicitZeros(req)...)))

			// every field absent
			zero := e.do(c, nil)
			empties[c.name()] = zero.String()
			if len(c.zero) == 0 {
				if zero.err != nil {
					t.Errorf("the empty request: %v, want success", zero.err)
				}
			} else {
				ok := false
				for _, w := range c.zero {
					ok = ok || (w == zero.code && zero.err != nil) || (w == codeOK && zero.err == nil)
				}
				if !ok {
					t.Errorf("the empty request: %s, want one of %v", zero, c.zero)
				}
				if c.zeroReason != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED && zero.reason != c.zeroReason {
					t.Errorf("the empty request: reason %s, want %s", zero.reason, c.zeroReason)
				}
			}
			if zero.unknown {
				t.Errorf("a response carries unknown fields")
			}

			// every message field present but empty, nothing else set: a defined answer
			pe := e.do(c, mustMarshal(t, emptyMessages(t, c)))
			switch pe.code {
			case connect.CodeInternal, connect.CodeUnknown, connect.CodeUnavailable, connect.CodeDataLoss, connect.CodeUnimplemented:
				t.Errorf("present-but-empty messages: %s: %v", pe, pe.err)
			}
			if pe.reason == commonv1.ErrorReason_ERROR_REASON_INTERNAL {
				t.Errorf("present-but-empty messages: INTERNAL: %v", pe.err)
			}

			// an enum the server validates, with values it has never heard of
			if c.enum != "" {
				fd := c.newReq(t).ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(c.enum))
				for _, v := range []int64{99, -1, 2147483647, 77} {
					req := c.req(e)
					b := mustMarshal(t, req)
					b = append(b, protowire.AppendTag(nil, fd.Number(), protowire.VarintType)...)
					b = protowire.AppendVarint(b, uint64(v))
					o := e.do(c, b)
					if o.code != connect.CodeInvalidArgument {
						t.Errorf("%s=%d: %s, want invalid_argument (docs: anything but READY or DRAINING)", c.enum, v, o)
					}
					if o.reason != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
						t.Errorf("%s=%d: carries reason %s; a malformed request has none", c.enum, v, o.reason)
					}
				}
				// the enum's wire value is an int32: a varint beyond it is truncated by
				// every protobuf runtime, so 2^32+1 reads as READY. Not a defect, but
				// worth knowing; it is only required not to break anything.
				b := mustMarshal(t, c.req(e))
				b = append(b, protowire.AppendTag(nil, fd.Number(), protowire.VarintType)...)
				b = protowire.AppendVarint(b, 1<<32+1)
				if o := e.do(c, b); o.code == connect.CodeInternal || o.code == connect.CodeUnknown {
					t.Errorf("%s=2^32+1: %s", c.enum, o)
				}
			}
		})
	}
	e.t, e.s.t = t, t

	// the system still works
	if r := e.s.call(req{host: "compat.test", path: "/echo", method: "POST", body: "still fine"}); r.err != nil || string(r.body) != "still fine" {
		t.Errorf("after all of it, an invocation: %v %q", r.err, r.body)
	}
	// and no component wrote an unknown field into its log
	for name, sink := range e.logs() {
		if strings.Contains(sink.String(), unknownCanary) {
			t.Errorf("the %s's log contains the payload of an unknown field", name)
		}
	}
	for name, got := range empties {
		t.Logf("empty request to %-34s -> %s", name, got)
	}
}
