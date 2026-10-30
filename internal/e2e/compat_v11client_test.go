package e2e

// Part 1(c), one direction: a "newer" client (v1.1 descriptors built at run
// time, see compat_v11_test.go) talks to the real servers.

import (
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

func h2cClient() *http.Client {
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	return &http.Client{Transport: &http.Transport{Protocols: &p}}
}

func http1Client() *http.Client {
	var p http.Protocols
	p.SetHTTP1(true)
	return &http.Client{Transport: &http.Transport{Protocols: &p}}
}

func TestCompatNewerClientAgainstRealServers(t *testing.T) {
	e := newCEnv(t)
	v := buildV11(t)
	cases := catalogue()

	t.Run("a v1.1 request is answered as the v1.0 one is", func(t *testing.T) {
		for _, c := range cases {
			t.Run(c.name(), func(t *testing.T) {
				e.t, e.s.t = t, t
				clean := e.do(c, mustMarshal(t, c.req(e)))
				if clean.err != nil {
					t.Fatalf("the clean request fails: %v", clean.err)
				}
				up := e.do(c, v.upgrade(t, c.req(e)))
				if up.err != nil {
					t.Fatalf("the v1.1 request (new string and message fields, at every level) fails: %v", up.err)
				}
				if up.unknown {
					t.Errorf("the response carries unknown fields")
				}
				if !c.loose {
					for i := range up.msgs {
						if i < len(clean.msgs) && up.msgs[i] != clean.msgs[i] {
							t.Errorf("response %d differs\n  v1.0: %s\n  v1.1: %s", i, clean.msgs[i], up.msgs[i])
						}
					}
				}
				// what comes back is a v1.0 message: a v1.1 reader parses it with every
				// new field absent, none invented
				for _, raw := range up.raw {
					d := dynamicpb.NewMessage(v.message(t, string(c.desc(t).Output().FullName())))
					if err := proto.Unmarshal(raw, d); err != nil {
						t.Fatalf("a v1.1 reader cannot parse the response: %v", err)
					}
					if d.Has(d.Descriptor().Fields().ByName("v11_note")) || d.Has(d.Descriptor().Fields().ByName("v11_extra")) {
						t.Errorf("the v1.0 server invented a v1.1 field")
					}
				}
			})
		}
	})

	t.Run("an enum value that only v1.1 has is refused where the docs say", func(t *testing.T) {
		for _, c := range cases {
			if c.enum == "" {
				continue
			}
			req := c.req(e)
			d := dynamicpb.NewMessage(v.message(t, string(req.ProtoReflect().Descriptor().FullName())))
			if err := proto.Unmarshal(mustMarshal(t, req), d); err != nil {
				t.Fatal(err)
			}
			fd := d.Descriptor().Fields().ByName(protoreflect.Name(c.enum))
			if fd.Enum().Values().ByNumber(v11NewEnum) == nil {
				t.Fatalf("v1.1 has no new value in %s", fd.Enum().FullName())
			}
			d.Set(fd, protoreflect.ValueOfEnum(v11NewEnum))
			b, _ := proto.Marshal(d)
			o := e.do(c, b)
			if o.code != connect.CodeInvalidArgument {
				t.Errorf("%s with the v1.1-only %s: %s, want invalid_argument", c.name(), fd.Enum().Name(), o)
			}
		}
	})

	t.Run("an RPC that only v1.1 has is unimplemented, not a hang or a 5xx", func(t *testing.T) {
		target := map[string]rpcCase{}
		for _, c := range cases {
			target[c.service] = c
		}
		protocols := map[string][]connect.ClientOption{
			"connect":  nil,
			"grpc":     {connect.WithGRPC()},
			"grpc-web": {connect.WithGRPCWeb()},
		}
		seen := 0
		for _, sd := range v.services {
			c, ok := target[string(sd.FullName())]
			if !ok {
				t.Fatalf("service %s of v1.1 is not in the catalogue", sd.FullName())
			}
			for pname, opts := range protocols {
				rc := e.client(c)
				rc.hc, rc.opts = h2cClient(), opts
				extra := mustMarshal(t, &commonv1.ErrorInfo{Message: "v1.1 request"})
				for _, m := range []string{"V11Ping", "V11Watch"} {
					proc := "/" + string(sd.FullName()) + "/" + m
					cctx, cancel := ctxWithTimeout(10e9)
					var err error
					if m == "V11Watch" {
						_, err = rc.stream(cctx, proc, extra, nil)
					} else {
						_, err = rc.unary(cctx, proc, extra, nil)
					}
					cancel()
					seen++
					if connect.CodeOf(err) != connect.CodeUnimplemented {
						t.Errorf("%s over %s: %v, want unimplemented", proc, pname, err)
					}
				}
			}
		}
		t.Logf("%d calls to RPCs of a newer version, all unimplemented", seen)
	})

	// what a v1.1 server could not rely on: unknown fields were ignored, so the
	// state of the real system is what the known fields said
	e.t, e.s.t = t, t
	if r := e.s.call(req{host: "compat.test", path: "/echo", method: "POST", body: "after v1.1"}); r.err != nil || string(r.body) != "after v1.1" {
		t.Errorf("after all of it: %v %q", r.err, r.body)
	}
	for name, sink := range e.logs() {
		if contains(sink.String(), unknownCanary) {
			t.Errorf("the %s's log contains the payload of a v1.1 field", name)
		}
	}
	_ = protowire.MaxValidNumber
}
