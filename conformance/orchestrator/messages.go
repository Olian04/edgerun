package orchestratorconf

import (
	"fmt"
	"testing"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// invokeMessages: what an orchestrator does with the bytes of an HTTP message
// (docs/protocol.md, "HTTP messages"): it forwards header values as the opaque
// bytes they are, and refuses a request that is not well formed.
func (h *harness) invokeMessages(t *testing.T) {
	abi := h.abi()
	latin := "caf\xe9 \xff\xfe\x80"
	w := h.worker(t, []string{abi}, respondWith(200, "ok", "", []*commonv1.Header{hdr("x-out", latin), hdr("x-out", "\x80two")}))
	f := h.deploy(t, abi)
	h.served(t, f.host)

	t.Run("header values are bytes, forwarded in both directions unchanged", func(t *testing.T) {
		before := w.callCount()
		r := h.mustInvoke(t, call{host: f.host, headers: []*commonv1.Header{hdr("x-in", latin), hdr("x-in", "\xe9"), hdr("x-tab", "a\tb")}})
		calls := w.calls()
		if len(calls) != before+1 {
			t.Fatalf("%d calls to the worker, want 1", len(calls)-before)
		}
		var got []string
		for _, hd := range calls[len(calls)-1].req.GetRequest().GetHeaders() {
			got = append(got, hd.GetName()+"="+string(hd.GetValue()))
		}
		if want := fmt.Sprintf("%q", []string{"x-in=" + latin, "x-in=\xe9", "x-tab=a\tb"}); fmt.Sprintf("%q", got) != want {
			t.Errorf("request headers to the worker: %q, want %s", got, want)
		}
		var out []string
		for _, hd := range r.head.GetHttp().GetHeaders() {
			out = append(out, hd.GetName()+"="+string(hd.GetValue()))
		}
		if want := fmt.Sprintf("%q", []string{"x-out=" + latin, "x-out=\x80two"}); fmt.Sprintf("%q", out) != want {
			t.Errorf("response headers to the frontend: %q, want %s", out, want)
		}
	})

	t.Run("a request that is not well formed is refused and goes nowhere", func(t *testing.T) {
		before := w.callCount()
		recordsBefore := len(h.records(t, f.fn.GetId(), 0))
		for what, c := range map[string]call{
			"a path without its first slash":  {host: f.host, path: "no-slash"},
			"a NUL in the path":               {host: f.host, path: "/a\x00b"},
			"a newline in the path":           {host: f.host, path: "/a\nb"},
			"a control character in the path": {host: f.host, path: "/a\x1bb"},
			"a newline in the query":          {host: f.host, rawQuery: "a=\nb"},
			"a header name with a space":      {host: f.host, headers: []*commonv1.Header{hdr("bad name", "x")}},
			"an empty header name":            {host: f.host, headers: []*commonv1.Header{hdr("", "x")}},
			"a header value with CRLF":        {host: f.host, headers: []*commonv1.Header{hdr("x-a", "a\r\nx-b: c")}},
			"a header value with NUL":         {host: f.host, headers: []*commonv1.Header{hdr("x-a", "a\x00b")}},
		} {
			r := h.invoke(t, c)
			if r.err == nil || connect.CodeOf(r.err) != connect.CodeInvalidArgument {
				t.Errorf("%s: %v, want invalid_argument", what, r.err)
			}
			if rs, _ := check.ReasonOf(r.err); rs != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
				t.Errorf("%s: a malformed request carries no reason, got %v", what, rs)
			}
		}
		if w.callCount() != before {
			t.Errorf("a malformed request reached the worker (%d calls)", w.callCount()-before)
		}
		if len(h.records(t, f.fn.GetId(), 0)) != recordsBefore {
			t.Errorf("a malformed request left an invocation record: it was never an invocation")
		}
	})

	t.Run("a path is text of any language", func(t *testing.T) {
		r := h.invoke(t, call{host: f.host, path: "/caf\u00e9/\u65e5\u672c?"})
		if r.err != nil {
			t.Errorf("a UTF-8 path: %v", r.err)
		}
	})
}
