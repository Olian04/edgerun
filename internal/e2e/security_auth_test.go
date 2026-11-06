package e2e

// Authentication over raw HTTP, on every service of every role: what an
// attacker with no client library, or one that is buggy, can send.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

type secResp struct {
	status  int
	code    string // Connect error code, "" if none
	message string
	header  http.Header
	body    string
	err     error // the connection failed or was cut: the request did not get through
}

// outcome classifies what a request achieved against an authenticated RPC.
func (r secResp) outcome() string {
	switch {
	case r.err != nil:
		return "blocked"
	case r.code == "unauthenticated":
		return "unauthenticated"
	case r.code == "permission_denied":
		return "permission_denied"
	case r.status == 200 || r.code != "":
		// A call that got as far as being judged on its content (invalid_argument,
		// not_found...) was let in.
		return "passed"
	}
	return "blocked"
}

var secClient = &http.Client{
	Timeout:       20 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	Transport:     &http.Transport{DisableCompression: true, MaxIdleConnsPerHost: 4},
}

// secEnvelope frames one Connect streaming message.
func secEnvelope(flags byte, msg []byte) []byte {
	b := make([]byte, 5, 5+len(msg))
	b[0] = flags
	binary.BigEndian.PutUint32(b[1:], uint32(len(msg)))
	return append(b, msg...)
}

// secRPC calls one RPC the way a Connect client would, but with the headers the
// test chooses. Streaming procedures use the Connect streaming framing.
func secRPC(base, proc string, streaming bool, hdr http.Header, query string) secResp {
	return secRPCBody(base, proc, streaming, hdr, query, []byte("{}"), "")
}

func secRPCBody(base, proc string, streaming bool, hdr http.Header, query string, msg []byte, contentEncoding string) secResp {
	u := base + proc
	if query != "" {
		u += "?" + query
	}
	var body []byte
	ct := "application/json"
	if streaming {
		ct = "application/connect+json"
		body = secEnvelope(0, msg)
	} else {
		body = msg
	}
	r, err := http.NewRequest("POST", u, bytes.NewReader(body))
	if err != nil {
		return secResp{err: err}
	}
	r.Header.Set("Content-Type", ct)
	if contentEncoding != "" {
		r.Header.Set("Content-Encoding", contentEncoding)
		r.Header.Set("Connect-Content-Encoding", contentEncoding)
	}
	for k, vs := range hdr {
		r.Header[k] = vs
	}
	return secDo(r, streaming)
}

func secDo(r *http.Request, streaming bool) secResp {
	resp, err := secClient.Do(r)
	if err != nil {
		return secResp{err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	out := secResp{status: resp.StatusCode, header: resp.Header, body: string(b)}
	if err != nil {
		out.err = err
	}
	if streaming && resp.StatusCode == 200 {
		// The error, if any, is in the end-of-stream message (flag 0x02).
		for len(b) >= 5 {
			n := int(binary.BigEndian.Uint32(b[1:5]))
			if 5+n > len(b) {
				break
			}
			if b[0]&2 != 0 {
				var e struct {
					Error *struct{ Code, Message string } `json:"error"`
				}
				if json.Unmarshal(b[5:5+n], &e) == nil && e.Error != nil {
					out.code, out.message = e.Error.Code, e.Error.Message
				}
			}
			b = b[5+n:]
		}
		return out
	}
	var e struct{ Code, Message string }
	if json.Unmarshal([]byte(out.body), &e) == nil {
		out.code, out.message = e.Code, e.Message
	}
	return out
}

type secProc struct {
	name      string
	streaming bool
	scope     string
}

const (
	ctlPkg    = "/edgerun.control.v1."
	orchScope = "orchestrator"
)

func secOrchestratorProcs() []secProc {
	var ps []secProc
	add := func(scope string, streaming bool, names ...string) {
		for _, n := range names {
			ps = append(ps, secProc{n, streaming, scope})
		}
	}
	add("ingress", false, "/edgerun.ingress.v1.IngressService/Describe")
	add("ingress", true, "/edgerun.ingress.v1.IngressService/Invoke")
	add("worker", false, "/edgerun.worker.v1.FleetService/Register", "/edgerun.worker.v1.FleetService/Heartbeat", "/edgerun.worker.v1.FleetService/Deregister")
	add("worker", true, "/edgerun.worker.v1.ArtifactService/GetArtifact")
	add("network", false, "/edgerun.egress.v1.RegistryService/Register", "/edgerun.egress.v1.RegistryService/Heartbeat", "/edgerun.egress.v1.RegistryService/Deregister")
	for _, n := range []string{"RouteService/CreateRoute", "RouteService/ListRoutes", "RouteService/DeleteRoute", "RouteService/SetRouteLabels", "RouteService/PatchRouteLabels",
		"InspectionService/Describe", "InspectionService/ListWorkers", "InspectionService/ListInvocations", "InspectionService/GetInvocation", "InspectionService/ListNetworkServices",
		"FunctionService/CreateFunction", "FunctionService/GetFunction", "FunctionService/ListFunctions", "FunctionService/DeleteFunction", "FunctionService/CreateVersion",
		"FunctionService/ListVersions", "FunctionService/ActivateVersion", "FunctionService/SetEnv", "FunctionService/DeleteEnv", "FunctionService/ListEnv",
		"FunctionService/SetEgress", "FunctionService/SetLabels", "FunctionService/PatchLabels", "PlanService/Validate", "PlanService/Apply"} {
		add("admin", false, ctlPkg+n)
	}
	return ps
}

// authVariants are the ways of presenting tok that must not be accepted.
// soft ones are only reported: the protocol is silent or HTTP allows them.
type authVariant struct {
	name  string
	hdr   http.Header
	query string
	soft  bool
}

func secAuthVariants(tok string) []authVariant {
	b64 := base64.StdEncoding.EncodeToString
	h := func(k string, v ...string) http.Header { return http.Header{k: v} }
	bad := "Bearer not-the-token"
	vs := []authVariant{
		{name: "no credentials", hdr: http.Header{}},
		{name: "empty bearer", hdr: h("Authorization", "Bearer ")},
		{name: "bearer without a token", hdr: h("Authorization", "Bearer")},
		{name: "two spaces", hdr: h("Authorization", "Bearer  "+tok)},
		{name: "tab", hdr: h("Authorization", "Bearer\t"+tok)},
		{name: "token only", hdr: h("Authorization", tok)},
		{name: "scheme Token", hdr: h("Authorization", "Token "+tok)},
		{name: "scheme Basic user", hdr: h("Authorization", "Basic "+b64([]byte(tok+":")))},
		{name: "scheme Basic password", hdr: h("Authorization", "Basic "+b64([]byte(":"+tok)))},
		{name: "scheme Basic bare", hdr: h("Authorization", "Basic "+b64([]byte(tok)))},
		{name: "token with a suffix", hdr: h("Authorization", "Bearer "+tok+"x")},
		{name: "token with a prefix", hdr: h("Authorization", "Bearer x"+tok)},
		{name: "token truncated", hdr: h("Authorization", "Bearer "+tok[:len(tok)-1])},
		{name: "token upper-cased", hdr: h("Authorization", "Bearer "+strings.ToUpper(tok))},
		{name: "token and a comma", hdr: h("Authorization", "Bearer "+tok+", Bearer "+tok)},
		{name: "token then NBSP", hdr: h("Authorization", "Bearer "+tok+" ")},
		{name: "unicode token", hdr: h("Authorization", "Bearer tökén-☃")},
		{name: "unicode homoglyph of the token", hdr: h("Authorization", "Bearer "+strings.Replace(tok, "o", "о", 1))},
		{name: "very long token", hdr: h("Authorization", "Bearer "+strings.Repeat("A", 70<<10))},
		{name: "huge token", hdr: h("Authorization", "Bearer "+strings.Repeat("A", 2<<20))},
		{name: "the invalid one first", hdr: h("Authorization", bad, "Bearer "+tok)},
		{name: "token in the query", hdr: http.Header{}, query: "token=" + url.QueryEscape(tok)},
		{name: "access_token in the query", hdr: http.Header{}, query: "access_token=" + url.QueryEscape(tok)},
		{name: "Authorization in the query", hdr: http.Header{}, query: "Authorization=" + url.QueryEscape("Bearer "+tok)},
		{name: "authorization in the query, lower", hdr: http.Header{}, query: "authorization=Bearer+" + url.QueryEscape(tok)},
		{name: "token in a cookie", hdr: h("Cookie", "token="+tok)},
		{name: "Authorization in a cookie", hdr: h("Cookie", "Authorization=Bearer%20"+tok+"; authorization=Bearer "+tok)},
		{name: "session cookie", hdr: h("Cookie", "edgerun_session="+tok)},
		{name: "X-Auth-Token", hdr: h("X-Auth-Token", tok)},
		{name: "X-Api-Key", hdr: h("X-Api-Key", tok)},
		{name: "X-Edgerun-Token", hdr: h("X-Edgerun-Token", tok)},
		{name: "Proxy-Authorization", hdr: h("Proxy-Authorization", "Bearer "+tok)},
		{name: "X-Forwarded-Authorization", hdr: h("X-Forwarded-Authorization", "Bearer "+tok)},
		{name: "X-Original-Authorization", hdr: h("X-Original-Authorization", "Bearer "+tok)},
		{name: "an Authorization-like header name", hdr: h("Authorization-Token", "Bearer "+tok)},
		{name: "Authorization with a trailing space in its name", hdr: http.Header{"Authorization ": {"Bearer " + tok}}},
		{name: "trusted-looking source address", hdr: http.Header{"X-Forwarded-For": {"127.0.0.1"}, "X-Real-Ip": {"127.0.0.1"}, "Forwarded": {"for=127.0.0.1"}, "X-Forwarded-Proto": {"https"}}},
		{name: "a different scheme and the real token in another header", hdr: http.Header{"Authorization": {"Digest username=x"}, "X-Auth-Token": {tok}}},
		// Reported, not judged: RFC 9110 says the scheme is case-insensitive, and the
		// protocol writes "Bearer"; a duplicate header is ambiguous in HTTP.
		{name: "scheme in lower case", hdr: h("Authorization", "bearer "+tok), soft: true},
		{name: "scheme in upper case", hdr: h("Authorization", "BEARER "+tok), soft: true},
		{name: "the valid one first, an invalid one second", hdr: h("Authorization", "Bearer "+tok, bad), soft: true},
	}
	return vs
}

func (s *stack) workerAddr(t *testing.T) string {
	t.Helper()
	var addr string
	eventually(t, "a worker to register", 60*time.Second, func() bool {
		r, err := s.ctl.ListWorkers(ctx, &controlv1.ListWorkersRequest{})
		if err != nil {
			return false
		}
		for _, w := range r.Workers {
			if w.Alive {
				addr = strings.TrimPrefix(w.Endpoint, "http://")
				return true
			}
		}
		return false
	})
	return addr
}

func TestSecurityAuthenticationMatrix(t *testing.T) {
	s := newStack(t)
	s.secNetWorker(2)
	egAddr := s.secRealEgress(t, []string{"*"})
	workerAddr := s.workerAddr(t)

	type target struct {
		name   string
		base   string
		procs  []secProc
		tokens map[string]string // scope -> token this server knows
	}
	targets := []target{
		{"orchestrator", "http://" + s.addr, secOrchestratorProcs(), map[string]string{"ingress": ingressToken, "worker": workerToken, "admin": adminToken, "network": networkToken}},
		{"worker", "http://" + workerAddr, []secProc{{"/edgerun.worker.v1.ExecutionService/Describe", false, "execute"}, {"/edgerun.worker.v1.ExecutionService/Execute", true, "execute"}},
			map[string]string{"execute": executionToken}},
		{"egress", "http://" + egAddr, []secProc{{"/edgerun.egress.v1.EgressService/Describe", false, "egress"}, {"/edgerun.egress.v1.EgressService/Fetch", true, "egress"}},
			map[string]string{"egress": egressToken}},
	}
	// Every token of the deployment, so that a token of another scope or another
	// server is tried everywhere.
	all := map[string]string{"ingress": ingressToken, "worker": workerToken, "admin": adminToken, "network": networkToken, "execute": executionToken, "egress": egressToken}
	bearer := func(tok string) http.Header { return http.Header{"Authorization": {"Bearer " + tok}} }

	for _, tg := range targets {
		tg := tg
		t.Run(tg.name, func(t *testing.T) {
			var soft []string
			for _, p := range tg.procs {
				good := tg.tokens[p.scope]
				// The right token is let in.
				if got := secRPC(tg.base, p.name, p.streaming, bearer(good), "").outcome(); got != "passed" {
					t.Errorf("%s with its own token: %s", p.name, got)
				}
				// A token of another scope or of another server is not.
				var names []string
				for n := range all {
					names = append(names, n)
				}
				sort.Strings(names)
				for _, scope := range names {
					if all[scope] == good {
						continue
					}
					want := "unauthenticated" // a server only knows its own tokens
					if _, known := tg.tokens[scope]; known {
						want = "permission_denied"
					}
					if got := secRPC(tg.base, p.name, p.streaming, bearer(all[scope]), "").outcome(); got != want {
						t.Errorf("%s with the %s token: %s, want %s", p.name, scope, got, want)
					}
				}
				// Every way of presenting the right token that is not the right way.
				for _, v := range secAuthVariants(good) {
					got := secRPC(tg.base, p.name, p.streaming, v.hdr, v.query).outcome()
					if v.soft {
						if got == "passed" {
							soft = append(soft, v.name)
						}
						continue
					}
					if got == "passed" {
						t.Errorf("AUTH BYPASS %s: %s was accepted", p.name, v.name)
					}
				}
			}
			if len(soft) > 0 {
				seen := map[string]bool{}
				var uniq []string
				for _, x := range soft {
					if !seen[x] {
						seen[x] = true
						uniq = append(uniq, x)
					}
				}
				t.Logf("%s accepts (not a bypass, reported): %v", tg.name, uniq)
			}
		})
	}
}

// The same RPCs by other routes to the same handler: other HTTP methods, a GET
// with the message in the query, gRPC-Web, and paths that are not quite the
// procedure's path. None of them may skip the credential check.
func TestSecurityAuthenticationOtherRoutes(t *testing.T) {
	s := newStack(t)
	base := "http://" + s.addr
	fn := ctlPkg + "FunctionService/ListFunctions"

	do := func(method, path, ct string, body []byte, hdr http.Header) secResp {
		r, _ := http.NewRequest(method, base+path, bytes.NewReader(body))
		if ct != "" {
			r.Header.Set("Content-Type", ct)
		}
		for k, v := range hdr {
			r.Header[k] = v
		}
		return secDo(r, false)
	}
	// Without credentials, nothing is returned by any route.
	for _, c := range []struct {
		name         string
		method, path string
		ct           string
		body         []byte
	}{
		{"GET with the message in the query", "GET", fn + "?connect=v1&encoding=json&message=%7B%7D", "", nil},
		{"GET, base64 message", "GET", fn + "?connect=v1&encoding=proto&base64=1&message=", "", nil},
		{"PUT", "PUT", fn, "application/json", []byte("{}")},
		{"PATCH", "PATCH", fn, "application/json", []byte("{}")},
		{"DELETE", "DELETE", fn, "application/json", []byte("{}")},
		{"HEAD", "HEAD", fn, "", nil},
		{"OPTIONS (CORS-style)", "OPTIONS", fn, "", nil},
		{"proto", "POST", fn, "application/proto", nil},
		{"gRPC-Web", "POST", fn, "application/grpc-web+proto", secEnvelope(0, nil)},
		{"gRPC", "POST", fn, "application/grpc", secEnvelope(0, nil)},
		{"streaming content type on a unary procedure", "POST", fn, "application/connect+json", secEnvelope(0, []byte("{}"))},
		{"trailing slash", "POST", fn + "/", "application/json", []byte("{}")},
		{"double slash", "POST", "/" + fn, "application/json", []byte("{}")},
		{"upper case", "POST", strings.ToUpper(fn), "application/json", []byte("{}")},
		{"dot segments", "POST", "/edgerun.ingress.v1.IngressService/../" + fn[1:], "application/json", []byte("{}")},
		{"encoded dot segments", "POST", "/edgerun.ingress.v1.IngressService/%2e%2e/" + fn[1:], "application/json", []byte("{}")},
		{"encoded slash", "POST", "/edgerun.control.v1.FunctionService%2FListFunctions", "application/json", []byte("{}")},
		{"path parameter", "POST", fn + ";x=1", "application/json", []byte("{}")},
		{"fragment-looking", "POST", fn + "%23x", "application/json", []byte("{}")},
	} {
		r := do(c.method, c.path, c.ct, c.body, http.Header{})
		if strings.HasPrefix(c.ct, "application/grpc") {
			// gRPC answers 200 and carries the status in a header or trailer.
			if gs := secGRPCStatus(r); gs == "0" {
				t.Errorf("AUTH BYPASS %s: grpc-status 0 (%q)", c.name, r.body)
			}
			continue
		}
		if r.err == nil && (r.status == 200 || (r.code != "" && r.code != "unauthenticated" && r.code != "permission_denied")) {
			t.Errorf("AUTH BYPASS %s: status %d code %q body %q", c.name, r.status, r.code, r.body)
		}
		if r.err == nil && strings.Contains(r.body, "functions") {
			t.Errorf("%s: a listing came back without credentials: %q", c.name, r.body)
		}
	}
	// With another scope's token, by other paths: a token that may call ingress must
	// not reach control by a path trick.
	for _, path := range []string{
		"/edgerun.ingress.v1.IngressService/../" + fn[1:], "/edgerun.ingress.v1.IngressService/%2e%2e/" + fn[1:], "//" + fn[1:],
	} {
		r := do("POST", path, "application/json", []byte("{}"), http.Header{"Authorization": {"Bearer " + ingressToken}})
		if r.err == nil && (r.status == 200 || (r.code != "" && r.code != "permission_denied" && r.code != "unauthenticated")) {
			t.Errorf("AUTH BYPASS: the ingress token reached ListFunctions through %q: %d %s", path, r.status, r.body)
		}
	}
}

// Unauthenticated endpoints expose nothing: health says "ok", and nothing else
// of a service (profiling, metrics, version, configuration) is served.
func TestSecurityUnauthenticatedEndpointsExposeNothing(t *testing.T) {
	s := newStack(t)
	s.secNetWorker(2)
	egAddr := s.secRealEgress(t, []string{"*"})
	workerAddr := s.workerAddr(t)
	web := s.startFrontend(5 * time.Second)

	bases := map[string]string{"orchestrator": "http://" + s.addr, "worker": "http://" + workerAddr, "egress": "http://" + egAddr, "frontend": web.base}
	secrets := []string{adminToken, ingressToken, workerToken, executionToken, egressToken, networkToken, "postgres", "password", "secret"}
	for name, base := range bases {
		get := func(path string) secResp {
			r, _ := http.NewRequest("GET", base+path, nil)
			return secDo(r, false)
		}
		if name != "frontend" {
			r := get("/healthz")
			if r.status != 200 || strings.TrimSpace(r.body) != "ok" {
				t.Errorf("%s /healthz: %d %q: it should say ok and nothing else", name, r.status, r.body)
			}
			for k := range r.header {
				if k == "Server" || strings.HasPrefix(k, "X-Powered") {
					t.Errorf("%s /healthz carries %s: %v", name, k, r.header[k])
				}
			}
		}
		for _, path := range []string{
			"/", "/metrics", "/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/goroutine?debug=2", "/debug/vars", "/debug/requests", "/debug/events",
			"/admin", "/.env", "/.git/config", "/server-status", "/api", "/api/v1/functions", "/config", "/version", "/status", "/health", "/healthz/", "/healthz/../metrics",
			"/openapi.json", "/swagger", "/graphql", "/edgerun.control.v1.FunctionService", "/edgerun.control.v1.FunctionService/", "/edgerun.worker.v1.ExecutionService/",
			"/%2e%2e/%2e%2e/etc/passwd", "/..%2f..%2fetc/passwd",
		} {
			r := get(path)
			if r.err != nil {
				continue
			}
			if r.status == 200 && path != "/healthz/" && path != "/healthz/../metrics" || r.status >= 500 {
				t.Errorf("%s GET %s: status %d %q", name, path, r.status, secTrunc(r.body, 200))
			}
			for _, bad := range secrets {
				if strings.Contains(r.body, bad) {
					t.Errorf("%s GET %s leaks %q", name, path, bad)
				}
			}
			for _, bad := range []string{"goroutine", "heap profile", "HeapAlloc", "go1.", "postgres://", "DATABASE_URL"} {
				if strings.Contains(r.body, bad) {
					t.Errorf("%s GET %s exposes runtime detail %q", name, path, bad)
				}
			}
		}
	}
	// What an unauthenticated call is told reveals nothing about which tokens exist.
	base := "http://" + s.addr
	none := secRPC(base, ctlPkg+"FunctionService/ListFunctions", false, http.Header{}, "")
	wrong := secRPC(base, ctlPkg+"FunctionService/ListFunctions", false, http.Header{"Authorization": {"Bearer wrong"}}, "")
	empty := secRPC(base, ctlPkg+"FunctionService/ListFunctions", false, http.Header{"Authorization": {"Bearer "}}, "")
	if none.message != wrong.message || none.message != empty.message || none.status != wrong.status {
		t.Errorf("a missing, an empty and a wrong credential are answered differently: %q / %q / %q", none.message, empty.message, wrong.message)
	}
	// A listener that is plain HTTP must not be a way to read what the other side would hide:
	// a request to a service with an absolute-form target for another host is not proxied.
	c, err := net.DialTimeout("tcp", s.addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	io.WriteString(c, "GET http://127.0.0.1:1/ HTTP/1.1\r\nHost: 127.0.0.1:1\r\nConnection: close\r\n\r\n")
	b, _ := io.ReadAll(c)
	if !strings.HasPrefix(string(b), "HTTP/1.1 404") && !strings.HasPrefix(string(b), "HTTP/1.1 400") {
		t.Errorf("an absolute-form request line: %q", secTrunc(string(b), 120))
	}
}

func secTrunc(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

var _ = context.Background

func secGRPCStatus(r secResp) string {
	if gs := r.header.Get("Grpc-Status"); gs != "" {
		return gs
	}
	if i := strings.Index(r.body, "grpc-status:"); i >= 0 {
		return strings.TrimSpace(strings.SplitN(r.body[i+len("grpc-status:"):], "\r\n", 2)[0])
	}
	return ""
}

// KNOWN DEFECT SEC-AUTH-DUP-HEADER (hardening): two Authorization headers are
// not refused. The first is used and the second ignored, so a request carrying a
// valid token and an invalid one is let in. Components in front (a proxy that
// appends, or one that keeps the last) can disagree with this server about
// which header counts; the safe answer to an ambiguous credential is to refuse.
func TestSecurityDuplicateAuthorizationIsRefused(t *testing.T) {

	s := newStack(t)
	h := http.Header{"Authorization": {"Bearer " + adminToken, "Bearer something-else"}}
	if got := secRPC("http://"+s.addr, ctlPkg+"FunctionService/ListFunctions", false, h, "").outcome(); got == "passed" {
		t.Errorf("two Authorization headers were accepted")
	}
}
