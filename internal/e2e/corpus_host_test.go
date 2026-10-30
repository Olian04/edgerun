package e2e

import "testing"

const (
	specRoute = "docs/protocol.md \"Invocation semantics\" 2: the Host header is compared lower-cased, without port or trailing dot"
	specHost  = "docs/protocol.md \"Frontend behaviour\": host is the Host header as it arrived; SERVER_NAME is the request's Host"
)

func TestCorpusHost(t *testing.T) {
	e := newCorpusEnv(t)
	host := func(h string, extra ...string) string {
		return rawReq("GET /h HTTP/1.1", append([]string{"Host: " + h}, extra...)...)
	}
	rows := []crow{
		{name: "plain", raw: host("corpus.test"), guest: g("WHO", "exact", "SERVER_NAME", "corpus.test", "HTTP_X_FORWARDED_HOST", "corpus.test", "-HTTP_HOST"),
			spec:   specHost,
			silent: "there is no HTTP_HOST: Host travels in HttpRequest.host and the ABI maps it only to SERVER_NAME, so a CGI-style guest that reads HTTP_HOST (as most do) finds nothing"},
		{name: "upper case", raw: host("CORPUS.TEST"), guest: g("WHO", "exact", "SERVER_NAME", "CORPUS.TEST", "HTTP_X_FORWARDED_HOST", "CORPUS.TEST"), spec: specRoute + "; SERVER_NAME keeps the case as it arrived"},
		{name: "mixed case", raw: host("CoRpUs.TeSt"), guest: g("WHO", "exact", "SERVER_NAME", "CoRpUs.TeSt")},
		{name: "trailing dot", raw: host("corpus.test."), guest: g("WHO", "exact", "SERVER_NAME", "corpus.test."), spec: specRoute},
		{name: "with a port", raw: host("corpus.test:8080"), guest: g("WHO", "exact", "SERVER_NAME", "corpus.test:8080"), spec: specRoute},
		{name: "trailing dot and port", raw: host("corpus.test.:8080"), guest: g("WHO", "exact", "SERVER_NAME", "corpus.test.:8080"), spec: specRoute},
		{name: "empty port", raw: host("corpus.test:"), guest: g("WHO", "exact"), silent: "RFC 9110 7.2 allows an empty port; accepted and stripped"},
		{name: "non-numeric port", raw: host("corpus.test:abc"), guest: g("WHO", "exact", "SERVER_NAME", "corpus.test:abc"),
			silent: "RFC 9110 7.2 port = *DIGIT, so this Host is malformed; nothing validates it and the router strips whatever follows the last colon"},
		{name: "two ports", raw: host("corpus.test:80:80"), guest: g("WHO", "any"), silent: "only the last colon is stripped: falls to the catch-all"},
		{name: "text after the port colon", raw: host("corpus.test:evil.test"), guest: g("WHO", "exact"), silent: "router strips everything after the last colon"},
		{name: "userinfo in Host", raw: host("user@corpus.test"), want: []string{"400"}, silent: "RFC 9110 7.2 forbids userinfo in Host; the HTTP server refuses it, the spec does not say"},
		{name: "userinfo that fakes a port", raw: host("evil.test:80@corpus.test"), want: []string{"400"}},
		{name: "a name that is a prefix", raw: host("corpus.testx"), guest: g("WHO", "any")},
		{name: "a name with the host as a subdomain", raw: host("corpus.test.evil.test"), guest: g("WHO", "any")},
		{name: "double trailing dot", raw: host("corpus.test.."), guest: g("WHO", "any"), spec: specRoute + " (one trailing dot)"},
		{name: "only a dot", raw: host("."), guest: g("WHO", "any")},
		{name: "IPv6 literal", raw: host("[::1]"), guest: g("WHO", "any", "SERVER_NAME", "[::1]")},
		{name: "IPv6 literal with a port", raw: host("[::1]:8000"), guest: g("WHO", "any", "SERVER_NAME", "[::1]:8000")},
		{name: "IPv6 literal with an empty port", raw: host("[::1]:"), guest: g("WHO", "any")},
		{name: "unterminated IPv6 literal", raw: host("[::1"), guest: g("WHO", "any"), silent: "accepted"},
		{name: "bracketed name", raw: host("[corpus.test]:80"), guest: g("WHO", "any"), silent: "brackets only make sense around an IP literal; accepted, does not match corpus.test"},
		{name: "unbracketed IPv6", raw: host("::1"), guest: g("WHO", "any"), silent: "RFC 3986 requires brackets; accepted"},
		{name: "IPv4 with a port", raw: host("127.0.0.1:1234"), guest: g("WHO", "any", "SERVER_NAME", "127.0.0.1:1234")},
		{name: "percent-encoded host", raw: host("%63orpus.test"), guest: g("WHO", "any", "SERVER_NAME", "%63orpus.test"), silent: "the Host is never percent-decoded, so %63orpus.test is not corpus.test"},
		{name: "comma list", raw: host("corpus.test, evil.test"), want: []string{"400"}},
		{name: "path-looking Host", raw: host("corpus.test/x"), want: []string{"400"}},
		{name: "query-looking Host", raw: host("corpus.test?x"), want: []string{"400"}},
		{name: "backslash in Host", raw: host(`corpus.test\x`), want: []string{"400"}},
		{name: "space inside Host", raw: host("corpus.test evil.test"), want: []string{"400"}, silent: "RFC 9110 7.2: not a valid host"},
		{name: "hash inside Host", raw: host("corpus.test#x"), want: []string{"400"}},
		{name: "non-ASCII Host", raw: host("cörpus.test"), want: []string{"400"}},
		{name: "NUL in Host", raw: host("corpus.test\x00"), want: []string{"400"}},
		{name: "control character in Host", raw: host("corpus.\x01test"), want: []string{"400"}},
		{name: "empty Host", raw: host(""), guest: g("WHO", "any", "SERVER_NAME", ""),
			silent: "RFC 9112 3.2 allows an empty Host value; it reaches the router as the empty host and the catch-all route answers, SERVER_NAME is empty"},
		{name: "whitespace around Host is trimmed", raw: host("  corpus.test \t"), guest: g("WHO", "exact", "SERVER_NAME", "corpus.test")},
		{name: "Host header name in other case", raw: rawReq("GET /h HTTP/1.1", "hOsT: corpus.test"), guest: g("WHO", "exact")},
		{name: "Host header missing, HTTP/1.1", raw: rawReq("GET /h HTTP/1.1"), want: []string{"400"},
			silent: "RFC 9112 3.2: 400. The spec does not require it, nor say what the guest's SERVER_NAME would be"},
		{name: "Host header missing, HTTP/1.0", raw: rawReq("GET /h HTTP/1.0"), guest: g("WHO", "any", "SERVER_NAME", ""),
			silent: "allowed by HTTP/1.0; routed to the catch-all with an empty host (a deployment without a catch-all answers 404)"},
		{name: "two Host headers that differ", raw: rawReq("GET /h HTTP/1.1", "Host: corpus.test", "Host: evil.test"), want: []string{"400"},
			silent: "RFC 9112 3.2: 400; the spec is silent"},
		{name: "two identical Host headers", raw: rawReq("GET /h HTTP/1.1", "Host: corpus.test", "Host: corpus.test"), want: []string{"400"}},
		{name: "Host after an empty first Host", raw: rawReq("GET /h HTTP/1.1", "Host:", "Host: corpus.test"), want: []string{"400"}},
		{name: "Host folded over two lines", raw: rawReq("GET /h HTTP/1.1", "Host: corpus.test", " evil.test"), want: []string{"400"},
			silent: "obs-fold inside Host would otherwise become 'corpus.test evil.test'"},
		{name: "Host with a comma-joined duplicate in X-Forwarded-Host", raw: host("CORPUS.test:81", "X-Forwarded-Host: evil.test", "Forwarded: host=evil.test"),
			guest: g("HTTP_X_FORWARDED_HOST", "CORPUS.test:81", "-HTTP_FORWARDED", "WHO", "exact"),
			spec:  "Frontend behaviour: the frontend sets x-forwarded-host itself, from the connection; client copies are discarded"},
		{name: "X-Forwarded-Host with underscores is not discarded", raw: host("corpus.test", "X_Forwarded_Host: evil.test"),
			guest:  g("HTTP_X_FORWARDED_HOST", "corpus.test"),
			defect: "CORPUS-UNDERSCORE: a client header x_forwarded_host maps to the same guest variable HTTP_X_FORWARDED_HOST as the frontend's x-forwarded-host, so the guest environment holds two entries of the name and the client's can win (docs/protocol.md \"Frontend behaviour\": client-supplied forwarding headers are discarded)"},
	}
	runRows(t, e, rows)
}
