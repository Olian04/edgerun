// Package runtime executes wagi-1 guests under wazero.
package runtime

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	"github.com/Olian04/edgerun/internal/wagi"
)

// ABI is the guest ABI this package implements. See docs/protocol.md.
const ABI = wagi.ABI

// KV is one environment variable.
type KV struct{ Name, Value string }

// Env builds the guest environment: CGI variables first, then user variables.
// It rejects user variables that would shadow a CGI variable or corrupt the
// environment block; the orchestrator is supposed to have refused those when
// they were set, so hitting one here is a protocol violation.
func Env(req *commonv1.HttpRequest, user []*commonv1.EnvVar) ([]KV, error) {
	env := []KV{
		{"GATEWAY_INTERFACE", "CGI/1.1"},
		{"SERVER_PROTOCOL", "HTTP/1.1"},
		{"REQUEST_METHOD", req.GetMethod()},
		{"SERVER_NAME", req.GetHost()},
		{"PATH_INFO", req.GetPath()},
		{"QUERY_STRING", req.GetRawQuery()},
		{"REMOTE_ADDR", req.GetRemoteAddr()},
		{"CONTENT_LENGTH", strconv.Itoa(len(req.GetBody()))},
	}

	var contentType string
	joined := map[string][]string{}
	for _, h := range req.GetHeaders() {
		name := strings.ToLower(h.GetName())
		switch name {
		case "content-type":
			contentType = h.GetValue()
		case "content-length":
			// Replaced by the measured CONTENT_LENGTH above.
		default:
			joined[name] = append(joined[name], h.GetValue())
		}
	}
	if contentType != "" {
		env = append(env, KV{"CONTENT_TYPE", contentType})
	}
	names := make([]string, 0, len(joined))
	for n := range joined {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		key := "HTTP_" + strings.ToUpper(strings.ReplaceAll(n, "-", "_"))
		env = append(env, KV{key, strings.Join(joined[n], ", ")})
	}

	for _, u := range user {
		if !wagi.ValidEnvName(u.GetName()) {
			return nil, fmt.Errorf("invalid environment variable name %q", u.GetName())
		}
		if wagi.ReservedEnvName(u.GetName()) {
			return nil, fmt.Errorf("environment variable %q collides with a reserved CGI variable", u.GetName())
		}
		if strings.ContainsRune(u.GetValue(), 0) {
			return nil, fmt.Errorf("environment variable %q contains NUL", u.GetName())
		}
		env = append(env, KV{u.GetName(), u.GetValue()})
	}
	return env, nil
}

// ParseResponse parses a guest's stdout: header lines, a blank line, the body.
// `Status: NNN [reason]` sets the status (default 200); every other header is
// passed through with a lower-cased name, duplicates preserved in order.
func ParseResponse(out []byte) (*commonv1.HttpResponseHead, []byte, error) {
	if len(out) == 0 {
		return nil, nil, fmt.Errorf("guest produced no output")
	}
	head := &commonv1.HttpResponseHead{Status: 200}
	rest := out
	for {
		nl := bytes.IndexByte(rest, '\n')
		if nl < 0 {
			return nil, nil, fmt.Errorf("headers are not terminated by a blank line")
		}
		line := bytes.TrimSuffix(rest[:nl], []byte("\r"))
		rest = rest[nl+1:]
		if len(line) == 0 {
			return head, rest, nil
		}
		name, value, ok := bytes.Cut(line, []byte(":"))
		if !ok {
			return nil, nil, fmt.Errorf("malformed header line %q", truncate(line))
		}
		n := strings.ToLower(string(name))
		if !validHeaderName(n) {
			return nil, nil, fmt.Errorf("invalid header name %q", truncate(name))
		}
		v := strings.TrimSpace(string(value))
		if strings.ContainsRune(v, 0) {
			return nil, nil, fmt.Errorf("header %q contains NUL", n)
		}
		if n == "status" {
			code, err := parseStatus(v)
			if err != nil {
				return nil, nil, err
			}
			head.Status = code
			continue
		}
		head.Headers = append(head.Headers, &commonv1.Header{Name: n, Value: v})
	}
}

func parseStatus(v string) (uint32, error) {
	num, _, _ := strings.Cut(v, " ")
	code, err := strconv.Atoi(num)
	if err != nil || code < 200 || code > 599 {
		return 0, fmt.Errorf("invalid status %q: want a code in 200-599", v)
	}
	return uint32(code), nil
}

// validHeaderName reports whether s is an RFC 9110 token.
func validHeaderName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0
		if !ok {
			return false
		}
	}
	return true
}

func truncate(b []byte) string {
	if len(b) > 40 {
		return string(b[:40]) + "..."
	}
	return string(b)
}
