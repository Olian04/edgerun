// Package httpmsg judges the HTTP messages the protocols carry (common.v1
// HttpRequest and Header) by the rules in docs/protocol.md, "HTTP messages":
// what a hop must be able to rely on, so that no hop has to guess at a
// byte it was handed.
package httpmsg

import (
	"fmt"
	"unicode/utf8"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// ValidText reports whether s may be a path, a query or any other text the
// protocol carries: valid UTF-8 and no control character (the C0 controls and
// DEL). A path is the request's path *decoded*, so this is what a decoded path
// may hold: a %00 or a %0a in the target does not make a path with a NUL or a
// newline in it, and a %ff does not make one that is not text.
func ValidText(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c == 0x7f {
			return false
		}
	}
	return true
}

// ValidHeaderValue reports whether b may be a header value: any bytes except the
// control characters other than horizontal tab, which includes NUL, CR and LF.
// Bytes of 0x80 and above are fine: a value is opaque, not text.
func ValidHeaderValue(b []byte) bool {
	for _, c := range b {
		if (c < 0x20 && c != '\t') || c == 0x7f {
			return false
		}
	}
	return true
}

// ValidHeaderName reports whether s is an RFC 9110 token.
func ValidHeaderName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c != 0 && containsByte("!#$%&'*+-.^_`|~", c):
		default:
			return false
		}
	}
	return true
}

func containsByte(s string, c byte) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return true
		}
	}
	return false
}

// ValidateHeaders checks every header.
func ValidateHeaders(hs []*commonv1.Header) error {
	for _, h := range hs {
		if !ValidHeaderName(h.GetName()) {
			return fmt.Errorf("header name %q is not a token", h.GetName())
		}
		if !ValidHeaderValue(h.GetValue()) {
			return fmt.Errorf("the value of header %q contains a control character", h.GetName())
		}
	}
	return nil
}

// ValidateRequest checks a request as an orchestrator or a worker is handed it:
// the path starts with '/' and is text, the query is text, and the headers are
// well formed. A request that fails is a bug of whoever sent it.
func ValidateRequest(r *commonv1.HttpRequest) error {
	if p := r.GetPath(); p == "" || p[0] != '/' {
		return fmt.Errorf("path %q must start with '/'", p)
	}
	if !ValidText(r.GetPath()) {
		return fmt.Errorf("the path is not text: it must be valid UTF-8 without control characters (it is the path decoded)")
	}
	if !ValidText(r.GetRawQuery()) {
		return fmt.Errorf("the query is not text: it must be valid UTF-8 without control characters")
	}
	return ValidateHeaders(r.GetHeaders())
}
