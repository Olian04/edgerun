package httpmsg

import (
	"testing"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

func TestValidText(t *testing.T) {
	for s, want := range map[string]bool{
		"/": true, "/a b": true, "/café": true, "/日本": true, "": true,
		"/\xff": false, "/a\x00b": false, "/a\nb": false, "/a\tb": false, "/a\x7f": false, "/\xc3": false,
	} {
		if got := ValidText(s); got != want {
			t.Errorf("ValidText(%q) = %v, want %v", s, got, want)
		}
	}
}

func TestValidHeaderValue(t *testing.T) {
	for s, want := range map[string]bool{
		"": true, "plain": true, "a\tb": true, "caf\xe9": true, "\xff\xfe": true,
		"a\x00": false, "a\r\nb": false, "a\nb": false, "\x7f": false, "a\x01": false,
	} {
		if got := ValidHeaderValue([]byte(s)); got != want {
			t.Errorf("ValidHeaderValue(%q) = %v, want %v", s, got, want)
		}
	}
}

func TestValidateRequest(t *testing.T) {
	ok := &commonv1.HttpRequest{Method: "GET", Path: "/a", RawQuery: "x=%ff", Headers: []*commonv1.Header{{Name: "x-a", Value: []byte("caf\xe9")}}}
	if err := ValidateRequest(ok); err != nil {
		t.Errorf("a request whose header value is not text is fine: %v", err)
	}
	for name, r := range map[string]*commonv1.HttpRequest{
		"no method":               {Path: "/"},
		"a bad method":            {Method: "GE T", Path: "/"},
		"a NUL in the host":       {Host: "a\x00b", Path: "/"},
		"a newline in remote":     {RemoteAddr: "1.2.3.4\n", Path: "/"},
		"no path":                 {Path: ""},
		"relative path":           {Path: "a"},
		"a path with a NUL":       {Path: "/a\x00"},
		"a path that is not text": {Path: "/\xff"},
		"a query with a newline":  {Path: "/", RawQuery: "a\nb"},
		"a bad header name":       {Path: "/", Headers: []*commonv1.Header{{Name: "a b", Value: []byte("x")}}},
		"an empty header name":    {Path: "/", Headers: []*commonv1.Header{{Name: "", Value: []byte("x")}}},
		"a header value with CR":  {Path: "/", Headers: []*commonv1.Header{{Name: "a", Value: []byte("x\ry")}}},
	} {
		if r.Method == "" && name != "no method" {
			r.Method = "GET"
		}
		if err := ValidateRequest(r); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
