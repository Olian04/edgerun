package runtime

import (
	"reflect"
	"strings"
	"testing"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

func hdr(n, v string) *commonv1.Header { return &commonv1.Header{Name: n, Value: []byte(v)} }

func TestEnv(t *testing.T) {
	req := &commonv1.HttpRequest{
		Method: "POST", Host: "a.example", Path: "/x", RawQuery: "k=v", RemoteAddr: "1.2.3.4:5",
		Body: []byte("hello"),
		Headers: []*commonv1.Header{
			hdr("content-type", "text/plain"), hdr("content-length", "999"),
			hdr("x-forwarded-for", "a"), hdr("x-forwarded-for", "b"), hdr("accept", "*/*"),
		},
	}
	got, err := Env(req, []*commonv1.EnvVar{{Name: "MY_VAR", Value: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	want := []KV{
		{"GATEWAY_INTERFACE", "CGI/1.1"}, {"SERVER_PROTOCOL", "HTTP/1.1"},
		{"REQUEST_METHOD", "POST"}, {"SERVER_NAME", "a.example"}, {"PATH_INFO", "/x"},
		{"QUERY_STRING", "k=v"}, {"REMOTE_ADDR", "1.2.3.4:5"},
		{"CONTENT_LENGTH", "5"}, // measured, not the client's claim
		{"CONTENT_TYPE", "text/plain"},
		{"HTTP_ACCEPT", "*/*"}, {"HTTP_X_FORWARDED_FOR", "a, b"},
		{"MY_VAR", "1"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("env mismatch\n got: %v\nwant: %v", got, want)
	}
}

func TestEnvRejectsBadUserVars(t *testing.T) {
	for _, name := range []string{"REQUEST_METHOD", "PATH_INFO", "HTTP_HOST", "CONTENT_TYPE", "", "A=B", "1ABC", "A B"} {
		_, err := Env(&commonv1.HttpRequest{}, []*commonv1.EnvVar{{Name: name, Value: "x"}})
		if err == nil {
			t.Errorf("name %q accepted", name)
		}
	}
	if _, err := Env(&commonv1.HttpRequest{}, []*commonv1.EnvVar{{Name: "OK", Value: "a\x00b"}}); err == nil {
		t.Error("NUL in value accepted")
	}
}

func TestParseResponse(t *testing.T) {
	type h = [2]string
	tests := []struct {
		name    string
		in      string
		status  uint32
		headers []h
		body    string
		wantErr string
	}{
		{"minimal", "Content-Type: text/plain\n\nhi", 200, []h{{"content-type", "text/plain"}}, "hi", ""},
		{"crlf", "Content-Type: a\r\n\r\nhi", 200, []h{{"content-type", "a"}}, "hi", ""},
		{"status with reason", "Status: 201 Created\n\nb", 201, nil, "b", ""},
		{"status bare", "Status: 404\n\n", 404, nil, "", ""},
		{"no headers just blank line", "\nbody", 200, nil, "body", ""},
		{"duplicates keep order", "Set-Cookie: a\nSet-Cookie: b\n\n", 200, []h{{"set-cookie", "a"}, {"set-cookie", "b"}}, "", ""},
		{"value trimmed, colon in value", "Location:   http://x:1/y  \n\n", 200, []h{{"location", "http://x:1/y"}}, "", ""},
		{"body may contain blank lines", "A: b\n\nline1\n\nline2\n", 200, []h{{"a", "b"}}, "line1\n\nline2\n", ""},
		{"empty", "", 0, nil, "", "no output"},
		{"unterminated headers", "Content-Type: text/plain", 0, nil, "", "not terminated"},
		{"headers without newline end", "A: b\nC: d", 0, nil, "", "not terminated"},
		{"no colon", "garbage\n\n", 0, nil, "", "malformed header"},
		{"bad name", "Bad Name: x\n\n", 0, nil, "", "invalid header name"},
		{"empty name", ": x\n\n", 0, nil, "", "invalid header name"},
		{"status 1xx", "Status: 100 Continue\n\n", 0, nil, "", "invalid status"},
		{"status too big", "Status: 600\n\n", 0, nil, "", "invalid status"},
		{"status nan", "Status: abc\n\n", 0, nil, "", "invalid status"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			head, body, err := ParseResponse([]byte(tc.in))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if head.Status != tc.status {
				t.Errorf("status = %d, want %d", head.Status, tc.status)
			}
			var got []h
			for _, x := range head.Headers {
				got = append(got, h{x.Name, string(x.Value)})
			}
			if !reflect.DeepEqual(got, tc.headers) {
				t.Errorf("headers = %v, want %v", got, tc.headers)
			}
			if string(body) != tc.body {
				t.Errorf("body = %q, want %q", body, tc.body)
			}
		})
	}
}
