// Package wagi holds the parts of the wagi-1 guest ABI that more than one
// component must agree on. The authoritative description is docs/protocol.md.
package wagi

import "strings"

// ABI is the name of this guest ABI.
const ABI = "wagi-1"

// ABI2 is wagi-1 plus outbound HTTP through the host module HTTPModule.
const ABI2 = "wagi-2"

// HTTPModule is the host module wagi-2 guests may import, besides WASI.
const HTTPModule = "edgerun_http"

// reserved are the CGI variables the worker sets itself; user-defined
// variables may not shadow them.
var reserved = map[string]bool{
	"GATEWAY_INTERFACE": true, "SERVER_PROTOCOL": true, "REQUEST_METHOD": true,
	"SERVER_NAME": true, "PATH_INFO": true, "QUERY_STRING": true,
	"REMOTE_ADDR": true, "CONTENT_LENGTH": true, "CONTENT_TYPE": true,
}

// ValidEnvName reports whether s is a legal environment variable name:
// [A-Za-z_][A-Za-z0-9_]*.
func ValidEnvName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || i > 0 && c >= '0' && c <= '9'
		if !ok {
			return false
		}
	}
	return true
}

// ReservedEnvName reports whether a user-defined variable may not use name:
// it is a CGI variable, or it would collide with the HTTP_* header namespace.
func ReservedEnvName(name string) bool {
	return reserved[name] || strings.HasPrefix(name, "HTTP_")
}
