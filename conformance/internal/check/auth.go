package check

import (
	"strings"
	"testing"

	"connectrpc.com/connect"
)

// Authentication walks one service with every credential, as
// docs/protocol.md says it must be judged: the token of the scope that owns the
// service gets through, a missing or unknown token is UNAUTHENTICATED, and a
// valid token of another scope is PERMISSION_DENIED.
//
// scopes maps each scope the host knows to its token, owner names the scope
// that must be let in, and unknown are extra tokens no scope has (such as a
// token valid on some other component). call makes one call to the service
// presenting token ("" for none) and returns its error; a call that passes
// authentication may still fail on its merits, which is not what is judged here.
func Authentication(t *testing.T, scopes map[string]string, owner string, unknown []string, call func(token string) error) {
	t.Helper()
	ScopeMatrix(t, scopes, []string{owner}, unknown, call)
}

// ScopeMatrix is Authentication for a call that more than one scope may make
// (every scope in allowed gets through; every other scope the host knows is
// PERMISSION_DENIED).
func ScopeMatrix(t *testing.T, scopes map[string]string, allowed []string, unknown []string, call func(token string) error) {
	t.Helper()
	is := map[string]bool{}
	for _, a := range allowed {
		is[a] = true
	}
	var bad []string
	for scope, tok := range scopes {
		err := call(tok)
		code := connect.CodeOf(err)
		switch {
		case is[scope]:
			if code == connect.CodeUnauthenticated || code == connect.CodePermissionDenied {
				t.Errorf("the %s token was refused here: %v", scope, err)
			}
			bad = append(bad, strings.ToUpper(tok))
		case code != connect.CodePermissionDenied:
			t.Errorf("a %s token must be PERMISSION_DENIED here, got %v", scope, err)
		}
	}
	for _, b := range append(append([]string{"", "not-a-token"}, bad...), unknown...) {
		WantCode(t, call(b), connect.CodeUnauthenticated)
	}
}
