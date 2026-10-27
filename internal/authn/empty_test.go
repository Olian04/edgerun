package authn

import (
	"net/http"
	"testing"
)

// HTTP trims trailing whitespace from header values, so "Bearer " cannot
// arrive over the wire as such; this guards the function itself, so a
// configuration that (wrongly) contains an empty token can never be matched
// by an empty credential.
func TestEmptyCredentialNeverMatchesEmptyToken(t *testing.T) {
	v := Verifier{"": "execute"}
	for _, h := range []string{"Bearer ", "Bearer", "", "bearer x"} {
		if scope, ok := v.scopeOf(http.Header{"Authorization": {h}}); ok {
			t.Errorf("header %q authenticated as %q", h, scope)
		}
	}
}
