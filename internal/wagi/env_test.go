package wagi

import "testing"

func TestEnvNames(t *testing.T) {
	for _, ok := range []string{"A", "_x", "MY_VAR_2", "a1"} {
		if !ValidEnvName(ok) {
			t.Errorf("%q should be valid", ok)
		}
	}
	for _, bad := range []string{"", "1A", "A-B", "A B", "A=B", "é"} {
		if ValidEnvName(bad) {
			t.Errorf("%q should be invalid", bad)
		}
	}
	for _, r := range []string{"REQUEST_METHOD", "PATH_INFO", "CONTENT_LENGTH", "HTTP_HOST", "HTTP_"} {
		if !ReservedEnvName(r) {
			t.Errorf("%q should be reserved", r)
		}
	}
	for _, free := range []string{"MY_VAR", "HTTPS_PROXY", "REQUEST", "http_x"} {
		if ReservedEnvName(free) {
			t.Errorf("%q should be free", free)
		}
	}
}
