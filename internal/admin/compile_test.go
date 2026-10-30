package admin

import (
	"net/http"
	"testing"
)

// webui checks the whole declaration when it is compiled. A mistake (a missing
// Key, an unknown icon, a bad pattern) is reported here, not to a visitor.
func TestDeclarationCompiles(t *testing.T) {
	_, err := App(Config{Control: NewControl(http.DefaultClient, "http://unused.invalid", "token")}).Compile("/admin")
	if err != nil {
		t.Fatalf("the declaration does not compile:\n%v", err)
	}
}
