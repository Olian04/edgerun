package e2e

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Olian04/edgerun/conformance/guest"
)

// stCheckAny passes if any verdict of the sequence is met; otherwise it reports
// what the first (lenient) verdict says is wrong.
func stCheckAny(exps []stExpect, check func(stExpect) (string, string)) (string, string) {
	var id, msg string
	for i, e := range exps {
		eid, emsg := check(e)
		if emsg == "" {
			return "", ""
		}
		if i == 0 {
			id, msg = eid, emsg
		}
	}
	return id, msg
}

func stFreePort(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// withBudget gives the callee a budget (the Connect-Timeout-Ms of the call) that is
// shorter than the caller's own timer, so that the callee's typed error arrives first.
func withBudget(ctx context.Context, d time.Duration) context.Context {
	return context.WithValue(ctx, budgetKey{}, d)
}

func stBuildNet(t testing.TB) []byte { return guest.BuildNet(t) }
