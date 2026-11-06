package e2e

import (
	"context"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// KNOWN DEFECT MO-4: a control call whose own budget (Connect-Timeout-Ms) runs
// out while the database does not answer is reported as `internal` ("internal
// error", with the context error logged as a store error), not as
// deadline_exceeded. docs/protocol.md says every hop must enforce the deadline
// and that a caller treats its expiry as DEADLINE_EXCEEDED; control errors are
// "plain Connect codes", and the table lists none for "the budget ran out",
// so an operator cannot tell a timeout from a bug.
func TestMultiOrchControlDeadlineInDatabaseIsDeadlineExceeded(t *testing.T) {

	c, proxy := moOutageCluster(t)
	ctl := edgerun.NewControl(c.A().url(), adminToken, edgerun.WithHTTPClient(&http.Client{Transport: budgetTransport{}}))
	proxy.Blackhole(true)
	// The orchestrator's budget is short; the caller's own timer is long, so the
	// orchestrator's deadline is the one that fires.
	cctx, cancel := context.WithTimeout(context.WithValue(context.Background(), budgetKey{}, 500*time.Millisecond), 20*time.Second)
	defer cancel()
	_, err := ctl.ListFunctions(cctx, &controlv1.ListFunctionsRequest{})
	proxy.Blackhole(false)
	proxy.Cut()
	if connect.CodeOf(err) != connect.CodeDeadlineExceeded {
		t.Errorf("a control call that ran out of budget waiting for the database: %v (code %v), want deadline_exceeded", err, connect.CodeOf(err))
	}
}
