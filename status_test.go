package edgerun_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// The table in docs/protocol.md ("Frontend mapping of platform errors"), row by row.
func TestFrontendStatusIsTheDocumentedTable(t *testing.T) {
	r := func(reason commonv1.ErrorReason) error { return edgerun.NewError(reason, "x") }
	for name, tc := range map[string]struct {
		err    error
		ctxErr error
		status int
		retry  bool
	}{
		"function not found":   {r(commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND), nil, http.StatusNotFound, false},
		"no capacity":          {r(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY), nil, http.StatusServiceUnavailable, true},
		"deadline exceeded":    {r(commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED), nil, http.StatusGatewayTimeout, false},
		"function trapped":     {r(commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED), nil, http.StatusBadGateway, false},
		"abi mismatch":         {r(commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH), nil, http.StatusBadGateway, false},
		"response too large":   {r(commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE), nil, http.StatusBadGateway, false},
		"artifact not found":   {r(commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND), nil, http.StatusBadGateway, false},
		"request too large":    {r(commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE), nil, http.StatusRequestEntityTooLarge, false},
		"internal":             {r(commonv1.ErrorReason_ERROR_REASON_INTERNAL), nil, http.StatusInternalServerError, false},
		"untyped, our timer":   {connect.NewError(connect.CodeUnknown, errors.New("x")), context.DeadlineExceeded, http.StatusGatewayTimeout, false},
		"untyped, a deadline":  {connect.NewError(connect.CodeDeadlineExceeded, errors.New("x")), nil, http.StatusGatewayTimeout, false},
		"the client went away": {connect.NewError(connect.CodeCanceled, errors.New("x")), context.Canceled, 0, false},
		"the transport's cap":  {connect.NewError(connect.CodeResourceExhausted, errors.New("x")), nil, http.StatusRequestEntityTooLarge, false},
		"orchestrator down":    {connect.NewError(connect.CodeUnavailable, errors.New("x")), nil, http.StatusServiceUnavailable, true},
		"our credentials":      {connect.NewError(connect.CodeUnauthenticated, errors.New("x")), nil, http.StatusBadGateway, false},
		"our scope":            {connect.NewError(connect.CodePermissionDenied, errors.New("x")), nil, http.StatusBadGateway, false},
		"the peer's violation": {fmt.Errorf("%w: no head", edgerun.ErrProtocol), nil, http.StatusBadGateway, false},
		"anything else":        {errors.New("boom"), nil, http.StatusInternalServerError, false},
	} {
		if s, retry := edgerun.FrontendStatus(tc.err, tc.ctxErr); s != tc.status || retry != tc.retry {
			t.Errorf("%s: %d retry=%v, want %d retry=%v", name, s, retry, tc.status, tc.retry)
		}
	}
}
