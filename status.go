package edgerun

import (
	"context"
	"errors"
	"net/http"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// FrontendStatus is the HTTP status a frontend answers with when an Invoke
// fails before the response head, and whether to advise the client to retry
// (Retry-After: 1). It is the table in docs/protocol.md, "Frontend mapping of
// platform errors", as code, so that every frontend maps the same way.
// 0 means the client cancelled and no response is needed. ctxErr is the
// request context's error, if any: our own deadline expiring is a 504 whether
// or not the orchestrator's typed error got there first.
func FrontendStatus(err error, ctxErr error) (status int, retryAfter bool) {
	switch Reason(err) {
	case commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND:
		return http.StatusNotFound, false
	case commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY:
		return http.StatusServiceUnavailable, true
	case commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED:
		return http.StatusGatewayTimeout, false
	case commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED,
		commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH,
		commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE,
		commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND:
		return http.StatusBadGateway, false
	case commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE:
		return http.StatusRequestEntityTooLarge, false
	case commonv1.ErrorReason_ERROR_REASON_INTERNAL:
		return http.StatusInternalServerError, false
	}
	// No typed reason: classify by what we can observe.
	switch {
	case errors.Is(err, ErrProtocol):
		return http.StatusBadGateway, false
	case errors.Is(ctxErr, context.DeadlineExceeded) || connect.CodeOf(err) == connect.CodeDeadlineExceeded:
		return http.StatusGatewayTimeout, false
	case errors.Is(ctxErr, context.Canceled) || connect.CodeOf(err) == connect.CodeCanceled:
		return 0, false
	}
	switch connect.CodeOf(err) {
	case connect.CodeResourceExhausted: // the transport's message cap (docs/protocol.md)
		return http.StatusRequestEntityTooLarge, false
	case connect.CodeUnavailable: // the orchestrator is down or unreachable
		return http.StatusServiceUnavailable, true
	case connect.CodeUnauthenticated, connect.CodePermissionDenied: // our own credentials: a deployment error
		return http.StatusBadGateway, false
	}
	return http.StatusInternalServerError, false
}
