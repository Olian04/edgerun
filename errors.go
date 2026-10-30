package edgerun

import (
	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	"github.com/Olian04/edgerun/internal/errs"
)

// Platform failures (invoking, executing, fetching, artifacts) arrive as
// Connect errors carrying a common.v1.ErrorInfo. Branch on the reason, never on
// the code or the message: the reason is the contract (docs/protocol.md lists
// them), the rest is for humans.
//
// The control protocol has no reasons. Its errors are plain Connect codes
// (not_found, already_exists, invalid_argument...), which is what you branch on
// there, with connect.CodeOf.
//
//	res, err := ingress.Invoke(ctx, req)
//	if edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY) {
//		// try again later
//	}

// Reason returns the ErrorReason err carries, or ERROR_REASON_UNSPECIFIED if it
// carries none (a transport failure, a malformed request, a foreign peer).
func Reason(err error) commonv1.ErrorReason { return errs.Reason(err) }

// IsReason reports whether err carries reason.
func IsReason(err error, reason commonv1.ErrorReason) bool { return errs.Reason(err) == reason }

// Info returns the ErrorInfo detail of err, or nil if it carries none.
func Info(err error) *commonv1.ErrorInfo { return errs.Info(err) }

// InvocationID returns the id of the invocation record an Invoke error refers
// to (control.v1 GetInvocation finds it), or "" if err carries none: the call
// was not routed to a function, or err is no platform error.
func InvocationID(err error) string { return errs.Info(err).GetInvocationId() }

// NewError builds the error a peer must return for reason: its canonical
// Connect code and the ErrorInfo detail. It is for implementing a role.
func NewError(reason commonv1.ErrorReason, format string, args ...any) *connect.Error {
	return errs.New(reason, format, args...)
}

// InvalidArgument is the error for a malformed request. It carries no reason:
// the caller has a bug, and nothing should branch on it.
func InvalidArgument(format string, args ...any) *connect.Error {
	return errs.Invalid(format, args...)
}
