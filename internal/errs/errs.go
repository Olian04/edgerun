// Package errs builds and inspects platform errors: a canonical Connect code
// plus a common.v1.ErrorInfo detail carrying the reason peers branch on.
package errs

import (
	"errors"
	"fmt"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// codes is the canonical Connect code for each reason; docs/protocol.md lists
// the same table. Peers must branch on the reason, not on the code.
var codes = map[commonv1.ErrorReason]connect.Code{
	commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND: connect.CodeNotFound,
	commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY:        connect.CodeResourceExhausted,
	commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED:  connect.CodeDeadlineExceeded,
	commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED:   connect.CodeAborted,
	commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH:       connect.CodeFailedPrecondition,
	commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE:  connect.CodeInvalidArgument,
	commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE: connect.CodeResourceExhausted,
	commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND: connect.CodeNotFound,
	commonv1.ErrorReason_ERROR_REASON_INTERNAL:           connect.CodeInternal,
}

// Code returns the canonical Connect code for a reason.
func Code(reason commonv1.ErrorReason) connect.Code {
	if c, ok := codes[reason]; ok {
		return c
	}
	return connect.CodeUnknown
}

// New returns a Connect error for reason, with the ErrorInfo detail attached.
func New(reason commonv1.ErrorReason, format string, args ...any) *connect.Error {
	msg := fmt.Sprintf(format, args...)
	err := connect.NewError(Code(reason), errors.New(msg))
	if d, derr := connect.NewErrorDetail(&commonv1.ErrorInfo{Reason: reason, Message: msg}); derr == nil {
		err.AddDetail(d)
	}
	return err
}

// Invalid returns an InvalidArgument error for a malformed request. It has no
// reason: the caller has a bug, and no platform behaviour should depend on it.
func Invalid(format string, args ...any) *connect.Error {
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(format, args...))
}

// Info extracts the ErrorInfo detail from err, or nil if it carries none.
func Info(err error) *commonv1.ErrorInfo {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return nil
	}
	for _, d := range ce.Details() {
		v, verr := d.Value()
		if verr != nil {
			continue
		}
		if info, ok := v.(*commonv1.ErrorInfo); ok {
			return info
		}
	}
	return nil
}

// Reason extracts the ErrorReason from err, or UNSPECIFIED if it carries none.
func Reason(err error) commonv1.ErrorReason {
	if info := Info(err); info != nil {
		return info.GetReason()
	}
	return commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED
}
