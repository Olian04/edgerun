// Package check holds what every conformance suite needs and none may take
// from an implementation: reading a protocol error, the canonical code of each
// reason, a bearer-token client option, and polling. Suites depend on the
// generated protocol code and this package, and on nothing else of edgerun.
package check

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// CanonicalCode is the Connect code each reason must travel with
// (docs/protocol.md). It is kept here, independent of any implementation's
// copy, so an implementation that gets the table wrong cannot also get the
// test wrong.
var CanonicalCode = map[commonv1.ErrorReason]connect.Code{
	commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND: connect.CodeNotFound,
	commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY:        connect.CodeResourceExhausted,
	commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED:  connect.CodeDeadlineExceeded,
	commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED:   connect.CodeAborted,
	commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH:       connect.CodeFailedPrecondition,
	commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE:  connect.CodeInvalidArgument,
	commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE: connect.CodeResourceExhausted,
	commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND: connect.CodeNotFound,
	commonv1.ErrorReason_ERROR_REASON_INTERNAL:           connect.CodeInternal,
	commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED:      connect.CodePermissionDenied,
	commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED:      connect.CodeUnavailable,
}

// ReasonOf extracts the ErrorInfo reason and message from a Connect error. A
// plain error, or one without the detail, has reason UNSPECIFIED.
func ReasonOf(err error) (commonv1.ErrorReason, string) {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED, ""
	}
	for _, d := range ce.Details() {
		if v, verr := d.Value(); verr == nil {
			if info, ok := v.(*commonv1.ErrorInfo); ok {
				return info.GetReason(), info.GetMessage()
			}
		}
	}
	return commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED, ""
}

// WantReason fails the test unless err carries want, travelling with the
// canonical code for it. It returns the error's message.
func WantReason(t testing.TB, err error, want commonv1.ErrorReason) string {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %v, got success", want)
	}
	got, msg := ReasonOf(err)
	if got != want {
		t.Fatalf("reason = %v, want %v (error: %v)", got, want, err)
	}
	if code, ok := CanonicalCode[want]; ok && connect.CodeOf(err) != code {
		t.Errorf("code = %v, want the canonical %v for %v", connect.CodeOf(err), code, want)
	}
	return msg
}

// WantCode fails the test unless err is a Connect error with code want. Control
// calls use plain codes without a reason, and so, if err does carry a reason,
// that is reported too: nothing may branch on a reason there.
func WantCode(t testing.TB, err error, want connect.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %v, got success", want)
	}
	if got := connect.CodeOf(err); got != want {
		t.Fatalf("code = %v, want %v (error: %v)", got, want, err)
	}
}

// Bearer returns a client option that presents token on every call, or none
// at all when token is empty.
func Bearer(token string) connect.ClientOption {
	return connect.WithInterceptors(bearer{token})
}

type bearer struct{ token string }

func (b bearer) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if b.token != "" {
			req.Header().Set("Authorization", "Bearer "+b.token)
		}
		return next(ctx, req)
	}
}

func (b bearer) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		if b.token != "" {
			conn.RequestHeader().Set("Authorization", "Bearer "+b.token)
		}
		return conn
	}
}

func (bearer) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// Eventually polls cond until it holds or the limit passes, then fails the
// test saying what it was waiting for.
func Eventually(t testing.TB, what string, limit time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", limit, what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// Error builds the error a peer answers with for a platform failure: the
// canonical code for reason, and the ErrorInfo detail carrying it.
func Error(reason commonv1.ErrorReason, message string) error {
	code, ok := CanonicalCode[reason]
	if !ok {
		code = connect.CodeUnknown
	}
	err := connect.NewError(code, errors.New(message))
	if d, derr := connect.NewErrorDetail(&commonv1.ErrorInfo{Reason: reason, Message: message}); derr == nil {
		err.AddDetail(d)
	}
	return err
}
