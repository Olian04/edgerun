package errs

import (
	"errors"
	"testing"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

func TestRoundTripAndCodes(t *testing.T) {
	for reason := range codes {
		err := New(reason, "boom %d", 7)
		if got := Reason(err); got != reason {
			t.Errorf("Reason(New(%v)) = %v", reason, got)
		}
		if got, want := connect.CodeOf(err), Code(reason); got != want {
			t.Errorf("code for %v = %v, want %v", reason, got, want)
		}
	}
}

func TestEveryReasonHasACode(t *testing.T) {
	for n, name := range commonv1.ErrorReason_name {
		r := commonv1.ErrorReason(n)
		if r == commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
			continue
		}
		if _, ok := codes[r]; !ok {
			t.Errorf("reason %s has no canonical code; add it to codes and docs/protocol.md", name)
		}
	}
}

func TestReasonOfForeignErrors(t *testing.T) {
	if got := Reason(errors.New("plain")); got != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
		t.Errorf("plain error: %v", got)
	}
	if got := Reason(Invalid("bad")); got != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
		t.Errorf("Invalid carries no reason, got %v", got)
	}
	if connect.CodeOf(Invalid("bad")) != connect.CodeInvalidArgument {
		t.Error("Invalid must be InvalidArgument")
	}
}

func TestWithInvocationKeepsTheErrorAndNamesTheRecord(t *testing.T) {
	err := WithInvocation(New(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, "busy %d", 3), "inv-1")
	if Reason(err) != commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY || connect.CodeOf(err) != Code(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY) {
		t.Errorf("the reason and the code must survive: %v", err)
	}
	if info := Info(err); info.GetInvocationId() != "inv-1" || info.GetMessage() != "busy 3" {
		t.Errorf("info: %+v", info)
	}
	if n := len(err.(*connect.Error).Details()); n != 1 {
		t.Errorf("%d details, want exactly one ErrorInfo", n)
	}
	plain := Invalid("bad")
	if WithInvocation(plain, "x") != error(plain) {
		t.Error("an error without ErrorInfo is returned as it is")
	}
}
