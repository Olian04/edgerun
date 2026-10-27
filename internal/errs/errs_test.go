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
