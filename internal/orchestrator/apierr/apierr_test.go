package apierr

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

func TestFromStore(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range []struct {
		err  error
		want connect.Code
	}{
		{fmt.Errorf("%w: function x", store.ErrNotFound), connect.CodeNotFound},
		{fmt.Errorf("%w: route y", store.ErrAlreadyExists), connect.CodeAlreadyExists},
		{fmt.Errorf("%w: bad name", store.ErrInvalid), connect.CodeInvalidArgument},
		{errors.New("pq: connection refused to 10.1.2.3:5432"), connect.CodeInternal},
		{fmt.Errorf("tx: %w", &pgconn.PgError{Code: "40P01", Message: "deadlock detected: relation env_vars"}), connect.CodeAborted},
		{&pgconn.PgError{Code: "40001"}, connect.CodeAborted},
		{&pgconn.PgError{Code: "23505"}, connect.CodeInternal}, // anything else the store did not classify
	} {
		got := FromStore(log, tc.err)
		if connect.CodeOf(got) != tc.want {
			t.Errorf("%v -> %v, want %v", tc.err, connect.CodeOf(got), tc.want)
		}
	}
	if FromStore(log, nil) != nil {
		t.Error("nil must stay nil")
	}
	if got := FromStore(log, errors.New("password authentication failed for user edgerun")); strings.Contains(got.Error(), "password") {
		t.Errorf("database detail leaked to the wire: %v", got)
	}
	// An error that is already a protocol error passes through unchanged.
	pe := connect.NewError(connect.CodeUnavailable, errors.New("x"))
	if FromStore(log, pe) != pe {
		t.Error("protocol errors must pass through")
	}
}
