// Package apierr turns store errors into protocol errors for the orchestrator's
// services.
package apierr

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

// FromStore maps a store error to a Connect error. Errors the store does not
// classify are logged and reported as a generic internal error: database
// details do not belong on the wire.
func FromStore(log *slog.Logger, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, store.ErrAlreadyExists):
		return connect.NewError(connect.CodeAlreadyExists, err)
	case errors.Is(err, store.ErrInvalid):
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	var ce *connect.Error
	if errors.As(err, &ce) {
		return err
	}
	// The call's own budget ran out, or its caller went away, while it waited for the database: that is its
	// deadline or its cancellation, not a fault of ours.
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.NewError(connect.CodeDeadlineExceeded, errors.New("the call's deadline expired"))
	}
	if errors.Is(err, context.Canceled) {
		return connect.NewError(connect.CodeCanceled, errors.New("the call was cancelled"))
	}
	// The database gave up on one of two changes that touched the same things
	// at once (a deadlock, a serialization failure): nothing was changed, and
	// asking again is the answer.
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch {
		case pe.Code == "40P01" || pe.Code == "40001":
			return connect.NewError(connect.CodeAborted, errors.New("the change conflicted with another and was not made; try again"))
		case strings.HasPrefix(pe.Code, "22") || strings.HasPrefix(pe.Code, "54"):
			// A value the database cannot hold (a NUL in text, bytes that are not UTF-8, a time out of range, a
			// row too big for an index): the caller's, whatever the field was, and not an internal error.
			return connect.NewError(connect.CodeInvalidArgument, errors.New("a value is malformed or too large"))
		}
	}
	log.Error("store error", "error", err)
	return connect.NewError(connect.CodeInternal, errors.New("internal error"))
}
