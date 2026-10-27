// Package apierr turns store errors into protocol errors for the orchestrator's
// services.
package apierr

import (
	"errors"
	"log/slog"

	"connectrpc.com/connect"

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
	log.Error("store error", "error", err)
	return connect.NewError(connect.CodeInternal, errors.New("internal error"))
}
