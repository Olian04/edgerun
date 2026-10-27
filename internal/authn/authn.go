// Package authn implements the bearer-token scheme every edgerun seam uses.
//
// A token carries exactly one scope. Missing or unknown credentials fail with
// UNAUTHENTICATED; a known token of the wrong scope fails with
// PERMISSION_DENIED (see docs/protocol.md).
package authn

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"connectrpc.com/connect"
)

const bearer = "Bearer "

// Verifier maps a token to its scope.
type Verifier map[string]string

// scopeOf returns the scope of the presented token. It visits every known
// token so that timing does not reveal which one matched.
func (v Verifier) scopeOf(h http.Header) (string, bool) {
	auth := h.Get("Authorization")
	if !strings.HasPrefix(auth, bearer) {
		return "", false
	}
	presented := []byte(auth[len(bearer):])
	var scope string
	var found bool
	for token, s := range v {
		if subtle.ConstantTimeCompare([]byte(token), presented) == 1 {
			scope, found = s, true
		}
	}
	return scope, found
}

func (v Verifier) check(h http.Header, allow []string) error {
	scope, ok := v.scopeOf(h)
	if !ok {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("missing or invalid bearer token"))
	}
	for _, a := range allow {
		if a == scope {
			return nil
		}
	}
	return connect.NewError(connect.CodePermissionDenied, errors.New("token scope not allowed here"))
}

// Server returns an interceptor for a handler that admits the given scopes.
func Server(v Verifier, allow ...string) connect.Interceptor {
	return server{v: v, allow: allow}
}

type server struct {
	v     Verifier
	allow []string
}

func (s server) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if err := s.v.check(req.Header(), s.allow); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

func (s server) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (s server) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if err := s.v.check(conn.RequestHeader(), s.allow); err != nil {
			return err
		}
		return next(ctx, conn)
	}
}

// Client returns an interceptor that presents token on every call.
func Client(token string) connect.Interceptor {
	return client{header: bearer + token}
}

type client struct{ header string }

func (c client) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("Authorization", c.header)
		return next(ctx, req)
	}
}

func (c client) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set("Authorization", c.header)
		return conn
	}
}

func (c client) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
