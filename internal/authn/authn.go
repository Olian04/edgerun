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
	"io"
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
	if len(h.Values("Authorization")) > 1 {
		return "", false // an ambiguous credential is no credential: another hop may read the other one
	}
	auth := h.Get("Authorization")
	if !strings.HasPrefix(auth, bearer) {
		return "", false
	}
	presented := []byte(auth[len(bearer):])
	if len(presented) == 0 {
		return "", false // never let an empty credential match an empty token
	}
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

// CompressMinBytes is the size below which a server does not compress a
// message. Connect compresses every message by default, which for the small ones
// (a response head, a heartbeat) costs more than it saves: it is the largest
// allocator on the client and slows a megabyte response by a third.
const CompressMinBytes = 1 << 10

// SmallMessageBytes bounds the messages of the calls that carry nothing large
// (a registration, a heartbeat, a list request). Connect decodes a message before
// the interceptor that checks the credential runs, so a server that set no bound
// would allocate whatever an unauthenticated caller sent before saying 401.
const SmallMessageBytes = 1 << 20

// GuardServices refuses, before a single byte of the body is read, a call to a
// protocol service ("/edgerun.<package>.<Service>/<Method>") that does not
// present a token this verifier knows. The scope of the token is still judged
// per service by the interceptor; this is only the cheap, early "no".
func (v Verifier) GuardServices(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/edgerun.") {
			if _, ok := v.scopeOf(r.Header); !ok {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("WWW-Authenticate", "Bearer")
				w.WriteHeader(http.StatusUnauthorized)
				io.WriteString(w, `{"code":"unauthenticated","message":"missing or invalid bearer token"}`)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
