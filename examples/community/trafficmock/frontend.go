package main

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// This file is what "being a frontend" means beyond calling Invoke: the rules
// of docs/protocol.md, "Frontend behaviour". None of it is in the library (the
// library is the protocol and nothing else), so it is written out here, from
// the document, the way any other frontend has to.

// hopByHop headers describe one connection, not the message, and are dropped.
var hopByHop = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-authenticate": true, "proxy-authorization": true,
	"proxy-connection": true, "te": true, "trailer": true, "transfer-encoding": true, "upgrade": true,
}

// forwarding headers say who the client is; they are believed only behind a
// trusted proxy.
var forwarding = map[string]bool{
	"x-forwarded-for": true, "x-forwarded-proto": true, "x-forwarded-host": true, "forwarded": true, "x-real-ip": true,
}

// filterHeaders lower-cases names, keeps every value in order, and drops the
// connection-level headers (and, unless trust, the client-supplied forwarding
// ones). Host is not a header here: the protocol carries it separately.
func filterHeaders(h http.Header, trust bool) []*commonv1.Header {
	names := make([]string, 0, len(h))
	for n := range h {
		names = append(names, n)
	}
	sort.Strings(names)
	var out []*commonv1.Header
	for _, n := range names {
		l := strings.ToLower(n)
		if l == "host" || hopByHop[l] || (!trust && forwarding[l]) {
			continue
		}
		for _, v := range h[n] {
			out = append(out, &commonv1.Header{Name: l, Value: []byte(v)})
		}
	}
	return out
}

// appendForwarded adds what the frontend sets itself from the connection.
func appendForwarded(hs []*commonv1.Header, ip, proto, host string) []*commonv1.Header {
	return append(hs,
		&commonv1.Header{Name: "x-forwarded-for", Value: []byte(ip)},
		&commonv1.Header{Name: "x-forwarded-proto", Value: []byte(proto)},
		&commonv1.Header{Name: "x-forwarded-host", Value: []byte(host)})
}

// Mapped is a platform error as a real frontend would show it.
type Mapped struct {
	Reason     string // ErrorReason name without its prefix, or UNTYPED_<connect code>, or PROTOCOL
	Status     int    // HTTP status; 0 means the client cancelled and nobody is answered
	RetryAfter bool
	Typed      bool // the error carried an ErrorInfo
	// LocalDeadline: the caller's own timer fired (no typed error from the orchestrator).
	LocalDeadline bool
}

func reasonName(r commonv1.ErrorReason) string {
	return strings.TrimPrefix(r.String(), "ERROR_REASON_")
}

// mapError applies docs/protocol.md, "Frontend mapping of platform errors" and
// the rules for errors without ErrorInfo. ctxErr is the call context's error.
func mapError(err error, ctxErr error) Mapped {
	r := edgerun.Reason(err)
	switch r {
	case commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND:
		return Mapped{Reason: reasonName(r), Status: 404, Typed: true}
	case commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY:
		return Mapped{Reason: reasonName(r), Status: 503, RetryAfter: true, Typed: true}
	case commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED:
		return Mapped{Reason: reasonName(r), Status: 504, Typed: true}
	case commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH,
		commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE, commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND:
		return Mapped{Reason: reasonName(r), Status: 502, Typed: true}
	case commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE:
		return Mapped{Reason: reasonName(r), Status: 413, Typed: true}
	case commonv1.ErrorReason_ERROR_REASON_INTERNAL:
		return Mapped{Reason: reasonName(r), Status: 500, Typed: true}
	case commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED:
	default: // EGRESS_*, or a reason newer than this build: "INTERNAL, unknown -> 500"
		return Mapped{Reason: reasonName(r), Status: 500, Typed: true}
	}
	code := connect.CodeOf(err)
	switch {
	case errors.Is(err, edgerun.ErrProtocol):
		return Mapped{Reason: "PROTOCOL", Status: 502}
	case errors.Is(ctxErr, context.DeadlineExceeded) || code == connect.CodeDeadlineExceeded:
		return Mapped{Reason: "DEADLINE_EXCEEDED", Status: 504, LocalDeadline: true}
	case errors.Is(ctxErr, context.Canceled) || code == connect.CodeCanceled:
		return Mapped{Reason: "UNTYPED_CANCELED", Status: 0}
	}
	name := "UNTYPED_" + strings.ToUpper(code.String())
	switch code {
	case connect.CodeResourceExhausted: // the transport's size cap
		return Mapped{Reason: name, Status: 413}
	case connect.CodeUnavailable:
		return Mapped{Reason: name, Status: 503, RetryAfter: true}
	case connect.CodeUnauthenticated, connect.CodePermissionDenied:
		return Mapped{Reason: name, Status: 502}
	}
	return Mapped{Reason: name, Status: 500}
}
