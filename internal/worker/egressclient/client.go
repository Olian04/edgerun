// Package egressclient is the worker's side of the egress seam: it makes a
// guest's outbound requests through a network service (egress.v1) and turns
// the service's answers into what the runtime's host module needs.
package egressclient

import (
	"context"
	"errors"
	"net/http"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/egress/v1/egressv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/worker/runtime"
)

// Client implements runtime.Network over egress.v1.
type Client struct {
	svc egressv1connect.EgressServiceClient
}

// New returns a client of the network service at baseURL, presenting token.
func New(hc *http.Client, baseURL, token string) *Client {
	return &Client{svc: egressv1connect.NewEgressServiceClient(hc, baseURL, connect.WithInterceptors(authn.Client(token)))}
}

var _ runtime.Network = (*Client)(nil)

// Fetch implements runtime.Network. The context's deadline is the budget the
// service is given.
func (c *Client) Fetch(ctx context.Context, req runtime.NetRequest) (*runtime.NetResponse, error) {
	st, err := c.svc.Fetch(ctx, connect.NewRequest(&egressv1.FetchRequest{
		Method: req.Method, Url: req.URL, Headers: req.Headers, Body: req.Body,
		Allow: req.Allow, MaxResponseBytes: req.MaxResponseBytes, InvocationId: req.InvocationID,
	}))
	if err != nil {
		return nil, classify(err)
	}
	defer st.Close()

	var out runtime.NetResponse
	gotHead := false
	for st.Receive() {
		switch p := st.Msg().GetPart().(type) {
		case *egressv1.FetchResponse_Head:
			gotHead = true
			out.Status, out.Headers = p.Head.GetStatus(), p.Head.GetHeaders()
		case *egressv1.FetchResponse_Chunk:
			out.Body = append(out.Body, p.Chunk...)
			// A service that exceeds the bound it was given is not trusted to stop.
			if req.MaxResponseBytes > 0 && uint64(len(out.Body)) > req.MaxResponseBytes {
				return nil, &runtime.NetError{Kind: runtime.NetTooLarge, Msg: "the response exceeds the limit"}
			}
		}
	}
	if err := st.Err(); err != nil {
		return nil, classify(err)
	}
	if !gotHead {
		return nil, &runtime.NetError{Kind: runtime.NetFailed, Msg: "the network service answered without a response head"}
	}
	return &out, nil
}

// classify turns a failure of the call into a runtime.NetError.
func classify(err error) error {
	var kind runtime.NetErrorKind
	switch errs.Reason(err) {
	case commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED:
		kind = runtime.NetDenied
	case commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE:
		kind = runtime.NetTooLarge
	case commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE:
		kind = runtime.NetInvalid
	case commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED:
		// No reason: a malformed request is the caller's, anything else (a
		// network service that is down, that rejected our credentials) is a
		// failure of the exchange.
		if connect.CodeOf(err) == connect.CodeInvalidArgument {
			kind = runtime.NetInvalid
		} else {
			kind = runtime.NetFailed
		}
	default:
		kind = runtime.NetFailed
	}
	var ce *connect.Error
	msg := err.Error()
	if errors.As(err, &ce) {
		msg = ce.Message()
	}
	return &runtime.NetError{Kind: kind, Msg: msg}
}
