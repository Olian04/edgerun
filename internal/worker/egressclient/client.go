// Package egressclient is the worker's side of the egress seam: it makes a
// guest's outbound requests through a network service (egress.v1) and turns
// the service's answers into what the runtime's host module needs.
package egressclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/internal/errs"
	"github.com/Olian04/edgerun/internal/worker/runtime"
)

// Client implements runtime.Network over egress.v1. It speaks to whichever
// network service a request is routed to, presenting one credential to all.
type Client struct {
	hc    *http.Client
	token string
	svcs  sync.Map // base URL -> *edgerun.Egress
}

// New returns a client that presents token to the network services it calls.
func New(hc *http.Client, token string) *Client { return &Client{hc: hc, token: token} }

func (c *Client) service(baseURL string) *edgerun.Egress {
	if s, ok := c.svcs.Load(baseURL); ok {
		return s.(*edgerun.Egress)
	}
	s := edgerun.NewEgress(baseURL, c.token, edgerun.WithHTTPClient(c.hc))
	actual, _ := c.svcs.LoadOrStore(baseURL, s)
	return actual.(*edgerun.Egress)
}

var _ runtime.Network = (*Client)(nil)

// Fetch implements runtime.Network. The context's deadline is the budget the
// service is given.
func (c *Client) Fetch(ctx context.Context, req runtime.NetRequest) (*runtime.NetResponse, error) {
	res, err := c.service(req.Service).Fetch(ctx, &egressv1.FetchRequest{
		Method: req.Method, Url: req.URL, Headers: req.Headers, Body: req.Body,
		Allow: req.Allow, MaxResponseBytes: req.MaxResponseBytes, InvocationId: req.InvocationID,
	})
	if err != nil {
		return nil, classify(err)
	}
	defer res.Body.Close()

	// A service that exceeds the bound it was given is not trusted to stop.
	var body io.Reader = res.Body
	if req.MaxResponseBytes > 0 {
		body = io.LimitReader(body, int64(req.MaxResponseBytes)+1)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, classify(err)
	}
	if req.MaxResponseBytes > 0 && uint64(len(data)) > req.MaxResponseBytes {
		return nil, &runtime.NetError{Kind: runtime.NetTooLarge, Msg: "the response exceeds the limit"}
	}
	return &runtime.NetResponse{Status: res.Head.GetStatus(), Headers: res.Head.GetHeaders(), Body: data}, nil
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
