package edgerun

import (
	"net/http"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/internal/authn"
)

// Option configures a client.
type Option func(*options)

type options struct {
	http connect.HTTPClient
	opts []connect.ClientOption
}

// WithHTTPClient sets the HTTP client calls go through. The default is
// http.DefaultClient. Timeouts belong on the context of each call: it becomes
// the RPC deadline, which is what the protocols mean by a budget.
func WithHTTPClient(c connect.HTTPClient) Option {
	return func(o *options) { o.http = c }
}

// WithClientOptions passes options through to the underlying Connect client
// (another wire protocol, compression, interceptors of your own).
func WithClientOptions(opts ...connect.ClientOption) Option {
	return func(o *options) { o.opts = append(o.opts, opts...) }
}

// settings resolves options for a client that presents token. An empty token
// presents none, which is only useful against a peer that does not ask for one.
func settings(token string, opts []Option) (connect.HTTPClient, []connect.ClientOption) {
	o := options{http: http.DefaultClient}
	for _, f := range opts {
		f(&o)
	}
	co := append([]connect.ClientOption(nil), o.opts...)
	if token != "" {
		co = append(co, connect.WithInterceptors(authn.Client(token)))
	}
	return o.http, co
}
