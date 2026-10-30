package edgerun

import (
	"net/http"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"go.opentelemetry.io/otel/propagation"

	"github.com/Olian04/edgerun/internal/authn"
)

// defaultHTTP is what calls go through unless WithHTTPClient says otherwise. It
// is not http.DefaultClient: that keeps two idle connections per host, which for
// a program that makes many calls at once means a new TCP connection for most of
// them (1,100 connections for 3,000 requests at a concurrency of 64, in a
// measurement, against 70-90 with a pool that fits). It has no timeout of its
// own: the deadline of the call's context is the budget.
var defaultHTTP = &http.Client{Transport: func() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 256
	t.MaxIdleConnsPerHost = 64
	return t
}()}

// Option configures a client.
type Option func(*options)

type options struct {
	http connect.HTTPClient
	opts []connect.ClientOption
}

// WithHTTPClient sets the HTTP client calls go through. The default is a client
// with a connection pool sized for many concurrent calls. Timeouts belong on the
// context of each call: it becomes the RPC deadline, which is what the protocols
// mean by a budget.
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
	o := options{http: defaultHTTP}
	for _, f := range opts {
		f(&o)
	}
	// Calls carry the caller's trace and are traced themselves (OpenTelemetry): with
	// no OpenTelemetry set up by the program this passes a trace along untouched
	// and costs a few allocations.
	co := []connect.ClientOption{connect.WithInterceptors(traceInterceptor())}
	co = append(co, o.opts...)
	if token != "" {
		co = append(co, connect.WithInterceptors(authn.Client(token)))
	}
	return o.http, co
}

func traceInterceptor() connect.Interceptor {
	i, err := otelconnect.NewInterceptor(otelconnect.WithPropagator(propagation.TraceContext{}))
	if err != nil {
		panic("edgerun: " + err.Error()) // only for a bad option, which is a bug
	}
	return i
}
