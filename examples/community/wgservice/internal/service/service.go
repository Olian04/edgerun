// Package service is the EgressService of edgerun.egress.v1, implemented from
// docs/protocol.md ("What a network service does with a Fetch") on top of a
// Dialer that reaches only what is inside a WireGuard tunnel.
package service

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http/httpguts"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/egress/v1/egressv1connect"

	"example.com/edgerun-wgservice/internal/allow"
	"example.com/edgerun-wgservice/internal/guard"
)

// Dialer is the network the service reaches: names are resolved by it and
// connections are made by it, in two separate steps so that every address a
// name resolves to can be checked before any is connected to.
type Dialer interface {
	LookupHost(ctx context.Context, host string) ([]netip.Addr, error)
	DialIP(ctx context.Context, ap netip.AddrPort) (net.Conn, error)
}

// Config configures a Server.
type Config struct {
	Dialer Dialer
	// Coverage is what the service carries; CoverageText its canonical form.
	Coverage     allow.Coverage
	CoverageText []string
	// AllowNetworks are ranges the operator opened although their class is forbidden.
	AllowNetworks []netip.Prefix
	// MaxRequestBody and MaxResponseBody bound a request's body and a response's.
	MaxRequestBody, MaxResponseBody uint64
	// MaxDuration bounds an exchange whose caller gave no deadline.
	MaxDuration time.Duration
	// RootCAs, if set, replaces the system roots for https (private CAs).
	RootCAs *x509.CertPool
	Version string
	Log     *slog.Logger
}

// Server implements egressv1connect.EgressServiceHandler.
type Server struct {
	cfg       Config
	transport *http.Transport
	client    *http.Client
	inflight  atomic.Int64
	bufs      sync.Pool
}

var _ egressv1connect.EgressServiceHandler = (*Server)(nil)

const chunkSize = 256 << 10 // at most 1 MiB by the protocol

// New returns a Server.
func New(cfg Config) *Server {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.MaxDuration == 0 {
		cfg.MaxDuration = 60 * time.Second
	}
	s := &Server{cfg: cfg}
	s.bufs.New = func() any { b := make([]byte, chunkSize); return &b }
	s.transport = &http.Transport{
		Proxy:                  nil, // never the environment's: the tunnel is the way out
		DialContext:            s.dialContext,
		DisableCompression:     true, // neither adds nor undoes content encodings
		ForceAttemptHTTP2:      false,
		MaxIdleConns:           64,
		MaxIdleConnsPerHost:    4,
		IdleConnTimeout:        30 * time.Second,
		TLSHandshakeTimeout:    15 * time.Second,
		MaxResponseHeaderBytes: 256 << 10,
		TLSClientConfig:        &tls.Config{RootCAs: cfg.RootCAs, MinVersion: tls.VersionTLS12},
	}
	s.client = &http.Client{
		Transport: s.transport,
		// Redirects are not followed: the caller sees the 3xx and decides.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return s
}

// InFlight is the number of Fetch calls running.
func (s *Server) InFlight() int { return int(s.inflight.Load()) }

// Close drops idle upstream connections.
func (s *Server) Close() { s.transport.CloseIdleConnections() }

// Handler returns the path and handler to mount, with the egress token required.
func (s *Server) Handler(verifier edgerun.Verifier, opts ...connect.HandlerOption) (string, http.Handler) {
	opts = append(opts,
		connect.WithInterceptors(edgerun.Authenticate(verifier, edgerun.ScopeEgress)),
		// A request body over max_request_body_bytes but under this cap is
		// REQUEST_TOO_LARGE; far over, the transport refuses it untyped.
		connect.WithReadMaxBytes(int(s.cfg.MaxRequestBody)*2+(1<<20)),
	)
	return egressv1connect.NewEgressServiceHandler(s, opts...)
}

func (s *Server) Describe(context.Context, *connect.Request[egressv1.DescribeRequest]) (*connect.Response[egressv1.DescribeResponse], error) {
	return connect.NewResponse(&egressv1.DescribeResponse{
		Implementation:       Implementation(s.cfg.Version),
		MaxRequestBodyBytes:  s.cfg.MaxRequestBody,
		MaxResponseBodyBytes: s.cfg.MaxResponseBody,
		Covers:               s.cfg.CoverageText,
	}), nil
}

// Implementation is what the service calls itself.
func Implementation(version string) *commonv1.Implementation {
	return &commonv1.Implementation{Name: "wgservice", Version: version, Protocols: []string{"edgerun.egress.v1"}}
}

// connection-level headers that are dropped in both directions.
var connectionLevel = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-authenticate": true, "proxy-authorization": true,
	"proxy-connection": true, "te": true, "trailer": true, "transfer-encoding": true, "upgrade": true,
}

// deniedError is a refusal made while connecting (an address the service never
// reaches).
type deniedError struct{ msg string }

func (e *deniedError) Error() string { return e.msg }

// dialContext is the transport's only way to the network. It receives the host
// the request names, resolves it through the tunnel, refuses the connection if
// ANY address it resolves to is forbidden, and connects to an address it has
// checked, by address: the name is never looked up a second time between the
// check and the connection (wireguard-go's netstack DialContext would do
// exactly that if it were handed the name).
func (s *Server) dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	var ips []netip.Addr
	if a, err := netip.ParseAddr(host); err == nil {
		ips = []netip.Addr{a.Unmap()}
	} else {
		if guard.IsLocalName(host) {
			return nil, &deniedError{"localhost names are loopback"}
		}
		ips, err = s.cfg.Dialer.LookupHost(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("resolving %s: %w", host, err)
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("resolving %s: no addresses", host)
		}
	}
	for _, ip := range ips {
		if err := guard.Check(ip, s.cfg.AllowNetworks); err != nil {
			return nil, &deniedError{fmt.Sprintf("%s resolves to a destination this service never reaches: %v", host, err)}
		}
	}
	var first error
	for _, ip := range ips {
		c, err := s.cfg.Dialer.DialIP(ctx, netip.AddrPortFrom(ip, uint16(port)))
		if err == nil {
			return c, nil
		}
		if first == nil {
			first = err
		}
		if ctx.Err() != nil {
			break
		}
	}
	return nil, first
}

func invalid(format string, args ...any) error { return edgerun.InvalidArgument(format, args...) }

// request is a validated FetchRequest.
type request struct {
	method string
	url    *url.URL
	dest   allow.Dest
	allow  []allow.Entry
	header http.Header
	body   []byte
	limit  uint64
}

// validate is step 1: a malformed request is invalid_argument with no reason; a
// body over the limit is REQUEST_TOO_LARGE.
func (s *Server) validate(r *egressv1.FetchRequest) (*request, error) {
	out := &request{method: r.GetMethod(), body: r.GetBody()}
	if !httpguts.ValidHeaderFieldName(out.method) {
		return nil, invalid("the method is not an HTTP token")
	}
	if strings.EqualFold(out.method, "CONNECT") {
		return nil, invalid("CONNECT is not supported")
	}
	u, dest, err := allow.ParseURL(r.GetUrl())
	if err != nil {
		return nil, invalid("url: %v", err)
	}
	out.url, out.dest = u, dest
	if out.allow, err = allow.ParseList(r.GetAllow()); err != nil {
		return nil, invalid("allow: %v", err)
	}
	out.header = http.Header{}
	for _, h := range r.GetHeaders() {
		if !httpguts.ValidHeaderFieldName(h.GetName()) {
			return nil, invalid("invalid header name %q", h.GetName())
		}
		if !httpguts.ValidHeaderFieldValue(string(h.GetValue())) {
			return nil, invalid("invalid value for header %q", h.GetName())
		}
		name := strings.ToLower(h.GetName())
		if connectionLevel[name] || name == "host" || name == "content-length" {
			continue
		}
		out.header.Add(name, string(h.GetValue()))
	}
	if uint64(len(out.body)) > s.cfg.MaxRequestBody {
		return nil, edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE,
			"request body of %d bytes exceeds the limit of %d", len(out.body), s.cfg.MaxRequestBody)
	}
	out.limit = s.cfg.MaxResponseBody
	if m := r.GetMaxResponseBytes(); m != 0 && m < out.limit {
		out.limit = m
	}
	return out, nil
}

// Fetch implements steps 1-5.
func (s *Server) Fetch(ctx context.Context, creq *connect.Request[egressv1.FetchRequest], stream *connect.ServerStream[egressv1.FetchResponse]) error {
	s.inflight.Add(1)
	defer s.inflight.Add(-1)

	// 1. malformed, or too large.
	req, err := s.validate(creq.Msg)
	if err != nil {
		return err
	}
	// 2. the allow list and the coverage, before any name is resolved.
	if !allow.Allowed(req.allow, req.dest) {
		return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED, "%s is not on the allow list for this request", req.dest.Host)
	}
	if !s.cfg.Coverage.Carries(req.dest) {
		return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED, "this service does not carry %s", req.dest.Host)
	}

	// The RPC deadline is the budget; an exchange that came without one gets
	// the service's own.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.MaxDuration)
		defer cancel()
	}

	// 3 and 4. the connection (checked inside dialContext) and the request.
	out := &http.Request{
		Method: req.method,
		URL: &url.URL{
			Scheme: req.dest.Scheme, Host: hostPort(req.dest, req.url),
			Path: req.url.Path, RawPath: req.url.RawPath, RawQuery: req.url.RawQuery,
		},
		Host:   hostPort(req.dest, req.url), // from the URL; a caller's Host was dropped
		Header: req.header,
	}
	if out.URL.Path == "" {
		out.URL.Path = "/"
	}
	if len(req.body) > 0 {
		out.Body = io.NopCloser(bytes.NewReader(req.body))
		out.ContentLength = int64(len(req.body))
		out.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(req.body)), nil }
	}
	if out.Header.Get("User-Agent") == "" {
		out.Header.Set("User-Agent", "") // net/http would add its own; the guest's request is the guest's
	}
	out = out.WithContext(ctx)

	resp, err := s.client.Do(out)
	if err != nil {
		return s.fail(ctx, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 599 {
		return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED, "upstream answered with status %d", resp.StatusCode)
	}
	hasBody := req.method != http.MethodHead && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotModified
	if hasBody && resp.ContentLength >= 0 && uint64(resp.ContentLength) > req.limit {
		return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE,
			"the upstream announces a body of %d bytes; the limit is %d", resp.ContentLength, req.limit)
	}

	head := &commonv1.HttpResponseHead{Status: uint32(resp.StatusCode)}
	for name, values := range resp.Header {
		ln := strings.ToLower(name)
		if connectionLevel[ln] {
			continue
		}
		for _, v := range values {
			head.Headers = append(head.Headers, &commonv1.Header{Name: ln, Value: []byte(v)})
		}
	}
	if err := stream.Send(&egressv1.FetchResponse{Part: &egressv1.FetchResponse_Head{Head: head}}); err != nil {
		return err
	}

	// 5. the body, in chunks, never more than the bound.
	bp := s.bufs.Get().(*[]byte)
	defer s.bufs.Put(bp)
	buf := *bp
	var sent uint64
	for {
		// Read at most one byte past the bound: that is how "exceeds" is seen
		// without ever delivering more than the bound.
		want := uint64(len(buf))
		if rem := req.limit - sent + 1; rem < want {
			want = rem
		}
		n, rerr := resp.Body.Read(buf[:want])
		if n > 0 {
			deliver := uint64(n)
			if sent+deliver > req.limit {
				deliver = req.limit - sent
			}
			if deliver > 0 {
				if err := stream.Send(&egressv1.FetchResponse{Part: &egressv1.FetchResponse_Chunk{Chunk: buf[:deliver]}}); err != nil {
					return err
				}
				sent += deliver
			}
			if uint64(n) > deliver {
				return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE, "the response body exceeds %d bytes", req.limit)
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return s.fail(ctx, rerr)
		}
	}
}

// hostPort is the authority of the outgoing request: the canonical host, and
// the port only if the URL named one.
func hostPort(d allow.Dest, u *url.URL) string {
	h := d.Host
	if d.IsAddr() && d.Addr.Is6() {
		h = "[" + h + "]"
	}
	if u.Port() != "" {
		return h + ":" + strconv.Itoa(d.Port)
	}
	return h
}

// fail maps whatever went wrong upstream onto the reasons of the protocol:
// EGRESS_DENIED for a refusal made while connecting, DEADLINE_EXCEEDED when the
// budget ran out (whatever the transport called it), and EGRESS_FAILED for the
// rest. A caller that has gone away is not an error worth a reason.
func (s *Server) fail(ctx context.Context, err error) error {
	var denied *deniedError
	switch {
	case errors.As(err, &denied):
		return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED, "%s", denied.msg)
	case errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded):
		return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, "the deadline passed")
	case errors.Is(ctx.Err(), context.Canceled):
		return connect.NewError(connect.CodeCanceled, ctx.Err())
	}
	s.cfg.Log.Debug("upstream failure", "err", err)
	return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED, "%s", summarize(err))
}

// summarize keeps an upstream failure's text for the operator without the URL
// (which may carry a secret in its query).
func summarize(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err
	}
	return err.Error()
}
