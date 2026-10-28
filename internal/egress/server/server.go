// Package server is the reference network service: an implementation of
// egress.v1 that makes HTTP requests on behalf of guests, for the destinations
// their function's allow list names and no others. The rules it follows are
// those in docs/protocol.md ("Network access"); this is where they are
// enforced.
package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/egress/v1/egressv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	"github.com/Olian04/edgerun/internal/egress/policy"
	"github.com/Olian04/edgerun/internal/errs"
)

// Scope is the token scope workers present to a network service.
const Scope = "egress"

const (
	// ChunkBytes bounds one streamed chunk (docs/protocol.md: at most 1 MiB).
	ChunkBytes = 64 << 10
	// DefaultMaxRequestBody and DefaultMaxResponseBody bound one exchange.
	DefaultMaxRequestBody  = 1 << 20
	DefaultMaxResponseBody = 4 << 20
)

// Config configures a Service.
type Config struct {
	// AllowNetworks are address ranges the service may connect to although
	// they are in the ranges it refuses by default. Empty by default.
	AllowNetworks []netip.Prefix
	// Covers is what this service carries, canonical and non-empty (see
	// policy.CanonicalCovers). It refuses a request to any host outside it,
	// whatever the caller allows.
	Covers []string
	// MaxRequestBody and MaxResponseBody bound one exchange.
	MaxRequestBody, MaxResponseBody uint64
	// DialTimeout bounds connecting to one address. Default 10s.
	DialTimeout time.Duration
	// Lookup resolves a host name to all its addresses; nil uses the system's
	// resolver. It exists for tests: a name that resolves to several addresses
	// cannot otherwise be made to happen.
	Lookup  func(ctx context.Context, host string) ([]netip.Addr, error)
	Version string
	Log     *slog.Logger
}

// Service implements egress.v1.
type Service struct {
	egressv1connect.UnimplementedEgressServiceHandler
	cfg    Config
	covers policy.Covers
	client *http.Client
}

// New returns a service. cfg.Covers must be valid and non-empty: a service
// that does not say what it carries is a programming error, not a default.
func New(cfg Config) *Service {
	covers, err := policy.ParseCovers(cfg.Covers)
	if err != nil || len(covers) == 0 {
		panic("server.New: Config.Covers must be non-empty and valid")
	}
	if cfg.MaxRequestBody == 0 {
		cfg.MaxRequestBody = DefaultMaxRequestBody
	}
	if cfg.MaxResponseBody == 0 {
		cfg.MaxResponseBody = DefaultMaxResponseBody
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.Lookup == nil {
		cfg.Lookup = func(ctx context.Context, host string) ([]netip.Addr, error) {
			return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		}
	}
	if cfg.Version == "" {
		cfg.Version = "dev"
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	g := &guardedDialer{allow: cfg.AllowNetworks, lookup: cfg.Lookup, timeout: cfg.DialTimeout}
	tr := &http.Transport{
		// Never the environment's proxy: that would be a way around the dialer.
		Proxy:                  nil,
		DialContext:            g.DialContext,
		ForceAttemptHTTP2:      true,
		TLSHandshakeTimeout:    10 * time.Second,
		MaxIdleConns:           256,
		MaxIdleConnsPerHost:    8,
		IdleConnTimeout:        30 * time.Second,
		MaxResponseHeaderBytes: 64 << 10,
		// The caller sees the bytes the upstream sent, with the encoding it
		// declared, and chooses what to accept.
		DisableCompression: true,
	}
	return &Service{cfg: cfg, covers: covers, client: &http.Client{
		Transport: tr,
		// Redirects are the caller's to follow: each hop is a destination of
		// its own, and must pass the allow list on its own.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

// Handler returns the path and handler to mount, guarded by the verifier.
func (s *Service) Handler(v authn.Verifier) (string, http.Handler) {
	return egressv1connect.NewEgressServiceHandler(s,
		connect.WithInterceptors(authn.Server(v, Scope)),
		connect.WithReadMaxBytes(int(s.cfg.MaxRequestBody)+(256<<10)))
}

// Implementation identifies this service to peers.
func Implementation(version string) *commonv1.Implementation {
	if version == "" {
		version = "dev"
	}
	return &commonv1.Implementation{Name: "edgerun-egress", Version: version, Protocols: []string{"edgerun.egress.v1"}}
}

// Describe implements EgressService.
func (s *Service) Describe(context.Context, *connect.Request[egressv1.DescribeRequest]) (*connect.Response[egressv1.DescribeResponse], error) {
	return connect.NewResponse(&egressv1.DescribeResponse{
		Implementation:       Implementation(s.cfg.Version),
		MaxRequestBodyBytes:  s.cfg.MaxRequestBody,
		MaxResponseBodyBytes: s.cfg.MaxResponseBody,
		Covers:               s.cfg.Covers,
	}), nil
}

var token = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")

// hopByHop describes one connection, not the message.
var hopByHop = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-authenticate": true, "proxy-authorization": true,
	"proxy-connection": true, "te": true, "trailer": true, "transfer-encoding": true, "upgrade": true,
}

// Fetch implements EgressService.
func (s *Service) Fetch(ctx context.Context, req *connect.Request[egressv1.FetchRequest], st *connect.ServerStream[egressv1.FetchResponse]) error {
	r := req.Msg
	target, rules, err := s.check(r)
	if err != nil {
		return err
	}

	// The allow list comes before anything touches the network, DNS included.
	port := policy.DefaultPort(target.Scheme)
	if p := target.Port(); p != "" {
		port, _ = strconv.Atoi(p)
	}
	if !rules.Allows(target.Scheme, target.Hostname(), port) {
		s.cfg.Log.Info("egress denied", "invocation_id", r.GetInvocationId(), "host", target.Host, "reason", "not on the allow list")
		return errs.New(commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED, "%s is not on the function's allow list", target.Host)
	}

	// What this service carries is the second limit, after the function's own.
	if !s.covers.MatchesHost(target.Hostname()) {
		s.cfg.Log.Info("egress denied", "invocation_id", r.GetInvocationId(), "host", target.Host, "reason", "not carried by this service")
		return errs.New(commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED, "%s is not carried by this network service", target.Host)
	}

	out, err := http.NewRequestWithContext(ctx, r.GetMethod(), target.String(), bytes.NewReader(r.GetBody()))
	if err != nil {
		return errs.Invalid("request: %v", err)
	}
	if len(r.GetBody()) == 0 {
		out.Body, out.ContentLength = http.NoBody, 0
	}
	for _, h := range r.GetHeaders() {
		n := strings.ToLower(h.GetName())
		// Host is the URL's: a caller-supplied one would let an allowed name
		// reach another virtual host.
		if hopByHop[n] || n == "host" || n == "content-length" {
			continue
		}
		out.Header.Add(h.GetName(), h.GetValue())
	}
	// Without this Go would add its own Accept-Encoding and undo the encoding.
	// An absent header means "no preference"; leave it so.

	resp, err := s.client.Do(out)
	if err != nil {
		return s.upstreamErr(ctx, r, err)
	}
	defer resp.Body.Close()

	limit := s.cfg.MaxResponseBody
	if n := r.GetMaxResponseBytes(); n > 0 && n < limit {
		limit = n
	}
	// A body known to be too large is refused before anything is sent, so the
	// caller never receives a response it has to throw away.
	if resp.ContentLength >= 0 && uint64(resp.ContentLength) > limit {
		return errs.New(commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE, "the response is %d bytes; the limit is %d", resp.ContentLength, limit)
	}

	status := uint32(resp.StatusCode)
	if status < 200 || status > 599 {
		return errs.New(commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED, "the upstream answered with status %d", resp.StatusCode)
	}
	head := &commonv1.HttpResponseHead{Status: status}
	for name, values := range resp.Header {
		n := strings.ToLower(name)
		if hopByHop[n] {
			continue
		}
		for _, v := range values {
			head.Headers = append(head.Headers, &commonv1.Header{Name: n, Value: v})
		}
	}
	if err := st.Send(&egressv1.FetchResponse{Part: &egressv1.FetchResponse_Head{Head: head}}); err != nil {
		return err
	}

	buf := make([]byte, ChunkBytes)
	var sent uint64
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if sent+uint64(n) > limit {
				return errs.New(commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE, "the response exceeds the limit of %d bytes", limit)
			}
			sent += uint64(n)
			if err := st.Send(&egressv1.FetchResponse{Part: &egressv1.FetchResponse_Chunk{Chunk: append([]byte(nil), buf[:n]...)}}); err != nil {
				return err
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return s.upstreamErr(ctx, r, rerr)
		}
	}
}

// check validates a request and returns its parsed URL and allow list.
func (s *Service) check(r *egressv1.FetchRequest) (*url.URL, policy.List, error) {
	if !token.MatchString(r.GetMethod()) {
		return nil, nil, errs.Invalid("method %q is not an HTTP method", r.GetMethod())
	}
	if uint64(len(r.GetBody())) > s.cfg.MaxRequestBody {
		return nil, nil, errs.New(commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE,
			"request body of %d bytes exceeds the %d byte limit", len(r.GetBody()), s.cfg.MaxRequestBody)
	}
	rules, err := policy.Parse(r.GetAllow())
	if err != nil {
		return nil, nil, errs.Invalid("%v", err)
	}
	for _, h := range r.GetHeaders() {
		if !token.MatchString(h.GetName()) || strings.ContainsAny(h.GetValue(), "\r\n\x00") {
			return nil, nil, errs.Invalid("header %q is malformed", h.GetName())
		}
	}
	u, err := url.Parse(r.GetUrl())
	switch {
	case err != nil:
		return nil, nil, errs.Invalid("url: %v", err)
	case u.Scheme != "http" && u.Scheme != "https":
		return nil, nil, errs.Invalid("url %q: the scheme must be http or https", r.GetUrl())
	case u.Opaque != "" || u.Hostname() == "":
		return nil, nil, errs.Invalid("url %q: an absolute URL with a host is required", r.GetUrl())
	case u.User != nil:
		return nil, nil, errs.Invalid("url %q: credentials in a URL are not accepted", r.GetUrl())
	case !ascii(u.Host):
		return nil, nil, errs.Invalid("url %q: host names must be ASCII (punycode)", r.GetUrl())
	}
	if p := u.Port(); p != "" {
		if n, err := strconv.Atoi(p); err != nil || n < 1 || n > 65535 {
			return nil, nil, errs.Invalid("url %q: bad port", r.GetUrl())
		}
	}
	u.Fragment = ""
	return u, rules, nil
}

func ascii(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 || s[i] <= 0x20 {
			return false
		}
	}
	return true
}

// upstreamErr turns a failure of the exchange into a protocol error.
func (s *Service) upstreamErr(ctx context.Context, r *egressv1.FetchRequest, err error) error {
	switch {
	case isDenied(err):
		s.cfg.Log.Info("egress denied", "invocation_id", r.GetInvocationId(), "url_host", hostOf(r.GetUrl()), "reason", err.Error())
		return errs.New(commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED, "%v", err)
	case errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded):
		return errs.New(commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, "deadline exceeded")
	case errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, errors.New("cancelled"))
	}
	s.cfg.Log.Info("egress failed", "invocation_id", r.GetInvocationId(), "url_host", hostOf(r.GetUrl()), "error", err)
	return errs.New(commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED, "%v", cleanErr(err))
}

func hostOf(raw string) string {
	if u, err := url.Parse(raw); err == nil {
		return u.Host
	}
	return ""
}

// cleanErr drops the request URL that net/http prefixes to its errors: the
// caller knows it, and a URL's query may hold what must not reach a log.
func cleanErr(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}
