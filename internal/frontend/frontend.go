// Package frontend is the HTTP edge: it turns an HTTP request into an
// ingress.v1 Invoke call and streams the response back. It knows bytes, hosts
// and paths; it has no idea what a function, a version or a worker is.
package frontend

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

const (
	// DefaultMaxBodyBytes applies until the orchestrator reports its own limit.
	DefaultMaxBodyBytes = 6 << 20
	// writeTimeout bounds how long one write to the client may take, so a client
	// that stops reading cannot hold a connection (and its invocation) forever.
	writeTimeout = 30 * time.Second
)

// Config configures a Frontend.
type Config struct {
	Ingress *edgerun.Ingress
	// RequestTimeout is the whole-invocation budget handed to the orchestrator.
	// Default 30s.
	RequestTimeout time.Duration
	// MaxBodyBytes is the request body limit until the orchestrator reports its
	// own (see RefreshLimits). Default 6 MiB.
	MaxBodyBytes uint64
	// TrustForwarded keeps client-supplied X-Forwarded-* headers and appends to
	// them. Enable it only behind a proxy you control; by default they are
	// replaced with what this server observed.
	TrustForwarded bool
	Log            *slog.Logger
}

// Frontend is an http.Handler.
type Frontend struct {
	cfg     Config
	maxBody atomic.Uint64
	m       struct{ aborted metric.Int64Counter }
}

// New returns a frontend.
func New(cfg Config) *Frontend {
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = DefaultMaxBodyBytes
	}
	f := &Frontend{cfg: cfg}
	f.m.aborted, _ = otel.Meter("github.com/Olian04/edgerun/internal/frontend").Int64Counter("edgerun.frontend.aborted",
		metric.WithUnit("{response}"),
		metric.WithDescription("Responses whose connection was aborted because the function's response failed after its head was sent: the client saw a status and an incomplete body."))
	f.maxBody.Store(cfg.MaxBodyBytes)
	return f
}

// RefreshLimits asks the orchestrator for its request body limit.
func (f *Frontend) RefreshLimits(ctx context.Context) error {
	resp, err := f.cfg.Ingress.Describe(ctx, &ingressv1.DescribeRequest{})
	if err != nil {
		return err
	}
	if n := resp.GetMaxRequestBodyBytes(); n > 0 {
		f.maxBody.Store(n)
	}
	return nil
}

// RunRefresh refreshes the limits every interval until ctx is done.
func (f *Frontend) RunRefresh(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if err := f.RefreshLimits(ctx); err != nil && ctx.Err() == nil {
			f.cfg.Log.Warn("could not read the orchestrator's limits; keeping the previous value", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// hopByHop headers describe one connection, not the message: they are never
// forwarded in either direction (RFC 9110 §7.6.1).
var hopByHop = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-authenticate": true, "proxy-authorization": true,
	"proxy-connection": true, "te": true, "trailer": true, "transfer-encoding": true, "upgrade": true,
}

// forwarding headers say who the client is. They are only believed when the
// operator says there is a trusted proxy in front.
var forwarding = map[string]bool{
	"x-forwarded-for": true, "x-forwarded-proto": true, "x-forwarded-host": true, "forwarded": true, "x-real-ip": true,
}

// traceHeaders carry a trace between hops. The frontend takes the client's
// trace context into its own span and passes the trace on in the call to the
// orchestrator, not in the request the function is given: the function is given
// the worker's span (docs/protocol.md, "Observability").
var traceHeaders = map[string]bool{"traceparent": true, "tracestate": true, "baggage": true}

// ServeHTTP implements http.Handler.
func (f *Frontend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := requestID(r)
	w.Header().Set("X-Request-Id", reqID)
	trace.SpanFromContext(r.Context()).SetAttributes(attribute.String("edgerun.request.id", reqID))
	rec := &recorder{ResponseWriter: w}
	invocation := ""
	defer func() {
		f.cfg.Log.InfoContext(r.Context(), "request", "method", r.Method, "host", r.Host, "path", r.URL.Path, "status", rec.status,
			"bytes", rec.bytes, "duration", time.Since(start), "request_id", reqID, "invocation_id", invocation)
	}()

	if r.URL.Path == "" || r.URL.Path[0] != '/' { // e.g. "OPTIONS *"
		http.Error(rec, "Bad Request", http.StatusBadRequest)
		return
	}
	limit := f.maxBody.Load()
	if r.ContentLength > 0 && uint64(r.ContentLength) > limit {
		http.Error(rec, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
		return
	}
	// Bounded even when the length is not declared (chunked uploads).
	body, err := io.ReadAll(http.MaxBytesReader(rec, r.Body, int64(limit)))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(rec, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(rec, "Bad Request", http.StatusBadRequest)
		}
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), f.cfg.RequestTimeout)
	defer cancel()
	res, err := f.cfg.Ingress.Invoke(ctx, &ingressv1.InvokeRequest{RequestId: reqID, Request: f.buildRequest(r, body)})
	if err != nil {
		f.fail(rec, ctx, err)
		return
	}
	defer res.Body.Close()

	invocation = res.ID
	writeHead(rec, res, reqID)
	rc := http.NewResponseController(rec)
	buf := make([]byte, 64<<10)
	for {
		n, err := res.Body.Read(buf)
		if n > 0 {
			rc.SetWriteDeadline(time.Now().Add(writeTimeout))
			if _, werr := rec.Write(buf[:n]); werr != nil && !errors.Is(werr, http.ErrBodyNotAllowed) {
				return // the client went away; cancel() ends the invocation
			}
			rc.Flush()
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			// The status line is out: we can no longer say "502". Aborting the
			// connection is the only honest signal that the body is incomplete.
			f.m.aborted.Add(r.Context(), 1)
			f.cfg.Log.ErrorContext(r.Context(), "response failed after it had started", "request_id", reqID, "error", err)
			panic(http.ErrAbortHandler)
		}
	}
}

// buildRequest turns an HTTP request into the protocol's HttpRequest.
func (f *Frontend) buildRequest(r *http.Request, body []byte) *commonv1.HttpRequest {
	out := &commonv1.HttpRequest{
		Method: r.Method, Host: r.Host, Path: r.URL.Path, RawQuery: r.URL.RawQuery, Body: body, RemoteAddr: r.RemoteAddr,
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	proto := "http"
	if r.TLS != nil {
		proto = "https"
	}

	for name, values := range r.Header {
		n := strings.ToLower(name)
		if hopByHop[n] || traceHeaders[n] || (forwarding[n] && !f.cfg.TrustForwarded) {
			continue
		}
		if n == "x-forwarded-for" { // trusted: appended to below
			continue
		}
		for _, v := range values {
			out.Headers = append(out.Headers, &commonv1.Header{Name: n, Value: v})
		}
	}
	// Go moves Host out of the header map; the protocol carries it separately.
	xff := ip
	if f.cfg.TrustForwarded {
		if prior := strings.Join(r.Header.Values("X-Forwarded-For"), ", "); prior != "" {
			xff = prior + ", " + ip
		}
		if r.Header.Get("X-Forwarded-Proto") == "" {
			out.Headers = append(out.Headers, &commonv1.Header{Name: "x-forwarded-proto", Value: proto})
		}
	} else {
		out.Headers = append(out.Headers,
			&commonv1.Header{Name: "x-forwarded-proto", Value: proto},
			&commonv1.Header{Name: "x-forwarded-host", Value: r.Host})
	}
	out.Headers = append(out.Headers, &commonv1.Header{Name: "x-forwarded-for", Value: xff})
	return out
}

// writeHead writes the function's status and headers, minus the ones that
// describe the connection or the framing: this server frames the response.
func writeHead(w http.ResponseWriter, res *edgerun.Invocation, reqID string) {
	h := w.Header()
	for _, hd := range res.Head.GetHeaders() {
		n := strings.ToLower(hd.GetName())
		if hopByHop[n] || n == "content-length" {
			continue
		}
		h.Add(n, hd.GetValue())
	}
	h.Set("X-Request-Id", reqID)
	if id := res.ID; id != "" {
		h.Set("X-Edgerun-Invocation-Id", id)
	}
	status := res.Status()
	if status < 200 || status > 599 {
		status = http.StatusBadGateway
	}
	w.WriteHeader(status)
}

// fail answers with the HTTP status the protocol assigns to a platform error
// (docs/protocol.md). The body is generic: error messages are for operators.
func (f *Frontend) fail(w http.ResponseWriter, ctx context.Context, err error) {
	status, retry := edgerun.FrontendStatus(err, ctx.Err())
	if status == 0 {
		return // the client cancelled; nobody is listening
	}
	if status >= 500 {
		f.cfg.Log.WarnContext(ctx, "invocation failed", "status", status, "error", err) // detail is for operators, never for the client
	}
	if retry {
		w.Header().Set("Retry-After", "1")
	}
	http.Error(w, http.StatusText(status), status)
}

func requestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-Id"); id != "" && len(id) <= 128 && printable(id) {
		return id
	}
	var b [12]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func printable(s string) bool {
	for _, c := range s {
		if c < 0x21 || c > 0x7e {
			return false
		}
	}
	return true
}

// recorder remembers the status and size of a response for the access log, and
// passes everything else (including Flush and deadlines) through.
type recorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *recorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the real writer.
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
