package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

// defaultMaxBody is what docs/protocol.md says to assume until the
// orchestrator has answered Describe.
const defaultMaxBody = 6 << 20

// Limits holds the orchestrator's request body limit, read from Describe at
// start and refreshed from time to time, as a frontend must.
type Limits struct {
	ing    *edgerun.Ingress
	max    atomic.Uint64
	known  atomic.Bool
	Impl   atomic.Pointer[string]
	Protos atomic.Pointer[[]string]
}

func NewLimits(ing *edgerun.Ingress) *Limits {
	l := &Limits{ing: ing}
	l.max.Store(defaultMaxBody)
	return l
}

func (l *Limits) Max() uint64 { return l.max.Load() }
func (l *Limits) Known() bool { return l.known.Load() }

// Refresh asks the orchestrator for its limits.
func (l *Limits) Refresh(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	d, err := l.ing.Describe(ctx, &ingressv1.DescribeRequest{})
	if err != nil {
		return err
	}
	if n := d.GetMaxRequestBodyBytes(); n > 0 {
		l.max.Store(n)
	}
	name := d.GetImplementation().GetName() + " " + d.GetImplementation().GetVersion()
	l.Impl.Store(&name)
	p := d.GetImplementation().GetProtocols()
	l.Protos.Store(&p)
	l.known.Store(true)
	return nil
}

// Run refreshes every interval until ctx is done.
func (l *Limits) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.Refresh(ctx) // a failed refresh keeps the previous value
		}
	}
}

// Invoker does what a frontend does for one request, and measures it.
type Invoker struct {
	Ing    *edgerun.Ingress
	Limits *Limits
	Stats  *Stats
	Log    *ReqLog
	Keep   func(*Result) // called with every finished request, if set
	Start  time.Time     // offsets are measured from here

	bufs sync.Pool
}

func NewInvoker(ing *edgerun.Ingress, lim *Limits, st *Stats, log *ReqLog, start time.Time) *Invoker {
	return &Invoker{Ing: ing, Limits: lim, Stats: st, Log: log, Start: start,
		bufs: sync.Pool{New: func() any { b := make([]byte, 64<<10); return &b }}}
}

func (iv *Invoker) finish(r *Result) *Result {
	iv.Stats.Record(r)
	if iv.Log != nil {
		iv.Log.Write(r)
	}
	if iv.Keep != nil {
		iv.Keep(r)
	}
	return r
}

// Sink receives the response when the caller wants more than its measurement
// (listen mode writes it to a real client). Head is called once, with a valid
// head, before any body; a write or head error means the client went away.
type Sink interface {
	Head(inv *edgerun.Invocation) error
	Write(p []byte) (int, error)
}

// Do sends one planned request the way a frontend would and records the
// outcome. parent is the run's context: when it is cancelled the request is
// abandoned and counted as canceled.
func (iv *Invoker) Do(parent context.Context, p *Planned) *Result {
	r := &Result{Seq: p.Seq, RequestID: p.RequestID, Template: p.Template}
	r.Offset = time.Since(iv.Start)

	// A frontend answers 413 itself, without calling the orchestrator, to a body over the limit.
	if !p.AllowOversize && uint64(len(p.Req.Request.Body)) > iv.Limits.Max() {
		r.Outcome, r.Status, r.Reason = OutRejected, 413, "LOCAL_REQUEST_TOO_LARGE"
		return iv.finish(r)
	}

	ctx, cancel := context.WithTimeout(parent, p.Budget)
	defer cancel()
	iv.Stats.InFlight(1)
	defer iv.Stats.InFlight(-1)

	t0 := time.Now()
	if !p.Intended.IsZero() {
		r.Lag = max(0, t0.Sub(p.Intended))
	}
	inv, err := iv.Ing.Invoke(ctx, p.Req)
	// Invoke returns when the head has arrived, so this is the time to the head.
	r.TTFB = time.Since(t0)
	if err != nil {
		r.Total = r.TTFB
		m := mapError(err, ctx.Err())
		if parent.Err() != nil || (m.Status == 0 && ctx.Err() != nil) {
			r.Outcome = OutCanceled
			return iv.finish(r)
		}
		if m.Status == 0 {
			// The orchestrator said "cancelled" but nobody here cancelled anything (a
			// SIGKILLed worker has been seen to produce this: connect-go reports a
			// "connection reset by peer" as CodeCanceled). docs/protocol.md would have a
			// frontend send no answer to a client that is still waiting, which Go's
			// net/http turns into "200 OK" with an empty body. This tool counts it as
			// the platform error it is, answered 500.
			m = Mapped{Reason: ReasonCanceledByPeer, Status: 500}
		}
		r.Outcome, r.Status, r.Reason, r.Typed, r.LocalTimer, r.Error, r.RetryAfter = OutPlatformError, m.Status, m.Reason, m.Typed, m.LocalDeadline, errString(err), m.RetryAfter
		return iv.finish(r)
	}
	r.InvocationID = inv.ID
	r.FuncStatus = inv.Status()
	if r.FuncStatus < 200 || r.FuncStatus > 599 { // "anything else is a platform fault and is answered 502"
		inv.Body.Close()
		r.Total = time.Since(t0)
		r.Outcome, r.Status, r.Reason, r.Error = OutPlatformError, 502, "INVALID_HEAD", "response head status outside 200-599"
		return iv.finish(r)
	}
	var sink io.Writer
	var ec *echoCheck
	if p.Verify != verifyNone {
		ec = newEchoCheck(p.Verify, p.RequestID, p.Req.Request.Body)
		sink = ec
	}
	if p.Sink != nil {
		if err := p.Sink.Head(inv); err != nil {
			inv.Body.Close()
			r.Total, r.Outcome = time.Since(t0), OutCanceled
			return iv.finish(r)
		}
		if sink != nil {
			sink = io.MultiWriter(ec, p.Sink)
		} else {
			sink = p.Sink
		}
	}
	bp := iv.bufs.Get().(*[]byte)
	n, rerr := drain(inv.Body, *bp, sink)
	iv.bufs.Put(bp)
	inv.Body.Close()
	r.Total = time.Since(t0)
	r.Bytes = n
	if errors.Is(rerr, errSinkGone) {
		r.Outcome = OutCanceled
		return iv.finish(r)
	}
	if rerr != nil {
		if parent.Err() != nil {
			r.Outcome = OutCanceled
			return iv.finish(r)
		}
		m := mapError(rerr, ctx.Err())
		r.Outcome, r.Status, r.Reason, r.Typed, r.LocalTimer, r.Error = OutStreamFailure, 0, m.Reason, m.Typed, m.LocalDeadline, errString(rerr)
		return iv.finish(r)
	}
	r.Outcome, r.Status = OutOK, r.FuncStatus
	if ec != nil {
		verifyResult(r, ec, inv, p)
	}
	return iv.finish(r)
}

// ReasonCanceledByPeer is the reason of an Invoke error that says "cancelled"
// when the caller did not cancel.
const ReasonCanceledByPeer = "UNTYPED_CANCELED_BY_PEER"

var errSinkGone = errors.New("the client went away")

func drain(rc interface{ Read([]byte) (int, error) }, buf []byte, sink io.Writer) (int64, error) {
	var n int64
	for {
		k, err := rc.Read(buf)
		n += int64(k)
		if k > 0 && sink != nil {
			if _, werr := sink.Write(buf[:k]); werr != nil {
				return n, errSinkGone
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return n, nil
			}
			return n, err
		}
	}
}

func errString(err error) string {
	s := err.Error()
	if len(s) > 300 {
		s = s[:300]
	}
	return strings.ReplaceAll(s, "\n", " ")
}

// newHTTPClient builds the HTTP client the library should use. The library's
// default is http.DefaultClient, whose transport keeps two idle connections per
// host: under load that is a connection per request and a port per request.
func newHTTPClient(mode string, conns int) *http.Client {
	tr := &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        0,
		MaxIdleConnsPerHost: 4096,
		IdleConnTimeout:     90 * time.Second,
		MaxConnsPerHost:     conns,
	}
	if conns > 0 {
		tr.MaxIdleConnsPerHost = conns
	}
	var p http.Protocols
	switch mode {
	case "2":
		p.SetUnencryptedHTTP2(true) // h2c with prior knowledge, for http://
		p.SetHTTP2(true)            // ALPN, for https://
	default:
		p.SetHTTP1(true)
	}
	tr.Protocols = &p
	return &http.Client{Transport: tr}
}

var _ = connect.CodeOf

func netListen(addr string) (net.Listener, error) { return net.Listen("tcp", addr) }

// verifyResult says whether the response was the one for this request.
func verifyResult(r *Result, ec *echoCheck, inv *edgerun.Invocation, p *Planned) {
	body := ec.finish(p.RequestID)
	hdr := ""
	if p.VerifyHeader != "" {
		hdr = checkHeader(inv.Head.GetHeaders(), p.VerifyHeader, p.RequestID)
	}
	switch {
	case body == "" && hdr == "":
		r.Verify = VerifyOK
		return
	case body != "" && hdr != "":
		r.Verify = VerifyBodyHdr
	case body != "":
		r.Verify = VerifyBody
	default:
		r.Verify = VerifyHeader
	}
	r.VerifyDetail = fmt.Sprintf("status %d: %s", r.FuncStatus, strings.Join(nonEmpty(body, hdr), " | "))
}

func nonEmpty(s ...string) []string {
	var out []string
	for _, v := range s {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}
