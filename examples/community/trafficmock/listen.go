package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

// Listener is a minimal frontend: it accepts real HTTP, forwards each request
// over ingress.v1 the way docs/protocol.md "Frontend behaviour" says, and
// records the same measurements as a scenario run. With a recorder it also
// writes the traffic it sees as a trace that `run` can replay.
type Listener struct {
	Inv    *Invoker
	Budget time.Duration
	Trust  bool // keep client-supplied forwarding headers (behind a trusted proxy)
	Trace  *TraceWriter
	RunID  string
	seq    atomic.Uint64
}

func (l *Listener) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	seq := l.seq.Add(1)
	reqID := clientRequestID(r)
	w.Header().Set("X-Request-Id", reqID)
	l.Inv.Stats.Released(time.Now())

	if r.URL.Path == "" || r.URL.Path[0] != '/' { // "OPTIONS *" and the like are never forwarded
		http.Error(w, "Bad Request", http.StatusBadRequest)
		l.Inv.Stats.Record(&Result{Seq: seq, RequestID: reqID, Template: "listen", Outcome: OutRejected, Status: 400, Reason: "LOCAL_BAD_TARGET"})
		return
	}
	limit := l.Inv.Limits.Max()
	reject := func() {
		http.Error(w, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
		l.Inv.Stats.Record(&Result{Seq: seq, RequestID: reqID, Template: "listen", Outcome: OutRejected, Status: 413, Reason: "LOCAL_REQUEST_TOO_LARGE"})
	}
	if r.ContentLength > 0 && uint64(r.ContentLength) > limit {
		reject()
		return
	}
	// Bounded even when the length is not declared.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(limit)))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			reject()
		} else {
			http.Error(w, "Bad Request", http.StatusBadRequest)
		}
		return
	}
	ip, _, serr := net.SplitHostPort(r.RemoteAddr)
	if serr != nil {
		ip = r.RemoteAddr
	}
	proto := "http"
	if r.TLS != nil {
		proto = "https"
	}
	hs := filterHeaders(r.Header, l.Trust)
	if l.Trust {
		// behind a trusted proxy: keep what it said and append the peer
		xff := ip
		if prior := strings.Join(r.Header.Values("X-Forwarded-For"), ", "); prior != "" {
			xff = prior + ", " + ip
		}
		out := hs[:0:0]
		for _, h := range hs {
			if h.Name != "x-forwarded-for" {
				out = append(out, h)
			}
		}
		hs = append(out, &commonv1.Header{Name: "x-forwarded-for", Value: []byte(xff)})
		if r.Header.Get("X-Forwarded-Proto") == "" {
			hs = append(hs, &commonv1.Header{Name: "x-forwarded-proto", Value: []byte(proto)})
		}
	} else {
		hs = appendForwarded(hs, ip, proto, r.Host)
	}
	req := &ingressv1.InvokeRequest{RequestId: reqID, Request: &commonv1.HttpRequest{
		Method: r.Method, Host: r.Host, Path: r.URL.Path, RawQuery: r.URL.RawQuery, Headers: hs, Body: body, RemoteAddr: r.RemoteAddr,
	}}
	if l.Trace != nil {
		l.Trace.Add(r, body)
	}

	sink := &httpSink{w: w, rc: http.NewResponseController(w), reqID: reqID}
	// The request context ends the invocation if the client goes away.
	res := l.Inv.Do(r.Context(), &Planned{Seq: seq, RequestID: reqID, Template: "listen", Req: req, Budget: l.Budget, Sink: sink})
	switch {
	case res.Outcome == OutPlatformError && !sink.wrote:
		if res.RetryAfter {
			w.Header().Set("Retry-After", "1")
		}
		// a generic body: error messages are for operators
		http.Error(w, http.StatusText(res.Status), res.Status)
	case res.Outcome == OutStreamFailure:
		// The status line is out; the only honest signal is a failed transfer.
		panic(http.ErrAbortHandler)
	}
}

type httpSink struct {
	w     http.ResponseWriter
	rc    *http.ResponseController
	reqID string
	wrote bool
}

func (s *httpSink) Head(inv *edgerun.Invocation) error {
	h := s.w.Header()
	for _, hd := range inv.Head.GetHeaders() {
		n := strings.ToLower(hd.GetName())
		if hopByHop[n] || n == "content-length" {
			continue
		}
		h.Add(n, string(hd.GetValue()))
	}
	h.Set("X-Request-Id", s.reqID)
	s.wrote = true
	s.w.WriteHeader(inv.Status())
	return nil
}

func (s *httpSink) Write(p []byte) (int, error) {
	s.rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
	n, err := s.w.Write(p)
	if err == nil {
		err = s.rc.Flush()
	}
	if errors.Is(err, http.ErrBodyNotAllowed) {
		err = nil
	}
	return n, err
}

// clientRequestID is the client's X-Request-Id if it is printable ASCII of at
// most 128 characters, otherwise a fresh one.
func clientRequestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-Id"); id != "" && len(id) <= 128 {
		ok := true
		for _, c := range id {
			if c < 0x21 || c > 0x7e {
				ok = false
				break
			}
		}
		if ok {
			return id
		}
	}
	var b [12]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// TraceWriter records the traffic a Listener sees as a JSONL trace.
type TraceWriter struct {
	mu    sync.Mutex
	w     *bufio.Writer
	f     *os.File
	start time.Time
}

func NewTraceWriter(path string) (*TraceWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &TraceWriter{w: bufio.NewWriter(f), f: f, start: time.Now()}, nil
}

func (t *TraceWriter) Add(r *http.Request, body []byte) {
	e := map[string]any{
		"t": time.Since(t.start).Seconds(), "method": r.Method, "host": r.Host, "path": r.URL.Path, "query": r.URL.RawQuery,
	}
	hs := map[string][]string{}
	for _, h := range filterHeaders(r.Header, false) {
		hs[h.Name] = append(hs[h.Name], string(h.Value))
	}
	if len(hs) > 0 {
		e["headers"] = hs
	}
	if len(body) > 0 {
		if utf8.Valid(body) {
			e["body"] = string(body)
		} else {
			e["body_b64"] = base64.StdEncoding.EncodeToString(body)
		}
	}
	b, _ := json.Marshal(e)
	t.mu.Lock()
	t.w.Write(b)
	t.w.WriteByte('\n')
	t.mu.Unlock()
}

func (t *TraceWriter) Close() { t.mu.Lock(); t.w.Flush(); t.f.Close(); t.mu.Unlock() }

func listenCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("listen", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	addr := fs.String("addr", "127.0.0.1:8000", "address to accept HTTP on")
	budget := fs.Duration("request-timeout", 30*time.Second, "the budget handed to Invoke for each request")
	trust := fs.Bool("trust-forwarded", false, "keep client-supplied x-forwarded-* headers (only behind a proxy you control)")
	record := fs.String("record", "", "write the traffic seen as a replayable JSONL trace")
	healthAddr := fs.String("health-addr", "", "serve /healthz here (the main address forwards everything)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	sc := &Scenario{Name: "listen"}
	ing, url, err := c.client(sc)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// Bind first: a conformance suite (or a load balancer) may start sending the moment the
	// frontend has asked for its limits, so the port must already be accepting.
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 3
	}
	lim := NewLimits(ing)
	// A frontend asks for its limits when it starts and keeps trying: it serves
	// with the default until the orchestrator has answered.
	if err := lim.Refresh(ctx); err != nil {
		fmt.Fprintf(stderr, "Describe failed (%v): using %d until it answers\n", err, defaultMaxBody)
	}
	go lim.Run(ctx, 10*time.Second)
	if !lim.Known() {
		go func() {
			for t := time.NewTicker(time.Second); ; {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if lim.Refresh(ctx) == nil {
						return
					}
				}
			}
		}()
	}
	started := time.Now()
	runID := firstNonEmpty(c.runID, newRunID())
	st := NewStats(started)
	var rl *ReqLog
	if c.reqLogPath != "" {
		if rl, err = NewReqLog(c.reqLogPath); err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
	}
	inv := NewInvoker(ing, lim, st, rl, started)
	l := &Listener{Inv: inv, Budget: *budget, Trust: *trust, RunID: runID}
	if *record != "" {
		if l.Trace, err = NewTraceWriter(*record); err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
	}
	if c.prom != "" {
		var rld *ReqLog = rl
		stopProm, err := serveMetrics(c.prom, st, runID, lim, rld)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		defer stopProm()
	}
	var p http.Protocols
	p.SetHTTP1(true)
	srv := &http.Server{Addr: *addr, Handler: l, ReadHeaderTimeout: 10 * time.Second, Protocols: &p}
	if *healthAddr != "" {
		hs := &http.Server{Addr: *healthAddr, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })}
		hl, err := net.Listen("tcp", *healthAddr)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 3
		}
		go hs.Serve(hl)
		defer hs.Close()
	}
	fmt.Fprintf(stderr, "listening on http://%s, forwarding to %s (run %s)\n", *addr, url, runID)
	go func() {
		<-ctx.Done()
		sctx, c := context.WithTimeout(context.Background(), 10*time.Second)
		srv.Shutdown(sctx)
		c()
	}()
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(stderr, err)
		return 3
	}
	if l.Trace != nil {
		l.Trace.Close()
	}
	if rl != nil {
		rl.Close()
	}
	rep := BuildReport(sc, runID, "listen", url, firstNonEmpty(c.httpMode, "1"), started, time.Since(started), time.Since(started), st.Snapshot(), lim)
	rep.Expect = Evaluate(c.expects, st.Snapshot(), rep)
	if c.jsonOut != "" {
		if f, err := os.Create(c.jsonOut); err == nil {
			rep.JSON(f)
			f.Close()
		}
	}
	if !c.quiet {
		rep.Text(stdout)
	}
	for _, e := range rep.Expect {
		if e.Error != "" || !e.Pass {
			return 1
		}
	}
	return 0
}
