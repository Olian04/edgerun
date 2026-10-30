package e2e

// Shared machinery of the stream-grammar property tests (streams_*_test.go):
//
//   - a scripted PRODUCER of Connect server streams written from the Connect
//     wire format alone (envelopes, end-stream frame), so that it can do what a
//     well-behaved library never would: a message with no part set, a reset in
//     the middle of an envelope, a stream that stops, a stream that closes with
//     no end-stream frame;
//   - the enumeration of symbol sequences;
//   - resource accounting (goroutines and sockets of this process, which is
//     where every in-process component lives).

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// ---- known defects -------------------------------------------------------------

// stKnown reports whether the run asked for the known defects to be asserted.
func stKnownDefects() bool { return os.Getenv("EDGERUN_E2E_KNOWN_DEFECTS") != "" }

func stSkipKnown(t *testing.T, id, line string) {
	t.Helper()
	if !stKnownDefects() {
		t.Skip("KNOWN DEFECT " + id + ": " + line)
	}
}

// stFindings collects what a sweep found. A finding whose defect id is known
// is logged, and fails the sweep only when EDGERUN_E2E_KNOWN_DEFECTS is set; a
// finding with no id always fails it.
type stFindings struct {
	mu    sync.Mutex
	items []stFinding
}

type stFinding struct{ id, seq, msg string }

func (f *stFindings) add(id, seq, msg string) {
	f.mu.Lock()
	f.items = append(f.items, stFinding{id, seq, msg})
	f.mu.Unlock()
}

// report fails t for unknown findings (all of them with known defects asserted)
// and logs a digest of the known ones. Findings are grouped by (id, message
// shape) so that a thousand sequences with one cause read as one line.
func (f *stFindings) report(t *testing.T, known map[string]string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	type key struct{ id, msg string }
	groups := map[key][]string{}
	var order []key
	for _, it := range f.items {
		k := key{it.id, it.msg}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], it.seq)
	}
	for _, k := range order {
		seqs := groups[k]
		eg := seqs[0]
		line := fmt.Sprintf("[%s] %s: %d sequence(s), e.g. %s", k.id, k.msg, len(seqs), eg)
		if len(seqs) > 1 {
			line += " ; " + seqs[len(seqs)-1]
		}
		if _, isKnown := known[k.id]; isKnown && !stKnownDefects() {
			t.Logf("known defect %s (%s): %s", k.id, known[k.id], line)
			continue
		}
		t.Error(line)
	}
}

// ---- the producer: a Connect server stream written by hand ----------------------

// stEnvelope frames payload as a Connect streaming message.
func stEnvelope(flags byte, payload []byte) []byte {
	b := make([]byte, 5+len(payload))
	b[0] = flags
	binary.BigEndian.PutUint32(b[1:], uint32(len(payload)))
	copy(b[5:], payload)
	return b
}

// stReadRequest reads the single enveloped request message of a server-streaming call.
func stReadRequest(r *http.Request, into proto.Message) error {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	if len(raw) < 5 {
		return fmt.Errorf("short request: %d bytes", len(raw))
	}
	payload := raw[5:]
	if raw[0]&1 != 0 {
		zr, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return err
		}
		if payload, err = io.ReadAll(zr); err != nil {
			return err
		}
	}
	return proto.Unmarshal(payload, into)
}

// stOut is the response side of one scripted call.
type stOut struct {
	w http.ResponseWriter
	r *http.Request
	f http.Flusher
}

func stNewOut(w http.ResponseWriter, r *http.Request) *stOut {
	w.Header().Set("Content-Type", "application/connect+proto")
	w.WriteHeader(http.StatusOK)
	f, _ := w.(http.Flusher)
	o := &stOut{w: w, r: r, f: f}
	o.flush()
	return o
}

func (o *stOut) flush() {
	if o.f != nil {
		o.f.Flush()
	}
}

// msg sends one message (already marshalled).
func (o *stOut) msg(payload []byte) {
	o.w.Write(stEnvelope(0, payload))
	o.flush()
}

// end sends the end-stream frame: a success if err is nil.
func (o *stOut) end(code connect.Code, message string, info *commonv1.ErrorInfo) {
	body := map[string]any{}
	if code != 0 {
		e := map[string]any{"code": code.String(), "message": message}
		if info != nil {
			d, _ := connect.NewErrorDetail(info)
			e["details"] = []any{map[string]string{"type": d.Type(), "value": base64.RawStdEncoding.EncodeToString(d.Bytes())}}
		}
		body["error"] = e
	}
	b, _ := json.Marshal(body)
	o.w.Write(stEnvelope(2, b))
	o.flush()
}

// reset breaks the connection in the middle of an envelope: a header that
// promises 100 bytes, ten of them, then the connection (or the HTTP/2 stream) dies.
func (o *stOut) reset() {
	o.w.Write([]byte{0, 0, 0, 0, 100})
	o.w.Write([]byte("0123456789"))
	o.flush()
	panic(http.ErrAbortHandler)
}

// stall sends nothing more until the consumer gives up (or a generous bound).
func (o *stOut) stall() {
	select {
	case <-o.r.Context().Done():
	case <-time.After(20 * time.Second):
	}
}

// ---- servers --------------------------------------------------------------------

// stServer is an HTTP server (HTTP/1.1 and cleartext HTTP/2, as the components
// themselves serve) on a loopback port.
type stServer struct {
	URL  string
	Addr string
	srv  *http.Server
	ln   net.Listener
}

func stServe(t testing.TB, h http.Handler) *stServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	s := &stServer{URL: "http://" + ln.Addr().String(), Addr: ln.Addr().String(), ln: ln,
		srv: &http.Server{Handler: h, Protocols: &p, ErrorLog: nil}}
	go s.srv.Serve(ln)
	t.Cleanup(s.Close)
	return s
}

func (s *stServer) Close() { s.srv.Close() }

// ---- sequences --------------------------------------------------------------------

// stSeq is a sequence of symbols; every symbol but the last is a non-terminal
// (it is a message the producer sends), the last is a terminal (what ends the
// stream).
type stSeq []string

func (s stSeq) String() string { return strings.Join(s, ",") }

func (s stSeq) prefix() []string { return s[:len(s)-1] }
func (s stSeq) term() string     { return s[len(s)-1] }

// stEnumerate returns every prefix over syms of length 0..depth, each with
// every terminal in terms; terminals in deepTerms only after prefixes of at most
// shallow symbols (they are expensive: a stall costs a whole deadline).
func stEnumerate(syms []string, depth int, terms []string, shallowTerms []string, shallow int) []stSeq {
	if stRaceBuild && depth > 2 {
		depth-- // under -race, one level less
	}
	var prefixes [][]string
	var rec func(p []string)
	rec = func(p []string) {
		prefixes = append(prefixes, slices.Clone(p))
		if len(p) == depth {
			return
		}
		for _, s := range syms {
			rec(append(p, s))
		}
	}
	rec(nil)
	var out []stSeq
	for _, p := range prefixes {
		for _, t := range terms {
			out = append(out, append(slices.Clone(p), t))
		}
		if len(p) <= shallow {
			for _, t := range shallowTerms {
				out = append(out, append(slices.Clone(p), t))
			}
		}
	}
	return out
}

// stSample returns n random sequences of length 5 (four non-terminals and a
// terminal), from a fixed seed, so that a failure is a reproducible name.
func stSample(syms, terms []string, n int, seed int64) []stSeq {
	if stRaceBuild {
		n /= 3
	}
	rng := rand.New(rand.NewSource(seed))
	out := make([]stSeq, 0, n)
	for i := 0; i < n; i++ {
		var s stSeq
		for j := 0; j < 4; j++ {
			s = append(s, syms[rng.Intn(len(syms))])
		}
		s = append(s, terms[rng.Intn(len(terms))])
		out = append(out, s)
	}
	return out
}

// ---- resources ----------------------------------------------------------------------

type stRes struct{ goroutines, sockets int }

func (r stRes) String() string {
	return fmt.Sprintf("%d goroutines, %d sockets", r.goroutines, r.sockets)
}

// stSockets counts the sockets this process has open (listeners and
// connections of every in-process component and of the test).
func stSockets() int {
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range ents {
		if l, err := os.Readlink("/proc/self/fd/" + e.Name()); err == nil && strings.HasPrefix(l, "socket:") {
			n++
		}
	}
	return n
}

func stSnapshot() stRes { return stRes{runtime.NumGoroutine(), stSockets()} }

// stSettle waits until the process is back to within slack of base, and returns
// what it ended at.
func stSettle(base stRes, slackG, slackS int, within time.Duration) (stRes, bool) {
	deadline := time.Now().Add(within)
	for {
		cur := stSnapshot()
		if cur.goroutines <= base.goroutines+slackG && cur.sockets <= base.sockets+slackS {
			return cur, true
		}
		if time.Now().After(deadline) {
			return cur, false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// stCounter is a count that the producer bumps and the test reads.
type stCounter struct{ n atomic.Int64 }

func (c *stCounter) add()       { c.n.Add(1) }
func (c *stCounter) get() int64 { return c.n.Load() }

// stCtx is a context with a timeout.
func stCtx(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
