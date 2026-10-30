package e2e

// GetArtifact seam, consumer = the real worker; producer = a scripted
// orchestrator. The grammar of this stream is the thinnest: chunk*, a clean
// close, and the bytes must hash to the digest the worker asked for.
//
// Oracle: only a stream whose bytes reassemble to the module, and that closes
// cleanly, runs the function (and the function's answer is intact); anything else
// fails the invocation with a typed error (ARTIFACT_NOT_FOUND only when the
// orchestrator said so, INTERNAL for every other fault, DEADLINE_EXCEEDED for a
// stream that stops), never runs bytes that do not hash to the digest, and does
// not poison the worker's cache: the same digest, served correctly afterwards,
// runs. The worker stays whole.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

// stTinyWasm is a WASI command written byte by byte: it imports fd_write alone
// and prints a CGI response ("ok"). tag goes in a custom section, so that every
// tag is another module with another digest.
func stTinyWasm(tag string) []byte {
	sec := func(id byte, content []byte) []byte {
		out := []byte{id}
		out = binary.AppendUvarint(out, uint64(len(content)))
		return append(out, content...)
	}
	str := func(s string) []byte { return append(binary.AppendUvarint(nil, uint64(len(s))), s...) }
	text := "Content-Type: text/plain\n\nok\n"
	iov := []byte{16, 0, 0, 0, byte(len(text)), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	var m []byte
	m = append(m, 0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00)
	m = append(m, sec(1, []byte{2, 0x60, 4, 0x7f, 0x7f, 0x7f, 0x7f, 1, 0x7f, 0x60, 0, 0})...)
	imp := []byte{1}
	imp = append(imp, str("wasi_snapshot_preview1")...)
	imp = append(imp, str("fd_write")...)
	imp = append(imp, 0x00, 0x00)
	m = append(m, sec(2, imp)...)
	m = append(m, sec(3, []byte{1, 1})...)
	m = append(m, sec(5, []byte{1, 0, 1})...)
	exp := []byte{2}
	exp = append(exp, str("memory")...)
	exp = append(exp, 0x02, 0x00)
	exp = append(exp, str("_start")...)
	exp = append(exp, 0x00, 0x01)
	m = append(m, sec(7, exp)...)
	body := []byte{0, 0x41, 1, 0x41, 0, 0x41, 1, 0x41, 0xc8, 0x01, 0x10, 0, 0x1a, 0x0b}
	code := append([]byte{1}, binary.AppendUvarint(nil, uint64(len(body)))...)
	m = append(m, sec(10, append(code, body...))...)
	data := []byte{1, 0, 0x41, 0, 0x0b}
	data = binary.AppendUvarint(data, uint64(len(iov)+len(text)))
	data = append(data, iov...)
	data = append(data, text...)
	m = append(m, sec(11, data)...)
	m = append(m, sec(0, append(str("tag"), tag...))...)
	return m
}

// stArtifactModule is one module served by the scripted orchestrator.
type stArtifactModule struct {
	bytes  []byte
	pieces [][]byte
	digest string
	clean  atomic.Bool // serve it correctly, whatever the script says
	seq    stSeq
}

func stNewArtifactModule(tag string, n int) *stArtifactModule {
	b := stTinyWasm(tag)
	m := &stArtifactModule{bytes: b, digest: edgerun.Digest(b)}
	size := (len(b) + n - 1) / n
	for off := 0; off < len(b); off += size {
		m.pieces = append(m.pieces, b[off:min(off+size, len(b))])
	}
	return m
}

const (
	stP = "P" // the next piece of the module
	stD = "D" // the piece sent last, again
	stX = "X" // a byte that is not in the module
)

// stSent is what the script sends, concatenated.
func (m *stArtifactModule) sent(seq stSeq) []byte {
	var out []byte
	k := 0
	var last []byte
	for _, s := range seq.prefix() {
		switch s {
		case stP:
			if k < len(m.pieces) {
				last = m.pieces[k]
			} else {
				last = []byte("past the end")
			}
			k++
			out = append(out, last...)
		case stD:
			if last == nil {
				last = []byte("dup of nothing")
			}
			out = append(out, last...)
		case stX:
			out = append(out, 'X')
		}
	}
	return out
}

func (m *stArtifactModule) play(o *stOut, seq stSeq) {
	chunk := func(b []byte) { o.msg(stMust(&workerv1.GetArtifactResponse{Chunk: b})) }
	k := 0
	var last []byte
	for _, s := range seq.prefix() {
		switch s {
		case stP:
			if k < len(m.pieces) {
				last = m.pieces[k]
			} else {
				last = []byte("past the end")
			}
			k++
			chunk(last)
		case stD:
			if last == nil {
				last = []byte("dup of nothing")
			}
			chunk(last)
		case stX:
			chunk([]byte("X"))
		case stE:
			chunk(nil)
		}
	}
	// the terminal
	switch t := seq.term(); {
	case t == stCLEAN:
		o.end(0, "", nil)
	case t == stTRUNC:
	case t == stRESET:
		o.reset()
	case t == stSTALL:
		o.stall()
	default:
		kind := stErrs[t]
		var info *commonv1.ErrorInfo
		if kind.typed {
			info = &commonv1.ErrorInfo{Reason: kind.reason, Message: stSecret}
		}
		o.end(kind.code, stSecret, info)
	}
}

func (m *stArtifactModule) serveClean(o *stOut) {
	for _, p := range m.pieces {
		o.msg(stMust(&workerv1.GetArtifactResponse{Chunk: p}))
	}
	o.end(0, "", nil)
}

// stArtifactRig is a real worker with a scripted orchestrator that serves modules by digest.
type stArtifactRig struct {
	orch *stArtifactOrch
	exec *edgerun.Execution
	mu   sync.Mutex
	mods map[string]*stArtifactModule
}

func stNewArtifactRig(t *testing.T) *stArtifactRig {
	r := &stArtifactRig{mods: map[string]*stArtifactModule{}}
	r.orch = stNewArtifactOrch(t)
	serve := func(o *stOut, digest string) {
		r.mu.Lock()
		m := r.mods[digest]
		r.mu.Unlock()
		switch {
		case m == nil:
			o.end(connect.CodeNotFound, "unknown", &commonv1.ErrorInfo{Reason: commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND, Message: "unknown"})
		case m.clean.Load():
			m.serveClean(o)
		default:
			m.play(o, m.seq)
		}
	}
	r.orch.serve.Store(&serve)
	url := stStartWorker(t, r.orch.srv.URL, false, 4)
	r.exec = edgerun.NewExecution(url, executionToken, edgerun.WithHTTPClient(&http.Client{Transport: budgetTransport{}}))
	return r
}

func (r *stArtifactRig) add(m *stArtifactModule) {
	r.mu.Lock()
	r.mods[m.digest] = m
	r.mu.Unlock()
}

type stExecObs struct {
	head    *commonv1.HttpResponseHead
	body    []byte
	err     error
	elapsed time.Duration
}

func (r *stArtifactRig) execute(digest, id string, budget time.Duration) stExecObs {
	cctx, cancel := stCtx(30 * time.Second)
	defer cancel()
	cctx = withBudget(cctx, budget)
	start := time.Now()
	res, err := r.exec.Execute(cctx, &workerv1.ExecuteRequest{
		InvocationId: id, ArtifactDigest: digest, Abi: "wagi-1",
		Request: &commonv1.HttpRequest{Method: "GET", Host: "a.test", Path: "/"},
	})
	if err != nil {
		return stExecObs{err: err, elapsed: time.Since(start)}
	}
	b, err := res.ReadAll()
	return stExecObs{head: res.Head, body: b, err: err, elapsed: time.Since(start)}
}

func (o stExecObs) ok() bool {
	return o.err == nil && o.head.GetStatus() == 200 && string(o.body) == "ok\n"
}

func stCheckArtifact(m *stArtifactModule, seq stSeq, o stExecObs, budget time.Duration) (id, msg string) {
	bad := func(id, format string, a ...any) (string, string) { return id, fmt.Sprintf(format, a...) }
	if o.elapsed > budget+1500*time.Millisecond {
		return bad("AR-HANG", "took %v with a %v budget", o.elapsed, budget)
	}
	valid := seq.term() == stCLEAN && bytes.Equal(m.sent(seq), m.bytes)
	if valid {
		if !o.ok() {
			return bad("AR-REJECTED", "the module arrived intact and the invocation failed: %v %q", o.err, o.body)
		}
		return "", ""
	}
	if o.err == nil {
		return bad("AR-HIDDEN", "the invocation succeeded (%q) with bytes that are not the module (%d bytes sent, %d in the module)", o.body, len(m.sent(seq)), len(m.bytes))
	}
	want := commonv1.ErrorReason_ERROR_REASON_INTERNAL
	switch t := seq.term(); {
	case t == stSTALL:
		want = commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED
	case t == "ERR:ARTIFACT_NOT_FOUND":
		want = commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND
	}
	if got := edgerun.Reason(o.err); got != want {
		id := "AR-REASON"
		if got == commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND {
			id = "AR-MISCLASSIFIED-NOT-FOUND"
		}
		return bad(id, "reason %v, want %v (%v)", got, want, o.err)
	}
	return "", ""
}

func stArtifactSequences() (normal, stalls []stSeq) {
	syms := []string{stP, stE, stD, stX}
	terms := []string{stCLEAN, stTRUNC, stRESET, "ERR:ARTIFACT_NOT_FOUND", "ERR:INTERNAL", "ERR:NO_CAPACITY", "ERR:unavailable"}
	normal = stEnumerate(syms, 4, terms, nil, 0)
	normal = append(normal, stSample(syms, terms, 150, 20240604)...)
	for _, s := range stEnumerate(syms, 1, []string{stSTALL}, nil, 0) {
		stalls = append(stalls, s)
	}
	return
}

func TestStreams_GetArtifact_Worker(t *testing.T) {
	r := stNewArtifactRig(t)
	budget := 900 * time.Millisecond
	// the tiny module runs
	warm := stNewArtifactModule("warm", 3)
	warm.clean.Store(true)
	r.add(warm)
	eventually2(t, "the tiny module to run", 30*time.Second, func() bool { return r.execute(warm.digest, "warm", 5*time.Second).ok() })
	for i := 0; i < 5; i++ {
		r.execute(warm.digest, "warm", 5*time.Second)
	}
	base := stSnapshot()
	t.Logf("baseline: %v", base)

	var f stFindings
	normal, stalls := stArtifactSequences()
	start := time.Now()
	one := func(i int, seq stSeq, stalled bool) {
		m := stNewArtifactModule(fmt.Sprintf("seq-%d", i), 3)
		m.seq = seq
		r.add(m)
		b := budget + 1500*time.Millisecond
		if stalled {
			b = budget
		}
		o := r.execute(m.digest, fmt.Sprintf("seq-%d", i), b)
		if id, msg := stCheckArtifact(m, seq, o, b); msg != "" {
			f.add(id, seq.String(), msg)
		}
		// the cache is not poisoned: the same digest, served correctly, runs.
		// (After a stall it does not, for up to a minute: see D-WK-STUCK-FETCH.)
		m.clean.Store(true)
		if !stalled {
			if n := r.execute(m.digest, "again", 5*time.Second); !n.ok() {
				f.add("WK-POISONED", seq.String(), fmt.Sprintf("the same digest, served correctly afterwards: %v %q", n.err, n.body))
			}
		}
		if !stalled {
			if cur, ok := stSettle(base, 8, 6, 3*time.Second); !ok {
				f.add("WK-LEAK-RES", seq.String(), fmt.Sprintf("did not return to the baseline (%v): now %v", base, cur))
				base = cur
			}
		}
	}
	for i, seq := range normal {
		one(i, seq, false)
	}
	t.Logf("%d sequences in %v", len(normal), time.Since(start).Round(time.Millisecond))
	if cur, ok := stSettle(base, 8, 6, 5*time.Second); !ok {
		f.add("WK-LEAK-RES", "(end)", fmt.Sprintf("after all sequences: %v, baseline %v", cur, base))
	}
	// A stream that stops last: the worker's fetch outlives the invocation (see the known defect below).
	for i, seq := range stalls {
		one(len(normal)+i, seq, true)
	}
	f.report(t, stArtifactKnown)
}

var stArtifactKnown = map[string]string{}

// D-WK-STUCK-FETCH: the worker's artifact fetch is shared by every invocation of
// a digest and is "not tied to any one caller's context" (cache.Store.Get), only
// to a fixed 60 s timeout. An orchestrator that stops answering mid-stream (a
// black hole, a hung instance) makes the first invocation fail at its deadline,
// as it must; but the stuck fetch is still the one every later invocation of
// that digest joins, so the digest stays unservable by this worker, and its
// goroutine and connection stay open, for up to a minute AFTER the orchestrator
// has recovered. The protocol says the deadline is the budget and that a worker
// must stop what it was doing; it says nothing about what an abandoned fetch may
// cost the next caller.
func TestStreams_KnownDefect_ArtifactStuckFetch(t *testing.T) {
	stSkipKnown(t, "D-WK-STUCK-FETCH", "a stalled artifact stream blocks every later invocation of the digest for up to 60 s, orchestrator recovered or not")
	r := stNewArtifactRig(t)
	m := stNewArtifactModule("stuck", 3)
	m.seq = stSeq{stP, stSTALL}
	r.add(m)
	warm := stNewArtifactModule("warm", 3)
	warm.clean.Store(true)
	r.add(warm)
	eventually2(t, "the tiny module to run", 30*time.Second, func() bool { return r.execute(warm.digest, "warm", 5*time.Second).ok() })
	base := stSnapshot()

	o := r.execute(m.digest, "stalled", 800*time.Millisecond)
	if edgerun.Reason(o.err) != commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED {
		t.Fatalf("a stalled artifact stream: %v, want DEADLINE_EXCEEDED", o.err)
	}
	// the orchestrator recovers
	m.clean.Store(true)
	if n := r.execute(m.digest, "after", 5*time.Second); !n.ok() {
		t.Errorf("the orchestrator is healthy again and the digest is still not served 5 s later: %v", n.err)
	}
	if cur, ok := stSettle(base, 8, 6, 3*time.Second); !ok {
		t.Errorf("the abandoned fetch is still open 3 s after the deadline: %v, baseline %v", cur, base)
	}
}
