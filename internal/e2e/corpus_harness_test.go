package e2e

// The HTTP corpus: raw bytes in through a real TCP connection, the frontend's
// answer out, and (when the request is forwarded) what the guest was given.
// This file is the machinery; the rows live in corpus_*_test.go.
//
// A row says what it sends, and which outcomes are acceptable. An outcome is a
// "signature" of the whole conversation: one entry per final response,
// "<status>" for an answer the frontend made itself, "<status> <PATH_INFO>
// len=<body length the guest read>" for one the guest made. Several acceptable
// outcomes are listed where the spec (or the RFC it defers to) allows a choice,
// for example "reject with 400 or accept"; what is never acceptable is the
// connection being desynchronised, and the sentinel request that the harness
// appends to a row is how that is seen: it must be answered exactly where
// the rows say, or not at all when the frontend has closed the connection.

import (
	"bufio"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Olian04/edgerun"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	orchapp "github.com/Olian04/edgerun/internal/orchestrator/app"
	"github.com/Olian04/edgerun/internal/testpg"
)

const (
	corpusHost      = "corpus.test"
	corpusBodyLimit = 2 << 20 // the orchestrator's (and so the frontend's) max_request_body_bytes
	rowDeadline     = 20 * time.Second
)

//go:embed corpus_guest/*.go
var corpusGuestSrc embed.FS

// buildCorpusGuest builds corpus_guest the way conformance/guest builds its
// guests (cached by source hash, safe against concurrent test binaries).
func buildCorpusGuest(t testing.TB) []byte {
	t.Helper()
	files, err := fs.ReadDir(corpusGuestSrc, "corpus_guest")
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	sources := map[string][]byte{}
	for _, f := range files {
		b, err := corpusGuestSrc.ReadFile("corpus_guest/" + f.Name())
		if err != nil {
			t.Fatal(err)
		}
		sources[f.Name()] = b
		h.Write([]byte(f.Name()))
		h.Write(b)
	}
	name := "corpus_guest-" + hex.EncodeToString(h.Sum(nil)[:6]) + ".wasm"
	cacheRoot, err := os.UserCacheDir()
	if err != nil {
		cacheRoot = os.TempDir()
	}
	cacheDir := filepath.Join(cacheRoot, "edgerun-conformance")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cached := filepath.Join(cacheDir, name)
	if b, err := os.ReadFile(cached); err == nil {
		return b
	}
	work := t.TempDir()
	for n, b := range sources {
		if err := os.WriteFile(filepath.Join(work, n), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(work, "go.mod"), []byte("module corpus_guest\n\ngo 1.21\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(work, "out.wasm")
	cmd := exec.Command("go", "build", "-trimpath", "-o", out, ".")
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0", "GOFLAGS=-mod=mod")
	if msg, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building corpus_guest: %v\n%s", err, msg)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := os.CreateTemp(cacheDir, "tmp-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp.Name(), cached); err != nil {
		t.Fatal(err)
	}
	return b
}

// corpusEnv is a stack with the corpus guest behind two routes, a worker and a
// real frontend.
type corpusEnv struct {
	s          *stack
	w          *web
	exact, any string // function ids: "corpus.test" and the catch-all "*"

	mu   sync.Mutex
	seen int // final responses that carried an X-Edgerun-Invocation-Id
}

func newCorpusEnv(t *testing.T) *corpusEnv {
	t.Helper()
	s := &stack{t: t, dsn: testpg.DSN(t), wasm: buildCorpusGuest(t)}
	cfg := orchConfig(s.dsn, "127.0.0.1:0")
	cfg.MaxRequestBodyBytes = corpusBodyLimit
	srv, err := orchapp.Start(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.orch, s.addr = srv, srv.Addr()
	url := "http://" + s.addr
	s.ctl = edgerun.NewControl(url, adminToken)
	s.ingress = edgerun.NewIngress(url, ingressToken)
	t.Cleanup(func() { srv.Stop(ctx) })
	s.startWorker(4)

	e := &corpusEnv{s: s}
	e.exact = s.createFunction("corpus-exact")
	s.uploadVersion(e.exact, s.wasm, nil, true)
	s.setEnv(e.exact, "WHO", "exact", false)
	s.route(corpusHost, "/", e.exact)
	e.any = s.createFunction("corpus-any")
	s.uploadVersion(e.any, s.wasm, nil, true)
	s.setEnv(e.any, "WHO", "any", false)
	s.route("*", "/", e.any)

	e.w = s.startFrontend(60 * time.Second) // the first call compiles the module, which is slow under the race detector

	eventually(t, "the stack to serve", 120*time.Second, func() bool {
		// through exchange, so that every answer that names an invocation is counted for the audit
		ex := e.exchange(&crow{raw: cget("/warmup"), deadline: 90 * time.Second})
		return ex.err == nil && len(ex.resps) == 1 && ex.resps[0].status == 200
	})
	// Warming up can leave an invocation behind that nobody saw an answer to (the
	// first call compiles the module; under the race detector that outlasts the
	// frontend's 15 s budget, the client gets a 504 that names no invocation, and
	// the orchestrator still writes the record). Settle, then count from here.
	settled, stable := -2, 0
	eventually(t, "the invocation records to settle", 60*time.Second, func() bool {
		n := e.records()
		if n == settled {
			stable++
		} else {
			settled, stable = n, 0
		}
		return stable >= 40
	})
	e.mu.Lock()
	e.seen = settled
	e.mu.Unlock()
	// The frontend uses 6 MiB until it has read the orchestrator's limit. A body
	// declared one byte over the limit and then abandoned is a 413 once it has
	// (it is never read), and a 400 (unexpected EOF) before.
	eventually(t, "the frontend to learn max_request_body_bytes", 30*time.Second, func() bool {
		ex := e.exchange(&crow{raw: rawReq("POST /probe HTTP/1.1", "Host: "+corpusHost, "Content-Length: "+strconv.Itoa(corpusBodyLimit+1)), closeWrite: true})
		return ex.err == nil && len(ex.resps) == 1 && ex.resps[0].status == 413
	})
	t.Cleanup(func() { e.audit(t) })
	return e
}

// records is the number of invocation records the orchestrator holds for the
// two functions, -1 if it cannot be read.
func (e *corpusEnv) records() int {
	n := 0
	for _, fid := range []string{e.exact, e.any} {
		tok := ""
		for {
			r, err := e.s.ctl.ListInvocations(ctx, &controlv1.ListInvocationsRequest{FunctionId: fid, PageSize: 200, PageToken: tok})
			if err != nil {
				return -1
			}
			n += len(r.Invocations)
			if tok = r.NextPageToken; tok == "" {
				break
			}
		}
	}
	return n
}

// audit checks that the orchestrator holds exactly one invocation record for
// every response that named one: nothing the frontend refused was forwarded, and
// nothing was forwarded that nobody got an answer to (a smuggled request).
func (e *corpusEnv) audit(t *testing.T) {
	if t.Failed() {
		return
	}
	e.mu.Lock()
	want := e.seen
	e.mu.Unlock()
	count := e.records
	deadline := time.Now().Add(30 * time.Second)
	got := count()
	for got != want && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		got = count()
	}
	if got != want {
		t.Errorf("audit: %d responses named an invocation, the orchestrator holds %d records (a request was forwarded that was not answered, or an answered one left no record)", want, got)
	}
}

// ---- building requests -------------------------------------------------------

// rawReq is a request line, header lines and the blank line, CRLF-terminated.
func rawReq(line string, hdr ...string) string {
	if len(hdr) == 0 {
		return line + "\r\n\r\n"
	}
	return line + "\r\n" + strings.Join(hdr, "\r\n") + "\r\n\r\n"
}

// cget is an HTTP/1.1 GET of target at the corpus host.
func cget(target string, hdr ...string) string {
	return rawReq("GET "+target+" HTTP/1.1", append([]string{"Host: " + corpusHost}, hdr...)...)
}

// withHost is any method and target at the corpus host.
func withHost(method, target string, hdr ...string) string {
	return rawReq(method+" "+target+" HTTP/1.1", append([]string{"Host: " + corpusHost}, hdr...)...)
}

// post is a POST with a Content-Length that matches its body.
func post(target, body string, hdr ...string) string {
	h := append([]string{"Host: " + corpusHost, "Content-Length: " + strconv.Itoa(len(body))}, hdr...)
	return rawReq("POST "+target+" HTTP/1.1", h...) + body
}

// chunked is a POST whose body is exactly the given chunked framing.
func chunked(target, framing string, hdr ...string) string {
	h := append([]string{"Host: " + corpusHost, "Transfer-Encoding: chunked"}, hdr...)
	return rawReq("POST "+target+" HTTP/1.1", h...) + framing
}

const sentinelReq = "GET /sentinel HTTP/1.1\r\nHost: " + corpusHost + "\r\n\r\n"

// ---- rows ----------------------------------------------------------------------

// crow is one row of a corpus table.
type crow struct {
	name string
	raw  string

	// parts replaces raw: the pieces are written in order, and with hold100 the
	// writer waits for the server's "100 Continue" (or a final answer) between
	// the first piece and the rest.
	parts   []string
	hold100 bool
	// sentinel appends a plain GET /sentinel and reads until it is answered or the
	// connection closes: it proves the server and the client still agree about
	// where each request ends.
	sentinel   bool
	deadline   time.Duration // default rowDeadline
	closeWrite bool          // half-close after sending (a truncated body is then EOF, not a wait)
	heads      []int         // indexes of responses that answer a HEAD request
	responses  int           // final responses to read when there is no sentinel (default 1)

	// want lists the acceptable outcome signatures (see the file comment). Empty
	// means: only the guest/check assertions below.
	want []string
	// guest asserts on the first response the guest made: an environment entry
	// by name ("-NAME": must be absent), or "BODY", "BODY_LEN", "BODY_SHA".
	guest map[string]string
	// check is for everything else.
	check func(t *testing.T, ex *exch)

	spec   string // the passage of docs/protocol.md (or RFC) the row follows
	silent string // set where the spec decides nothing: what was observed, pinned
	defect string // "ID: what" - a real defect: the row is skipped unless EDGERUN_E2E_KNOWN_DEFECTS is set
}

type exch struct {
	resps   []*rresp
	interim []int
	closed  bool  // the server closed the connection (EOF) before we stopped reading
	err     error // a read error that is not EOF: a deadline is a hang
	raw     string
}

type rresp struct {
	status int
	proto  string
	header http.Header
	te     []string
	close  bool
	body   []byte
	dump   *dump
}

type dump struct {
	env     []string
	bodyLen string
	bodySHA string
	body    string
}

func (d *dump) all(name string) []string {
	var out []string
	for _, e := range d.env {
		if k, v, ok := strings.Cut(e, "="); ok && k == name {
			out = append(out, v)
		}
	}
	return out
}

func (d *dump) get(name string) (string, bool) {
	if vs := d.all(name); len(vs) > 0 {
		return vs[0], true
	}
	return "", false
}

func parseDump(b []byte) *dump {
	d := &dump{}
	for _, line := range strings.Split(string(b), "\n") {
		switch {
		case strings.HasPrefix(line, "E "):
			if s, err := strconv.Unquote(line[2:]); err == nil {
				d.env = append(d.env, s)
			}
		case strings.HasPrefix(line, "BODY_LEN="):
			d.bodyLen = line[len("BODY_LEN="):]
		case strings.HasPrefix(line, "BODY_SHA="):
			d.bodySHA = line[len("BODY_SHA="):]
		case strings.HasPrefix(line, "BODY="):
			d.body, _ = strconv.Unquote(line[len("BODY="):])
		}
	}
	return d
}

func (r *rresp) sig() string {
	if r.dump == nil {
		return strconv.Itoa(r.status)
	}
	p, _ := r.dump.get("PATH_INFO")
	return fmt.Sprintf("%d %s len=%s", r.status, p, r.dump.bodyLen)
}

func (e *exch) sig() string {
	var parts []string
	for _, r := range e.resps {
		parts = append(parts, r.sig())
	}
	if len(parts) == 0 {
		return "(no response)"
	}
	return strings.Join(parts, " | ")
}

// first is the first response the guest made, if any.
func (e *exch) first() *rresp {
	for _, r := range e.resps {
		if r.dump != nil {
			return r
		}
	}
	return nil
}

// exchange sends the row's bytes on a fresh connection and reads what comes back.
func (e *corpusEnv) exchange(r *crow) *exch {
	ex := &exch{}
	conn, err := net.DialTimeout("tcp", e.w.fe.Addr(), 5*time.Second)
	if err != nil {
		ex.err = err
		return ex
	}
	defer conn.Close()
	dl := rowDeadline
	if r.deadline > 0 {
		dl = r.deadline
	}
	conn.SetDeadline(time.Now().Add(dl))

	parts := r.parts
	if len(parts) == 0 {
		parts = []string{r.raw}
	}
	if r.sentinel {
		parts = slices.Clone(parts)
		parts[len(parts)-1] += sentinelReq
	}
	hold, done := make(chan struct{}), make(chan struct{})
	var holdOnce sync.Once
	release := func() { holdOnce.Do(func() { close(hold) }) }
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i, p := range parts {
			if i > 0 && r.hold100 {
				select {
				case <-hold:
				case <-done:
					return
				}
			}
			if _, err := io.WriteString(conn, p); err != nil {
				return
			}
		}
		if r.closeWrite {
			conn.(*net.TCPConn).CloseWrite()
		}
	}()

	br := bufio.NewReaderSize(conn, 1<<16)
	want := max(r.responses, 1)
	for {
		method := "GET"
		if slices.Contains(r.heads, len(ex.resps)) {
			method = "HEAD"
		}
		resp, err := http.ReadResponse(br, &http.Request{Method: method})
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || (len(ex.resps) > 0 && errors.Is(err, syscall.ECONNRESET)) {
				// A server that closes with unread request bytes in its buffer resets the
				// connection: after an answer, that is a close.
				ex.closed = true
			} else {
				ex.err = err
			}
			break
		}
		if resp.StatusCode >= 100 && resp.StatusCode < 200 && resp.StatusCode != 101 {
			ex.interim = append(ex.interim, resp.StatusCode)
			if resp.StatusCode == 100 {
				release()
			}
			continue
		}
		release()
		body, berr := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		resp.Body.Close()
		rr := &rresp{status: resp.StatusCode, proto: resp.Proto, header: resp.Header, te: resp.TransferEncoding, close: resp.Close, body: body}
		if resp.Header.Get("Content-Type") == "text/x-corpus-dump" && resp.StatusCode == 200 && len(body) > 0 {
			rr.dump = parseDump(body)
		}
		ex.resps = append(ex.resps, rr)
		if resp.Header.Get("X-Edgerun-Invocation-Id") != "" {
			e.mu.Lock()
			e.seen++
			e.mu.Unlock()
		}
		if berr != nil {
			ex.err = berr
			break
		}
		if r.sentinel {
			if rr.dump != nil {
				if p, _ := rr.dump.get("PATH_INFO"); p == "/sentinel" {
					break
				}
			}
		} else if len(ex.resps) >= want {
			break
		}
	}
	close(done)
	conn.Close()
	wg.Wait()
	return ex
}

// healthy asserts a plain request still gets its 200 on a new connection.
func (e *corpusEnv) healthy(t *testing.T) {
	t.Helper()
	ex := e.exchange(&crow{raw: cget("/health")})
	if ex.err != nil || len(ex.resps) != 1 || ex.resps[0].status != 200 {
		t.Fatalf("the stack is not healthy after the row: %s (err %v)", ex.sig(), ex.err)
	}
}

func (r *crow) verify(t *testing.T, ex *exch) {
	t.Helper()
	if ex.err != nil {
		t.Fatalf("no clean answer within %v (a hang or a reset): %v; got so far: %s", rowDeadline, ex.err, ex.sig())
	}
	if r.spec != "" {
		t.Logf("spec: %s", r.spec)
	}
	if r.silent != "" {
		t.Logf("SPEC SILENT, observed behaviour pinned: %s", r.silent)
	}
	sig := ex.sig()
	t.Logf("outcome: %s", sig)
	if len(r.want) > 0 && !slices.Contains(r.want, sig) {
		t.Errorf("outcome %q, acceptable: %q", sig, r.want)
	}
	if len(r.guest) > 0 {
		f := ex.first()
		if f == nil {
			t.Errorf("the request was not forwarded to the guest (%s); wanted the guest to see %v", sig, r.guest)
		} else {
			for k, want := range r.guest {
				if name, absent := strings.CutPrefix(k, "-"); absent {
					if vs := f.dump.all(name); len(vs) > 0 {
						t.Errorf("the guest must not see %s, got %q", name, vs)
					}
					continue
				}
				var got []string
				switch k {
				case "BODY":
					got = []string{f.dump.body}
				case "BODY_LEN":
					got = []string{f.dump.bodyLen}
				case "BODY_SHA":
					got = []string{f.dump.bodySHA}
				default:
					got = f.dump.all(k)
				}
				if len(got) != 1 || got[0] != want {
					t.Errorf("guest %s = %q, want exactly one entry %q", k, got, want)
				}
			}
		}
	}
	if r.check != nil {
		r.check(t, ex)
	}
}

// runRows runs a table: each row on its own connection, then a health check.
func runRows(t *testing.T, e *corpusEnv, rows []crow) {
	t.Helper()
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			if r.defect != "" && os.Getenv("EDGERUN_E2E_KNOWN_DEFECTS") == "" {
				t.Skip("KNOWN DEFECT " + r.defect)
			}
			ex := e.exchange(&r)
			r.verify(t, ex)
			e.healthy(t)
		})
	}
}

// ---- small helpers for rows -----------------------------------------------------

func g(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

// ok is the signature of a guest answer 200 for path with a body of n bytes read.
func ok(path string, n int) string { return fmt.Sprintf("200 %s len=%d", path, n) }

func okSentinel(path string, n int) string { return ok(path, n) + " | " + ok("/sentinel", 0) }

func seq(sigs ...string) string { return strings.Join(sigs, " | ") }

func hasHeader(t *testing.T, r *rresp, name string) bool {
	t.Helper()
	_, ok := r.header[http.CanonicalHeaderKey(name)]
	return ok
}
