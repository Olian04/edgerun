package e2e

// Shared machinery of the multi-orchestrator tests (multiorch_*) and the soak
// (soak_*): several real orchestrators on one real database, workers that reach
// them through proxies we can cut, a guest that echoes the request's nonce, and
// a worker written from the protocol alone that records exactly what the
// orchestrator hands it. Every identifier here starts with mo, so as not to
// collide with the other files of this package.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	"github.com/Olian04/edgerun/internal/errs"
	feapp "github.com/Olian04/edgerun/internal/frontend/app"
	orchapp "github.com/Olian04/edgerun/internal/orchestrator/app"
	"github.com/Olian04/edgerun/internal/orchestrator/orchtest"
	"github.com/Olian04/edgerun/internal/testpg"
	workerapp "github.com/Olian04/edgerun/internal/worker/app"
	"github.com/Olian04/edgerun/migrations"
)

// ---------------------------------------------------------------- the guest

// moGuestSrc is a wagi-1 guest that says what it was given: the nonce the
// client put in the query, in a header and in the body (all three must come
// back the same), the environment variables STATE and FN, and a tag compiled in
// (so two builds are two "versions" that can be told apart). ?sleep=<ms> makes
// it slow.
const moGuestSrc = `package main

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"time"
)

var tag = "?"

func main() {
	q, _ := url.ParseQuery(os.Getenv("QUERY_STRING"))
	body, _ := io.ReadAll(os.Stdin)
	if ms, err := strconv.Atoi(q.Get("sleep")); err == nil {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	fmt.Printf("Content-Type: text/plain\nX-Guest-Tag: %s\n\n", tag)
	fmt.Printf("tag=%s\nstate=%s\nfn=%s\nnonce=%s\nhnonce=%s\nbody=%s\nhost=%s\npath=%s\n",
		tag, os.Getenv("STATE"), os.Getenv("FN"), q.Get("n"), os.Getenv("HTTP_X_NONCE"), body,
		os.Getenv("SERVER_NAME"), os.Getenv("PATH_INFO"))
}
`

var moGuests sync.Map // tag -> []byte

// moGuest returns the guest module for a tag, built once (and cached on disk
// across test binaries, like conformance/guest does).
func moGuest(t testing.TB, tag string) []byte {
	t.Helper()
	if b, ok := moGuests.Load(tag); ok {
		return b.([]byte)
	}
	h := sha256.Sum256([]byte(moGuestSrc + "\x00" + tag))
	cacheRoot, err := os.UserCacheDir()
	if err != nil {
		cacheRoot = os.TempDir()
	}
	dir := filepath.Join(cacheRoot, "edgerun-multiorch")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cached := filepath.Join(dir, "guest-"+hex.EncodeToString(h[:6])+".wasm")
	if b, err := os.ReadFile(cached); err == nil {
		moGuests.Store(tag, b)
		return b
	}
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "main.go"), []byte(moGuestSrc), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "go.mod"), []byte("module moguest\n\ngo 1.21\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(work, "out.wasm")
	cmd := exec.Command("go", "build", "-trimpath", "-ldflags=-X main.tag="+tag, "-o", out, ".")
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0")
	if msg, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building the guest: %v\n%s", err, msg)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := os.CreateTemp(dir, "tmp-*")
	if err == nil {
		tmp.Write(b)
		tmp.Close()
		os.Rename(tmp.Name(), cached)
	}
	moGuests.Store(tag, b)
	return b
}

func moLEB(n int) []byte {
	var out []byte
	for {
		b := byte(n & 0x7f)
		n >>= 7
		if n != 0 {
			out = append(out, b|0x80)
			continue
		}
		return append(out, b)
	}
}

// moDistinct returns wasm with a trailing custom section holding salt: still a
// valid module that behaves the same, with a digest of its own.
func moDistinct(wasm []byte, salt string) []byte {
	name := "salt"
	payload := append(moLEB(len(name)), name...)
	payload = append(payload, salt...)
	out := append([]byte{}, wasm...)
	out = append(out, 0)
	out = append(out, moLEB(len(payload))...)
	return append(out, payload...)
}

// moReply is what a frontend would get from one invocation of the guest.
type moReply struct {
	err     error
	status  uint32
	fields  map[string]string
	invID   string
	elapsed time.Duration
	// mismatch is set if the guest echoed another nonce than the one sent.
	mismatch string
}

func (r moReply) f(k string) string { return r.fields[k] }

func moFields(body []byte) map[string]string {
	m := map[string]string{}
	rest := string(body)
	// body= is last but one and may hold anything; every other field is one line.
	for _, line := range strings.Split(rest, "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			if _, dup := m[k]; !dup {
				m[k] = v
			}
		}
	}
	return m
}

var (
	moTrMu sync.Mutex
	moTrs  []*http.Transport
)

func moTransport(perHost int) *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = perHost
	tr.MaxIdleConns = 1024
	moTrMu.Lock()
	moTrs = append(moTrs, tr)
	moTrMu.Unlock()
	return tr
}

// moCloseIdle closes the idle connections of every client the harness made.
func moCloseIdle() {
	moTrMu.Lock()
	defer moTrMu.Unlock()
	for _, tr := range moTrs {
		tr.CloseIdleConnections()
	}
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
}

// soakGoroutines summarises where the process's goroutines are, by the first
// frames of their stacks.
func soakGoroutines() string {
	buf := make([]byte, 4<<20)
	buf = buf[:runtime.Stack(buf, true)]
	counts := map[string]int{}
	for _, g := range strings.Split(string(buf), "\n\n") {
		lines := strings.Split(g, "\n")
		if len(lines) < 2 {
			continue
		}
		key := lines[1]
		if len(lines) > 4 {
			key += " <- " + strings.TrimSpace(lines[3])
		}
		counts[key]++
	}
	type kv struct {
		k string
		n int
	}
	var all []kv
	for k, n := range counts {
		all = append(all, kv{k, n})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].n > all[j].n })
	var out []string
	for _, e := range all[:min(8, len(all))] {
		out = append(out, fmt.Sprintf("%d x %s", e.n, e.k))
	}
	return strings.Join(out, "\n  ")
}

func moIngress(addr string) *edgerun.Ingress {
	tr := moTransport(256)
	return edgerun.NewIngress("http://"+addr, ingressToken, edgerun.WithHTTPClient(&http.Client{Transport: tr}))
}

func moControl(addr string) *edgerun.Control {
	tr := moTransport(64)
	return edgerun.NewControl("http://"+addr, adminToken, edgerun.WithHTTPClient(&http.Client{Transport: tr}))
}

// moInvoke sends one request to the guest through an ingress client and checks
// that what comes back is the answer to this request (nonce in the query, a
// header and the body; all three echoed).
func moInvoke(ing *edgerun.Ingress, host, path, nonce string, sleepMs int, budget time.Duration) moReply {
	cctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	q := "n=" + nonce
	if sleepMs > 0 {
		q += "&sleep=" + strconv.Itoa(sleepMs)
	}
	start := time.Now()
	res, err := ing.Invoke(cctx, &ingressv1.InvokeRequest{
		RequestId: nonce,
		Request: &commonv1.HttpRequest{Method: "POST", Host: host, Path: path, RawQuery: q, Body: []byte(nonce),
			Headers: []*commonv1.Header{{Name: "x-nonce", Value: []byte(nonce)}}},
	})
	if err != nil {
		return moReply{err: err, elapsed: time.Since(start), invID: errs.Info(err).GetInvocationId()}
	}
	body, err := res.ReadAll()
	r := moReply{status: res.Head.GetStatus(), invID: res.ID, elapsed: time.Since(start), err: err}
	if err != nil {
		return r
	}
	r.fields = moFields(body)
	if r.status == 200 {
		for _, k := range []string{"nonce", "hnonce", "body"} {
			if r.fields[k] != nonce {
				r.mismatch = fmt.Sprintf("sent nonce %q, the %s field of the answer is %q", nonce, k, r.fields[k])
			}
		}
	}
	return r
}

// ------------------------------------------------------------- proxies

type moBackend struct{ network, addr string }

// moProxy is a TCP forwarder we can cut, as a failing machine or network would
// be: Down refuses connections (the port is closed) and drops the ones that are
// open, Up listens again at the same address, Cut drops open connections only,
// Blackhole stalls everything (no error, no data) until it is lifted.
type moProxy struct {
	backends func() []moBackend // tried in order for each new connection

	mu     sync.Mutex
	addr   string
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool

	hole  atomic.Bool
	dials atomic.Int64
	// per backend address: connections accepted
	perMu sync.Mutex
	per   map[string]int
}

func newMoProxy(t testing.TB, backends func() []moBackend) *moProxy {
	t.Helper()
	p := &moProxy{backends: backends, conns: map[net.Conn]struct{}{}, per: map[string]int{}}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p.addr, p.ln = ln.Addr().String(), ln
	go p.serve(ln)
	t.Cleanup(p.Close)
	return p
}

func (p *moProxy) Addr() string { return p.addr }

func (p *moProxy) serve(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *moProxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *moProxy) untrack(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
	c.Close()
}

func (p *moProxy) handle(c net.Conn) {
	if !p.track(c) {
		c.Close()
		return
	}
	defer p.untrack(c)
	var up net.Conn
	for _, b := range p.backends() {
		u, err := net.DialTimeout(b.network, b.addr, time.Second)
		if err == nil {
			up = u
			p.perMu.Lock()
			p.per[b.addr]++
			p.perMu.Unlock()
			break
		}
	}
	if up == nil {
		return // nothing to forward to: the client sees a closed connection
	}
	p.dials.Add(1)
	if !p.track(up) {
		up.Close()
		return
	}
	defer p.untrack(up)
	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				for p.hole.Load() {
					time.Sleep(10 * time.Millisecond)
				}
				if _, werr := dst.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}
	go pipe(up, c)
	go pipe(c, up)
	<-done
}

// Cut drops every open connection and keeps listening.
func (p *moProxy) Cut() {
	p.mu.Lock()
	for c := range p.conns {
		c.Close()
	}
	p.mu.Unlock()
}

// Down closes the port and drops every connection.
func (p *moProxy) Down() {
	p.mu.Lock()
	if p.ln != nil {
		p.ln.Close()
		p.ln = nil
	}
	p.mu.Unlock()
	p.Cut()
}

// Up listens again at the address it had.
func (p *moProxy) Up() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil || p.closed {
		return nil
	}
	var err error
	for range 40 {
		var ln net.Listener
		if ln, err = net.Listen("tcp", p.addr); err == nil {
			p.ln = ln
			go p.serve(ln)
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return err
}

func (p *moProxy) Blackhole(on bool) { p.hole.Store(on) }

func (p *moProxy) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.hole.Store(false)
	p.Down()
}

func (p *moProxy) accepted(addr string) int {
	p.perMu.Lock()
	defer p.perMu.Unlock()
	return p.per[addr]
}

// moPG is where the database is, as a proxy backend.
func moPG(baseDSN string) moBackend {
	u, err := url.Parse(baseDSN)
	if err != nil {
		panic(err)
	}
	if h := u.Query().Get("host"); h != "" && strings.HasPrefix(h, "/") {
		port := u.Port()
		if port == "" {
			port = "5432"
		}
		return moBackend{"unix", filepath.Join(h, ".s.PGSQL."+port)}
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "5432")
	}
	return moBackend{"tcp", host}
}

// moDSNVia is the DSN of the same database, reached through a TCP address.
func moDSNVia(baseDSN, addr string) string {
	u, _ := url.Parse(baseDSN)
	u.Host = addr
	q := u.Query()
	q.Del("host")
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	return u.String()
}

func moDSNApp(dsn, app string) string {
	u, _ := url.Parse(dsn)
	q := u.Query()
	q.Set("application_name", app)
	u.RawQuery = q.Encode()
	return u.String()
}

// -------------------------------------------------------------- logging

// moLogs records what an orchestrator logged at warning level or above, and how
// often each message occurred.
type moLogs struct {
	mu    sync.Mutex
	lines []string
	count map[string]int
}

func (l *moLogs) logger() *slog.Logger { return slog.New(moHandler{l}) }

type moHandler struct{ l *moLogs }

func (h moHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h moHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h moHandler) WithGroup(string) slog.Handler            { return h }
func (h moHandler) Handle(_ context.Context, r slog.Record) error {
	h.l.mu.Lock()
	defer h.l.mu.Unlock()
	if h.l.count == nil {
		h.l.count = map[string]int{}
	}
	h.l.count[r.Message]++
	if r.Level >= slog.LevelWarn && len(h.l.lines) < 500 {
		var b strings.Builder
		b.WriteString(r.Level.String() + " " + r.Message)
		r.Attrs(func(a slog.Attr) bool { fmt.Fprintf(&b, " %s=%v", a.Key, a.Value); return true })
		h.l.lines = append(h.l.lines, b.String())
	}
	return nil
}

func (l *moLogs) n(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count[msg]
}

func (l *moLogs) warnings() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// ------------------------------------------------------- the orchestrators

type moOrch struct {
	c    *moCluster
	name string
	cfg  orchapp.Config
	logs *moLogs

	mu   sync.Mutex
	srv  *orchapp.Server
	addr string
	up   bool
	ctl  *edgerun.Control
	ing  *edgerun.Ingress
}

func (o *moOrch) url() string  { o.mu.Lock(); defer o.mu.Unlock(); return "http://" + o.addr }
func (o *moOrch) Addr() string { o.mu.Lock(); defer o.mu.Unlock(); return o.addr }
func (o *moOrch) isUp() bool   { o.mu.Lock(); defer o.mu.Unlock(); return o.up }
func (o *moOrch) control() *edgerun.Control {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.ctl
}
func (o *moOrch) ingress() *edgerun.Ingress {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.ing
}

func (o *moOrch) start(listen string) error {
	cfg := o.cfg
	cfg.Listen = listen
	cfg.Log = o.logs.logger()
	srv, err := orchapp.Start(context.Background(), cfg)
	if err != nil {
		return err
	}
	o.mu.Lock()
	o.srv, o.addr, o.up = srv, srv.Addr(), true
	o.ctl, o.ing = moControl(o.addr), moIngress(o.addr)
	o.mu.Unlock()
	return nil
}

// kill stops it the way a crash does: connections are torn down at once, with
// no waiting for invocations in flight and no flush of what was queued.
func (o *moOrch) kill() {
	o.mu.Lock()
	srv := o.srv
	wasUp := o.up
	o.up = false
	o.mu.Unlock()
	if !wasUp {
		return
	}
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	srv.Stop(dead)
}

// stop is the graceful shutdown.
func (o *moOrch) stop() {
	o.mu.Lock()
	srv := o.srv
	wasUp := o.up
	o.up = false
	o.mu.Unlock()
	if wasUp {
		srv.Stop(context.Background())
	}
}

// restart starts it again at the address it had.
func (o *moOrch) restart() error {
	addr := o.Addr()
	var err error
	for range 40 {
		if err = o.start(addr); err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return err
}

type moOpts struct {
	n         int
	refresh   time.Duration // 0: the orchestrator's default
	heartbeat time.Duration // default 200ms
	retention time.Duration
	janitor   time.Duration
	// dsn, if set, is what every orchestrator connects with (default: the
	// database directly, each with its own application_name)
	dsnFor func(i int, base string) string
	// key per orchestrator index; default the shared test key
	keyFor func(i int) []byte
	// serial starts them one after the other instead of all at once
	serial bool
	// deadlock sets the database's deadlock_timeout (PostgreSQL's default of 1s
	// makes every deadlock a one second stall)
	deadlock time.Duration
}

type moCluster struct {
	t     *testing.T
	dsn   string
	admin *pgxpool.Pool
	orchs []*moOrch
	wasm  []byte
}

func (c *moCluster) A() *moOrch { return c.orchs[0] }
func (c *moCluster) B() *moOrch { return c.orchs[1] }
func (c *moCluster) C() *moOrch { return c.orchs[2] }

// newMoCluster starts n orchestrators on ONE fresh database, all at once (the
// first to take the migration lock migrates, the others wait for it).
func newMoCluster(t *testing.T, o moOpts) *moCluster {
	t.Helper()
	if o.heartbeat == 0 {
		// Three of these are a worker's life: on a loaded machine (or under -race) a
		// 200ms heartbeat is easily late by 600ms, and the worker looks dead.
		o.heartbeat = time.Second
	}
	base := testpg.DSN(t)
	admin, err := pgxpool.New(context.Background(), moDSNApp(base, "mo-admin"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	c := &moCluster{t: t, dsn: base, admin: admin, wasm: moGuest(t, "g1")}
	if o.deadlock > 0 {
		if _, err := admin.Exec(context.Background(), fmt.Sprintf(`ALTER DATABASE %s SET deadlock_timeout = '%dms'`, moQuoteIdent(base), o.deadlock.Milliseconds())); err != nil {
			t.Fatal(err)
		}
	}
	names := "abcdefgh"
	for i := range o.n {
		dsn := moDSNApp(base, "mo-orch-"+string(names[i]))
		if o.dsnFor != nil {
			dsn = o.dsnFor(i, base)
		}
		key := orchtest.Key
		if o.keyFor != nil {
			key = o.keyFor(i)
		}
		cfg := orchapp.Config{
			DatabaseURL:  dsn,
			IngressToken: ingressToken, WorkerToken: workerToken, AdminToken: adminToken,
			ExecutionToken: executionToken, NetworkToken: networkToken, SecretKey: key,
			HeartbeatInterval: o.heartbeat, RefreshEvery: o.refresh, MaxRequestBodyBytes: 4 << 20,
			ShutdownTimeout: 20 * time.Second, InvocationRetention: o.retention, JanitorEvery: o.janitor,
		}
		c.orchs = append(c.orchs, &moOrch{c: c, name: string(names[i]), cfg: cfg, logs: &moLogs{}})
	}
	t.Cleanup(func() {
		var wg sync.WaitGroup
		for _, orch := range c.orchs {
			wg.Add(1)
			go func() { defer wg.Done(); orch.stop() }()
		}
		wg.Wait()
	})
	errc := make(chan error, o.n)
	for _, orch := range c.orchs {
		if o.serial {
			errc <- orch.start("127.0.0.1:0")
			continue
		}
		go func() { errc <- orch.start("127.0.0.1:0") }()
	}
	for range o.n {
		if err := <-errc; err != nil {
			t.Fatalf("starting an orchestrator: %v", err)
		}
	}
	return c
}

func (c *moCluster) count(q string, args ...any) int {
	c.t.Helper()
	var n int
	if err := c.admin.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		c.t.Fatalf("%s: %v", q, err)
	}
	return n
}

// conns is how many database connections the sessions named app hold.
func (c *moCluster) conns(app string) int {
	return c.count(`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND application_name LIKE $1`, app)
}

// terminate kills the backends whose application_name matches (LIKE) and
// returns how many.
func (c *moCluster) terminate(app string) int {
	return c.count(`SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
		WHERE datname = current_database() AND application_name LIKE $1 AND pid <> pg_backend_pid()`, app)
}

func (c *moCluster) records() int { return c.count(`SELECT count(*) FROM invocations`) }

// ----------------------------------------------------------------- workers

// moWorker is the real worker. It reaches its orchestrator through one proxy
// and is reached through another, so that it can be made to vanish the way a
// dead machine does (no deregistration, no goodbye), and so that the orchestrator
// it dials can be changed or fail over (the proxy is the "stable URL").
type moWorker struct {
	c        *moCluster
	orchP    *moProxy
	execP    *moProxy
	cancel   context.CancelFunc
	done     chan error
	logs     *moLogs
	stopOnce sync.Once
	exitErr  error
}

// startWorker starts a real worker whose orchestrator URL is a proxy that
// forwards to whatever orch() lists first that accepts a connection.
func (c *moCluster) startWorker(t testing.TB, conc uint32, orch func() []moBackend) *moWorker {
	t.Helper()
	w := &moWorker{c: c, logs: &moLogs{}, done: make(chan error, 1)}
	w.orchP = newMoProxy(t, orch)
	port := freePort2(t)
	w.execP = newMoProxy(t, func() []moBackend { return []moBackend{{"tcp", port}} })
	wctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	go func() {
		w.done <- workerapp.Run(wctx, workerapp.Config{
			Listen: port, AdvertiseURL: "http://" + w.execP.Addr(), OrchestratorURL: "http://" + w.orchP.Addr(),
			WorkerToken: workerToken, ExecutionToken: executionToken,
			MaxConcurrency: conc, DrainTimeout: 10 * time.Second, Log: w.logs.logger(),
		})
	}()
	t.Cleanup(func() { w.stop() })
	return w
}

func freePort2(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func moOnly(o *moOrch) func() []moBackend {
	return func() []moBackend { return []moBackend{{"tcp", o.Addr()}} }
}

// moFailover lists the orchestrators in order: the first that accepts a
// connection takes it, as a health-checking load balancer with a priority list
// would.
func moFailover(os ...*moOrch) func() []moBackend {
	return func() []moBackend {
		var bs []moBackend
		for _, o := range os {
			bs = append(bs, moBackend{"tcp", o.Addr()})
		}
		return bs
	}
}

// stop is SIGTERM: drain, deregister, leave.
func (w *moWorker) stop() {
	w.stopOnce.Do(func() {
		w.cancel()
		select {
		case w.exitErr = <-w.done:
		case <-time.After(60 * time.Second):
			w.exitErr = errors.New("worker did not exit")
		}
		w.orchP.Close()
		w.execP.Close()
	})
}

// kill makes it vanish: its ports close at once, nothing is deregistered.
func (w *moWorker) kill() {
	w.stopOnce.Do(func() {
		w.execP.Close()
		w.orchP.Close()
		w.cancel()
		go func() { <-w.done }()
	})
}

// -------------------------------------------------------- the recording worker

// moRecWorker is a worker written from the protocol alone, ABI "rec-1": it
// answers every Execute with exactly what the orchestrator handed it (the
// digest, the environment, the network grants, the request's nonce), so a test
// sees the orchestrator's view of a function without a guest in between. It
// registers at one orchestrator and heartbeats there.
type moRecWorker struct {
	workerv1connect.UnimplementedExecutionServiceHandler
	srv      *httptest.Server
	fleet    *edgerun.Fleet
	stopped  chan struct{}
	once     sync.Once
	mu       sync.Mutex
	workerID string
	served   atomic.Int64
	sleep    atomic.Int64 // ms added to every answer
}

func (w *moRecWorker) info() *workerv1.WorkerInfo {
	return &workerv1.WorkerInfo{
		Implementation: &commonv1.Implementation{Name: "rec-worker", Version: "1", Protocols: []string{"edgerun.worker.v1"}},
		Abis:           []string{"rec-1"}, MaxConcurrency: 1000,
	}
}

// moView is the state of a function as an Execute carries it.
type moView struct {
	Digest string
	Env    map[string]string
	Egress []string // "entry>service"
	Nonce  string
}

func (v moView) String() string {
	var keys []string
	for k := range v.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var kv []string
	for _, k := range keys {
		kv = append(kv, k+"="+v.Env[k])
	}
	return fmt.Sprintf("digest=%s env[%s] egress[%s]", v.Digest[:min(len(v.Digest), 15)], strings.Join(kv, ","), strings.Join(v.Egress, ","))
}

func moParseView(body string) moView {
	v := moView{Env: map[string]string{}}
	for _, line := range strings.Split(body, "\n") {
		k, val, _ := strings.Cut(line, "=")
		switch k {
		case "digest":
			v.Digest = val
		case "nonce":
			v.Nonce = val
		case "env":
			for _, kv := range strings.Split(val, "\x1f") {
				if a, b, ok := strings.Cut(kv, "="); ok {
					v.Env[a] = b
				}
			}
		case "egress":
			if val != "" {
				v.Egress = strings.Split(val, "\x1f")
			}
		}
	}
	return v
}

func (w *moRecWorker) Execute(ctx context.Context, req *connect.Request[workerv1.ExecuteRequest], st *connect.ServerStream[workerv1.ExecuteResponse]) error {
	if req.Header().Get("Authorization") != "Bearer "+executionToken {
		return connect.NewError(connect.CodeUnauthenticated, nil)
	}
	m := req.Msg
	if d := w.sleep.Load(); d > 0 {
		select {
		case <-time.After(time.Duration(d) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var env, eg []string
	for _, e := range m.Env {
		env = append(env, e.Name+"="+e.Value)
	}
	sort.Strings(env)
	for _, g := range m.Egress {
		eg = append(eg, g.Allow+">"+g.Service)
	}
	var nonce string
	for _, h := range m.GetRequest().GetHeaders() {
		if h.Name == "x-nonce" {
			nonce = string(h.Value)
		}
	}
	w.served.Add(1)
	body := fmt.Sprintf("digest=%s\nnonce=%s\nenv=%s\negress=%s\n", m.ArtifactDigest, nonce, strings.Join(env, "\x1f"), strings.Join(eg, "\x1f"))
	st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Head{Head: &commonv1.HttpResponseHead{Status: 200}}})
	st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Chunk{Chunk: []byte(body)}})
	return st.Send(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Summary{Summary: &workerv1.ExecutionSummary{DurationMicros: 1}}})
}

// startRecWorker registers a recording worker at an orchestrator URL.
func moStartRecWorker(t testing.TB, orchURL string) *moRecWorker {
	t.Helper()
	w := &moRecWorker{stopped: make(chan struct{}), fleet: edgerun.NewFleet(orchURL, workerToken)}
	mux := http.NewServeMux()
	mux.Handle(workerv1connect.NewExecutionServiceHandler(w))
	w.srv = httptest.NewServer(mux)
	resp, err := w.fleet.Register(context.Background(), &workerv1.RegisterRequest{Endpoint: w.srv.URL, Info: w.info()})
	if err != nil {
		t.Fatal(err)
	}
	w.workerID = resp.WorkerId
	interval := resp.HeartbeatInterval.AsDuration()
	go func() {
		tk := time.NewTicker(interval)
		defer tk.Stop()
		for {
			select {
			case <-w.stopped:
				return
			case <-tk.C:
				w.mu.Lock()
				id := w.workerID
				w.mu.Unlock()
				if r, err := w.fleet.Heartbeat(context.Background(), &workerv1.HeartbeatRequest{WorkerId: id, State: workerv1.WorkerState_WORKER_STATE_READY}); err == nil && r.Reregister {
					if rr, err := w.fleet.Register(context.Background(), &workerv1.RegisterRequest{Endpoint: w.srv.URL, Info: w.info()}); err == nil {
						w.mu.Lock()
						w.workerID = rr.WorkerId
						w.mu.Unlock()
					}
				}
			}
		}
	}()
	t.Cleanup(w.stop)
	return w
}

func (w *moRecWorker) stop() {
	w.once.Do(func() {
		close(w.stopped)
		w.fleet.Deregister(context.Background(), &workerv1.DeregisterRequest{WorkerId: w.workerID})
		w.srv.Close()
	})
}

// moStartFakeNetwork registers a network service that carries everything and
// answers nothing: enough for an orchestrator to hand out grants naming it.
func moStartFakeNetwork(t testing.TB, orchURL, endpoint string) {
	t.Helper()
	reg := edgerun.NewRegistry(orchURL, networkToken)
	resp, err := reg.Register(context.Background(), &egressv1.RegisterRequest{Endpoint: endpoint,
		Info: &egressv1.ServiceInfo{Implementation: &commonv1.Implementation{Name: "fake-net", Version: "1"}, Covers: []string{"*"}}})
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	go func() {
		tk := time.NewTicker(resp.HeartbeatInterval.AsDuration())
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				reg.Heartbeat(context.Background(), &egressv1.HeartbeatRequest{ServiceId: resp.ServiceId, State: egressv1.ServiceState_SERVICE_STATE_READY})
			}
		}
	}()
	t.Cleanup(func() { close(stop) })
}

// ------------------------------------------------------------ control helpers

func moMust[T any](t testing.TB, v T, err error) T {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func moCreateFn(t testing.TB, ctl *edgerun.Control, name string) string {
	t.Helper()
	r, err := ctl.CreateFunction(context.Background(), &controlv1.CreateFunctionRequest{Name: name})
	if err != nil {
		t.Fatalf("CreateFunction %s: %v", name, err)
	}
	return r.Function.Id
}

func moDeploy(t testing.TB, ctl *edgerun.Control, fid, abi string, module []byte) string {
	t.Helper()
	r, err := ctl.CreateVersion(context.Background(), &controlv1.CreateVersionRequest{FunctionId: fid, Module: module, Abi: abi, Activate: true})
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	return r.Version.Id
}

func moRoute(t testing.TB, ctl *edgerun.Control, host, prefix, fid string) string {
	t.Helper()
	r, err := ctl.CreateRoute(context.Background(), &controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: prefix, FunctionId: fid})
	if err != nil {
		t.Fatalf("CreateRoute %s%s: %v", host, prefix, err)
	}
	return r.Route.Id
}

func moSetEnv(t testing.TB, ctl *edgerun.Control, fid, name, value string) {
	t.Helper()
	if _, err := ctl.SetEnv(context.Background(), &controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: name, Value: value}}); err != nil {
		t.Fatalf("SetEnv: %v", err)
	}
}

// moWaitAlive waits until the control API of o lists n alive workers.
func moWaitAlive(t testing.TB, o *moOrch, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := -1
	for time.Now().Before(deadline) {
		r, err := o.control().ListWorkers(context.Background(), &controlv1.ListWorkersRequest{})
		if err == nil {
			last = 0
			for _, w := range r.Workers {
				if w.Alive && w.State == workerv1.WorkerState_WORKER_STATE_READY {
					last++
				}
			}
			if last == n {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("orchestrator %s: %d alive workers after %v, want %d", o.name, last, timeout, n)
}

// moEventually polls cond until it holds or the deadline passes.
func moEventually(t testing.TB, what string, timeout time.Duration, cond func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < timeout {
		if cond() {
			return time.Since(start)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
	return 0
}

// moDist is a sample of durations, summarised the way the reports print it.
type moDist []time.Duration

func (d moDist) String() string {
	if len(d) == 0 {
		return "no samples"
	}
	s := append(moDist(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	q := func(p float64) time.Duration { return s[min(len(s)-1, int(p*float64(len(s))))] }
	var sum time.Duration
	for _, x := range s {
		sum += x
	}
	return fmt.Sprintf("n=%d min=%v p50=%v p90=%v p99=%v max=%v mean=%v", len(s),
		s[0].Round(time.Millisecond), q(.5).Round(time.Millisecond), q(.9).Round(time.Millisecond),
		q(.99).Round(time.Millisecond), s[len(s)-1].Round(time.Millisecond), (sum / time.Duration(len(s))).Round(time.Millisecond))
}

func (d moDist) max() time.Duration {
	var m time.Duration
	for _, x := range d {
		m = max(m, x)
	}
	return m
}

// moCode is the Connect code of an error ("ok" for nil).
func moCode(err error) string {
	if err == nil {
		return "ok"
	}
	return connect.CodeOf(err).String()
}

func moReq(host, path, nonce string) *ingressv1.InvokeRequest {
	return &ingressv1.InvokeRequest{RequestId: nonce, Request: &commonv1.HttpRequest{Method: "POST", Host: host, Path: path, RawQuery: "n=" + nonce,
		Body: []byte(nonce), Headers: []*commonv1.Header{{Name: "x-nonce", Value: []byte(nonce)}}}}
}

func moQuoteIdent(dsn string) string {
	u, _ := url.Parse(dsn)
	return `"` + strings.ReplaceAll(strings.TrimPrefix(u.Path, "/"), `"`, `""`) + `"`
}

func moLoadMigrations() ([]string, error) {
	es, err := migrations.FS.ReadDir(".")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range es {
		if strings.HasSuffix(e.Name(), ".sql") {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

func moGet(ctx context.Context, u string) int {
	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

var _ = bytes.Equal
var _ = io.EOF
var _ = feapp.Config{}
