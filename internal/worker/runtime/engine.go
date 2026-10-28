package runtime

import (
	"bytes"
	"container/list"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	"github.com/Olian04/edgerun/internal/wagi"
)

const (
	mib      = 1 << 20
	wasmPage = 64 << 10
)

// memoryBuckets are the memory limits a runtime exists for. wazero applies a
// memory limit per *runtime*, not per instance, so each distinct limit needs
// its own runtime; a request is rounded up to the next bucket. All runtimes
// share one compilation cache, so a module is compiled once, not per bucket.
var memoryBuckets = []uint64{16 * mib, 32 * mib, 64 * mib, 128 * mib, 256 * mib, 512 * mib}

// Config tunes the engine. Zero values select the defaults.
type Config struct {
	// DefaultMemoryBytes applies when a request sets no limit. Default 128 MiB.
	DefaultMemoryBytes uint64
	// MaxMemoryBytes clamps requested limits. Default 512 MiB.
	MaxMemoryBytes uint64
	// DefaultMaxResponseBytes applies when a request sets no limit. Default 6 MiB.
	DefaultMaxResponseBytes uint64
	// MaxResponseBytes clamps requested limits. Default 32 MiB.
	MaxResponseBytes uint64
	// StderrTailBytes bounds the captured stderr. Default 64 KiB.
	StderrTailBytes int
	// CompiledModules bounds compiled modules kept per memory bucket. Default 32.
	CompiledModules int
	// CompilationCacheDir persists compiled code across restarts if set.
	CompilationCacheDir string
	// Network makes guests' outbound requests. Without one the engine offers
	// wagi-1 only; with one it offers wagi-2 as well.
	Network Network
}

func (c *Config) setDefaults() {
	if c.DefaultMemoryBytes == 0 {
		c.DefaultMemoryBytes = 128 * mib
	}
	if c.MaxMemoryBytes == 0 {
		c.MaxMemoryBytes = 512 * mib
	}
	if c.DefaultMaxResponseBytes == 0 {
		c.DefaultMaxResponseBytes = 6 * mib
	}
	if c.MaxResponseBytes == 0 {
		c.MaxResponseBytes = 32 * mib
	}
	if c.StderrTailBytes == 0 {
		c.StderrTailBytes = 64 << 10
	}
	if c.CompiledModules == 0 {
		c.CompiledModules = 32
	}
}

// Error is a platform failure of an invocation, classified for the protocol.
type Error struct {
	Reason commonv1.ErrorReason
	Msg    string
}

func (e *Error) Error() string { return e.Reason.String() + ": " + e.Msg }

// InvalidError is a request the worker cannot make sense of: the caller has a
// bug, so it is invalid_argument with no reason (docs/protocol.md).
type InvalidError struct{ Msg string }

func (e *InvalidError) Error() string { return "invalid request: " + e.Msg }

func fail(reason commonv1.ErrorReason, format string, args ...any) *Error {
	return &Error{Reason: reason, Msg: fmt.Sprintf(format, args...)}
}

// Invocation is one request to run.
type Invocation struct {
	// Digest identifies Wasm and keys the compiled-module cache.
	Digest  string
	Wasm    []byte
	Request *commonv1.HttpRequest
	Env     []*commonv1.EnvVar
	Limits  *commonv1.Limits
	// ABI the module was built for; empty means wagi-1.
	ABI string
	// Egress is the network destinations the function may reach, each with the
	// service that carries it. Empty means none.
	Egress       []Grant
	InvocationID string
}

// Result is the outcome of a completed invocation. The function answering with
// any HTTP status is a success here.
type Result struct {
	Head            *commonv1.HttpResponseHead
	Body            []byte
	Stderr          []byte
	StderrTruncated bool
	Duration        time.Duration
	// PeakMemoryBytes is the guest's linear memory size at exit. Linear memory
	// never shrinks, so this is its peak.
	PeakMemoryBytes uint64
}

// Engine runs invocations. It is safe for concurrent use.
type Engine struct {
	cfg   Config
	cache wazero.CompilationCache

	mu      sync.Mutex
	buckets map[uint64]*bucket
	closed  bool
}

// New creates an engine.
func New(cfg Config) (*Engine, error) {
	cfg.setDefaults()
	var cache wazero.CompilationCache
	if cfg.CompilationCacheDir != "" {
		c, err := wazero.NewCompilationCacheWithDir(cfg.CompilationCacheDir)
		if err != nil {
			return nil, fmt.Errorf("compilation cache: %w", err)
		}
		cache = c
	} else {
		cache = wazero.NewCompilationCache()
	}
	return &Engine{cfg: cfg, cache: cache, buckets: map[uint64]*bucket{}}, nil
}

// ABIs lists the guest ABIs this engine can run.
func (e *Engine) ABIs() []string {
	if e.cfg.Network != nil {
		return []string{ABI, ABI2}
	}
	return []string{ABI}
}

// Close releases all runtimes and cached code.
func (e *Engine) Close(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	var errs []error
	for _, b := range e.buckets {
		errs = append(errs, b.rt.Close(ctx))
	}
	errs = append(errs, e.cache.Close(ctx))
	return errors.Join(errs...)
}

// bucketSize rounds a requested limit up to the next bucket, within the
// configured maximum.
func (e *Engine) bucketSize(requested uint64) uint64 {
	if requested == 0 {
		requested = e.cfg.DefaultMemoryBytes
	}
	if requested > e.cfg.MaxMemoryBytes {
		requested = e.cfg.MaxMemoryBytes
	}
	for _, b := range memoryBuckets {
		if requested <= b {
			return b
		}
	}
	return memoryBuckets[len(memoryBuckets)-1]
}

func (e *Engine) bucket(ctx context.Context, size uint64) (*bucket, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, fail(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "engine is closed")
	}
	if b, ok := e.buckets[size]; ok {
		return b, nil
	}
	rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
		WithCompilationCache(e.cache).
		WithMemoryLimitPages(uint32(size/wasmPage)).
		// The only way to interrupt a guest, and the source of the wall-clock
		// deadline. It costs a few times the speed of tight loops.
		WithCloseOnContextDone(true))
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
		_ = rt.Close(ctx)
		return nil, fail(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "instantiating WASI: %v", err)
	}
	if e.cfg.Network != nil {
		if err := instantiateHTTP(ctx, rt); err != nil {
			_ = rt.Close(ctx)
			return nil, fail(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "instantiating %s: %v", wagi.HTTPModule, err)
		}
	}
	b := &bucket{rt: rt, limit: e.cfg.CompiledModules, mods: map[string]*modEntry{}, lru: list.New()}
	e.buckets[size] = b
	return b, nil
}

// Run executes one invocation. The context deadline is the wall-clock budget.
// Errors are *Error, except a cancelled context, which is returned as is.
func (e *Engine) Run(ctx context.Context, inv Invocation) (*Result, error) {
	size := e.bucketSize(inv.Limits.GetMemoryLimitBytes())
	maxOut := inv.Limits.GetMaxResponseBytes()
	if maxOut == 0 {
		maxOut = e.cfg.DefaultMaxResponseBytes
	}
	maxOut = min(maxOut, e.cfg.MaxResponseBytes)

	env, err := Env(inv.Request, inv.Env)
	if err != nil {
		return nil, &InvalidError{Msg: err.Error()}
	}
	b, err := e.bucket(ctx, size)
	if err != nil {
		return nil, err
	}
	// Compilation is shared by concurrent callers, so it must not be aborted
	// by one caller's deadline; the deadline is re-checked afterwards.
	cm, err := b.module(context.WithoutCancel(ctx), inv.Digest, inv.Wasm)
	if err != nil {
		return nil, err
	}
	if cerr := ctx.Err(); cerr != nil {
		return nil, ctxFailure(cerr)
	}
	abi := inv.ABI
	if abi == "" {
		abi = ABI
	}
	// The module is checked against the ABI it was deployed as, here and not
	// when it is compiled: the same bytes may be deployed as either.
	if err := checkImports(cm, abi, e.cfg.Network != nil); err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if abi == ABI2 {
		runCtx = context.WithValue(runCtx, netKey{}, &netState{
			net: e.cfg.Network, grants: parseGrants(inv.Egress), id: inv.InvocationID, maxResp: maxOut,
		})
	}
	stdout := &cappedBuffer{max: maxOut, onOverflow: cancel}
	stderr := &tailBuffer{max: e.cfg.StderrTailBytes}
	// wazero's defaults are deterministic stubs: a fixed random source, fake
	// clocks and a no-op sleep. A guest must see real entropy and real time,
	// or every invocation would draw the same "random" bytes.
	cfg := wazero.NewModuleConfig().
		WithName(""). // anonymous: many instances may coexist
		WithStdin(bytes.NewReader(inv.Request.GetBody())).
		WithStdout(stdout).
		WithStderr(stderr).
		WithRandSource(rand.Reader).
		WithSysWalltime().
		WithSysNanotime().
		WithNanosleep(sleepUntil(runCtx))
	for _, kv := range env {
		cfg = cfg.WithEnv(kv.Name, kv.Value)
	}

	start := time.Now()
	mod, runErr := b.rt.InstantiateModule(runCtx, cm, cfg)
	duration := time.Since(start)
	var peak uint64
	if mod != nil {
		if m := mod.Memory(); m != nil {
			peak = uint64(m.Size())
		}
		// A command that returns normally leaves its instance open.
		defer mod.Close(context.WithoutCancel(ctx))
	}

	if stdout.overflow {
		return nil, fail(commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE,
			"guest output exceeded %d bytes", maxOut)
	}
	if runErr != nil {
		if err := classify(ctx, runErr, stderr); err != nil {
			return nil, err
		}
	}

	head, body, perr := ParseResponse(stdout.Bytes())
	if perr != nil {
		return nil, fail(commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH, "malformed %s response: %v", abi, perr)
	}
	return &Result{
		Head: head, Body: body,
		Stderr: stderr.Bytes(), StderrTruncated: stderr.truncated,
		Duration: duration, PeakMemoryBytes: peak,
	}, nil
}

// sleepUntil returns a guest sleep that ends early when ctx is done. wazero's
// sleep hook takes no context, so without this a sleeping guest would hold its
// worker slot long past its deadline.
func sleepUntil(ctx context.Context) sys.Nanosleep {
	return func(ns int64) {
		t := time.NewTimer(time.Duration(ns))
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
		}
	}
}

// classify maps an instantiation error to a protocol error. It returns nil for
// a clean exit (code 0), which wazero reports as an ExitError.
func classify(ctx context.Context, err error, stderr *tailBuffer) error {
	var ee *sys.ExitError
	if errors.As(err, &ee) {
		switch code := ee.ExitCode(); code {
		case 0:
			return nil
		case sys.ExitCodeDeadlineExceeded:
			return fail(commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, "deadline exceeded while running guest")
		case sys.ExitCodeContextCanceled:
			if cerr := ctx.Err(); cerr != nil {
				return ctxFailure(cerr)
			}
			return context.Canceled
		default:
			return fail(commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED,
				"guest exited with code %d%s", code, stderrSuffix(stderr))
		}
	}
	return fail(commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, "guest trapped: %v%s", err, stderrSuffix(stderr))
}

// ctxFailure maps a done context to a protocol error where it has one.
func ctxFailure(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fail(commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, "deadline exceeded")
	}
	return err
}

// stderrSuffix appends the stderr tail so a failed invocation, which has no
// summary to carry logs, is still diagnosable.
func stderrSuffix(s *tailBuffer) string {
	const max = 4 << 10
	b := s.Bytes()
	if len(b) == 0 {
		return ""
	}
	if len(b) > max {
		b = b[len(b)-max:]
	}
	return "\nstderr (tail):\n" + string(b)
}

// bucket is one runtime plus its LRU of compiled modules.
type bucket struct {
	rt    wazero.Runtime
	limit int

	mu   sync.Mutex
	mods map[string]*modEntry
	lru  *list.List // of *modEntry, front = most recent
}

type modEntry struct {
	digest string
	elem   *list.Element
	once   sync.Once
	ready  bool // guarded by bucket.mu; entries still compiling are never evicted
	cm     wazero.CompiledModule
	err    error
}

func (b *bucket) module(ctx context.Context, digest string, wasm []byte) (wazero.CompiledModule, error) {
	b.mu.Lock()
	ent, ok := b.mods[digest]
	if ok {
		b.lru.MoveToFront(ent.elem)
	} else {
		ent = &modEntry{digest: digest}
		ent.elem = b.lru.PushFront(ent)
		b.mods[digest] = ent
	}
	b.mu.Unlock()

	ent.once.Do(func() {
		ent.cm, ent.err = b.compile(ctx, wasm)
		b.mu.Lock()
		ent.ready = true
		b.evictLocked()
		b.mu.Unlock()
	})
	if ent.err != nil {
		return nil, ent.err
	}
	return ent.cm, nil
}

func (b *bucket) evictLocked() {
	for el := b.lru.Back(); el != nil && b.lru.Len() > b.limit; {
		prev := el.Prev()
		if ent := el.Value.(*modEntry); ent.ready {
			b.lru.Remove(el)
			delete(b.mods, ent.digest)
			if ent.cm != nil {
				// Safe while instances created from it are still running.
				_ = ent.cm.Close(context.Background())
			}
		}
		el = prev
	}
}

// compile compiles a module and checks that it is a WASI command. Which
// imports it may have depends on the ABI it is run as, and is checked then.
func (b *bucket) compile(ctx context.Context, wasm []byte) (wazero.CompiledModule, error) {
	cm, err := b.rt.CompileModule(ctx, wasm)
	if err != nil {
		return nil, fail(commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH, "invalid module: %v", err)
	}
	if _, ok := cm.ExportedFunctions()["_start"]; !ok {
		_ = cm.Close(ctx)
		return nil, fail(commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH,
			"module exports no _start: not a WASI command")
	}
	return cm, nil
}

// checkImports checks a module against the ABI it is run as: wagi-1 imports
// nothing but WASI, and wagi-2 may also import the edgerun_http host module.
func checkImports(cm wazero.CompiledModule, abi string, haveNetwork bool) error {
	allowed := map[string]bool{wasi_snapshot_preview1.ModuleName: true}
	switch abi {
	case ABI:
	case ABI2:
		if !haveNetwork {
			return fail(commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH, "this worker has no network service, so it cannot run %s", ABI2)
		}
		allowed[wagi.HTTPModule] = true
	default:
		return fail(commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH, "unknown ABI %q", abi)
	}
	for _, f := range cm.ImportedFunctions() {
		if mod, name, _ := f.Import(); !allowed[mod] {
			return fail(commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH,
				"module imports %s.%s, which %s does not provide", mod, name, abi)
		}
	}
	return nil
}

// cappedBuffer collects up to max bytes. Beyond that it discards, records the
// overflow, and calls onOverflow so the guest is stopped rather than left
// spinning against a full buffer.
type cappedBuffer struct {
	buf        bytes.Buffer
	max        uint64
	overflow   bool
	onOverflow func()
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if c.overflow {
		return len(p), nil
	}
	if uint64(c.buf.Len()+len(p)) > c.max {
		c.overflow = true
		c.onOverflow()
		return len(p), nil
	}
	return c.buf.Write(p)
}

func (c *cappedBuffer) Bytes() []byte { return c.buf.Bytes() }

// tailBuffer keeps the last max bytes written to it.
type tailBuffer struct {
	buf       []byte
	max       int
	truncated bool
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if len(p) >= t.max {
		t.truncated = t.truncated || len(p) > t.max || len(t.buf) > 0
		t.buf = append(t.buf[:0], p[len(p)-t.max:]...)
		return n, nil
	}
	if over := len(t.buf) + len(p) - t.max; over > 0 {
		t.buf = append(t.buf[:0], t.buf[over:]...)
		t.truncated = true
	}
	t.buf = append(t.buf, p...)
	return n, nil
}

func (t *tailBuffer) Bytes() []byte { return t.buf }
