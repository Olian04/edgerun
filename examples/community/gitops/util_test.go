package main

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// rpcLog records every control call the service makes, and can fail chosen
// ones: the tests' way of seeing what was written and of simulating a crash.
type rpcLog struct {
	mu    sync.Mutex
	calls []rpcCall
	// Fail, if set, is asked before each call goes out; a non-nil error is the
	// call's result (the request never reaches the orchestrator).
	Fail func(proc string, validate bool) error
}

type rpcCall struct {
	Proc     string // e.g. "SetEnv"
	Validate bool
	Req      any
}

func (l *rpcLog) Option() edgerun.Option {
	return edgerun.WithClientOptions(connect.WithInterceptors(connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			proc := req.Spec().Procedure[strings.LastIndex(req.Spec().Procedure, "/")+1:]
			vo := false
			if m, ok := req.Any().(interface{ GetValidateOnly() bool }); ok {
				vo = m.GetValidateOnly()
			}
			l.mu.Lock()
			l.calls = append(l.calls, rpcCall{proc, vo, req.Any()})
			fail := l.Fail
			l.mu.Unlock()
			if fail != nil {
				if err := fail(proc, vo); err != nil {
					return nil, err
				}
			}
			return next(ctx, req)
		}
	})))
}

func (l *rpcLog) Reset() { l.mu.Lock(); l.calls = nil; l.mu.Unlock() }

// Writes lists the calls that changed (or tried to change) the orchestrator.
func (l *rpcLog) Writes() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, c := range l.calls {
		if !c.Validate && !strings.HasPrefix(c.Proc, "List") && !strings.HasPrefix(c.Proc, "Get") && c.Proc != "Describe" && c.Proc != "Validate" {
			out = append(out, c.Proc)
		}
	}
	return out
}

func (l *rpcLog) Count(proc string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, c := range l.calls {
		if c.Proc == proc {
			n++
		}
	}
	return n
}

// Request is the request of the n-th call of a procedure (from 0), nil if none.
func (l *rpcLog) Request(proc string, n int) any {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.calls {
		if c.Proc == proc {
			if n == 0 {
				return c.Req
			}
			n--
		}
	}
	return nil
}

// ValidateOnlyCalls counts the calls that were themselves validate_only (not
// Validate, which is a call of its own).
func (l *rpcLog) ValidateOnlyCalls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, c := range l.calls {
		if c.Validate {
			n++
		}
	}
	return n
}

func (l *rpcLog) Total() int { l.mu.Lock(); defer l.mu.Unlock(); return len(l.calls) }
