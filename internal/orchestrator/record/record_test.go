package record

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type sink struct {
	mu      sync.Mutex
	batches [][]*controlv1.Invocation
	gate    chan struct{}
	failing atomic.Int32 // fail this many calls first
	calls   atomic.Int32
}

func (s *sink) InsertInvocations(ctx context.Context, invs []*controlv1.Invocation) error {
	s.calls.Add(1)
	if s.gate != nil {
		<-s.gate
	}
	if s.failing.Add(-1) >= 0 {
		return errors.New("db down")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, append([]*controlv1.Invocation(nil), invs...))
	return nil
}

func (s *sink) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, b := range s.batches {
		n += len(b)
	}
	return n
}

func inv(id string) *controlv1.Invocation { return &controlv1.Invocation{Id: id} }

func TestBatchesBySize(t *testing.T) {
	s := &sink{}
	r := New(s, quiet, Options{BatchSize: 3, FlushInterval: time.Hour})
	for i := range 7 {
		r.Record(inv(string(rune('a' + i))))
	}
	if err := r.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.total() != 7 {
		t.Errorf("recorded %d, want 7", s.total())
	}
	if len(s.batches[0]) != 3 {
		t.Errorf("first batch has %d, want the batch size 3", len(s.batches[0]))
	}
}

func TestFlushesByInterval(t *testing.T) {
	s := &sink{}
	r := New(s, quiet, Options{BatchSize: 1000, FlushInterval: 20 * time.Millisecond})
	defer r.Close(context.Background())
	r.Record(inv("a"))
	deadline := time.After(5 * time.Second)
	for s.total() == 0 {
		select {
		case <-deadline:
			t.Fatal("a lone record was never flushed")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestRecordNeverBlocksAndCountsDrops(t *testing.T) {
	s := &sink{gate: make(chan struct{})} // the store is stuck
	r := New(s, quiet, Options{QueueSize: 5, BatchSize: 1, FlushInterval: time.Hour})
	start := time.Now()
	for i := range 100 {
		r.Record(inv(string(rune(i))))
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("Record blocked for %v while the store was stuck", d)
	}
	if r.Dropped() == 0 {
		t.Error("overflow must be counted")
	}
	close(s.gate)
	r.Close(context.Background())
}

func TestRetriesOnceThenGivesUpAndCounts(t *testing.T) {
	s := &sink{}
	s.failing.Store(1) // first attempt fails, the retry succeeds
	r := New(s, quiet, Options{BatchSize: 1, FlushInterval: time.Hour})
	r.Record(inv("a"))
	r.Close(context.Background())
	if s.total() != 1 || r.Lost() != 0 {
		t.Errorf("one transient failure must be retried: total=%d lost=%d", s.total(), r.Lost())
	}

	s2 := &sink{}
	s2.failing.Store(1000)
	r2 := New(s2, quiet, Options{BatchSize: 2, FlushInterval: time.Hour})
	r2.Record(inv("a"))
	r2.Record(inv("b"))
	r2.Close(context.Background())
	if r2.Lost() != 2 || s2.calls.Load() != 2 {
		t.Errorf("lost=%d calls=%d; want 2 lost after exactly 2 attempts", r2.Lost(), s2.calls.Load())
	}
}

func TestCloseGivesUpWhenTheContextEnds(t *testing.T) {
	s := &sink{gate: make(chan struct{})}
	r := New(s, quiet, Options{BatchSize: 1, FlushInterval: time.Hour})
	r.Record(inv("a"))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := r.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Close = %v, want the deadline error", err)
	}
	close(s.gate)
}
