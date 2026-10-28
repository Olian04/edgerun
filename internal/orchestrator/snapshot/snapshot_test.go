package snapshot

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

type fakeSource struct {
	mu      sync.Mutex
	routes  []store.RouteEntry
	workers []store.Worker
	failR   bool
	failW   bool
	loads   int
}

func (f *fakeSource) LoadRouting(context.Context) ([]store.RouteEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads++
	if f.failR {
		return nil, errors.New("db down")
	}
	return append([]store.RouteEntry(nil), f.routes...), nil
}

func (f *fakeSource) ReadyWorkers(context.Context, time.Duration) ([]store.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failW {
		return nil, errors.New("db down")
	}
	return append([]store.Worker(nil), f.workers...), nil
}

func (f *fakeSource) ReadyNetworkServices(context.Context, time.Duration) ([]store.NetworkService, error) {
	return nil, nil
}

func (f *fakeSource) set(fn func()) { f.mu.Lock(); fn(); f.mu.Unlock() }

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestRefreshServesNewData(t *testing.T) {
	src := &fakeSource{routes: []store.RouteEntry{{RouteID: "r1", HostPattern: "a.com", PathPrefix: "/", FunctionName: "one"}}}
	c := New(src, time.Second, quiet)
	if c.Route("a.com", "/") != nil || len(c.Workers()) != 0 {
		t.Fatal("a new cache must be empty, not nil-panicking")
	}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r := c.Route("a.com", "/x"); r == nil || r.FunctionName != "one" {
		t.Fatalf("route = %+v", r)
	}
	src.set(func() {
		src.routes = []store.RouteEntry{{RouteID: "r2", HostPattern: "a.com", PathPrefix: "/", FunctionName: "two"}}
		src.workers = []store.Worker{{ID: "w", Endpoint: "http://w"}}
	})
	c.Refresh(context.Background())
	if r := c.Route("a.com", "/"); r.FunctionName != "two" || len(c.Workers()) != 1 {
		t.Error("refresh did not pick up changes")
	}
}

func TestFailedRefreshKeepsThePreviousView(t *testing.T) {
	src := &fakeSource{
		routes:  []store.RouteEntry{{RouteID: "r1", HostPattern: "a.com", PathPrefix: "/", FunctionName: "one"}},
		workers: []store.Worker{{ID: "w", Endpoint: "http://w"}},
	}
	c := New(src, time.Second, quiet)
	c.Refresh(context.Background())

	src.set(func() { src.failR, src.failW = true, true; src.routes, src.workers = nil, nil })
	if err := c.Refresh(context.Background()); err == nil {
		t.Error("the failure must be reported")
	}
	if c.Route("a.com", "/") == nil || len(c.Workers()) != 1 {
		t.Error("a database outage must not empty the routing table: serve stale, not nothing")
	}

	// Independent: workers fail while routes succeed.
	src.set(func() {
		src.failR, src.failW = false, true
		src.routes = []store.RouteEntry{{RouteID: "r2", HostPattern: "b.com", PathPrefix: "/", FunctionName: "two"}}
	})
	c.Refresh(context.Background())
	if c.Route("b.com", "/") == nil {
		t.Error("routes should have refreshed")
	}
	if len(c.Workers()) != 1 {
		t.Error("workers failed to load and must keep their previous value")
	}
}

func TestRunRefreshesUntilCancelled(t *testing.T) {
	src := &fakeSource{}
	c := New(src, time.Second, quiet)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx, 10*time.Millisecond); close(done) }()
	deadline := time.After(5 * time.Second)
	for {
		src.mu.Lock()
		n := src.loads
		src.mu.Unlock()
		if n >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Run did not refresh periodically")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop on cancel")
	}
}

func TestPokeRefreshesPromptlyAndCoalesces(t *testing.T) {
	src := &fakeSource{}
	c := New(src, time.Second, quiet)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx, time.Hour) // the tick will never fire: only a poke can refresh

	src.set(func() {
		src.routes = []store.RouteEntry{{RouteID: "r", HostPattern: "a.com", PathPrefix: "/", FunctionName: "new"}}
	})
	for range 100 {
		c.Poke() // must never block, however many arrive
	}
	deadline := time.After(5 * time.Second)
	for c.Route("a.com", "/") == nil {
		select {
		case <-deadline:
			t.Fatal("a poke did not trigger a refresh")
		case <-time.After(5 * time.Millisecond):
		}
	}
	src.mu.Lock()
	n := src.loads
	src.mu.Unlock()
	if n > 5 {
		t.Errorf("100 pokes caused %d loads: they must coalesce", n)
	}
}

// slowSource lets a test hold one load half-way, the way a slow database does.
type slowSource struct {
	fakeSource
	hold chan struct{} // when non-nil, the next LoadRouting reads, then waits for it to close
}

func (s *slowSource) LoadRouting(ctx context.Context) ([]store.RouteEntry, error) {
	rs, err := s.fakeSource.LoadRouting(ctx) // reads what the database holds now
	s.mu.Lock()
	hold := s.hold
	s.hold = nil
	s.mu.Unlock()
	if hold != nil {
		<-hold
	}
	return rs, err
}

// A refresh that began before a write must not be able to overwrite the view of
// a refresh that began after it: the writer is promised to see its own change.
func TestAnEarlierRefreshCannotOverwriteALaterOne(t *testing.T) {
	src := &slowSource{fakeSource: fakeSource{routes: []store.RouteEntry{{RouteID: "r1", HostPattern: "a.com", PathPrefix: "/", FunctionName: "old"}}}}
	c := New(src, time.Second, quiet)

	// The background refresh reads the old state and is slow to finish.
	hold := make(chan struct{})
	src.set(func() { src.hold = hold })
	bg := make(chan struct{})
	go func() { c.Refresh(context.Background()); close(bg) }()
	for { // wait until it has read
		src.mu.Lock()
		read, held := src.loads, src.hold == nil
		src.mu.Unlock()
		if read > 0 && held {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// A write lands, and its author refreshes, as control.v1 does.
	src.set(func() {
		src.routes = []store.RouteEntry{{RouteID: "r2", HostPattern: "a.com", PathPrefix: "/", FunctionName: "new"}}
	})
	writer := make(chan struct{})
	go func() { c.Refresh(context.Background()); close(writer) }()
	time.Sleep(50 * time.Millisecond) // the writer's refresh is under way, or waiting its turn

	close(hold) // now the slow background refresh finishes
	<-bg
	<-writer
	if r := c.Route("a.com", "/"); r == nil || r.FunctionName != "new" {
		t.Errorf("the view is %+v after both refreshes: a stale read overwrote the writer's own change", r)
	}
}
