// Package record persists invocation records without ever slowing down or
// failing the request that produced them. Records are queued in memory and
// written in batches; if the queue is full the record is dropped and counted.
// Observability must not become an outage.
package record

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// Sink stores batches of records. *store.Store implements it.
type Sink interface {
	InsertInvocations(ctx context.Context, invs []*controlv1.Invocation) error
}

// Options tunes a Recorder. Zero values select the defaults.
type Options struct {
	QueueSize     int           // default 4096
	BatchSize     int           // default 100
	FlushInterval time.Duration // default 200ms
}

// Recorder is safe for concurrent use.
type Recorder struct {
	sink Sink
	log  *slog.Logger
	opts Options

	queue   chan *controlv1.Invocation
	done    chan struct{}
	closing chan struct{}
	once    sync.Once

	dropped atomic.Uint64
	lost    atomic.Uint64
}

// New starts a recorder. Call Close to flush it.
func New(sink Sink, log *slog.Logger, opts Options) *Recorder {
	if opts.QueueSize == 0 {
		opts.QueueSize = 4096
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 100
	}
	if opts.FlushInterval == 0 {
		opts.FlushInterval = 200 * time.Millisecond
	}
	r := &Recorder{sink: sink, log: log, opts: opts,
		queue: make(chan *controlv1.Invocation, opts.QueueSize), done: make(chan struct{}), closing: make(chan struct{})}
	go r.loop()
	return r
}

// Record queues a record and returns immediately. If the queue is full the
// record is dropped.
func (r *Recorder) Record(inv *controlv1.Invocation) {
	select {
	case r.queue <- inv:
	default:
		r.dropped.Add(1)
	}
}

// Dropped is the number of records discarded because the queue was full.
func (r *Recorder) Dropped() uint64 { return r.dropped.Load() }

// Lost is the number of records discarded because the sink kept failing.
func (r *Recorder) Lost() uint64 { return r.lost.Load() }

// Close writes everything still queued, then stops. It gives up when ctx is
// done. Record must not be called after Close.
func (r *Recorder) Close(ctx context.Context) error {
	r.once.Do(func() { close(r.closing) })
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// maxRetained bounds the records kept in memory for a store that is failing: past it the
// oldest are dropped (and counted as lost). A database outage of seconds loses nothing.
const maxRetained = 100_000

func (r *Recorder) loop() {
	defer close(r.done)
	t := time.NewTicker(r.opts.FlushInterval)
	defer t.Stop()
	batch := make([]*controlv1.Invocation, 0, r.opts.BatchSize)
	var retained [][]*controlv1.Invocation // batches the store refused, oldest first
	retainedN := 0
	flush := func() {
		// What the store refused earlier goes first, in order; while it keeps refusing, nothing newer is
		// tried this round (it would only fail as well, and slowly).
		for len(retained) > 0 {
			if !r.tryWrite(retained[0]) {
				break
			}
			retainedN -= len(retained[0])
			retained = retained[1:]
		}
		if len(batch) == 0 {
			return
		}
		if len(retained) == 0 && r.tryWrite(batch) {
			batch = batch[:0]
			return
		}
		retained = append(retained, append([]*controlv1.Invocation(nil), batch...))
		retainedN += len(batch)
		batch = batch[:0]
		for retainedN > maxRetained && len(retained) > 0 {
			r.lost.Add(uint64(len(retained[0])))
			r.log.Error("dropping invocation records: the store keeps failing and the memory kept for them is full", "count", len(retained[0]))
			retainedN -= len(retained[0])
			retained = retained[1:]
		}
	}
	for {
		select {
		case inv := <-r.queue:
			batch = append(batch, inv)
			if len(batch) >= r.opts.BatchSize {
				flush()
			}
		case <-t.C:
			flush()
		case <-r.closing:
			for { // drain what is queued, then stop
				select {
				case inv := <-r.queue:
					batch = append(batch, inv)
					if len(batch) >= r.opts.BatchSize {
						flush()
					}
				default:
					flush()
					if retainedN > 0 { // the store is still refusing at shutdown: this is the last chance
						r.lost.Add(uint64(retainedN))
						r.log.Error("dropping invocation records at shutdown: the store keeps failing", "count", retainedN)
					}
					return
				}
			}
		}
	}
}

// tryWrite makes one attempt to store a batch.
func (r *Recorder) tryWrite(batch []*controlv1.Invocation) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.sink.InsertInvocations(ctx, batch); err != nil {
		r.log.Warn("could not store invocation records; keeping them to try again", "count", len(batch), "error", err)
		return false
	}
	return true
}
