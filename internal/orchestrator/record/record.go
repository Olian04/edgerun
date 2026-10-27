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

func (r *Recorder) loop() {
	defer close(r.done)
	t := time.NewTicker(r.opts.FlushInterval)
	defer t.Stop()
	batch := make([]*controlv1.Invocation, 0, r.opts.BatchSize)
	flush := func() {
		if len(batch) > 0 {
			r.write(batch)
			batch = batch[:0]
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
					return
				}
			}
		}
	}
}

// write tries a batch twice, then gives up on it.
func (r *Recorder) write(batch []*controlv1.Invocation) {
	var err error
	for attempt := range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = r.sink.InsertInvocations(ctx, batch)
		cancel()
		if err == nil {
			return
		}
		if attempt == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	r.lost.Add(uint64(len(batch)))
	r.log.Error("dropping invocation records: the store keeps failing", "count", len(batch), "error", err)
}
