package main

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
)

// ReqLog writes one JSON line per request to a file, from a writer goroutine so
// the hot path never waits for the disk. If the writer falls behind, lines are
// dropped and counted rather than slowing the measurement down.
type ReqLog struct {
	ch      chan *Result
	f       *os.File
	done    chan struct{}
	Dropped atomic.Int64
	once    sync.Once
}

func NewReqLog(path string) (*ReqLog, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	l := &ReqLog{ch: make(chan *Result, 1<<16), f: f, done: make(chan struct{})}
	go func() {
		defer close(l.done)
		w := bufio.NewWriterSize(f, 1<<20)
		enc := json.NewEncoder(w)
		for r := range l.ch {
			enc.Encode(r)
		}
		w.Flush()
	}()
	return l, nil
}

func (l *ReqLog) Write(r *Result) {
	select {
	case l.ch <- r:
	default:
		l.Dropped.Add(1)
	}
}

func (l *ReqLog) Close() {
	l.once.Do(func() {
		close(l.ch)
		<-l.done
		l.f.Close()
	})
}
