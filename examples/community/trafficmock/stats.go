package main

import (
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Outcome says how one request ended, from a client's point of view.
type Outcome string

const (
	OutOK            Outcome = "ok"             // head and the whole body arrived (any HTTP status: the function's own 500 is ok)
	OutPlatformError Outcome = "platform_error" // Invoke failed before the head
	OutStreamFailure Outcome = "stream_failure" // the head arrived, the body did not complete: a frontend would abort the connection
	OutRejected      Outcome = "rejected"       // refused locally (413) as a frontend does, never sent
	OutCanceled      Outcome = "canceled"       // the run was stopped with the request in flight
	OutDropped       Outcome = "dropped"        // not sent: too many requests already in flight
)

// Result is everything known about one request.
type Result struct {
	Seq          uint64        `json:"seq"`
	RequestID    string        `json:"request_id"`
	Template     string        `json:"template"`
	Offset       time.Duration `json:"offset_ns"` // start, since the run began
	Lag          time.Duration `json:"lag_ns"`    // intended start to actual start
	TTFB         time.Duration `json:"ttfb_ns"`   // until the response head arrived
	Total        time.Duration `json:"total_ns"`  // until the body ended (or failed)
	Outcome      Outcome       `json:"outcome"`
	Status       int           `json:"status"`      // what a client would see; 0 = connection aborted / nothing
	FuncStatus   int           `json:"func_status"` // the function's own status, 0 if no head
	Reason       string        `json:"reason,omitempty"`
	Typed        bool          `json:"typed,omitempty"`
	LocalTimer   bool          `json:"local_deadline,omitempty"`
	InvocationID string        `json:"invocation_id,omitempty"`
	Bytes        int64         `json:"bytes"`
	RetryAfter   bool          `json:"retry_after,omitempty"` // the frontend would add Retry-After: 1
	Error        string        `json:"error,omitempty"`
}

const (
	histMax   = int64(3_600_000_000) // one hour, in microseconds
	nShards   = 8
	maxSecond = 7200
	sampleErr = 50
	sampleTop = 20
)

func newHist() *hdrhistogram.Histogram { return hdrhistogram.New(1, histMax, 3) }

func rec(h *hdrhistogram.Histogram, d time.Duration) {
	v := d.Microseconds()
	if v < 1 {
		v = 1
	}
	if v > histMax {
		v = histMax
	}
	h.RecordValue(v)
}

// promBounds are the upper bounds (seconds) of the Prometheus histogram buckets.
var promBounds = []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

type promHist struct {
	buckets [16]atomic.Int64 // cumulative is computed at scrape; the last is +Inf
	sumNs   atomic.Int64
	count   atomic.Int64
}

func (p *promHist) observe(d time.Duration) {
	s := d.Seconds()
	i := sort.SearchFloat64s(promBounds, s)
	p.buckets[i].Add(1)
	p.sumNs.Add(int64(d))
	p.count.Add(1)
}

type tmplStats struct {
	Sent, OK, Errors int64
	Total            *hdrhistogram.Histogram
}

type shard struct {
	mu sync.Mutex

	outcomes  map[Outcome]int64
	status    map[int]int64 // as the client sees it
	funcStat  map[int]int64 // the function's own, from heads
	reasons   map[string]int64
	reasonHTT map[string]int // the mapped status of a reason
	deadlines [2]int64       // [typed, local timer]
	bytes     int64

	ttfb, total, errLat, lag *hdrhistogram.Histogram
	tmpl                     map[string]*tmplStats

	errs []*Result
	top  []*Result // slowest by Total
}

func newShard() *shard {
	return &shard{
		outcomes: map[Outcome]int64{}, status: map[int]int64{}, funcStat: map[int]int64{},
		reasons: map[string]int64{}, reasonHTT: map[string]int{},
		ttfb: newHist(), total: newHist(), errLat: newHist(), lag: newHist(),
		tmpl: map[string]*tmplStats{},
	}
}

// Stats collects results from many goroutines, sharded to keep contention off
// the hot path; Snapshot merges the shards.
type Stats struct {
	shards [nShards]*shard
	sent   atomic.Int64 // requests the load model released (including dropped)
	flight atomic.Int64
	peak   atomic.Int64

	// per-second timeline, by intended send time (sent) and by finish time
	tlSent, tlDone, tlErr [maxSecond]atomic.Int64

	promTTFB, promTotal promHist

	start time.Time
}

func NewStats(start time.Time) *Stats {
	s := &Stats{start: start}
	for i := range s.shards {
		s.shards[i] = newShard()
	}
	return s
}

func secOf(d time.Duration) int {
	i := int(d / time.Second)
	if i < 0 {
		return 0
	}
	if i >= maxSecond {
		return maxSecond - 1
	}
	return i
}

// Released is called when the load model releases a request.
func (s *Stats) Released(intended time.Time) {
	s.sent.Add(1)
	s.tlSent[secOf(intended.Sub(s.start))].Add(1)
}

func (s *Stats) InFlight(delta int64) {
	n := s.flight.Add(delta)
	for {
		p := s.peak.Load()
		if n <= p || s.peak.CompareAndSwap(p, n) {
			return
		}
	}
}

// Record adds one finished request.
func (s *Stats) Record(r *Result) {
	sh := s.shards[r.Seq%nShards]
	now := time.Since(s.start)
	if r.Outcome != OutDropped && r.Outcome != OutRejected && r.Outcome != OutCanceled {
		s.tlDone[secOf(now)].Add(1)
		if r.Outcome != OutOK {
			s.tlErr[secOf(now)].Add(1)
		}
	}
	if r.Outcome == OutOK || r.Outcome == OutStreamFailure {
		s.promTTFB.observe(r.TTFB)
	}
	if r.Outcome == OutOK {
		s.promTotal.observe(r.Total)
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.outcomes[r.Outcome]++
	ts := sh.tmpl[r.Template]
	if ts == nil {
		ts = &tmplStats{Total: newHist()}
		sh.tmpl[r.Template] = ts
	}
	ts.Sent++
	switch r.Outcome {
	case OutOK:
		ts.OK++
		sh.status[r.Status]++
		sh.funcStat[r.FuncStatus]++
		sh.bytes += r.Bytes
		rec(sh.ttfb, r.TTFB)
		rec(sh.total, r.Total)
		rec(ts.Total, r.Total)
		sh.top = keepSlowest(sh.top, r)
	case OutPlatformError, OutRejected:
		if r.Outcome == OutPlatformError {
			ts.Errors++
			rec(sh.errLat, r.Total)
			if r.Reason == "DEADLINE_EXCEEDED" {
				if r.LocalTimer {
					sh.deadlines[1]++
				} else {
					sh.deadlines[0]++
				}
			}
		}
		sh.status[r.Status]++
		sh.reasons[r.Reason]++
		sh.reasonHTT[r.Reason] = r.Status
		sh.keepErr(r)
	case OutStreamFailure:
		ts.Errors++
		sh.status[0]++
		sh.funcStat[r.FuncStatus]++
		sh.reasons["STREAM:"+r.Reason]++
		sh.bytes += r.Bytes
		rec(sh.ttfb, r.TTFB)
		rec(sh.errLat, r.Total)
		sh.keepErr(r)
	case OutCanceled, OutDropped:
		ts.Errors += 0
	}
	if r.Outcome != OutDropped {
		rec(sh.lag, r.Lag)
	}
}

func (sh *shard) keepErr(r *Result) {
	if len(sh.errs) < sampleErr {
		c := *r
		sh.errs = append(sh.errs, &c)
	}
}

func keepSlowest(top []*Result, r *Result) []*Result {
	if len(top) < sampleTop {
		c := *r
		return append(top, &c)
	}
	min := 0
	for i := range top {
		if top[i].Total < top[min].Total {
			min = i
		}
	}
	if r.Total > top[min].Total {
		c := *r
		top[min] = &c
	}
	return top
}

// Snapshot is the merged state at one moment.
type Snapshot struct {
	Sent, InFlight, PeakInFlight int64
	Outcomes                     map[Outcome]int64
	Status                       map[int]int64
	FuncStatus                   map[int]int64
	Reasons                      map[string]int64
	ReasonStatus                 map[string]int
	DeadlineTyped                int64
	DeadlineLocal                int64
	Bytes                        int64
	TTFB, Total                  *hdrhistogram.Histogram
	ErrLat, Lag                  *hdrhistogram.Histogram
	Templates                    map[string]*tmplStats
	Errors                       []*Result
	Slowest                      []*Result
	Timeline                     []TimelinePoint
}

type TimelinePoint struct {
	T         int   `json:"t"`
	Sent      int64 `json:"sent"`
	Completed int64 `json:"completed"`
	Errors    int64 `json:"errors"`
}

func (s *Stats) Snapshot() *Snapshot {
	sn := &Snapshot{
		Sent: s.sent.Load(), InFlight: s.flight.Load(), PeakInFlight: s.peak.Load(),
		Outcomes: map[Outcome]int64{}, Status: map[int]int64{}, FuncStatus: map[int]int64{},
		Reasons: map[string]int64{}, ReasonStatus: map[string]int{},
		TTFB: newHist(), Total: newHist(), ErrLat: newHist(), Lag: newHist(),
		Templates: map[string]*tmplStats{},
	}
	for _, sh := range s.shards {
		sh.mu.Lock()
		for k, v := range sh.outcomes {
			sn.Outcomes[k] += v
		}
		for k, v := range sh.status {
			sn.Status[k] += v
		}
		for k, v := range sh.funcStat {
			sn.FuncStatus[k] += v
		}
		for k, v := range sh.reasons {
			sn.Reasons[k] += v
			sn.ReasonStatus[k] = sh.reasonHTT[k]
		}
		sn.DeadlineTyped += sh.deadlines[0]
		sn.DeadlineLocal += sh.deadlines[1]
		sn.Bytes += sh.bytes
		sn.TTFB.Merge(sh.ttfb)
		sn.Total.Merge(sh.total)
		sn.ErrLat.Merge(sh.errLat)
		sn.Lag.Merge(sh.lag)
		for k, v := range sh.tmpl {
			t := sn.Templates[k]
			if t == nil {
				t = &tmplStats{Total: newHist()}
				sn.Templates[k] = t
			}
			t.Sent += v.Sent
			t.OK += v.OK
			t.Errors += v.Errors
			t.Total.Merge(v.Total)
		}
		for _, e := range sh.errs {
			sn.Errors = append(sn.Errors, e)
		}
		sn.Slowest = append(sn.Slowest, sh.top...)
		sh.mu.Unlock()
	}
	sort.Slice(sn.Errors, func(i, j int) bool { return sn.Errors[i].Seq < sn.Errors[j].Seq })
	if len(sn.Errors) > sampleErr {
		sn.Errors = sn.Errors[:sampleErr]
	}
	sort.Slice(sn.Slowest, func(i, j int) bool { return sn.Slowest[i].Total > sn.Slowest[j].Total })
	if len(sn.Slowest) > sampleTop {
		sn.Slowest = sn.Slowest[:sampleTop]
	}
	last := -1
	for i := 0; i < maxSecond; i++ {
		if s.tlSent[i].Load() > 0 || s.tlDone[i].Load() > 0 {
			last = i
		}
	}
	for i := 0; i <= last; i++ {
		sn.Timeline = append(sn.Timeline, TimelinePoint{T: i, Sent: s.tlSent[i].Load(), Completed: s.tlDone[i].Load(), Errors: s.tlErr[i].Load()})
	}
	return sn
}

// Attempted is the number of requests that went through Invoke and finished
// (not canceled): the denominator of every error rate.
func (sn *Snapshot) Attempted() int64 {
	return sn.Outcomes[OutOK] + sn.Outcomes[OutPlatformError] + sn.Outcomes[OutStreamFailure]
}

// Errors is platform errors plus stream failures.
func (sn *Snapshot) ErrorCount() int64 {
	return sn.Outcomes[OutPlatformError] + sn.Outcomes[OutStreamFailure]
}

func itoa(i int) string { return strconv.Itoa(i) }
