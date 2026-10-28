package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

// Runner drives one scenario through an Invoker.
type Runner struct {
	Sc    *Scenario
	Inv   *Invoker
	RunID string

	seq      atomic.Uint64
	inflight atomic.Int64
	wg       sync.WaitGroup

	comp  *compiled
	trace []*TraceEntry
	pool  []byte // generated trace bodies are cut from this

	// LoadEnd is how long the load lasted (before draining).
	LoadEnd time.Duration
}

func NewRunner(sc *Scenario, inv *Invoker, runID string) (*Runner, error) {
	r := &Runner{Sc: sc, Inv: inv, RunID: runID}
	if sc.Load != nil {
		c, err := sc.compile()
		if err != nil {
			return nil, err
		}
		r.comp = c
	} else {
		if err := r.loadTrace(); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// ExpectedDuration is how long the load will last, when known up front.
func (r *Runner) ExpectedDuration() time.Duration {
	if r.Sc.Load != nil {
		return r.Sc.Load.Duration
	}
	sp, _ := r.Sc.Replay.speed()
	if sp > 0 && len(r.trace) > 0 {
		return time.Duration(float64(r.trace[len(r.trace)-1].at) / sp)
	}
	return 0
}

func (r *Runner) maxBudget() time.Duration {
	b := r.Sc.Defaults.Budget
	for _, t := range r.compTmpls() {
		b = max(b, t.budget)
	}
	for _, e := range r.trace {
		if e.Budget != "" {
			if d, err := time.ParseDuration(e.Budget); err == nil {
				b = max(b, d)
			}
		}
	}
	return b
}

func (r *Runner) compTmpls() []*tmpl {
	if r.comp == nil {
		return nil
	}
	return r.comp.tmpls
}

// Run generates load until it is over, then waits for what is in flight (up to
// the drain time) and cancels the rest.
func (r *Runner) Run(ctx context.Context) {
	hard, hardCancel := context.WithCancel(context.WithoutCancel(ctx))
	defer hardCancel()
	start := time.Now()
	switch {
	case r.Sc.Replay != nil:
		r.replay(ctx, hard, start)
	case r.Sc.Load.Model == "closed":
		r.closed(ctx, hard, start)
	default:
		r.open(ctx, hard, start)
	}
	r.LoadEnd = time.Since(start)

	drain := r.maxBudget()
	if r.Sc.Load != nil && r.Sc.Load.Drain > 0 {
		drain = r.Sc.Load.Drain
	}
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(drain + time.Second):
		hardCancel()
		<-done
	}
}

func (r *Runner) newPlan(t *tmpl, rng *rand.Rand, intended time.Time) *Planned {
	p := t.build(rng, r.seq.Add(1), r.RunID, r.Sc.Clients)
	p.Intended = intended
	return p
}

// launch starts a request in its own goroutine unless too many are in flight
// already, in which case it is dropped and counted: an open-loop generator must
// not turn into a closed-loop one by waiting.
func (r *Runner) launch(hard context.Context, p *Planned, maxInFlight int) {
	r.Inv.Stats.Released(p.Intended)
	if int(r.inflight.Add(1)) > maxInFlight {
		r.inflight.Add(-1)
		r.Inv.Stats.Record(&Result{Seq: p.Seq, RequestID: p.RequestID, Template: p.Template, Outcome: OutDropped})
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer r.inflight.Add(-1)
		r.Inv.Do(hard, p)
	}()
}

// rateAt is the arrival rate of an open model at offset t.
func rateAt(l *LoadSpec, t time.Duration) float64 {
	switch l.Model {
	case "ramp":
		f := float64(t) / float64(l.Duration)
		return l.From + (l.To-l.From)*f
	case "spike":
		for _, s := range l.Spikes {
			if t >= s.At && t < s.At+s.Duration {
				return s.Rate
			}
		}
		return l.Rate
	}
	return l.Rate
}

func (r *Runner) open(ctx, hard context.Context, start time.Time) {
	l := r.Sc.Load
	rng := rand.New(rand.NewPCG(r.Sc.Seed, 1))
	bursts := slices.Clone(l.Bursts)
	slices.SortFunc(bursts, func(a, b Burst) int { return int(a.At - b.At) })
	next := time.Duration(0)
	for {
		// the next event is the next arrival or the next burst, whichever is first
		at, isBurst := next, false
		if len(bursts) > 0 && bursts[0].At <= at {
			at, isBurst = bursts[0].At, true
		}
		if at >= l.Duration {
			return
		}
		if wait := time.Until(start.Add(at)); wait > 100*time.Microsecond {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return
			}
		} else if ctx.Err() != nil {
			return
		}
		intended := start.Add(at)
		if isBurst {
			b := bursts[0]
			bursts = bursts[1:]
			for range b.Count {
				r.launch(hard, r.newPlan(r.comp.pick(rng), rng, intended), l.MaxInFlight)
			}
			continue
		}
		rate := rateAt(l, at)
		if rate <= 0 { // between spikes of a model with no base rate
			next = at + 10*time.Millisecond
			continue
		}
		r.launch(hard, r.newPlan(r.comp.pick(rng), rng, intended), l.MaxInFlight)
		next = at + time.Duration(float64(time.Second)/rate)
	}
}

func (r *Runner) closed(ctx, hard context.Context, start time.Time) {
	l := r.Sc.Load
	var users sync.WaitGroup
	end := start.Add(l.Duration)
	for u := range l.Users {
		users.Add(1)
		go func() {
			defer users.Done()
			rng := rand.New(rand.NewPCG(r.Sc.Seed, uint64(u)+100))
			if l.RampUp > 0 {
				select {
				case <-time.After(time.Duration(int64(l.RampUp) / int64(l.Users) * int64(u))):
				case <-ctx.Done():
					return
				}
			}
			for i := 0; l.Iterations == 0 || i < l.Iterations; i++ {
				if ctx.Err() != nil || l.Duration > 0 && !time.Now().Before(end) {
					return
				}
				now := time.Now()
				p := r.newPlan(r.comp.pick(rng), rng, now)
				r.Inv.Stats.Released(now)
				r.Inv.Do(hard, p)
				if d := think(l, rng); d > 0 {
					select {
					case <-time.After(d):
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	users.Wait()
}

func think(l *LoadSpec, rng *rand.Rand) time.Duration {
	switch l.ThinkDist {
	case "exp":
		return time.Duration(-math.Log(1-rng.Float64()) * float64(l.Think))
	case "uniform":
		return time.Duration(rng.Float64() * 2 * float64(l.Think))
	}
	return l.Think
}

// loadTrace reads the JSONL trace of a replay.
func (r *Runner) loadTrace() error {
	rp := r.Sc.Replay
	path := rp.File
	if !filepath.IsAbs(path) {
		path = filepath.Join(r.Sc.dir, path)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	maxBody := 0
	for line := 1; sc.Scan(); line++ {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		var e TraceEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return fmt.Errorf("%s:%d: %w", path, line, err)
		}
		if e.at, err = e.offset(); err != nil {
			return fmt.Errorf("%s:%d: t: %w", path, line, err)
		}
		if e.Path == "" || e.Path[0] != '/' {
			return fmt.Errorf("%s:%d: path must start with /", path, line)
		}
		if e.Host == "" {
			e.Host = rp.Host
		}
		if e.Host == "" {
			e.Host = r.Sc.Defaults.Host
		}
		if e.Host == "" {
			return fmt.Errorf("%s:%d: no host (entry, replay.host or defaults.host)", path, line)
		}
		maxBody = max(maxBody, e.BodySize)
		r.trace = append(r.trace, &e)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if len(r.trace) == 0 {
		return fmt.Errorf("%s: empty trace", path)
	}
	slices.SortStableFunc(r.trace, func(a, b *TraceEntry) int { return int(a.at - b.at) })
	r.pool = make([]byte, maxBody)
	rng := rand.New(rand.NewPCG(r.Sc.Seed, 9))
	for i := range r.pool {
		r.pool[i] = byte(rng.Uint32())
	}
	return nil
}

func (r *Runner) tracePlan(e *TraceEntry, intended time.Time) (*Planned, error) {
	seq := r.seq.Add(1)
	id := requestID(r.RunID, seq)
	method := strings.ToUpper(e.Method)
	if method == "" {
		method = "GET"
	}
	h := http.Header{}
	for k, vs := range e.Headers {
		if strings.EqualFold(k, "x-request-id") {
			continue
		}
		for _, v := range vs {
			h.Add(k, v)
		}
	}
	var body []byte
	switch {
	case e.Body != nil:
		body = []byte(*e.Body)
	case e.BodyB64 != "":
		var err error
		if body, err = base64.StdEncoding.DecodeString(e.BodyB64); err != nil {
			return nil, err
		}
	case e.BodySize > 0:
		body = r.pool[:e.BodySize]
	}
	budget := r.Sc.Defaults.Budget
	if e.Budget != "" {
		d, err := time.ParseDuration(e.Budget)
		if err != nil {
			return nil, err
		}
		budget = d
	}
	c := int(seq % uint64(max(r.Sc.Clients, 1)))
	ip := fmt.Sprintf("10.%d.%d.%d", (c>>16)&255, (c>>8)&255, c&255|1)
	hs := filterHeaders(h, false)
	hs = append(hs, &commonv1.Header{Name: "x-request-id", Value: id})
	hs = appendForwarded(hs, ip, "http", e.Host)
	return &Planned{Seq: seq, RequestID: id, Template: "replay", Budget: budget, Intended: intended,
		Req: &ingressv1.InvokeRequest{RequestId: id, Request: &commonv1.HttpRequest{
			Method: method, Host: e.Host, Path: e.Path, RawQuery: e.Query, Headers: hs, Body: body, RemoteAddr: fmt.Sprintf("%s:%d", ip, 30000+c),
		}}}, nil
}

func (r *Runner) replay(ctx, hard context.Context, start time.Time) {
	rp := r.Sc.Replay
	speed, _ := rp.speed()
	maxInFlight := 10000
	if speed == 0 {
		// Maximum speed: as fast as `concurrency` requests in flight allow.
		n := rp.Concurrency
		if n <= 0 {
			n = 64
		}
		var next atomic.Int64
		var users sync.WaitGroup
		for range n {
			users.Add(1)
			go func() {
				defer users.Done()
				for ctx.Err() == nil {
					i := int(next.Add(1)) - 1
					if i >= len(r.trace) {
						return
					}
					now := time.Now()
					p, err := r.tracePlan(r.trace[i], now)
					if err != nil {
						continue
					}
					r.Inv.Stats.Released(now)
					r.Inv.Do(hard, p)
				}
			}()
		}
		users.Wait()
		return
	}
	for _, e := range r.trace {
		at := time.Duration(float64(e.at) / speed)
		if wait := time.Until(start.Add(at)); wait > 100*time.Microsecond {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return
			}
		} else if ctx.Err() != nil {
			return
		}
		p, err := r.tracePlan(e, start.Add(at))
		if err != nil {
			continue
		}
		r.launch(hard, p, maxInFlight)
	}
}
