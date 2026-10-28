package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

// Scenario is what a scenario file (YAML, or JSON: JSON is YAML) decodes to.
type Scenario struct {
	Name     string       `yaml:"name"`
	Target   TargetSpec   `yaml:"target"`
	Defaults Defaults     `yaml:"defaults"`
	Requests []TemplateSp `yaml:"requests"`
	Load     *LoadSpec    `yaml:"load"`
	Replay   *ReplaySpec  `yaml:"replay"`
	Expect   []string     `yaml:"expect"`
	Seed     uint64       `yaml:"seed"`
	// Clients is how many distinct client addresses to synthesise (remote_addr and
	// x-forwarded-for), as a real edge would see. Default 64.
	Clients int `yaml:"clients"`

	dir string // directory of the file, for relative body files
}

type TargetSpec struct {
	URL      string `yaml:"url"`
	TokenEnv string `yaml:"token_env"` // the token itself does not belong in a file
	HTTP     string `yaml:"http"`      // "1" (default) or "2" (h2c / ALPN)
	Conns    int    `yaml:"conns"`     // idle connections kept per host; 0 = as many as in flight
	// Compression: "none" asks the orchestrator not to gzip response messages.
	Compression string `yaml:"compression"`
}

type Defaults struct {
	Host   string        `yaml:"host"`
	Budget time.Duration `yaml:"budget"` // per-request deadline, the RPC budget. Default 10s.
}

type TemplateSp struct {
	Name          string              `yaml:"name"`
	Weight        float64             `yaml:"weight"`
	Method        string              `yaml:"method"`
	Host          string              `yaml:"host"`
	Hosts         []string            `yaml:"hosts"`
	Path          string              `yaml:"path"`
	Paths         []string            `yaml:"paths"`
	Query         string              `yaml:"query"`
	Headers       map[string]StrOrSeq `yaml:"headers"`
	Body          *BodySpec           `yaml:"body"`
	Budget        time.Duration       `yaml:"budget"`
	AllowOversize bool                `yaml:"allow_oversize"` // send even if over the orchestrator's limit (to see REQUEST_TOO_LARGE)
}

// StrOrSeq is a header value or a list of them.
type StrOrSeq []string

func (s *StrOrSeq) UnmarshalJSON(b []byte) error {
	var one string
	if json.Unmarshal(b, &one) == nil {
		*s = []string{one}
		return nil
	}
	return json.Unmarshal(b, (*[]string)(s))
}

func (s *StrOrSeq) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.ScalarNode:
		*s = []string{n.Value}
	case yaml.SequenceNode:
		for _, c := range n.Content {
			*s = append(*s, c.Value)
		}
	default:
		return fmt.Errorf("line %d: a header is a string or a list of strings", n.Line)
	}
	return nil
}

// BodySpec: a string is an inline body; otherwise exactly one of inline, file
// or a generated size (size, or size_min..size_max drawn per request).
type BodySpec struct {
	Inline  *string  `yaml:"inline"`
	File    string   `yaml:"file"`
	Size    ByteSize `yaml:"size"`
	SizeMin ByteSize `yaml:"size_min"`
	SizeMax ByteSize `yaml:"size_max"`
	Kind    string   `yaml:"kind"` // random (default), zeros, text
}

func (b *BodySpec) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		s := n.Value
		b.Inline = &s
		return nil
	}
	type plain BodySpec
	return n.Decode((*plain)(b))
}

type ByteSize int64

func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	v, err := parseSize(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*b = ByteSize(v)
	return nil
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	units := []struct {
		suf string
		mul int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1}}
	mul := int64(1)
	for _, u := range units {
		if strings.HasSuffix(s, u.suf) {
			s, mul = strings.TrimSpace(strings.TrimSuffix(s, u.suf)), u.mul
			break
		}
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("bad size %q (want 1024, 64KiB, 6MiB, ...)", s)
	}
	return int64(f * float64(mul)), nil
}

type LoadSpec struct {
	Model    string        `yaml:"model"` // constant | ramp | spike | closed
	Duration time.Duration `yaml:"duration"`
	// constant, and the base rate of spike
	Rate float64 `yaml:"rate"`
	// ramp
	From float64 `yaml:"from"`
	To   float64 `yaml:"to"`
	// spike: windows of a different total rate; bursts: N requests at one instant
	Spikes []Spike `yaml:"spikes"`
	Bursts []Burst `yaml:"bursts"`
	// closed loop
	Users      int           `yaml:"users"`
	Think      time.Duration `yaml:"think"`
	ThinkDist  string        `yaml:"think_dist"` // fixed (default) | exp | uniform (0..2*think)
	RampUp     time.Duration `yaml:"ramp_up"`    // users start spread over this long
	Iterations int           `yaml:"iterations"` // per user; 0 = until duration ends
	// open models: requests in flight above this are dropped (counted), not queued
	MaxInFlight int `yaml:"max_in_flight"`
	// how long to wait for requests still in flight when the load ends. Default: the largest budget.
	Drain time.Duration `yaml:"drain"`
}

type Spike struct {
	At       time.Duration `yaml:"at"`
	Duration time.Duration `yaml:"duration"`
	Rate     float64       `yaml:"rate"`
}

type Burst struct {
	At    time.Duration `yaml:"at"`
	Count int           `yaml:"count"`
}

type ReplaySpec struct {
	File        string `yaml:"file"`
	Speed       string `yaml:"speed"`       // "1" (default), "0.5", "4", or "max"
	Concurrency int    `yaml:"concurrency"` // for speed max: requests in flight (default 64)
	Host        string `yaml:"host"`        // override every host
}

// LoadScenario reads and validates a scenario file.
func LoadScenario(path string) (*Scenario, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var sc Scenario
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&sc); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	sc.dir = filepath.Dir(path)
	if sc.Name == "" {
		sc.Name = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	return &sc, sc.Validate()
}

func (sc *Scenario) Validate() error {
	if sc.Clients == 0 {
		sc.Clients = 64
	}
	if sc.Defaults.Budget == 0 {
		sc.Defaults.Budget = 10 * time.Second
	}
	if (sc.Load == nil) == (sc.Replay == nil) {
		return fmt.Errorf("a scenario has exactly one of load: and replay:")
	}
	if sc.Replay != nil {
		if sc.Replay.File == "" {
			return fmt.Errorf("replay.file is required")
		}
		if _, err := sc.Replay.speed(); err != nil {
			return err
		}
		return nil
	}
	if len(sc.Requests) == 0 {
		return fmt.Errorf("a load scenario needs at least one request template")
	}
	l := sc.Load
	if l.MaxInFlight == 0 {
		l.MaxInFlight = 10000
	}
	switch l.Model {
	case "constant":
		if l.Rate <= 0 || l.Duration <= 0 {
			return fmt.Errorf("load constant needs rate > 0 and duration")
		}
	case "ramp":
		if l.To <= 0 && l.From <= 0 || l.Duration <= 0 {
			return fmt.Errorf("load ramp needs from, to and duration")
		}
	case "spike":
		if l.Duration <= 0 || (l.Rate <= 0 && len(l.Spikes) == 0 && len(l.Bursts) == 0) {
			return fmt.Errorf("load spike needs duration and a rate, spikes or bursts")
		}
		for _, b := range l.Bursts {
			if b.Count <= 0 {
				return fmt.Errorf("a burst needs count > 0")
			}
		}
	case "closed":
		if l.Users <= 0 || (l.Duration <= 0 && l.Iterations <= 0) {
			return fmt.Errorf("load closed needs users and a duration or iterations")
		}
		switch l.ThinkDist {
		case "", "fixed", "exp", "uniform":
		default:
			return fmt.Errorf("think_dist is fixed, exp or uniform")
		}
	default:
		return fmt.Errorf("load.model %q: want constant, ramp, spike or closed", l.Model)
	}
	return nil
}

func (r *ReplaySpec) speed() (float64, error) {
	switch r.Speed {
	case "", "1":
		return 1, nil
	case "max":
		return 0, nil
	}
	f, err := strconv.ParseFloat(r.Speed, 64)
	if err != nil || f <= 0 {
		return 0, fmt.Errorf("replay.speed %q: want a positive number or max", r.Speed)
	}
	return f, nil
}

// tmpl is a request template, compiled: everything that does not change per
// request is computed once and shared (read-only) between requests.
type tmpl struct {
	name          string
	weight        float64
	method        string
	hosts         []string
	paths         []pathQuery
	rawQuery      string
	static        []*commonv1.Header // already filtered the way a frontend filters
	body          []byte             // fixed body, or the pool a variable one is cut from
	bodyMin       int
	bodyMax       int
	budget        time.Duration
	allowOversize bool
}

type pathQuery struct{ path, query string }

type compiled struct {
	tmpls []*tmpl
	cum   []float64
}

func (sc *Scenario) compile() (*compiled, error) {
	c := &compiled{}
	total := 0.0
	for i, sp := range sc.Requests {
		t := &tmpl{name: sp.Name, weight: sp.Weight, method: strings.ToUpper(sp.Method), budget: sp.Budget, allowOversize: sp.AllowOversize}
		if t.name == "" {
			t.name = fmt.Sprintf("req%d", i+1)
		}
		if t.weight == 0 {
			t.weight = 1
		}
		if t.method == "" {
			t.method = "GET"
		}
		if t.budget == 0 {
			t.budget = sc.Defaults.Budget
		}
		t.hosts = sp.Hosts
		if sp.Host != "" {
			t.hosts = append(t.hosts, sp.Host)
		}
		if len(t.hosts) == 0 && sc.Defaults.Host != "" {
			t.hosts = []string{sc.Defaults.Host}
		}
		if len(t.hosts) == 0 {
			return nil, fmt.Errorf("request %q has no host (set host, hosts or defaults.host)", t.name)
		}
		ps := sp.Paths
		if sp.Path != "" {
			ps = append(ps, sp.Path)
		}
		if len(ps) == 0 {
			ps = []string{"/"}
		}
		t.rawQuery = strings.TrimPrefix(sp.Query, "?")
		for _, p := range ps {
			if !strings.HasPrefix(p, "/") {
				return nil, fmt.Errorf("request %q: path %q must start with /", t.name, p)
			}
			pq := pathQuery{path: p, query: t.rawQuery}
			if k := strings.IndexByte(p, '?'); k >= 0 { // "/x?y=1" is the path /x and the query y=1
				pq.path, pq.query = p[:k], p[k+1:]
			}
			t.paths = append(t.paths, pq)
		}
		h := http.Header{}
		for k, vs := range sp.Headers {
			for _, v := range vs {
				h.Add(k, v)
			}
		}
		t.static = filterHeaders(h, false)
		if err := sc.compileBody(t, sp.Body); err != nil {
			return nil, fmt.Errorf("request %q: %w", t.name, err)
		}
		total += t.weight
		c.tmpls = append(c.tmpls, t)
		c.cum = append(c.cum, total)
	}
	return c, nil
}

func (sc *Scenario) compileBody(t *tmpl, b *BodySpec) error {
	if b == nil {
		return nil
	}
	n := 0
	if b.Inline != nil {
		n++
	}
	if b.File != "" {
		n++
	}
	if b.Size > 0 || b.SizeMax > 0 {
		n++
	}
	if n != 1 {
		return fmt.Errorf("body: exactly one of inline, file, size (or size_min and size_max)")
	}
	switch {
	case b.Inline != nil:
		t.body = []byte(*b.Inline)
	case b.File != "":
		p := b.File
		if !filepath.IsAbs(p) {
			p = filepath.Join(sc.dir, p)
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		t.body = data
	default:
		lo, hi := int(b.Size), int(b.Size)
		if b.SizeMax > 0 {
			lo, hi = int(b.SizeMin), int(b.SizeMax)
		}
		if lo > hi {
			return fmt.Errorf("size_min > size_max")
		}
		t.bodyMin, t.bodyMax = lo, hi
		t.body = make([]byte, hi)
		switch b.Kind {
		case "zeros":
		case "text":
			const lorem = "the quick brown fox jumps over the lazy dog\n"
			for i := range t.body {
				t.body[i] = lorem[i%len(lorem)]
			}
		case "", "random":
			r := rand.New(rand.NewPCG(sc.Seed, 7))
			for i := 0; i < len(t.body); i += 8 {
				v := r.Uint64()
				for j := 0; j < 8 && i+j < len(t.body); j++ {
					t.body[i+j] = byte(v >> (8 * j))
				}
			}
		default:
			return fmt.Errorf("body.kind %q: random, zeros or text", b.Kind)
		}
	}
	return nil
}

func (c *compiled) pick(rng *rand.Rand) *tmpl {
	if len(c.tmpls) == 1 {
		return c.tmpls[0]
	}
	x := rng.Float64() * c.cum[len(c.cum)-1]
	i := sort.SearchFloat64s(c.cum, x)
	if i >= len(c.tmpls) {
		i = len(c.tmpls) - 1
	}
	return c.tmpls[i]
}

// Planned is one request ready to be sent.
type Planned struct {
	Seq           uint64
	RequestID     string
	Template      string
	Req           *ingressv1.InvokeRequest
	Budget        time.Duration
	AllowOversize bool
	Intended      time.Time // when the load model wanted it sent
	Sink          Sink      // optional, see Invoker.Do
}

// build makes one request out of the template: pick a host, path and body size,
// and add what a frontend adds (the client's address, the request id).
func (t *tmpl) build(rng *rand.Rand, seq uint64, runID string, clients int) *Planned {
	id := requestID(runID, seq)
	host := t.hosts[0]
	if len(t.hosts) > 1 {
		host = t.hosts[rng.IntN(len(t.hosts))]
	}
	pq := t.paths[0]
	if len(t.paths) > 1 {
		pq = t.paths[rng.IntN(len(t.paths))]
	}
	body := t.body
	if t.bodyMax > 0 {
		n := t.bodyMin
		if t.bodyMax > t.bodyMin {
			n += rng.IntN(t.bodyMax - t.bodyMin + 1)
		}
		body = t.body[:n]
	}
	c := int(seq % uint64(max(clients, 1)))
	ip := fmt.Sprintf("10.%d.%d.%d", (c>>16)&255, (c>>8)&255, c&255|1)
	hs := make([]*commonv1.Header, 0, len(t.static)+4)
	hs = append(hs, t.static...)
	hs = append(hs, &commonv1.Header{Name: "x-request-id", Value: id})
	hs = appendForwarded(hs, ip, "http", host)
	return &Planned{
		Seq: seq, RequestID: id, Template: t.name, Budget: t.budget, AllowOversize: t.allowOversize,
		Req: &ingressv1.InvokeRequest{RequestId: id, Request: &commonv1.HttpRequest{
			Method: t.method, Host: host, Path: pq.path, RawQuery: pq.query, Headers: hs, Body: body,
			RemoteAddr: ip + ":" + strconv.Itoa(30000+c%30000),
		}},
	}
}

func requestID(runID string, seq uint64) string {
	return "tm-" + runID + "-" + strconv.FormatUint(seq, 10)
}

// Trace entries (replay): one JSON object per line.
//
//	{"t": 0.25, "method": "POST", "host": "a.test", "path": "/x", "query": "a=1",
//	 "headers": {"content-type": "text/plain"}, "body": "hello", "budget": "2s"}
//
// t is seconds since the start of the trace (a number), or a duration string
// ("250ms"). The body is "body" (text), "body_b64", or "body_size" (that many
// generated bytes).
type TraceEntry struct {
	T        json.RawMessage     `json:"t"`
	Method   string              `json:"method"`
	Host     string              `json:"host"`
	Path     string              `json:"path"`
	Query    string              `json:"query"`
	Headers  map[string]StrOrSeq `json:"headers"`
	Body     *string             `json:"body"`
	BodyB64  string              `json:"body_b64"`
	BodySize int                 `json:"body_size"`
	Budget   string              `json:"budget"`
	ID       string              `json:"id"` // original request id, informational

	at time.Duration
}

func (t *TraceEntry) offset() (time.Duration, error) {
	s := strings.TrimSpace(string(t.T))
	if s == "" || s == "null" {
		return 0, nil
	}
	if strings.HasPrefix(s, `"`) {
		var str string
		if err := json.Unmarshal(t.T, &str); err != nil {
			return 0, err
		}
		return time.ParseDuration(str)
	}
	f, err := strconv.ParseFloat(s, 64)
	return time.Duration(f * float64(time.Second)), err
}
