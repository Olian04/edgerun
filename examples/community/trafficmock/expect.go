package main

import (
	"fmt"
	"github.com/HdrHistogram/hdrhistogram-go"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// An assertion is `<metric> <op> <value>`:
//
//	p99<200ms   ttfb.p99<50ms   errors<1%   status.5xx==0   rps>=500   reason.NO_CAPACITY.count<=10
//
// Metrics:
//	[total.|ttfb.|all.]{min,mean,p50,p90,p95,p99,p999,max}   latency (total = head to end of body; plain pNN is total; all.* includes errored requests)
//	errors, status.<N|Nxx>, reason.<NAME>                    percent of attempted requests (add .count for a count)
//	rps, sent, completed, ok, dropped, canceled, rejected, stream_failures   numbers
//	echo_checked, echo_mismatches, echo_body_mismatches, echo_header_mismatches   numbers (templates with verify:): e.g. echo_mismatches==0
// Values: a duration (200ms), a percentage (1%), or a number.

var exprRe = regexp.MustCompile(`^\s*([A-Za-z0-9_.:\-]+)\s*(<=|>=|==|!=|<|>)\s*(\S+)\s*$`)

type Expectation struct {
	Expr   string  `json:"expr"`
	Metric string  `json:"metric"`
	Value  float64 `json:"value"` // the measured value, in the unit of the metric
	Unit   string  `json:"unit"`
	Pass   bool    `json:"pass"`
	Error  string  `json:"error,omitempty"`
}

type value struct {
	v    float64
	unit string // "s", "%", ""
}

func parseValue(s string) (value, error) {
	if strings.HasSuffix(s, "%") {
		f, err := strconv.ParseFloat(strings.TrimSuffix(s, "%"), 64)
		return value{f, "%"}, err
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return value{f, ""}, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return value{}, fmt.Errorf("bad value %q (want 200ms, 1%% or a number)", s)
	}
	return value{d.Seconds(), "s"}, nil
}

// Evaluate checks the expectations against a finished run.
func Evaluate(exprs []string, sn *Snapshot, rep *Report) []Expectation {
	var out []Expectation
	for _, e := range exprs {
		x := Expectation{Expr: e}
		m := exprRe.FindStringSubmatch(e)
		if m == nil {
			x.Error = "cannot parse: want <metric><op><value>, like p99<200ms"
			out = append(out, x)
			continue
		}
		x.Metric = m[1]
		want, err := parseValue(m[3])
		if err != nil {
			x.Error = err.Error()
			out = append(out, x)
			continue
		}
		got, err := metric(m[1], sn, rep)
		if err != nil {
			x.Error = err.Error()
			out = append(out, x)
			continue
		}
		x.Value, x.Unit = got.v, got.unit
		if want.unit != got.unit && !(want.unit == "" && got.unit == "%") {
			x.Error = fmt.Sprintf("%s is in %s, the value is in %s", m[1], unitName(got.unit), unitName(want.unit))
			out = append(out, x)
			continue
		}
		const eps = 1e-9
		switch m[2] {
		case "<":
			x.Pass = got.v < want.v
		case "<=":
			x.Pass = got.v <= want.v+eps
		case ">":
			x.Pass = got.v > want.v
		case ">=":
			x.Pass = got.v >= want.v-eps
		case "==":
			x.Pass = got.v >= want.v-eps && got.v <= want.v+eps
		case "!=":
			x.Pass = got.v < want.v-eps || got.v > want.v+eps
		}
		out = append(out, x)
	}
	return out
}

func unitName(u string) string {
	switch u {
	case "s":
		return "time"
	case "%":
		return "percent"
	}
	return "a plain number"
}

func metric(name string, sn *Snapshot, rep *Report) (value, error) {
	att := float64(sn.Attempted())
	pct := func(n int64) value {
		if att == 0 {
			return value{0, "%"}
		}
		return value{100 * float64(n) / att, "%"}
	}
	count := strings.HasSuffix(name, ".count")
	base := strings.TrimSuffix(name, ".count")
	ratio := func(n int64) value {
		if count {
			return value{float64(n), ""}
		}
		return pct(n)
	}
	switch base {
	case "errors", "error_rate":
		return ratio(sn.ErrorCount()), nil
	case "rps":
		return value{rep.Totals.RPSCompleted, ""}, nil
	case "rps.sent":
		return value{rep.Totals.RPSSent, ""}, nil
	case "sent":
		return value{float64(sn.Sent), ""}, nil
	case "completed":
		return value{float64(sn.Attempted()), ""}, nil
	case "ok":
		return value{float64(sn.Outcomes[OutOK]), ""}, nil
	case "dropped":
		return value{float64(sn.Outcomes[OutDropped]), ""}, nil
	case "canceled":
		return value{float64(sn.Outcomes[OutCanceled]), ""}, nil
	case "rejected":
		return value{float64(sn.Outcomes[OutRejected]), ""}, nil
	case "stream_failures":
		return value{float64(sn.Outcomes[OutStreamFailure]), ""}, nil
	case "echo_checked":
		return value{float64(sn.EchoChecked), ""}, nil
	case "echo_mismatches": // requests whose response was not the one for them (body or header)
		return value{float64(sn.EchoMismatches), ""}, nil
	case "echo_body_mismatches":
		return value{float64(sn.EchoBodyMismatch), ""}, nil
	case "echo_header_mismatches":
		return value{float64(sn.EchoHdrMismat), ""}, nil
	}
	if rest, ok := strings.CutPrefix(base, "status."); ok {
		var n int64
		if strings.HasSuffix(rest, "xx") && len(rest) == 3 {
			c := int(rest[0] - '0')
			for st, k := range sn.Status {
				if st/100 == c {
					n += k
				}
			}
		} else if st, err := strconv.Atoi(rest); err == nil {
			n = sn.Status[st]
		} else {
			return value{}, fmt.Errorf("unknown metric %q", name)
		}
		return ratio(n), nil
	}
	if rest, ok := strings.CutPrefix(base, "reason."); ok {
		return ratio(sn.Reasons[rest]), nil
	}
	// latency
	h := sn.Total
	key := base
	switch {
	case strings.HasPrefix(base, "ttfb."):
		h, key = sn.TTFB, strings.TrimPrefix(base, "ttfb.")
	case strings.HasPrefix(base, "total."):
		key = strings.TrimPrefix(base, "total.")
	case strings.HasPrefix(base, "all."):
		h, key = sn.allLatency(), strings.TrimPrefix(base, "all.")
	}
	us := func(v float64) value { return value{v / 1e6, "s"} }
	switch key {
	case "min":
		return us(float64(h.Min())), nil
	case "max":
		return us(float64(h.Max())), nil
	case "mean":
		return us(h.Mean()), nil
	}
	if q, ok := strings.CutPrefix(key, "p"); ok {
		f, err := strconv.ParseFloat(q, 64)
		if err == nil {
			// p99 = 99, p999 = 99.9, p9999 = 99.99
			if len(q) > 2 {
				f = f / pow10(len(q)-2)
			}
			return us(float64(h.ValueAtQuantile(f))), nil
		}
	}
	return value{}, fmt.Errorf("unknown metric %q", name)
}

func pow10(n int) float64 {
	f := 1.0
	for range n {
		f *= 10
	}
	return f
}

func (sn *Snapshot) allLatency() *hdrhistogram.Histogram {
	all := newHist()
	all.Merge(sn.Total)
	all.Merge(sn.ErrLat)
	return all
}
