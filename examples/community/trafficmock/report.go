package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

type Report struct {
	Scenario string    `json:"scenario"`
	RunID    string    `json:"run_id"`
	Started  time.Time `json:"started"`
	Mode     string    `json:"mode"`
	Target   string    `json:"target"`
	HTTP     string    `json:"http"`

	Orchestrator struct {
		Implementation    string   `json:"implementation,omitempty"`
		Protocols         []string `json:"protocols,omitempty"`
		MaxRequestBody    uint64   `json:"max_request_body_bytes"`
		LimitFromDescribe bool     `json:"limit_from_describe"`
	} `json:"orchestrator"`

	LoadSeconds    float64 `json:"load_seconds"`
	ElapsedSeconds float64 `json:"elapsed_seconds"`

	Totals struct {
		Sent           int64   `json:"sent"`
		Attempted      int64   `json:"attempted"`
		OK             int64   `json:"ok"`
		PlatformErrors int64   `json:"platform_errors"`
		StreamFailures int64   `json:"stream_failures"`
		Rejected       int64   `json:"rejected_locally"`
		Canceled       int64   `json:"canceled"`
		Dropped        int64   `json:"dropped"`
		BytesReceived  int64   `json:"bytes_received"`
		ErrorPercent   float64 `json:"error_percent"`
		RPSSent        float64 `json:"rps_sent"`
		RPSCompleted   float64 `json:"rps_completed"`
		PeakInFlight   int64   `json:"peak_in_flight"`
	} `json:"totals"`

	// Echo is present when any template verifies its responses (verify: echo).
	// A mismatch is a response that is not the one for the request it answers.
	Echo *EchoSummary `json:"echo_verification,omitempty"`

	// Status is what a client of a real frontend would have seen (the function's
	// own status, or the mapped status of a platform error). "aborted" is a
	// connection a frontend would have cut after the head.
	Status         map[string]int64      `json:"status"`
	FunctionStatus map[string]int64      `json:"function_status"`
	Reasons        map[string]ReasonStat `json:"platform_errors_by_reason"`
	DeadlineSource struct {
		Orchestrator int64 `json:"typed_by_orchestrator"`
		LocalTimer   int64 `json:"local_timer"`
	} `json:"deadline_exceeded_source"`

	Latency   map[string]LatencyStat  `json:"latency"`
	Templates map[string]TemplateStat `json:"templates"`
	Timeline  []TimelinePoint         `json:"timeline"`
	Samples   struct {
		Errors  []*Result `json:"errors"`
		Slowest []*Result `json:"slowest"`
		// Mismatches is a bounded sample (50) of the requests whose response
		// was not the right one, with what was received.
		Mismatches []*Result `json:"echo_mismatches,omitempty"`
	} `json:"samples"`

	Invocations *InvocationSummary `json:"invocations,omitempty"`
	Expect      []Expectation      `json:"expect,omitempty"`
	Warnings    []string           `json:"warnings,omitempty"`
}

type EchoSummary struct {
	Checked        int64 `json:"checked"`
	Mismatches     int64 `json:"mismatches"` // requests with any mismatch
	BodyMismatches int64 `json:"body_mismatches"`
	HdrMismatches  int64 `json:"header_mismatches"`
}

type ReasonStat struct {
	Count      int64 `json:"count"`
	HTTPStatus int   `json:"http_status"` // the frontend mapping
}

type LatencyStat struct {
	Count int64   `json:"count"`
	Min   float64 `json:"min_ms"`
	Mean  float64 `json:"mean_ms"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
	Max   float64 `json:"max_ms"`
}

type TemplateStat struct {
	Sent   int64   `json:"sent"`
	OK     int64   `json:"ok"`
	Errors int64   `json:"errors"`
	P50    float64 `json:"p50_ms"`
	P99    float64 `json:"p99_ms"`
}

func ms(us int64) float64 { return float64(us) / 1000 }

func latency(h *hdrhistogram.Histogram) LatencyStat {
	if h.TotalCount() == 0 {
		return LatencyStat{}
	}
	return LatencyStat{
		Count: h.TotalCount(), Min: ms(h.Min()), Mean: h.Mean() / 1000, P50: ms(h.ValueAtQuantile(50)),
		P90: ms(h.ValueAtQuantile(90)), P95: ms(h.ValueAtQuantile(95)), P99: ms(h.ValueAtQuantile(99)),
		P999: ms(h.ValueAtQuantile(99.9)), Max: ms(h.Max()),
	}
}

// BuildReport turns a snapshot into the report.
func BuildReport(sc *Scenario, runID, mode, target, httpMode string, started time.Time, load, elapsed time.Duration, sn *Snapshot, lim *Limits) *Report {
	r := &Report{Scenario: sc.Name, RunID: runID, Started: started, Mode: mode, Target: target, HTTP: httpMode,
		LoadSeconds: load.Seconds(), ElapsedSeconds: elapsed.Seconds()}
	if p := lim.Impl.Load(); p != nil {
		r.Orchestrator.Implementation = *p
	}
	if p := lim.Protos.Load(); p != nil {
		r.Orchestrator.Protocols = *p
	}
	r.Orchestrator.MaxRequestBody = lim.Max()
	r.Orchestrator.LimitFromDescribe = lim.Known()
	t := &r.Totals
	t.Sent, t.Attempted = sn.Sent, sn.Attempted()
	t.OK, t.PlatformErrors, t.StreamFailures = sn.Outcomes[OutOK], sn.Outcomes[OutPlatformError], sn.Outcomes[OutStreamFailure]
	t.Rejected, t.Canceled, t.Dropped = sn.Outcomes[OutRejected], sn.Outcomes[OutCanceled], sn.Outcomes[OutDropped]
	t.BytesReceived, t.PeakInFlight = sn.Bytes, sn.PeakInFlight
	if t.Attempted > 0 {
		t.ErrorPercent = 100 * float64(sn.ErrorCount()) / float64(t.Attempted)
	}
	if load > 0 {
		t.RPSSent = float64(sn.Sent) / load.Seconds()
	}
	if elapsed > 0 {
		t.RPSCompleted = float64(t.Attempted) / elapsed.Seconds()
	}
	r.Status, r.FunctionStatus = map[string]int64{}, map[string]int64{}
	for st, n := range sn.Status {
		if st == 0 {
			r.Status["aborted"] = n
		} else {
			r.Status[itoa(st)] = n
		}
	}
	for st, n := range sn.FuncStatus {
		if st != 0 {
			r.FunctionStatus[itoa(st)] = n
		}
	}
	r.Reasons = map[string]ReasonStat{}
	for k, n := range sn.Reasons {
		r.Reasons[k] = ReasonStat{Count: n, HTTPStatus: sn.ReasonStatus[k]}
	}
	r.DeadlineSource.Orchestrator, r.DeadlineSource.LocalTimer = sn.DeadlineTyped, sn.DeadlineLocal
	r.Latency = map[string]LatencyStat{
		"ttfb": latency(sn.TTFB), "total": latency(sn.Total), "errors": latency(sn.ErrLat), "send_lag": latency(sn.Lag),
	}
	r.Templates = map[string]TemplateStat{}
	for k, v := range sn.Templates {
		ls := latency(v.Total)
		r.Templates[k] = TemplateStat{Sent: v.Sent, OK: v.OK, Errors: v.Errors, P50: ls.P50, P99: ls.P99}
	}
	r.Timeline = sn.Timeline
	r.Samples.Errors, r.Samples.Slowest = sn.Errors, sn.Slowest
	if sn.EchoChecked > 0 {
		r.Echo = &EchoSummary{Checked: sn.EchoChecked, Mismatches: sn.EchoMismatches, BodyMismatches: sn.EchoBodyMismatch, HdrMismatches: sn.EchoHdrMismat}
		r.Samples.Mismatches = sn.Mismatched
		if sn.EchoMismatches > 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf("%d of %d verified responses were NOT the response to their request (see the echo mismatches)", sn.EchoMismatches, sn.EchoChecked))
		}
	}
	return r
}

func (r *Report) JSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

func sortedKeys[V any](m map[string]V) []string {
	k := make([]string, 0, len(m))
	for x := range m {
		k = append(k, x)
	}
	sort.Strings(k)
	return k
}

// Text writes the human summary.
func (r *Report) Text(w io.Writer) {
	t := r.Totals
	fmt.Fprintf(w, "scenario %s  run %s  (%s) -> %s  [http %s]\n", r.Scenario, r.RunID, r.Mode, r.Target, r.HTTP)
	if r.Orchestrator.Implementation != "" {
		fmt.Fprintf(w, "orchestrator %s, request body limit %s (%s)\n", strings.TrimSpace(r.Orchestrator.Implementation),
			byteStr(r.Orchestrator.MaxRequestBody), map[bool]string{true: "from Describe", false: "assumed"}[r.Orchestrator.LimitFromDescribe])
	}
	fmt.Fprintf(w, "load %.2fs, elapsed %.2fs\n\n", r.LoadSeconds, r.ElapsedSeconds)
	fmt.Fprintf(w, "requests   sent %d   attempted %d   ok %d   platform errors %d   stream failures %d\n", t.Sent, t.Attempted, t.OK, t.PlatformErrors, t.StreamFailures)
	if t.Rejected+t.Canceled+t.Dropped > 0 {
		fmt.Fprintf(w, "           rejected locally (413) %d   canceled %d   dropped (too many in flight) %d\n", t.Rejected, t.Canceled, t.Dropped)
	}
	fmt.Fprintf(w, "rate       %.1f req/s released, %.1f req/s completed   errors %.3f%%   %s received\n\n", t.RPSSent, t.RPSCompleted, t.ErrorPercent, byteStr(uint64(t.BytesReceived)))

	fmt.Fprintf(w, "latency (ms)            n      min     mean      p50      p90      p95      p99    p99.9      max\n")
	for _, k := range []string{"ttfb", "total", "errors", "send_lag"} {
		l := r.Latency[k]
		name := map[string]string{"ttfb": "time to head", "total": "total (to body end)", "errors": "failed requests", "send_lag": "send lag (intended)"}[k]
		fmt.Fprintf(w, "%-20s %6d %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f\n", name, l.Count, l.Min, l.Mean, l.P50, l.P90, l.P95, l.P99, l.P999, l.Max)
	}

	fmt.Fprintf(w, "\nstatus as a client would see it\n")
	keys := sortedKeys(r.Status)
	for _, k := range keys {
		fmt.Fprintf(w, "  %-8s %d\n", k, r.Status[k])
	}
	if len(r.Reasons) > 0 {
		fmt.Fprintf(w, "\nplatform errors by reason (frontend mapping)\n")
		for _, k := range sortedKeys(r.Reasons) {
			rs := r.Reasons[k]
			st := strconv.Itoa(rs.HTTPStatus)
			if rs.HTTPStatus == 0 {
				st = "connection aborted"
			}
			fmt.Fprintf(w, "  %-28s %6d  -> %s\n", k, rs.Count, st)
		}
		if r.DeadlineSource.LocalTimer > 0 {
			fmt.Fprintf(w, "  (DEADLINE_EXCEEDED: %d typed by the orchestrator, %d from this tool's own timer)\n", r.DeadlineSource.Orchestrator, r.DeadlineSource.LocalTimer)
		}
	}
	if e := r.Echo; e != nil {
		fmt.Fprintf(w, "\nresponse verification: %d checked, %d mismatches (body %d, header %d)\n", e.Checked, e.Mismatches, e.BodyMismatches, e.HdrMismatches)
		for i, m := range r.Samples.Mismatches {
			if i == 10 {
				fmt.Fprintf(w, "  ... and %d more in the JSON report\n", len(r.Samples.Mismatches)-i)
				break
			}
			fmt.Fprintf(w, "  %s  invocation %s  %s: %s\n", m.RequestID, m.InvocationID, m.Verify, m.VerifyDetail)
		}
	}
	if len(r.Templates) > 1 {
		fmt.Fprintf(w, "\nper template\n")
		for _, k := range sortedKeys(r.Templates) {
			t := r.Templates[k]
			fmt.Fprintf(w, "  %-20s sent %-7d ok %-7d errors %-6d p50 %.2fms p99 %.2fms\n", k, t.Sent, t.OK, t.Errors, t.P50, t.P99)
		}
	}
	if r.Invocations != nil {
		fmt.Fprintf(w, "\ninvocation records (control.v1): %d of %d found\n", r.Invocations.Found, r.Invocations.Expected)
		for k, n := range r.Invocations.LocalDeadlineRecordedAs {
			fmt.Fprintf(w, "  %d request(s) ended by this tool's own deadline are recorded with reason %s\n", n, k)
		}
		for _, k := range sortedKeys(r.Invocations.ByWorker) {
			fmt.Fprintf(w, "  worker %-38s %d\n", k, r.Invocations.ByWorker[k])
		}
		for _, k := range sortedKeys(r.Invocations.ByVersion) {
			fmt.Fprintf(w, "  version %-37s %d\n", k, r.Invocations.ByVersion[k])
		}
	}
	for _, wn := range r.Warnings {
		fmt.Fprintf(w, "\nwarning: %s\n", wn)
	}
	if len(r.Expect) > 0 {
		fmt.Fprintf(w, "\nexpectations\n")
		for _, e := range r.Expect {
			switch {
			case e.Error != "":
				fmt.Fprintf(w, "  ERROR %-28s %s\n", e.Expr, e.Error)
			case e.Pass:
				fmt.Fprintf(w, "  PASS  %-28s (%s)\n", e.Expr, fmtVal(e))
			default:
				fmt.Fprintf(w, "  FAIL  %-28s (%s)\n", e.Expr, fmtVal(e))
			}
		}
	}
}

func fmtVal(e Expectation) string {
	switch e.Unit {
	case "s":
		return fmt.Sprintf("%.3fms", e.Value*1000)
	case "%":
		return fmt.Sprintf("%.4g%%", e.Value)
	}
	return fmt.Sprintf("%.6g", e.Value)
}

func byteStr(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}
