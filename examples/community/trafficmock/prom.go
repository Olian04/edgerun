package main

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// metricsHandler serves the live state of a run in the Prometheus text format.
func metricsHandler(st *Stats, runID string, lim *Limits, reqLogDropped func() int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sn := st.Snapshot()
		var b strings.Builder
		p := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }
		lbl := fmt.Sprintf(`run_id=%q`, runID)

		p("# HELP trafficmock_requests_released_total Requests the load model released.\n# TYPE trafficmock_requests_released_total counter\n")
		p("trafficmock_requests_released_total{%s} %d\n", lbl, sn.Sent)
		p("# HELP trafficmock_requests_total Finished requests by outcome.\n# TYPE trafficmock_requests_total counter\n")
		for _, o := range []Outcome{OutOK, OutPlatformError, OutStreamFailure, OutRejected, OutCanceled, OutDropped} {
			p("trafficmock_requests_total{%s,outcome=%q} %d\n", lbl, o, sn.Outcomes[o])
		}
		p("# HELP trafficmock_responses_total Responses by the HTTP status a client would see (0: connection aborted).\n# TYPE trafficmock_responses_total counter\n")
		sts := make([]int, 0, len(sn.Status))
		for s := range sn.Status {
			sts = append(sts, s)
		}
		sort.Ints(sts)
		for _, s := range sts {
			p("trafficmock_responses_total{%s,status=\"%d\"} %d\n", lbl, s, sn.Status[s])
		}
		p("# HELP trafficmock_platform_errors_total Failures by common.v1 ErrorReason (UNTYPED_*: no ErrorInfo; STREAM:*: after the head).\n# TYPE trafficmock_platform_errors_total counter\n")
		for _, k := range sortedKeys(sn.Reasons) {
			p("trafficmock_platform_errors_total{%s,reason=%q,http_status=\"%d\"} %d\n", lbl, k, sn.ReasonStatus[k], sn.Reasons[k])
		}
		{
			p("# HELP trafficmock_echo_checked_total Responses whose content was verified against the request.\n# TYPE trafficmock_echo_checked_total counter\ntrafficmock_echo_checked_total{%s} %d\n", lbl, sn.EchoChecked)
			p("# HELP trafficmock_echo_mismatches_total Responses that were not the response to their request, by what differed (body, header).\n# TYPE trafficmock_echo_mismatches_total counter\n")
			p("trafficmock_echo_mismatches_total{%s,kind=\"body\"} %d\n", lbl, sn.EchoBodyMismatch)
			p("trafficmock_echo_mismatches_total{%s,kind=\"header\"} %d\n", lbl, sn.EchoHdrMismat)
		}
		p("# HELP trafficmock_in_flight Requests in flight.\n# TYPE trafficmock_in_flight gauge\ntrafficmock_in_flight{%s} %d\n", lbl, sn.InFlight)
		p("# HELP trafficmock_response_bytes_total Body bytes received.\n# TYPE trafficmock_response_bytes_total counter\ntrafficmock_response_bytes_total{%s} %d\n", lbl, sn.Bytes)
		p("# HELP trafficmock_max_request_body_bytes The orchestrator's limit.\n# TYPE trafficmock_max_request_body_bytes gauge\ntrafficmock_max_request_body_bytes{%s} %d\n", lbl, lim.Max())
		if reqLogDropped != nil {
			p("# TYPE trafficmock_request_log_dropped_total counter\ntrafficmock_request_log_dropped_total{%s} %d\n", lbl, reqLogDropped())
		}
		hist := func(name, help string, h *promHist) {
			p("# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
			var cum int64
			for i, le := range promBounds {
				cum += h.buckets[i].Load()
				p("%s_bucket{%s,le=\"%g\"} %d\n", name, lbl, le, cum)
			}
			cum += h.buckets[len(promBounds)].Load()
			p("%s_bucket{%s,le=\"+Inf\"} %d\n", name, lbl, cum)
			p("%s_sum{%s} %g\n%s_count{%s} %d\n", name, lbl, time.Duration(h.sumNs.Load()).Seconds(), name, lbl, h.count.Load())
		}
		hist("trafficmock_ttfb_seconds", "Time to the response head.", &st.promTTFB)
		hist("trafficmock_total_seconds", "Time to the end of the body.", &st.promTotal)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.Write([]byte(b.String()))
	})
}

var _ atomic.Int64
