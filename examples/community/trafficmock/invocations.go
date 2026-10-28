package main

import (
	"context"
	"sync"
	"time"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// Collector remembers the requests that should have an invocation record, so
// they can be looked up through control.v1 after the run.
type Collector struct {
	mu sync.Mutex
	m  map[string]*Result
}

func NewCollector() *Collector { return &Collector{m: map[string]*Result{}} }

func (c *Collector) Add(r *Result) {
	// A request with no route has no record (docs/protocol.md); one that never
	// reached the orchestrator has none either.
	switch r.Outcome {
	case OutRejected, OutDropped, OutCanceled:
		return
	}
	if r.Reason == "FUNCTION_NOT_FOUND" || (r.Outcome == OutPlatformError && !r.Typed) {
		return
	}
	c.mu.Lock()
	c.m[r.RequestID] = r
	c.mu.Unlock()
}

type InvocationSummary struct {
	Expected   int            `json:"expected"`
	Found      int            `json:"found"`
	ByFunction map[string]int `json:"by_function"`
	ByVersion  map[string]int `json:"by_version"`
	ByWorker   map[string]int `json:"by_worker"`
	// Disagreements between what the client observed and what the record says.
	IDMismatch     int      `json:"id_mismatch"`
	StatusMismatch int      `json:"status_mismatch"`
	ReasonMismatch int      `json:"reason_mismatch"`
	Missing        []string `json:"missing_request_ids,omitempty"`
}

// ResolveInvocations scans the orchestrator's invocation records, newest
// first, for the requests of this run. There is no lookup by request id or
// invocation id in control.v1, so this is a scan of everything since the run
// began.
func ResolveInvocations(ctx context.Context, ctl *edgerun.Control, col *Collector, since time.Time, wait time.Duration) (*InvocationSummary, error) {
	want := map[string]*Result{}
	col.mu.Lock()
	for k, v := range col.m {
		want[k] = v
	}
	col.mu.Unlock()
	sum := &InvocationSummary{Expected: len(want), ByFunction: map[string]int{}, ByVersion: map[string]int{}, ByWorker: map[string]int{}}

	names := map[string]string{}
	for f, err := range ctl.AllFunctions(ctx) {
		if err != nil {
			return nil, err
		}
		names[f.Id] = f.Name
	}
	vnum := map[string]string{}
	found := map[string]bool{}
	deadline := time.Now().Add(wait)
	for {
		for rec, err := range ctl.AllInvocations(ctx, "") {
			if err != nil {
				return nil, err
			}
			if rec.StartTime != nil && rec.StartTime.AsTime().Before(since.Add(-2*time.Second)) {
				break
			}
			res, ok := want[rec.RequestId]
			if !ok || found[rec.RequestId] {
				continue
			}
			found[rec.RequestId] = true
			sum.Found++
			sum.ByFunction[names[rec.FunctionId]]++
			sum.ByWorker[rec.WorkerId]++
			if _, ok := vnum[rec.VersionId]; !ok {
				vnum[rec.VersionId] = rec.VersionId
			}
			sum.ByVersion[names[rec.FunctionId]+"@"+rec.VersionId]++
			if res.InvocationID != "" && res.InvocationID != rec.Id {
				sum.IDMismatch++
			}
			if res.Outcome == OutOK && uint32(res.FuncStatus) != rec.Status {
				sum.StatusMismatch++
			}
			if res.Outcome == OutPlatformError && res.Typed && rec.ErrorReason != reasonFromName(res.Reason) {
				sum.ReasonMismatch++
			}
		}
		if sum.Found >= len(want) || time.Now().After(deadline) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	for id := range want {
		if !found[id] && len(sum.Missing) < 20 {
			sum.Missing = append(sum.Missing, id)
		}
	}
	return sum, nil
}

func reasonFromName(n string) commonv1.ErrorReason {
	return commonv1.ErrorReason(commonv1.ErrorReason_value["ERROR_REASON_"+n])
}
