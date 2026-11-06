package orchsuite

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/gen/edgerun/control/v1/controlv1connect"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

// Records judges the orchestrator as the host of control.v1 RecordsService: the
// record of every invocation that was routed, listed, paged and looked up by the
// id a response or a failure carries. The suite makes the invocations through
// Ingress with scripted fake workers, then plays the operator reading them.
func Records(t *testing.T, target Target) {
	h := start(t, target)
	t.Run("Authentication", h.recordsAuthentication)
	t.Run("InvocationRecords", h.invocationRecords)
	t.Run("InvocationLookup", h.invocationLookup)
	t.Run("Pagination", h.recordsPagination)
}

func (h *harness) recordsAuthentication(t *testing.T) {
	check.Authentication(t, h.scopes(), "admin", []string{h.env.ExecutionToken}, func(token string) error {
		ctx, cancel := ctxT(settle)
		defer cancel()
		_, err := controlv1connect.NewRecordsServiceClient(http.DefaultClient, h.url, check.Bearer(token)).ListInvocations(ctx, connect.NewRequest(&controlv1.ListInvocationsRequest{PageSize: 1}))
		return err
	})
}

func (h *harness) recordsPagination(t *testing.T) {
	ctx, cancel := ctxT(2 * settle)
	defer cancel()
	t.Run("malformed tokens", func(t *testing.T) {
		_, err := h.rec.ListInvocations(ctx, connect.NewRequest(&controlv1.ListInvocationsRequest{PageToken: "!!not a token!!"}))
		check.WantCode(t, err, connect.CodeInvalidArgument)
	})
}

func (h *harness) records(t testing.TB, fnID string, pageSize uint32) []*controlv1.Invocation {
	t.Helper()
	ctx, cancel := ctxT(2 * settle)
	defer cancel()
	var out []*controlv1.Invocation
	tok := ""
	for i := 0; i < 10000; i++ {
		r, err := h.rec.ListInvocations(ctx, connect.NewRequest(&controlv1.ListInvocationsRequest{FunctionId: fnID, PageSize: pageSize, PageToken: tok}))
		if err != nil {
			t.Fatalf("ListInvocations: %v", err)
		}
		out = append(out, r.Msg.GetInvocations()...)
		if tok = r.Msg.GetNextPageToken(); tok == "" {
			return out
		}
	}
	t.Fatal("ListInvocations never ended")
	return nil
}

func (h *harness) invocationRecords(t *testing.T) {
	trapMsg := "trapped: exit status 3"
	w, f := h.stack(t, func(ctx context.Context, req *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
		switch req.GetRequest().GetPath() {
		case "/trap":
			return check.Error(commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, trapMsg)
		default:
			return respondWith(201, "made", "a log line", nil)(ctx, req, st)
		}
	})
	h.served(t, f.host)

	r := h.mustInvoke(t, call{method: "POST", host: f.host, path: "/made", requestID: "rec-ok-" + h.run})
	bad := h.invoke(t, call{host: f.host, path: "/trap", requestID: "rec-bad-" + h.run})
	check.WantReason(t, bad.err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED)
	// A request nothing routes leaves no record.
	h.invoke(t, call{host: "unrouted-" + h.run + ".conformance.test", requestID: "rec-none-" + h.run})

	find := func(requestID string) *controlv1.Invocation {
		for _, rec := range h.records(t, f.fn.GetId(), 0) {
			if rec.GetRequestId() == requestID {
				return rec
			}
		}
		return nil
	}
	var ok, failed *controlv1.Invocation
	check.Eventually(t, "the invocation records to appear", settle, func() bool {
		ok, failed = find("rec-ok-"+h.run), find("rec-bad-"+h.run)
		return ok != nil && failed != nil
	})

	t.Run("a successful invocation", func(t *testing.T) {
		if ok.GetId() != r.invocationID {
			t.Errorf("id = %q, want the invocation id the frontend got (%q)", ok.GetId(), r.invocationID)
		}
		if ok.GetFunctionId() != f.fn.GetId() || ok.GetVersionId() != f.version.GetId() || ok.GetWorkerId() != w.workerID() {
			t.Errorf("function/version/worker = %s/%s/%s, want %s/%s/%s", ok.GetFunctionId(), ok.GetVersionId(), ok.GetWorkerId(),
				f.fn.GetId(), f.version.GetId(), w.workerID())
		}
		if ok.GetMethod() != "POST" || ok.GetPath() != "/made" || ok.GetStatus() != 201 {
			t.Errorf("%s %s -> %d", ok.GetMethod(), ok.GetPath(), ok.GetStatus())
		}
		if ok.GetErrorReason() != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
			t.Errorf("error_reason = %v on a success", ok.GetErrorReason())
		}
		if string(ok.GetStderr()) != "a log line" || ok.GetDurationMicros() != 1234 || ok.GetPeakMemoryBytes() != 1<<20 {
			t.Errorf("stderr %q, duration %d, peak memory %d: want what the worker's summary reported", ok.GetStderr(), ok.GetDurationMicros(), ok.GetPeakMemoryBytes())
		}
		if ok.GetStartTime() == nil || time.Since(ok.GetStartTime().AsTime()) > time.Minute {
			t.Errorf("start_time = %v", ok.GetStartTime())
		}
	})

	t.Run("a failed invocation", func(t *testing.T) {
		if failed.GetErrorReason() != commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED || failed.GetStatus() != 0 {
			t.Errorf("reason %v, status %d: want FUNCTION_TRAPPED and no status (no response was produced)", failed.GetErrorReason(), failed.GetStatus())
		}
		if !strings.Contains(string(failed.GetStderr()), trapMsg) {
			t.Errorf("stderr = %q: a failure with no summary records the error's message", failed.GetStderr())
		}
	})

	t.Run("unrouted requests leave no record", func(t *testing.T) {
		for _, rec := range h.records(t, "", 0) {
			if rec.GetRequestId() == "rec-none-"+h.run {
				t.Error("a request that matched no function was recorded")
			}
		}
	})

	t.Run("newest first, paged", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			h.mustInvoke(t, call{host: f.host, path: "/more"})
		}
		check.Eventually(t, "six records", settle, func() bool { return len(h.records(t, f.fn.GetId(), 0)) >= 6 })
		all := h.records(t, f.fn.GetId(), 0)
		for _, size := range []uint32{1, 2, 4} {
			paged := h.records(t, f.fn.GetId(), size)
			if len(paged) != len(all) {
				t.Fatalf("page size %d returned %d records, want %d", size, len(paged), len(all))
			}
			seen := map[string]bool{}
			for i, rec := range paged {
				if seen[rec.GetId()] {
					t.Errorf("page size %d: record %s twice", size, rec.GetId())
				}
				seen[rec.GetId()] = true
				if rec.GetId() != all[i].GetId() {
					t.Errorf("page size %d: record %d differs from the unpaged order", size, i)
				}
			}
		}
		for i := 1; i < len(all); i++ {
			if all[i].GetStartTime().AsTime().After(all[i-1].GetStartTime().AsTime()) {
				t.Error("records are not newest first")
			}
		}
		// Every function's records are in the unfiltered list too.
		if got := len(h.records(t, "", 500)); got < len(all) {
			t.Errorf("the unfiltered list has %d records, fewer than one function's %d", got, len(all))
		}
	})
}

// invocationLookup: from a response, or an error, to the record of its invocation.
func (h *harness) invocationLookup(t *testing.T) {
	_, f := h.stack(t, func(ctx context.Context, req *workerv1.ExecuteRequest, st *connect.ServerStream[workerv1.ExecuteResponse]) error {
		if req.GetRequest().GetPath() == "/trap" {
			return check.Error(commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, "trapped")
		}
		return respond("ok")(ctx, req, st)
	})
	h.served(t, f.host)

	good := h.mustInvoke(t, call{host: f.host, requestID: "lk-ok-" + h.run})
	bad := h.invoke(t, call{host: f.host, path: "/trap", requestID: "lk-bad-" + h.run})
	check.WantReason(t, bad.err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED)
	unrouted := h.invoke(t, call{host: "unrouted-" + h.run + ".conformance.test"})
	check.WantReason(t, unrouted.err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)

	errInvocation := func(err error) string {
		var ce *connect.Error
		if !errors.As(err, &ce) {
			return ""
		}
		for _, d := range ce.Details() {
			if v, verr := d.Value(); verr == nil {
				if info, ok := v.(*commonv1.ErrorInfo); ok {
					return info.GetInvocationId()
				}
			}
		}
		return ""
	}
	get := func(id string) (*controlv1.Invocation, error) {
		ctx, cancel := ctxT(settle)
		defer cancel()
		r, err := h.rec.GetInvocation(ctx, connect.NewRequest(&controlv1.GetInvocationRequest{InvocationId: id}))
		if err != nil {
			return nil, err
		}
		return r.Msg.GetInvocation(), nil
	}

	t.Run("an error names the invocation that failed", func(t *testing.T) {
		id := errInvocation(bad.err)
		if id == "" {
			t.Fatal("an Invoke error for a routed invocation carries no invocation_id in its ErrorInfo")
		}
		var rec *controlv1.Invocation
		check.Eventually(t, "the record to be readable", settle, func() bool {
			var err error
			rec, err = get(id)
			return err == nil
		})
		if rec.GetId() != id || rec.GetRequestId() != "lk-bad-"+h.run || rec.GetErrorReason() != commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED {
			t.Errorf("the record the id leads to: %v", rec)
		}
	})

	t.Run("an error that has no invocation names none", func(t *testing.T) {
		if id := errInvocation(unrouted.err); id != "" {
			t.Errorf("FUNCTION_NOT_FOUND carries invocation_id %q: no record is written for it", id)
		}
	})

	t.Run("a response names its invocation, and so does its record", func(t *testing.T) {
		var rec *controlv1.Invocation
		check.Eventually(t, "the record to be readable", settle, func() bool {
			var err error
			rec, err = get(good.invocationID)
			return err == nil
		})
		if rec.GetId() != good.invocationID || rec.GetRequestId() != "lk-ok-"+h.run || rec.GetFunctionId() != f.fn.GetId() || rec.GetStatus() != 200 {
			t.Errorf("the record: %v", rec)
		}
	})

	t.Run("nothing else is found", func(t *testing.T) {
		for _, id := range []string{"00000000-0000-0000-0000-000000000000", "not-an-id", ""} {
			_, err := get(id)
			checkCode(t, err, connect.CodeNotFound, fmt.Sprintf("GetInvocation(%q)", id))
		}
	})

	t.Run("the list can be asked for one request id", func(t *testing.T) {
		list := func(req *controlv1.ListInvocationsRequest) []*controlv1.Invocation {
			t.Helper()
			var out []*controlv1.Invocation
			for {
				ctx, cancel := ctxT(2 * settle)
				r, err := h.rec.ListInvocations(ctx, connect.NewRequest(req))
				cancel()
				if err != nil {
					t.Fatalf("ListInvocations: %v", err)
				}
				out = append(out, r.Msg.GetInvocations()...)
				if req.PageToken = r.Msg.GetNextPageToken(); req.PageToken == "" {
					return out
				}
			}
		}
		// Three invocations share one request id (a client may send the same X-Request-Id more than once).
		shared := "lk-shared-" + h.run
		for i := 0; i < 3; i++ {
			h.mustInvoke(t, call{host: f.host, requestID: shared})
		}
		check.Eventually(t, "the three records", settle, func() bool {
			return len(list(&controlv1.ListInvocationsRequest{RequestId: shared})) == 3
		})
		paged := list(&controlv1.ListInvocationsRequest{RequestId: shared, PageSize: 1})
		if len(paged) != 3 {
			t.Errorf("paged one at a time: %d records, want 3", len(paged))
		}
		for _, r := range paged {
			if r.GetRequestId() != shared {
				t.Errorf("a record of request %q in the list for %q", r.GetRequestId(), shared)
			}
		}
		if got := list(&controlv1.ListInvocationsRequest{RequestId: "lk-ok-" + h.run}); len(got) != 1 || got[0].GetId() != good.invocationID {
			t.Errorf("one request id, one record: %v", got)
		}
		if got := list(&controlv1.ListInvocationsRequest{RequestId: "lk-nobody-" + h.run}); len(got) != 0 {
			t.Errorf("an unknown request id: %v", got)
		}
		if got := list(&controlv1.ListInvocationsRequest{RequestId: shared, FunctionId: "00000000-0000-0000-0000-000000000000"}); len(got) != 0 {
			t.Errorf("both filters must hold: %v", got)
		}
		if got := list(&controlv1.ListInvocationsRequest{RequestId: shared, FunctionId: f.fn.GetId()}); len(got) != 3 {
			t.Errorf("both filters, both true: %d records", len(got))
		}
	})
}
