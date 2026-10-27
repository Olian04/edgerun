package e2e

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/guest"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressapp "github.com/Olian04/edgerun/internal/egress/app"
	workerapp "github.com/Olian04/edgerun/internal/worker/app"
)

const egressToken = "egress-token"

// A function reaches nothing until its operator says where. The whole path is
// real: orchestrator and its database, a worker running a wagi-2 module, and a
// network service making the request.
func TestAFunctionsNetworkIsClosedUntilItsOperatorOpensIt(t *testing.T) {
	s := newStack(t)

	var hits atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("X-Origin", "yes")
		fmt.Fprintf(w, "hello from the origin, %s %s", r.Method, r.URL.Path)
	}))
	t.Cleanup(origin.Close)
	originAddr := strings.TrimPrefix(origin.URL, "http://")

	// The network service may reach loopback (the origin is there); by default it may not.
	eg, err := egressapp.Start(ctx, egressapp.Config{
		Listen: "127.0.0.1:0", Token: egressToken, Log: quiet,
		AllowNetworks: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eg.Stop(context.Background()) })

	wctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- workerapp.Run(wctx, workerapp.Config{
			Listen: "127.0.0.1:0", OrchestratorURL: "http://" + s.addr,
			WorkerToken: workerToken, ExecutionToken: executionToken,
			EgressURL: "http://" + eg.Addr(), EgressToken: egressToken,
			MaxConcurrency: 4, DrainTimeout: 10 * time.Second, Log: quiet,
		})
	}()
	t.Cleanup(func() { cancel(); <-done })

	fid := s.createFunction("talker")
	module := guest.BuildNet(t)
	v, err := s.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{
		FunctionId: fid, Module: module, Abi: "wagi-2", Activate: true}))
	if err != nil {
		t.Fatal(err)
	}
	s.route("net.test", "/", fid)

	fetch := func() string {
		t.Helper()
		r := s.mustCall(req{host: "net.test", path: "/fetch", method: "POST", body: "GET http://" + originAddr + "/page\nAccept: text/plain\n\n"})
		return string(r.body)
	}
	eventually(t, "the wagi-2 function to run", 120*time.Second, func() bool {
		return s.call(req{host: "net.test", path: "/noop"}).err == nil
	})

	// Closed: the guest is told so, and nothing leaves.
	if got := fetch(); !strings.Contains(got, "code=-1") {
		t.Fatalf("a function nobody opened up: %q, want code=-1 (denied)", got)
	}
	if hits.Load() != 0 {
		t.Fatalf("the origin was reached %d times by a function that may reach nothing", hits.Load())
	}

	// Opened by the operator: the very next invocation reaches it.
	if _, err := s.fn.SetEgress(ctx, connect.NewRequest(&controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{originAddr}})); err != nil {
		t.Fatal(err)
	}
	got := fetch()
	if strings.Contains(got, "code=-") || !strings.Contains(got, "Status: 200") || !strings.Contains(got, "hello from the origin, GET /page") || !strings.Contains(got, "x-origin: yes") {
		t.Fatalf("an allowed request: %q", got)
	}
	if hits.Load() != 1 {
		t.Errorf("the origin was reached %d times, want 1", hits.Load())
	}

	// Another destination is still closed, though the function may reach this one.
	other := s.mustCall(req{host: "net.test", path: "/fetch", method: "POST", body: "GET http://127.0.0.1:9/\n\n"})
	if !strings.Contains(string(other.body), "code=-1") {
		t.Errorf("a destination that is not listed: %q", other.body)
	}

	// Closed again.
	if _, err := s.fn.SetEgress(ctx, connect.NewRequest(&controlv1.SetEgressRequest{FunctionId: fid})); err != nil {
		t.Fatal(err)
	}
	if got := fetch(); !strings.Contains(got, "code=-1") || hits.Load() != 1 {
		t.Errorf("after closing the function off again: %q (origin hits %d)", got, hits.Load())
	}

	// The same bytes deployed as wagi-1 are refused: they import the network.
	if _, err := s.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{
		FunctionId: fid, Module: module, Abi: "wagi-1", Activate: true})); err != nil {
		t.Fatal(err)
	}
	r := s.call(req{host: "net.test", path: "/noop"})
	if reason(r.err) != commonv1.ErrorReason_ERROR_REASON_ABI_MISMATCH {
		t.Errorf("a module that imports the network, deployed as wagi-1: %v, want ABI_MISMATCH", r.err)
	}
	_ = v
}
