package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
)

// FakeIngress is an orchestrator's ingress.v1 side, answering from the request
// path instead of running functions: for developing scenarios without a stack,
// for tests of this tool, and for measuring what the client side of the
// protocol costs with nothing behind it. The paths mean the same as in
// testdata/guest, the wasm function the real-stack tests deploy:
//
//	/status/N     answer N
//	/sleep/MS     wait, then 200
//	/bytes/N      200 with N bytes
//	/echo         200 with the request body
//	/trap         FUNCTION_TRAPPED
//	/spin         never answers: DEADLINE_EXCEEDED when the budget runs out
//	/midfail/N    the head and N bytes, then the stream fails
//	/head/N       a head with status N, which a real function cannot send (N outside 200-599)
//	anything else 200 "ok"
//
// A host starting "missing." matches no route (FUNCTION_NOT_FOUND). With
// Capacity > 0, more than that many invocations at once get NO_CAPACITY.
type FakeIngress struct {
	ingressv1connect.UnimplementedIngressServiceHandler
	MaxBody  uint64
	Capacity int64
	inflight atomic.Int64
	Seen     atomic.Int64
	Reqs     chan *ingressv1.InvokeRequest // if not nil, every request is offered here (non-blocking)
}

func (f *FakeIngress) Describe(context.Context, *connect.Request[ingressv1.DescribeRequest]) (*connect.Response[ingressv1.DescribeResponse], error) {
	return connect.NewResponse(&ingressv1.DescribeResponse{
		Implementation:      &commonv1.Implementation{Name: "trafficmock-fake", Version: "1", Protocols: []string{"edgerun.ingress.v1"}},
		MaxRequestBodyBytes: f.MaxBody,
	}), nil
}

func (f *FakeIngress) Invoke(ctx context.Context, req *connect.Request[ingressv1.InvokeRequest], st *connect.ServerStream[ingressv1.InvokeResponse]) error {
	f.Seen.Add(1)
	if f.Reqs != nil {
		select {
		case f.Reqs <- req.Msg:
		default:
		}
	}
	r := req.Msg.GetRequest()
	if uint64(len(r.GetBody())) > f.MaxBody {
		return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_REQUEST_TOO_LARGE, "body too large")
	}
	if strings.HasPrefix(r.GetHost(), "missing.") {
		return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND, "no route")
	}
	if f.Capacity > 0 {
		if f.inflight.Add(1) > f.Capacity {
			f.inflight.Add(-1)
			return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, "no free slot")
		}
		defer f.inflight.Add(-1)
	}
	head := func(status uint32, hs ...string) error {
		h := &commonv1.HttpResponseHead{Status: status}
		for i := 0; i+1 < len(hs); i += 2 {
			h.Headers = append(h.Headers, &commonv1.Header{Name: hs[i], Value: hs[i+1]})
		}
		return st.Send(&ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Head{
			Head: &ingressv1.InvokeHead{Http: h, InvocationId: "fake-" + req.Msg.GetRequestId()}}})
	}
	chunk := func(b []byte) error {
		return st.Send(&ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Chunk{Chunk: b}})
	}
	parts := strings.Split(strings.Trim(r.GetPath(), "/"), "/")
	arg := 0
	if len(parts) > 1 {
		arg, _ = strconv.Atoi(parts[1])
	}
	switch parts[0] {
	case "status":
		if err := head(uint32(arg), "content-type", "text/plain"); err != nil {
			return err
		}
		return chunk([]byte("status " + strconv.Itoa(arg) + "\n"))
	case "head":
		return head(uint32(arg))
	case "sleep":
		select {
		case <-time.After(time.Duration(arg) * time.Millisecond):
		case <-ctx.Done():
			return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, "budget spent")
		}
	case "trap":
		return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, "guest exited 3")
	case "spin":
		<-ctx.Done()
		return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, "budget spent")
	case "bytes", "midfail":
		if err := head(200, "content-type", "application/octet-stream"); err != nil {
			return err
		}
		buf := make([]byte, 16<<10)
		for left := arg; left > 0; {
			n := min(left, len(buf))
			if err := chunk(buf[:n]); err != nil {
				return err
			}
			left -= n
		}
		if parts[0] == "midfail" {
			return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_INTERNAL, "worker died mid-body")
		}
		return nil
	case "echo":
		if err := head(200, "content-type", "application/octet-stream"); err != nil {
			return err
		}
		if len(r.GetBody()) > 0 {
			return chunk(r.GetBody())
		}
		return nil
	}
	if err := head(200, "content-type", "text/plain"); err != nil {
		return err
	}
	return chunk([]byte("ok\n"))
}

// Handler serves the fake behind the ingress token.
func (f *FakeIngress) Handler(token string) http.Handler {
	mux := http.NewServeMux()
	path, h := ingressv1connect.NewIngressServiceHandler(f,
		connect.WithInterceptors(edgerun.Authenticate(edgerun.Verifier{token: edgerun.ScopeIngress}, edgerun.ScopeIngress)),
		connect.WithReadMaxBytes(int(f.MaxBody)+(256<<10)))
	mux.Handle(path, h)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	return mux
}

func fakeCmd(args []string) int {
	fs := flag.NewFlagSet("fake", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:24030", "listen address")
	token := fs.String("token", envOr("EDGERUN_INGRESS_TOKEN", "fake-ingress-token"), "ingress token to require")
	maxBody := fs.Uint64("max-body", 6<<20, "max_request_body_bytes to report")
	capacity := fs.Int64("capacity", 0, "concurrent invocations before NO_CAPACITY (0: unlimited)")
	fs.Parse(args)
	f := &FakeIngress{MaxBody: *maxBody, Capacity: *capacity}
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	srv := &http.Server{Addr: *addr, Handler: f.Handler(*token), Protocols: &p}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); srv.Shutdown(context.Background()) }()
	fmt.Fprintf(os.Stderr, "fake ingress.v1 on http://%s (token %q)\n", *addr, *token)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	return 0
}
