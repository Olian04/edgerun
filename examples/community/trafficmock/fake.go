package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
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
//	/echo         200 with the request body, and the request id in the header x-echo-request-id
//	/echo-with-id 200 with the request id, a newline and the request body (and the same header)
//	/nohead       the stream ends without a head (a protocol violation)
//	/trap         FUNCTION_TRAPPED
//	/canceled     a "cancelled" error with no reason, though the caller did not cancel (what a killed worker has produced)
//	/spin         never answers: DEADLINE_EXCEEDED when the budget runs out
//	/midfail/N    the head and N bytes, then the stream fails
//	/drip/MS      the head, then a 1 KiB chunk every MS milliseconds until the budget ends or the caller goes away
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
	Sent     atomic.Int64                  // body bytes the fake has handed to the transport
	Reqs     chan *ingressv1.InvokeRequest // if not nil, every request is offered here (non-blocking)
	// Corrupt, if set, sees the body /echo is about to send (and the request id
	// it answers) and returns what to send instead: a way to misdeliver on purpose,
	// to test that verification catches it.
	Corrupt func(reqID string, body []byte) []byte
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
			h.Headers = append(h.Headers, &commonv1.Header{Name: hs[i], Value: []byte(hs[i+1])})
		}
		return st.Send(&ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Head{
			Head: &ingressv1.InvokeHead{Http: h, InvocationId: "fake-" + req.Msg.GetRequestId()}}})
	}
	chunk := func(b []byte) error {
		f.Sent.Add(int64(len(b)))
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
	case "nohead":
		return nil // a stream that ends cleanly without a head
	case "canceled":
		return connect.NewError(connect.CodeCanceled, errors.New("request cancelled"))
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
		rand.NewChaCha8([32]byte{1}).Read(buf) // incompressible: gzip would hide a slow reader
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
	case "drip":
		if err := head(200, "content-type", "application/octet-stream"); err != nil {
			return err
		}
		tick := time.NewTicker(time.Duration(max(arg, 1)) * time.Millisecond)
		defer tick.Stop()
		buf := make([]byte, 1024)
		for {
			select {
			case <-tick.C:
				if err := chunk(buf); err != nil {
					return err
				}
			case <-ctx.Done():
				return edgerun.NewError(commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, "budget spent")
			}
		}
	case "echo", "echo-with-id":
		if err := head(200, "content-type", "application/octet-stream", "x-echo-request-id", req.Msg.GetRequestId()); err != nil {
			return err
		}
		out := r.GetBody()
		if parts[0] == "echo-with-id" {
			out = append([]byte(req.Msg.GetRequestId()+"\n"), out...)
		}
		if f.Corrupt != nil {
			out = f.Corrupt(req.Msg.GetRequestId(), out)
		}
		if len(out) > 0 {
			return chunk(out)
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
