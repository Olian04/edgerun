package authn_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"

	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	"github.com/Olian04/edgerun/internal/authn"
)

type stub struct {
	workerv1connect.UnimplementedExecutionServiceHandler
}

func (stub) Describe(context.Context, *connect.Request[workerv1.DescribeRequest]) (*connect.Response[workerv1.DescribeResponse], error) {
	return connect.NewResponse(&workerv1.DescribeResponse{}), nil
}

func (stub) Execute(context.Context, *connect.Request[workerv1.ExecuteRequest], *connect.ServerStream[workerv1.ExecuteResponse]) error {
	return nil
}

func TestScopes(t *testing.T) {
	v := authn.Verifier{"exec-secret": "execute", "other-secret": "ingress"}
	mux := http.NewServeMux()
	mux.Handle(workerv1connect.NewExecutionServiceHandler(stub{},
		connect.WithInterceptors(authn.Server(v, "execute"))))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cases := []struct {
		name  string
		token string // "" means: send no credentials at all
		want  connect.Code
	}{
		{"valid", "exec-secret", 0},
		{"none", "", connect.CodeUnauthenticated},
		{"unknown", "nope", connect.CodeUnauthenticated},
		{"wrong scope", "other-secret", connect.CodePermissionDenied},
	}
	for _, tc := range cases {
		var opts []connect.ClientOption
		if tc.token != "" {
			opts = append(opts, connect.WithInterceptors(authn.Client(tc.token)))
		}
		c := workerv1connect.NewExecutionServiceClient(srv.Client(), srv.URL, opts...)

		// Unary path.
		_, err := c.Describe(context.Background(), connect.NewRequest(&workerv1.DescribeRequest{}))
		check(t, tc.name+"/unary", err, tc.want)

		// Server-streaming path goes through different interceptor hooks.
		st, err := c.Execute(context.Background(), connect.NewRequest(&workerv1.ExecuteRequest{}))
		if err == nil {
			for st.Receive() {
			}
			err = st.Err()
			st.Close()
		}
		check(t, tc.name+"/stream", err, tc.want)
	}
}

func check(t *testing.T, name string, err error, want connect.Code) {
	t.Helper()
	if want == 0 {
		if err != nil {
			t.Errorf("%s: unexpected error %v", name, err)
		}
		return
	}
	if got := connect.CodeOf(err); got != want {
		t.Errorf("%s: code = %v (err %v), want %v", name, got, err, want)
	}
}
