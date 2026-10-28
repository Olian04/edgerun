package edgerun_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

// Deploy a module and route a host to it, the way an operator would.
func ExampleControl() {
	ctx := context.Background()
	ctl := edgerun.NewControl("http://orchestrator:8080", os.Getenv("EDGERUN_ADMIN_TOKEN"))

	module, err := os.ReadFile("hello.wasm")
	if err != nil {
		log.Fatal(err)
	}
	fn, err := ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "hello"})
	if err != nil {
		log.Fatal(err)
	}
	if _, err := ctl.CreateVersion(ctx, &controlv1.CreateVersionRequest{
		FunctionId: fn.Function.Id, Module: module, Abi: "wagi-1", Activate: true,
	}); err != nil {
		log.Fatal(err)
	}
	if _, err := ctl.CreateRoute(ctx, &controlv1.CreateRouteRequest{
		HostPattern: "hello.example.com", PathPrefix: "/", FunctionId: fn.Function.Id,
	}); err != nil {
		log.Fatal(err)
	}
}

// Walk a paged list without handling page tokens.
func ExampleControl_AllInvocations() {
	ctl := edgerun.NewControl("http://orchestrator:8080", os.Getenv("EDGERUN_ADMIN_TOKEN"))
	for inv, err := range ctl.AllInvocations(context.Background(), "") {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(inv.Id, inv.Status)
	}
}

// Be a frontend: hand a request to the orchestrator and stream the answer.
func ExampleIngress_Invoke() {
	ing := edgerun.NewIngress("http://orchestrator:8080", os.Getenv("EDGERUN_INGRESS_TOKEN"))

	// The deadline is the invocation's wall-clock budget, and the context must
	// outlive the body: cancelling it ends the response.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{
		Method: "GET", Host: "hello.example.com", Path: "/",
	}})
	switch {
	case edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND):
		fmt.Println("no function serves that host")
		return
	case edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY):
		fmt.Println("busy: try again")
		return
	case err != nil:
		log.Fatal(err)
	}
	defer res.Body.Close()

	fmt.Println(res.Status(), res.Header().Get("Content-Type"), "invocation", res.ID)
	if _, err := io.Copy(os.Stdout, res.Body); err != nil {
		// The body failed after the head: what was read is not the whole answer.
		log.Fatal(err)
	}
}

// Be a network service of your own: say what you carry, and keep saying that
// you are there. Workers then reach it through the same orchestrators as every
// other service.
func ExampleRegistry() {
	ctx := context.Background()
	reg := edgerun.NewRegistry("http://orchestrator:8080", os.Getenv("EDGERUN_NETWORK_TOKEN"))

	r, err := reg.Register(ctx, &egressv1.RegisterRequest{
		Endpoint: "http://10.0.0.7:9100", // where workers reach this service
		Info: &egressv1.ServiceInfo{
			Implementation: &commonv1.Implementation{Name: "my-proxy", Version: "1"},
			Covers:         []string{"*.corp.example.com", "10.0.0.0/8"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	for range time.Tick(r.HeartbeatInterval.AsDuration()) {
		hb, err := reg.Heartbeat(ctx, &egressv1.HeartbeatRequest{ServiceId: r.ServiceId, State: egressv1.ServiceState_SERVICE_STATE_READY})
		if err != nil {
			log.Print(err)
			continue
		}
		if hb.Reregister {
			// The orchestrator forgot us (it restarted, or we were too quiet).
			return // a real service registers again here
		}
	}
}
