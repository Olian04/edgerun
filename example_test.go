package edgerun_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"

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

// Be a network service of your own: say what you carry, keep saying that you
// are there, and leave properly. Workers then reach it through the same
// orchestrators as every other service.
//
// This is the whole lifecycle, and it is deliberately not a library function:
// what to do when a registration is refused, how long to go on answering while
// draining, and how to count what is in flight are the service's decisions.
func ExampleRegistry() {
	reg := edgerun.NewRegistry("http://orchestrator:8080", os.Getenv("EDGERUN_NETWORK_TOKEN"))
	info := &egressv1.ServiceInfo{
		Implementation: &commonv1.Implementation{Name: "my-proxy", Version: "1"},
		Covers:         []string{"*.corp.example.com", "10.0.0.0/8"}, // canonical: lower-case, sorted
	}
	endpoint := "http://10.0.0.7:9100" // where workers reach this service

	var draining atomic.Bool // set when the process is asked to stop
	stop := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, os.Interrupt)
		<-sig
		draining.Store(true)
		close(stop)
	}()
	state := func() egressv1.ServiceState {
		if draining.Load() {
			return egressv1.ServiceState_SERVICE_STATE_DRAINING
		}
		return egressv1.ServiceState_SERVICE_STATE_READY
	}
	ctx := context.Background()

	// Register, retrying with a growing pause: the orchestrator may not be up yet.
	register := func() (id string, every time.Duration) {
		for pause := 500 * time.Millisecond; ; pause = min(pause*2, 30*time.Second) {
			r, err := reg.Register(ctx, &egressv1.RegisterRequest{Endpoint: endpoint, Info: info})
			if err == nil {
				return r.ServiceId, r.HeartbeatInterval.AsDuration()
			}
			if connect.CodeOf(err) == connect.CodeInvalidArgument {
				log.Fatalf("the orchestrator refuses this registration, which retrying will not change: %v", err)
			}
			log.Printf("registering: %v; trying again in %v", err, pause)
			select {
			case <-time.After(pause):
			case <-stop:
				return "", 0
			}
		}
	}

	id, every := register()
	tick := time.NewTicker(every)
	defer tick.Stop()
	for id != "" {
		select {
		case <-tick.C:
		case <-stop:
			// Say so at once, then go on answering for a heartbeat interval: grants are
			// made when an invocation starts, so older invocations still call.
			reg.Heartbeat(ctx, &egressv1.HeartbeatRequest{ServiceId: id, State: state()})
			time.Sleep(every)
			// ... wait here for the requests in flight to finish, then:
			reg.Deregister(ctx, &egressv1.DeregisterRequest{ServiceId: id})
			return
		}
		hb, err := reg.Heartbeat(ctx, &egressv1.HeartbeatRequest{ServiceId: id, State: state()})
		switch {
		case err != nil:
			log.Printf("heartbeat: %v", err) // transient: the next one will tell
		case hb.Reregister && !draining.Load():
			// The orchestrator forgot us (it restarted, or we were too quiet).
			if id, every = register(); id != "" {
				tick.Reset(every)
			}
		}
	}
}
