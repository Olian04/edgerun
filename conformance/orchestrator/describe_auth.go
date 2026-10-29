package orchestratorconf

import (
	"strings"
	"testing"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/gen/edgerun/control/v1/controlv1connect"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/egress/v1/egressv1connect"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
	"net/http"
)

func (h *harness) describe(t *testing.T) {
	ctx, cancel := ctxT(settle)
	defer cancel()

	ing, err := h.ing.Describe(ctx, connect.NewRequest(&ingressv1.DescribeRequest{}))
	if err != nil {
		t.Fatalf("ingress Describe: %v", err)
	}
	if ing.Msg.GetImplementation().GetName() == "" {
		t.Error("ingress: the implementation has no name")
	}
	if !contains(ing.Msg.GetImplementation().GetProtocols(), "edgerun.ingress.v1") {
		t.Errorf("ingress: protocols %v do not include edgerun.ingress.v1", ing.Msg.GetImplementation().GetProtocols())
	}
	if ing.Msg.GetMaxRequestBodyBytes() == 0 {
		t.Error("ingress: max_request_body_bytes must be reported (frontends enforce it)")
	}

	ctl, err := h.insp.Describe(ctx, connect.NewRequest(&controlv1.DescribeRequest{}))
	if err != nil {
		t.Fatalf("control Describe: %v", err)
	}
	if ctl.Msg.GetImplementation().GetName() == "" {
		t.Error("control: the implementation has no name")
	}
	if !contains(ctl.Msg.GetImplementation().GetProtocols(), "edgerun.control.v1") {
		t.Errorf("control: protocols %v do not include edgerun.control.v1", ctl.Msg.GetImplementation().GetProtocols())
	}
	if ctl.Msg.GetMaxModuleBytes() == 0 {
		t.Error("control: max_module_bytes must be reported (uploads are bounded by it)")
	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// authentication walks every service with every credential. A missing or
// unknown token is UNAUTHENTICATED; a token that is valid for another seam is
// PERMISSION_DENIED; the right one gets through.
func (h *harness) authentication(t *testing.T) {
	type seam struct {
		name  string
		token string // the one that must work
		call  func(token string) error
	}
	hc := http.DefaultClient
	ingress := func(token string) error {
		ctx, cancel := ctxT(settle)
		defer cancel()
		_, err := ingressv1connect.NewIngressServiceClient(hc, h.url, check.Bearer(token)).Describe(ctx, connect.NewRequest(&ingressv1.DescribeRequest{}))
		return err
	}
	controlCalls := map[string]func(token string) error{
		"InspectionService": func(token string) error {
			ctx, cancel := ctxT(settle)
			defer cancel()
			_, err := controlv1connect.NewInspectionServiceClient(hc, h.url, check.Bearer(token)).Describe(ctx, connect.NewRequest(&controlv1.DescribeRequest{}))
			return err
		},
		"FunctionService": func(token string) error {
			ctx, cancel := ctxT(settle)
			defer cancel()
			_, err := controlv1connect.NewFunctionServiceClient(hc, h.url, check.Bearer(token)).ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{}))
			return err
		},
		"PlanService": func(token string) error {
			ctx, cancel := ctxT(settle)
			defer cancel()
			_, err := controlv1connect.NewPlanServiceClient(hc, h.url, check.Bearer(token)).Validate(ctx, connect.NewRequest(&controlv1.ValidateRequest{}))
			return err
		},
		"RouteService": func(token string) error {
			ctx, cancel := ctxT(settle)
			defer cancel()
			_, err := controlv1connect.NewRouteServiceClient(hc, h.url, check.Bearer(token)).ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{}))
			return err
		},
	}
	fleet := func(token string) error {
		ctx, cancel := ctxT(settle)
		defer cancel()
		_, err := workerv1connect.NewFleetServiceClient(hc, h.url, check.Bearer(token)).Heartbeat(ctx,
			connect.NewRequest(&workerv1.HeartbeatRequest{WorkerId: "unknown", State: workerv1.WorkerState_WORKER_STATE_READY}))
		return err
	}
	artifacts := func(token string) error {
		ctx, cancel := ctxT(settle)
		defer cancel()
		st, err := workerv1connect.NewArtifactServiceClient(hc, h.url, check.Bearer(token)).GetArtifact(ctx,
			connect.NewRequest(&workerv1.GetArtifactRequest{Digest: sha([]byte("nothing"))}))
		if err != nil {
			return err
		}
		defer st.Close()
		for st.Receive() {
		}
		return st.Err()
	}

	registry := func(token string) error {
		ctx, cancel := ctxT(settle)
		defer cancel()
		_, err := egressv1connect.NewRegistryServiceClient(hc, h.url, check.Bearer(token)).Heartbeat(ctx,
			connect.NewRequest(&egressv1.HeartbeatRequest{ServiceId: "unknown", State: egressv1.ServiceState_SERVICE_STATE_READY}))
		return err
	}

	seams := []seam{{"IngressService", h.env.IngressToken, ingress}, {"FleetService", h.env.WorkerToken, fleet}, {"ArtifactService", h.env.WorkerToken, artifacts},
		{"RegistryService", h.env.NetworkToken, registry}}
	for name, c := range controlCalls {
		seams = append(seams, seam{"control." + name, h.env.AdminToken, c})
	}
	all := map[string]string{"ingress": h.env.IngressToken, "worker": h.env.WorkerToken, "admin": h.env.AdminToken, "network": h.env.NetworkToken}

	for _, s := range seams {
		t.Run(s.name, func(t *testing.T) {
			// A call that passes authentication may still fail on its merits (an
			// unknown artifact, say): what matters is that it is not an auth error.
			if code := connect.CodeOf(s.call(s.token)); code == connect.CodeUnauthenticated || code == connect.CodePermissionDenied {
				t.Errorf("the right token was refused: %v", code)
			}
			for _, bad := range []string{"", "not-a-token", h.env.ExecutionToken, strings.ToUpper(s.token)} {
				check.WantCode(t, s.call(bad), connect.CodeUnauthenticated)
			}
			for scope, tok := range all {
				if tok == s.token {
					continue
				}
				if err := s.call(tok); connect.CodeOf(err) != connect.CodePermissionDenied {
					t.Errorf("a %s token must be PERMISSION_DENIED here, got %v", scope, err)
				}
			}
		})
	}
}
