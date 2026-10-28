package edgerun_test

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"

	"github.com/Olian04/edgerun"
	"github.com/Olian04/edgerun/gen/edgerun/control/v1/controlv1connect"
	"github.com/Olian04/edgerun/gen/edgerun/egress/v1/egressv1connect"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
)

// The library is the protocols, not a selection of them. Every RPC of every
// service in gen/ must be a method of the client that stands for its service,
// so a proto change that the library does not follow fails here.
func TestTheLibraryCoversEveryRPCOfEveryService(t *testing.T) {
	covered := map[string]struct {
		client  reflect.Type
		service reflect.Type
	}{
		"FunctionServiceClient":   {reflect.TypeFor[*edgerun.Control](), reflect.TypeFor[controlv1connect.FunctionServiceClient]()},
		"RouteServiceClient":      {reflect.TypeFor[*edgerun.Control](), reflect.TypeFor[controlv1connect.RouteServiceClient]()},
		"InspectionServiceClient": {reflect.TypeFor[*edgerun.Control](), reflect.TypeFor[controlv1connect.InspectionServiceClient]()},
		"IngressServiceClient":    {reflect.TypeFor[*edgerun.Ingress](), reflect.TypeFor[ingressv1connect.IngressServiceClient]()},
		"ExecutionServiceClient":  {reflect.TypeFor[*edgerun.Execution](), reflect.TypeFor[workerv1connect.ExecutionServiceClient]()},
		"FleetServiceClient":      {reflect.TypeFor[*edgerun.Fleet](), reflect.TypeFor[workerv1connect.FleetServiceClient]()},
		"ArtifactServiceClient":   {reflect.TypeFor[*edgerun.Artifacts](), reflect.TypeFor[workerv1connect.ArtifactServiceClient]()},
		"EgressServiceClient":     {reflect.TypeFor[*edgerun.Egress](), reflect.TypeFor[egressv1connect.EgressServiceClient]()},
		"RegistryServiceClient":   {reflect.TypeFor[*edgerun.Registry](), reflect.TypeFor[egressv1connect.RegistryServiceClient]()},
	}

	for name, c := range covered {
		for i := 0; i < c.service.NumMethod(); i++ {
			rpc := c.service.Method(i).Name
			if _, ok := c.client.MethodByName(rpc); !ok {
				t.Errorf("%s has no method %s: %s.%s is not reachable through the library", c.client, rpc, name, rpc)
			}
		}
	}

	// And no service may be missing altogether.
	found := 0
	re := regexp.MustCompile(`(?m)^type (\w+ServiceClient) interface`)
	files, _ := filepath.Glob("gen/edgerun/*/v1/*connect/*.connect.go")
	if len(files) == 0 {
		t.Fatal("found no generated services: is the test running in the module root?")
	}
	for _, f := range files {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range re.FindAllStringSubmatch(string(src), -1) {
			found++
			if _, ok := covered[m[1]]; !ok {
				t.Errorf("%s (%s) is not exposed by the library", m[1], f)
			}
		}
	}
	if found != len(covered) {
		t.Errorf("gen/ has %d service clients, the library covers %d", found, len(covered))
	}
}
