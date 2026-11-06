package wgservice_test

import (
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"

	"example.com/edgerun-wgservice/internal/wgtest"
)

const (
	tIngress = "chain-ingress"
	tWorker  = "chain-worker"
	tAdmin   = "chain-admin"
	tExec    = "chain-exec"
	tNetwork = "chain-network"
	tEgress  = "chain-egress"
)

// The whole chain, as real processes: guest -> worker -> wgservice -> WireGuard
// -> origin. The orchestrator and the worker are built from the edgerun
// checkout; wgservice is this module's program; the site is in this process.
func TestChain(t *testing.T) {
	orchURL := fmt.Sprintf("http://127.0.0.1:%d", portOrch)
	workerURL := fmt.Sprintf("http://127.0.0.1:%d", portWorker)
	svcURL := fmt.Sprintf("http://127.0.0.1:%d", portChainSvc)

	// The far side of the tunnel.
	site, err := wgtest.NewSite(portChainSite, map[string][]netip.Addr{"origin.corp.internal": {wgtest.OriginAddr}})
	if err != nil {
		t.Fatal(err)
	}
	defer site.Close()
	if err := site.ServeOrigin(); err != nil {
		t.Fatal(err)
	}
	conf := filepath.Join(tmpRoot, "chain-tunnel.conf")
	if err := os.WriteFile(conf, []byte(site.GatewayConfig(fmt.Sprintf("127.0.0.1:%d", portChainSite), portChainGW)), 0o600); err != nil {
		t.Fatal(err)
	}

	start(t, "chain-orchestrator", nil, orchestratorBin(t),
		"--listen", fmt.Sprintf("127.0.0.1:%d", portOrch), "--database-url", newDatabase(t),
		"--ingress-token", tIngress, "--worker-token", tWorker, "--admin-token", tAdmin, "--execution-token", tExec,
		"--network-token", tNetwork, "--heartbeat-interval", "1s", "--secret-key", "ZGV2LW9ubHktc2VjcmV0LWtleS0zMi1ieXRlcyEhISE=")
	eventually(t, "the orchestrator", 30*time.Second, func() bool {
		r, err := http.Get(orchURL + "/healthz")
		if err == nil {
			r.Body.Close()
		}
		return err == nil && r.StatusCode == 200
	})
	ctl := edgerun.NewControl(orchURL, tAdmin)
	ing := edgerun.NewIngress(orchURL, tIngress)

	svc := start(t, "chain-wgservice", nil, wgserviceBin(t),
		"--listen", fmt.Sprintf("127.0.0.1:%d", portChainSvc), "--advertise-url", svcURL, "--orchestrator-url", orchURL,
		"--network-token", tNetwork, "--egress-token", tEgress,
		"--covers", "10.20.0.0/16,*.corp.internal", "--allow-network", "10.20.0.0/16", "--wg-config", conf)
	start(t, "chain-worker", nil, workerBin(t),
		"--orchestrator-url", orchURL, "--advertise-url", workerURL, "--listen", fmt.Sprintf("127.0.0.1:%d", portWorker),
		"--worker-token", tWorker, "--execution-token", tExec, "--egress-token", tEgress)

	t.Run("registration", func(t *testing.T) {
		eventually(t, "the service and the worker to register", 30*time.Second, func() bool {
			ns, err := ctl.ListNetworkServices(ctx10(t), &controlv1.ListNetworkServicesRequest{})
			ws, err2 := ctl.ListWorkers(ctx10(t), &controlv1.ListWorkersRequest{})
			return err == nil && err2 == nil && len(ns.Services) == 1 && ns.Services[0].Alive && len(ws.Workers) == 1 && ws.Workers[0].Alive
		})
		ns, _ := ctl.ListNetworkServices(ctx10(t), &controlv1.ListNetworkServicesRequest{})
		got := ns.Services[0]
		if got.Endpoint != svcURL || fmt.Sprint(got.Info.Covers) != "[*.corp.internal 10.20.0.0/16]" || got.State != egressv1.ServiceState_SERVICE_STATE_READY {
			t.Errorf("registered as %+v", got)
		}
	})

	// The guest: examples/fetch, built for wasip1, deployed as wagi-2.
	fn, err := ctl.CreateFunction(ctx10(t), &controlv1.CreateFunctionRequest{Name: "fetch"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ctl.CreateVersion(ctx10(t), &controlv1.CreateVersionRequest{FunctionId: fn.Function.Id, Module: fetchWasm(t), Abi: "wagi-2", Activate: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := ctl.CreateRoute(ctx10(t), &controlv1.CreateRouteRequest{HostPattern: "fetch.test", PathPrefix: "/", FunctionId: fn.Function.Id}); err != nil {
		t.Fatal(err)
	}
	setAllow := func(entries ...string) {
		t.Helper()
		if _, err := ctl.SetEgress(ctx10(t), &controlv1.SetEgressRequest{FunctionId: fn.Function.Id, Allow: entries}); err != nil {
			t.Fatal(err)
		}
	}
	invoke := func(target string) (int, string) {
		t.Helper()
		var lastErr error
		for i := 0; i < 20; i++ { // NO_CAPACITY until the worker has registered and been seen
			ctx := ctx10(t)
			res, err := ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{
				Method: "GET", Host: "fetch.test", Path: "/", RawQuery: "url=" + url.QueryEscape(target)}})
			if err != nil {
				lastErr = err
				time.Sleep(300 * time.Millisecond)
				continue
			}
			b, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			return res.Status(), string(b)
		}
		t.Fatalf("invoke %s: %v", target, lastErr)
		return 0, ""
	}

	t.Run("closed by default", func(t *testing.T) {
		before := len(site.Seen())
		st, body := invoke("http://10.20.0.2/hello")
		if st != 403 || !strings.Contains(body, "not on this function's allow list") {
			t.Errorf("%d %q", st, body)
		}
		if len(site.Seen()) != before {
			t.Error("the origin was reached by a function with no allow list")
		}
	})

	t.Run("allowed: guest, worker, service, WireGuard, origin", func(t *testing.T) {
		setAllow("10.20.0.2", "*.corp.internal", "10.20.0.2:81", "10.20.0.3")
		before := len(site.Seen())
		st, body := invoke("http://10.20.0.2/hello")
		if st != 200 || !strings.Contains(body, "Status: 200") && !strings.Contains(body, "answered") || !strings.Contains(body, "bytes of body") {
			t.Fatalf("%d %q", st, body)
		}
		seen := site.Seen()
		if len(seen) != before+1 || !strings.HasPrefix(seen[before].Remote, "10.20.0.1:") || seen[before].Method != "GET" || seen[before].URI != "/hello" {
			t.Errorf("the origin saw %+v: the request must arrive from the tunnel address 10.20.0.1", seen[before:])
		}
		// By name: resolved by the DNS inside the tunnel.
		q := site.DNSLookup.Load()
		st, body = invoke("http://origin.corp.internal/hello")
		if st != 200 || site.DNSLookup.Load() == q {
			t.Errorf("by name: %d %q (DNS queries %d)", st, body, site.DNSLookup.Load()-q)
		}
		if seen := site.Seen(); seen[len(seen)-1].Host != "origin.corp.internal" {
			t.Errorf("Host %q", seen[len(seen)-1].Host)
		}
		// Redirects are the guest's: the 302 comes back as it is.
		if st, body := invoke("http://10.20.0.2/redirect"); st != 200 || !strings.Contains(body, "Status: 302") && !strings.Contains(body, "302") {
			t.Errorf("redirect: %d %q", st, body)
		}
	})

	t.Run("the allow list still decides", func(t *testing.T) {
		before := len(site.Seen())
		for _, target := range []string{
			"http://10.20.0.9/",           // carried by the service, not allowed (no entry)
			"http://10.20.0.2:8080/hello", // another port than the entry's
			"http://10.20.0.2:443/hello",  // 443 is the default of https, not of http
			"http://203.0.113.5/",         // not carried by anybody: no grant
		} {
			if st, body := invoke(target); st != 403 {
				t.Errorf("%s: %d %q, want 403 (denied)", target, st, body)
			}
		}
		if len(site.Seen()) != before {
			t.Error("a denied request reached the origin")
		}
	})

	t.Run("the service's own refusals hold whatever the function allows", func(t *testing.T) {
		site.SetName("meta.corp.internal", netip.MustParseAddr("169.254.169.254"))
		site.SetName("rebind.corp.internal", wgtest.OriginAddr, netip.MustParseAddr("127.0.0.1"))
		before := len(site.Seen())
		for _, target := range []string{"http://meta.corp.internal/", "http://rebind.corp.internal/hello", "http://x.localhost/"} {
			setAllow("*.corp.internal", "*.localhost")
			if st, body := invoke(target); st != 403 {
				t.Errorf("%s: %d %q, want 403", target, st, body)
			}
		}
		if len(site.Seen()) != before {
			t.Error("a forbidden destination reached the origin")
		}
		if l := tail(filepath.Join(tmpRoot, "logs", "chain-wgservice.log"), 20000); strings.Contains(l, "panic") {
			t.Errorf("service log:\n%s", l)
		}
	})

	t.Run("an allowed destination that cannot be reached fails, it is not denied", func(t *testing.T) {
		setAllow("10.20.0.2:81", "nope.corp.internal")
		if st, body := invoke("http://10.20.0.2:81/"); st != 502 {
			t.Errorf("closed port: %d %q", st, body)
		}
		if st, body := invoke("http://nope.corp.internal/"); st != 502 {
			t.Errorf("unknown name: %d %q", st, body)
		}
	})

	t.Run("stopping the service drains and deregisters; nothing is routed to it afterwards", func(t *testing.T) {
		setAllow("10.20.0.2")
		svc.stop()
		ns, err := ctl.ListNetworkServices(ctx10(t), &controlv1.ListNetworkServicesRequest{})
		if err != nil || len(ns.Services) != 0 {
			t.Errorf("after SIGTERM the orchestrator still lists %v (%v)", ns.GetServices(), err)
		}
		before := len(site.Seen())
		if st, body := invoke("http://10.20.0.2/hello"); st != 403 {
			t.Errorf("no service carries it any more: %d %q", st, body)
		}
		if len(site.Seen()) != before {
			t.Error("the origin was reached with no service registered")
		}
	})
}

var _ = edgerun.ScopeAdmin
