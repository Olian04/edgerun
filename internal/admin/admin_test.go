package admin_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	"github.com/Olian04/edgerun/gen/edgerun/ingress/v1/ingressv1connect"
	"github.com/Olian04/edgerun/internal/authn"
	workerapp "github.com/Olian04/edgerun/internal/worker/app"
)

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

// startWorker runs a real worker against the rig's orchestrator.
func (r *rig) startWorker() {
	r.t.Helper()
	wctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		workerapp.Run(wctx, workerapp.Config{Listen: "127.0.0.1:0", OrchestratorURL: "http://" + r.orch.Addr(),
			WorkerToken: workerToken, ExecutionToken: execToken, MaxConcurrency: 4, DrainTimeout: 5 * time.Second, Log: quiet})
	}()
	r.t.Cleanup(func() { cancel(); <-done })
	eventually(r.t, "the worker to register", 30*time.Second, func() bool {
		ws, err := r.ctl.Workers(context.Background())
		return err == nil && len(ws) > 0 && ws[0].Alive
	})
}

// invoke sends a request the way a frontend would.
func (r *rig) invoke(host, pathAndQuery string) (status uint32, body string, err error) {
	path, query, _ := strings.Cut(pathAndQuery, "?")
	client := ingressv1connect.NewIngressServiceClient(http.DefaultClient, "http://"+r.orch.Addr(),
		connect.WithInterceptors(authn.Client(ingressToken)))
	ictx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	st, err := client.Invoke(ictx, connect.NewRequest(&ingressv1.InvokeRequest{
		Request: &commonv1.HttpRequest{Method: "GET", Host: host, Path: path, RawQuery: query}}))
	if err != nil {
		return 0, "", err
	}
	defer st.Close()
	var sb strings.Builder
	for st.Receive() {
		switch p := st.Msg().GetPart().(type) {
		case *ingressv1.InvokeResponse_Head:
			status = p.Head.GetHttp().GetStatus()
		case *ingressv1.InvokeResponse_Chunk:
			sb.Write(p.Chunk)
		}
	}
	return status, sb.String(), st.Err()
}

// --- the sign-in gate ---------------------------------------------------------------

func TestEverythingIsBehindSignIn(t *testing.T) {
	r := newRig(t)
	b := r.browser()

	// Page loads are sent to the sign-in form, remembering where they were going.
	resp, err := b.noRedirect().Get(r.site.URL + "/admin/function?x=1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	loc, _ := url.Parse(resp.Header.Get("Location"))
	if resp.StatusCode != http.StatusSeeOther || loc.Path != "/login" || loc.Query().Get("next") != "/admin/function?x=1" {
		t.Errorf("%d -> %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	// State changes without a session are refused outright.
	if p := b.post("/admin/function/new", url.Values{"_leaf": {"p"}, "f0": {"sneaky"}}); p.status != http.StatusUnauthorized {
		t.Errorf("anonymous POST: %d", p.status)
	}
	if fns, _ := r.ctl.Functions(ctx); len(fns) != 0 {
		t.Error("an anonymous request created a function")
	}

	if p := b.signIn("alice", "wrong"); p.status != http.StatusUnauthorized || !p.has(t, "Wrong username or password.") {
		t.Errorf("wrong password: %d", p.status)
	}
	p := b.signIn("alice", "alice-pw")
	if p.status != 200 || !p.has(t, "System") || !strings.HasSuffix(p.url, "/admin/") {
		t.Fatalf("after sign-in: %d %s", p.status, p.url)
	}
	if got := b.get("/"); !strings.HasSuffix(got.url, "/admin/") {
		t.Errorf("/ should lead to the app: %s", got.url)
	}

	b.get("/logout")
	resp, _ = b.noRedirect().Get(r.site.URL + "/admin/")
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Errorf("after signing out the app must be closed again: %d", resp.StatusCode)
	}
}

// --- an operator's whole job, through the UI, serving real traffic ---------------------

func TestOperatorDeploysAFunctionThroughTheUI(t *testing.T) {
	r := newRig(t)
	r.startWorker()
	b := r.browser()
	b.signIn("alice", "alice-pw")

	// 1. Create a function. Invalid and duplicate names are refused with a message.
	create := formWith(t, b.get("/admin/function/new").body, "Create", "")
	if p := b.submit(t, create, map[string]string{"Name": "Bad Name"}); p.status != http.StatusUnprocessableEntity || !p.has(t, "must be lower-case") {
		t.Errorf("a bad name: %d", p.status)
	}
	p := b.submit(t, create, map[string]string{"Name": "hello"})
	if p.status != 200 || !strings.Contains(p.url, "/admin/function/") || !p.has(t, "hello") {
		t.Fatalf("created: %d %s", p.status, p.url)
	}
	detail := p.url
	if q := b.submit(t, create, map[string]string{"Name": "hello"}); q.status != http.StatusUnprocessableEntity || !q.has(t, "already exists") {
		t.Errorf("a duplicate name: %d", q.status)
	}
	fns, _ := r.ctl.Functions(ctx)
	if len(fns) != 1 {
		t.Fatalf("functions = %d", len(fns))
	}
	fid := fns[0].Id

	// 2. Deploy a real module and make it live.
	deploy := formWith(t, b.get("/admin/function/"+fid+"/deploy").body, "Upload", "")
	// A file that is not WebAssembly must not be made live: it would fail every request.
	if p := b.submitFile(t, deploy, map[string]string{}, "Module", "not-wasm.wasm", []byte("this is not a module")); p.status != http.StatusUnprocessableEntity || !p.has(t, "WebAssembly") {
		t.Errorf("a non-module upload: %d", p.status)
	}
	if vs, _ := r.ctl.Versions(ctx, fid); len(vs) != 0 {
		t.Fatalf("the rejected upload created a version: %d", len(vs))
	}
	p = b.submitFile(t, deploy, map[string]string{"ABI": "wagi-1", "Memory limit (MiB)": "64"}, "Module", "hello.wasm", moduleBytes(t))
	if p.status != 200 || !strings.HasSuffix(p.url, "/admin/function/"+fid) || !p.has(t, "Active version v1") {
		t.Fatalf("after deploying: %d %s", p.status, p.url)
	}

	// 3. Route traffic to it, using the select of known functions.
	routeForm := formWith(t, b.get("/admin/route/new?function="+fid).body, "Create route", "")
	if got := routeForm.fields.Get(routeForm.labels["Function"]); got != fid {
		t.Errorf("?function= should preselect the function: got %q", got)
	}
	if p := b.submit(t, routeForm, map[string]string{"Host": "ui.test", "Path prefix": "/"}); p.status != 200 || !p.has(t, "ui.test") {
		t.Fatalf("route: %d", p.status)
	}
	if p := b.submit(t, routeForm, map[string]string{"Host": "ui.test", "Path prefix": "/"}); p.status != http.StatusUnprocessableEntity || !p.has(t, "already routed") {
		t.Errorf("a duplicate route: %d", p.status)
	}

	// 4. Environment: a plain variable and a secret.
	envForm := formWith(t, b.get("/admin/function/"+fid+"/env/new").body, "Save", "")
	if p := b.submit(t, envForm, map[string]string{"Name": "REQUEST_METHOD", "Value": "x"}); p.status != http.StatusUnprocessableEntity {
		t.Errorf("a reserved name: %d", p.status)
	}
	b.submit(t, envForm, map[string]string{"Name": "GREETING", "Value": "hi from the ui"})
	secretForm := formWith(t, b.get("/admin/function/"+fid+"/secret/new").body, "Save", "")
	b.submit(t, secretForm, map[string]string{"Name": "API_KEY", "Value": "super-secret-value"})

	envTab := b.get("/admin/function/" + fid + "?tabs.tab=environment")
	if !envTab.has(t, "GREETING") || !envTab.has(t, "hi from the ui") || !envTab.has(t, "API_KEY") {
		t.Errorf("the environment tab is missing entries")
	}
	if strings.Contains(envTab.body, "super-secret-value") {
		t.Error("a secret's value must never appear in a page")
	}
	if !envTab.has(t, "••••••••") {
		t.Error("a secret should be shown as a mask, so the operator can see it is set")
	}

	// 5. It serves traffic, with the environment, from a real worker.
	eventually(t, "the deployed function to answer", 120*time.Second, func() bool {
		st, body, err := r.invoke("ui.test", "/env")
		return err == nil && st == 200 && strings.Contains(body, "GREETING=hi from the ui\n") && strings.Contains(body, "API_KEY=super-secret-value\n")
	})

	// 6. Upload a second version without activating it, then roll over with the row button.
	deploy = formWith(t, b.get("/admin/function/"+fid+"/deploy").body, "Upload", "")
	b.submitFile(t, deploy, map[string]string{"Make it live": "false"}, "Module", "v-next.wasm", append(moduleBytes(t), 0, 20, 3, 'p', 'a', 'd', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16))
	versions, _ := r.ctl.Versions(ctx, fid)
	if len(versions) != 2 {
		t.Fatalf("versions = %d", len(versions))
	}
	fn, _ := r.ctl.Function(ctx, fid)
	newest := versions[0]
	if fn.ActiveVersionId == newest.Id {
		t.Fatal("'Make it live: No' must not activate the new version")
	}
	tab := b.get("/admin/function/" + fid)
	activate := formWith(t, tab.body, "Activate", newest.Id)
	if p := b.post(activate.action, activate.values(t, nil)); p.status != 200 {
		t.Fatalf("activate: %d", p.status)
	}
	if fn, _ = r.ctl.Function(ctx, fid); fn.ActiveVersionId != newest.Id {
		t.Error("the Activate row button did not move the live version")
	}
	if st, _, err := r.invoke("ui.test", "/echo"); err != nil || st != 200 {
		t.Errorf("the function must keep serving after the roll-over: %d %v", st, err)
	}

	// 7. Delete the variable, the route, then the function.
	envTab = b.get("/admin/function/" + fid + "?tabs.tab=environment")
	del := formWith(t, envTab.body, "Delete", "GREETING")
	b.post(del.action, del.values(t, nil))
	if es, _ := r.ctl.Env(ctx, fid); len(es) != 1 || es[0].Name != "API_KEY" {
		t.Errorf("env after delete = %v", es)
	}
	routes := b.get("/admin/route")
	rdel := formWith(t, routes.body, "Delete", "")
	b.post(rdel.action, rdel.values(t, nil))
	if rs, _ := r.ctl.Routes(ctx); len(rs) != 0 {
		t.Errorf("routes after delete = %v", rs)
	}
	toolbar := formWith(t, b.get("/admin/function/"+fid).body, "Delete function", "")
	if p := b.post(toolbar.action, toolbar.values(t, nil)); p.status != 200 || !strings.HasSuffix(p.url, "/admin/function") {
		t.Errorf("deleting the function: %d %s", p.status, p.url)
	}
	if fns, _ := r.ctl.Functions(ctx); len(fns) != 0 {
		t.Errorf("the function survived: %v", fns)
	}
	_ = detail
}

// --- read-only accounts ---------------------------------------------------------------

func TestAViewerCanLookButNotChange(t *testing.T) {
	r := newRig(t)
	fn, err := r.ctl.CreateFunction(ctx, "watched")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ctl.Deploy(ctx, fn.Id, []byte("module"), "wagi-1", nil, true); err != nil {
		t.Fatal(err)
	}
	b := r.browser()
	b.signIn("bob", "bob-pw")

	// Looking is fine.
	for _, path := range []string{"/admin/", "/admin/function", "/admin/function/" + fn.Id, "/admin/route", "/admin/worker", "/admin/invocation"} {
		if p := b.get(path); p.status != 200 {
			t.Errorf("%s: %d", path, p.status)
		}
	}
	// Pages that only exist to change things are refused, with the reason.
	for _, path := range []string{"/admin/function/new", "/admin/function/" + fn.Id + "/deploy", "/admin/route/new",
		"/admin/function/" + fn.Id + "/env/new", "/admin/function/" + fn.Id + "/secret/new", "/admin/function/" + fn.Id + "/network/new"} {
		if p := b.get(path); p.status != http.StatusForbidden || !p.has(t, "read-only") {
			t.Errorf("%s: %d, want 403 with the reason", path, p.status)
		}
	}
	// The controls are drawn disabled, not hidden: the viewer should know they exist.
	detail := b.get("/admin/function/" + fn.Id)
	disabled := false
	for _, btn := range find(parse(t, detail.body), tag("button")) {
		if strings.Contains(text(btn), "Delete function") && hasAttr(btn, "disabled") {
			disabled = true
		}
	}
	if !disabled {
		t.Error("the Delete button should be drawn disabled for a viewer")
	}
	// Links to pages they may not open are left out.
	if p := b.get("/admin/function"); strings.Contains(text(parse(t, p.body)), "New function") {
		t.Error("a link to a page the viewer is refused must not be drawn")
	}

	// A forged request is refused by the same guard that drew the buttons. The
	// forgery is the real thing: the exact requests an admin's browser would send,
	// replayed with a viewer's session.
	admin := r.browser()
	admin.signIn("alice", "alice-pw")
	createForm := formWith(t, admin.get("/admin/function/new").body, "Create", "")
	deleteForm := formWith(t, admin.get("/admin/function/"+fn.Id).body, "Delete function", "")
	for name, replay := range map[string]struct {
		path string
		form url.Values
	}{
		"create a function": {createForm.action, createForm.values(t, map[string]string{"Name": "forged"})},
		"delete a function": {deleteForm.action, deleteForm.values(t, nil)},
	} {
		p := b.post(replay.path, replay.form)
		if !p.has(t, "read-only") {
			t.Errorf("%s: the viewer should be told why (status %d)", name, p.status)
		}
	}
	fns, _ := r.ctl.Functions(ctx)
	if len(fns) != 1 || fns[0].Name != "watched" {
		t.Errorf("a viewer changed the system: %v", fns)
	}
}

// --- output is escaped ------------------------------------------------------------------

func TestOperatorSuppliedTextIsEscaped(t *testing.T) {
	r := newRig(t)
	fn, _ := r.ctl.CreateFunction(ctx, "xss")
	const payload = `<script>alert("owned")</script><img src=x onerror=alert(1)>`
	b := r.browser()
	b.signIn("alice", "alice-pw")
	form := formWith(t, b.get("/admin/function/"+fn.Id+"/env/new").body, "Save", "")
	b.submit(t, form, map[string]string{"Name": "NOTE", "Value": payload})

	tab := b.get("/admin/function/" + fn.Id + "?tabs.tab=environment")
	if strings.Contains(tab.body, "<script>alert") || strings.Contains(tab.body, "<img src=x") {
		t.Error("stored text was rendered as markup")
	}
	if !tab.has(t, `<script>alert("owned")</script>`) {
		t.Error("the text should still be shown, as text")
	}
}

// --- not found, fleet and invocations --------------------------------------------------

func TestMissingFunctionIsA404WithAWayBack(t *testing.T) {
	r := newRig(t)
	b := r.browser()
	b.signIn("alice", "alice-pw")
	p := b.get("/admin/function/00000000-0000-0000-0000-000000000000")
	if p.status != http.StatusNotFound || !p.has(t, "There is no function") || !strings.Contains(p.body, `href="/admin/function"`) {
		t.Errorf("%d", p.status)
	}
}

func TestFleetAndInvocationsAreVisible(t *testing.T) {
	r := newRig(t)
	r.startWorker()
	fn, _ := r.ctl.CreateFunction(ctx, "observed")
	if _, err := r.ctl.Deploy(ctx, fn.Id, moduleBytes(t), "wagi-1", nil, true); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ctl.CreateRoute(ctx, "obs.test", "/", fn.Id); err != nil {
		t.Fatal(err)
	}
	b := r.browser()
	b.signIn("alice", "alice-pw")

	workers := b.get("/admin/worker")
	for _, want := range []string{"edgerun-worker-wazero", "wagi-1", "Ready"} {
		if !workers.has(t, want) {
			t.Errorf("the workers page lacks %q", want)
		}
	}
	if ov := b.get("/admin/"); !ov.has(t, "1 alive of 1 registered") || !ov.has(t, "edgerun-orchestrator") {
		t.Error("the overview should summarise the fleet and name the orchestrator")
	}

	eventually(t, "a request to succeed", 120*time.Second, func() bool {
		st, _, err := r.invoke("obs.test", "/echo")
		return err == nil && st == 200
	})
	r.invoke("obs.test", "/exit?code=3") // a platform failure
	r.invoke("obs.test", "/status?code=503")

	eventually(t, "the invocations to be listed", 10*time.Second, func() bool {
		p := b.get("/admin/invocation")
		return p.has(t, "function trapped") && p.has(t, "observed") && p.has(t, "503")
	})
	p := b.get("/admin/invocation")
	if !p.has(t, "exiting on request") {
		t.Error("a failed invocation's log (the guest's stderr) should be visible to the operator")
	}
	// The same records, narrowed to the function, on its own page.
	if q := b.get("/admin/function/" + fn.Id + "?tabs.tab=invocations"); !q.has(t, "function trapped") {
		t.Error("the function's invocations tab should list its records")
	}
}

// Views reload themselves; anything with a form on it must not, because a
// reload would throw away what someone is typing.
func TestOnlyReadOnlyPagesReloadThemselves(t *testing.T) {
	r := newRig(t)
	fn, _ := r.ctl.CreateFunction(ctx, "reloads")
	b := r.browser()
	b.signIn("alice", "alice-pw")

	for _, path := range []string{"/admin/", "/admin/worker", "/admin/invocation"} {
		if got := b.get(path).header.Get("Refresh"); got != "7" {
			t.Errorf("%s: Refresh = %q, want 7", path, got)
		}
	}
	for _, path := range []string{
		"/admin/function", "/admin/function/new", "/admin/function/" + fn.Id,
		"/admin/function/" + fn.Id + "/deploy", "/admin/route", "/admin/route/new", "/admin/missing",
	} {
		if got := b.get(path).header.Get("Refresh"); got != "" {
			t.Errorf("%s reloads itself (Refresh = %q) and would discard what is being typed", path, got)
		}
	}
	// A page that did not render (here: signed out, so redirected) is not reloaded.
	anon := r.browser()
	resp, err := anon.noRedirect().Get(r.site.URL + "/admin/worker")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Header.Get("Refresh") != "" {
		t.Error("a redirect to sign-in must not carry Refresh")
	}
}

// A function reaches nothing until an operator says where; the Network tab is
// where they say it.
func TestOperatorOpensAndClosesTheNetwork(t *testing.T) {
	r := newRig(t)
	fn, err := r.ctl.CreateFunction(ctx, "talker")
	if err != nil {
		t.Fatal(err)
	}
	b := r.browser()
	b.signIn("alice", "alice-pw")
	tab := "/admin/function/" + fn.Id + "?tabs.tab=network"

	if p := b.get(tab); p.status != 200 || !p.has(t, "Closed by default") {
		t.Fatalf("the network tab of a new function: %d, and it must say the function reaches nothing", p.status)
	}

	allow := formWith(t, b.get("/admin/function/"+fn.Id+"/network/new").body, "Allow", "")
	if p := b.submit(t, allow, map[string]string{"Destination": "not a host!"}); p.status != http.StatusUnprocessableEntity || !p.has(t, "allow entry") {
		t.Errorf("a malformed destination: %d, want it refused with the reason", p.status)
	}
	if p := b.submit(t, allow, map[string]string{"Destination": "*"}); p.status != http.StatusUnprocessableEntity {
		t.Errorf("a bare wildcard: %d, want it refused: a list names destinations", p.status)
	}
	if got, _ := r.ctl.Function(ctx, fn.Id); len(got.EgressAllow) != 0 {
		t.Fatalf("a refused destination was stored: %v", got.EgressAllow)
	}

	if p := b.submit(t, allow, map[string]string{"Destination": "API.Example.com"}); p.status != 200 {
		t.Fatalf("allowing a host: %d", p.status)
	}
	b.submit(t, allow, map[string]string{"Destination": "*.cdn.example.com:8443"})
	b.submit(t, allow, map[string]string{"Destination": "192.0.2.7"})
	if got, _ := r.ctl.Function(ctx, fn.Id); fmt.Sprint(got.EgressAllow) != "[*.cdn.example.com:8443 192.0.2.7 api.example.com]" {
		t.Fatalf("stored list = %v, want the canonical form", got.EgressAllow)
	}
	p := b.get(tab)
	for _, want := range []string{"api.example.com", "*.cdn.example.com:8443", "192.0.2.7", "any host under it", "that address", "8443"} {
		if !p.has(t, want) {
			t.Errorf("the network tab does not show %q", want)
		}
	}

	rm := formWith(t, p.body, "Remove", "api.example.com")
	b.post(rm.action, rm.values(t, nil))
	if got, _ := r.ctl.Function(ctx, fn.Id); fmt.Sprint(got.EgressAllow) != "[*.cdn.example.com:8443 192.0.2.7]" {
		t.Errorf("after removing one: %v", got.EgressAllow)
	}
}
