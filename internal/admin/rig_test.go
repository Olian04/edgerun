package admin_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/html"

	"github.com/Olian04/edgerun/conformance/guest"
	"github.com/Olian04/edgerun/internal/admin"
	"github.com/Olian04/edgerun/internal/admin/auth"
	orchapp "github.com/Olian04/edgerun/internal/orchestrator/app"
	"github.com/Olian04/edgerun/internal/orchestrator/orchtest"
	"github.com/Olian04/edgerun/internal/testpg"
)

func TestMain(m *testing.M) { testpg.Main(m) }

const (
	ingressToken = "ingress-token"
	workerToken  = "worker-token"
	adminToken   = "admin-token"
	execToken    = "execution-token"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// rig is a real orchestrator on a real PostgreSQL with the admin site in front
// of it, driven the way a browser would.
type rig struct {
	t    *testing.T
	orch *orchapp.Server
	site *httptest.Server
	ctl  *admin.Control
	pw   map[string]string
}

func newRig(t *testing.T) *rig {
	t.Helper()
	orch, err := orchapp.Start(context.Background(), orchapp.Config{
		Listen: "127.0.0.1:0", DatabaseURL: testpg.DSN(t),
		IngressToken: ingressToken, WorkerToken: workerToken, AdminToken: adminToken, ExecutionToken: execToken,
		SecretKey: orchtest.Key, HeartbeatInterval: 200 * time.Millisecond, RefreshEvery: 100 * time.Millisecond, Log: quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { orch.Stop(context.Background()) })

	pw := map[string]string{"alice": "alice-pw", "bob": "bob-pw"}
	users := []auth.User{}
	for name, role := range map[string]string{"alice": auth.RoleAdmin, "bob": auth.RoleViewer} {
		h, err := auth.HashPassword(pw[name])
		if err != nil {
			t.Fatal(err)
		}
		us, err := auth.ParseUsers(name + ":" + role + ":" + h)
		if err != nil {
			t.Fatal(err)
		}
		users = append(users, us...)
	}
	ctl := admin.NewControl(http.DefaultClient, "http://"+orch.Addr(), adminToken)
	h, err := admin.Handler(admin.HandlerConfig{Control: ctl, Users: users, SessionKey: auth.NewSessionKey(), AutoRefresh: 7 * time.Second, Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	site := httptest.NewServer(h)
	t.Cleanup(site.Close)
	return &rig{t: t, orch: orch, site: site, ctl: ctl, pw: pw}
}

// browser is an HTTP client with a cookie jar that follows redirects.
type browser struct {
	t    *testing.T
	base string
	c    *http.Client
}

func (r *rig) browser() *browser {
	jar, _ := cookiejar.New(nil)
	return &browser{t: r.t, base: r.site.URL, c: &http.Client{Jar: jar}}
}

// noRedirect returns a client that reports redirects instead of following them.
func (b *browser) noRedirect() *http.Client {
	c := *b.c
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &c
}

type page struct {
	status int
	url    string
	body   string
	header http.Header
}

func (b *browser) do(req *http.Request) page {
	b.t.Helper()
	resp, err := b.c.Do(req)
	if err != nil {
		b.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return page{status: resp.StatusCode, url: resp.Request.URL.String(), body: string(body), header: resp.Header}
}

func (b *browser) get(path string) page {
	b.t.Helper()
	req, _ := http.NewRequest("GET", b.base+path, nil)
	return b.do(req)
}

func (b *browser) post(path string, form url.Values) page {
	b.t.Helper()
	req, _ := http.NewRequest("POST", b.base+path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return b.do(req)
}

func (b *browser) signIn(user, password string) page {
	b.t.Helper()
	return b.post("/login", url.Values{"username": {user}, "password": {password}, "next": {"/admin/"}})
}

// --- reading the HTML ----------------------------------------------------------

// button is one submit button: forms such as tables hold many, each carrying
// its own name and value (webui's row actions are "_act" = "row:<n>:<key>").
type button struct {
	label, name, value string
	disabled           bool
}

type form struct {
	action, method, enctype string
	fields                  url.Values
	buttons                 []button
	labels                  map[string]string   // visible label -> control name
	options                 map[string][]string // control name -> option values
	submitName, submitValue string
}

func parse(t *testing.T, body string) *html.Node {
	t.Helper()
	doc, err := html.Parse(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

func hasAttr(n *html.Node, key string) bool {
	for _, a := range n.Attr {
		if a.Key == key {
			return true
		}
	}
	return false
}

func text(n *html.Node) string {
	var sb strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.TextNode {
			sb.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return strings.Join(strings.Fields(sb.String()), " ")
}

func find(n *html.Node, pred func(*html.Node) bool) []*html.Node {
	var out []*html.Node
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && pred(n) {
			out = append(out, n)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return out
}

func tag(name string) func(*html.Node) bool { return func(n *html.Node) bool { return n.Data == name } }

// forms returns every <form> on the page with the values a browser would send
// by default.
func forms(t *testing.T, body string) []form {
	t.Helper()
	var out []form
	for _, f := range find(parse(t, body), tag("form")) {
		fm := form{action: attr(f, "action"), method: strings.ToUpper(attr(f, "method")), enctype: attr(f, "enctype"),
			fields: url.Values{}, labels: map[string]string{}, options: map[string][]string{}}
		byID := map[string]*html.Node{}
		for _, el := range find(f, func(n *html.Node) bool { return attr(n, "id") != "" }) {
			byID[attr(el, "id")] = el
		}
		for _, l := range find(f, tag("label")) {
			if target := byID[attr(l, "for")]; target != nil {
				label := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(text(l)), "*"))
				fm.labels[label] = attr(target, "name")
			}
		}
		for _, in := range find(f, func(n *html.Node) bool {
			return n.Data == "input" || n.Data == "textarea" || n.Data == "select" || n.Data == "button"
		}) {
			name := attr(in, "name")
			switch in.Data {
			case "button":
				fm.buttons = append(fm.buttons, button{label: text(in), name: name, value: attr(in, "value"), disabled: hasAttr(in, "disabled")})
			case "textarea":
				if name != "" {
					fm.fields.Set(name, text(in))
				}
			case "select":
				if name == "" {
					continue
				}
				for _, o := range find(in, tag("option")) {
					if v := attr(o, "value"); v != "" {
						fm.options[name] = append(fm.options[name], v)
					}
					if hasAttr(o, "selected") {
						fm.fields.Add(name, attr(o, "value"))
					}
				}
			case "input":
				typ := attr(in, "type")
				switch typ {
				case "submit", "button", "file":
					continue
				case "checkbox", "radio":
					if !hasAttr(in, "checked") {
						continue
					}
				}
				if name != "" {
					fm.fields.Add(name, attr(in, "value"))
				}
			}
		}
		out = append(out, fm)
	}
	return out
}

func moduleBytes(t *testing.T) []byte { return guest.Build(t) }

var ctx = context.Background()

// --- driving forms ---------------------------------------------------------------

// formWith returns the form holding an enabled button with this label, set up
// to be submitted by that button. When row is not empty the button must belong
// to that row (its value names the row's key), or the form must carry a hidden
// field with that value.
func formWith(t *testing.T, body, label, row string) form {
	t.Helper()
	var fallback *form
	var fallbackBtn button
	for _, f := range forms(t, body) {
		f := f
		for _, b := range f.buttons {
			if b.disabled || !strings.Contains(b.label, label) {
				continue
			}
			if row != "" && !strings.Contains(b.value, row) && !f.hasValue(row) {
				continue
			}
			// A confirmable action has two buttons with the same label: one that
			// opens the dialog and submits nothing, and the real one inside it,
			// which carries the action. Prefer the one that carries an action.
			if b.name != "" {
				f.submitName, f.submitValue = b.name, b.value
				return f
			}
			if fallback == nil {
				fallback, fallbackBtn = &f, b
			}
		}
	}
	if fallback != nil {
		fallback.submitName, fallback.submitValue = fallbackBtn.name, fallbackBtn.value
		return *fallback
	}
	t.Fatalf("no form with an enabled %q button (row %q) on the page", label, row)
	return form{}
}

func (f form) hasValue(v string) bool {
	for _, vs := range f.fields {
		for _, x := range vs {
			if x == v {
				return true
			}
		}
	}
	return false
}

// values fills the form's fields by visible label.
func (f form) values(t *testing.T, byLabel map[string]string) url.Values {
	t.Helper()
	out := url.Values{}
	for k, v := range f.fields {
		out[k] = append([]string(nil), v...)
	}
	for label, v := range byLabel {
		name, ok := f.labels[label]
		if !ok {
			t.Fatalf("the form has no field labelled %q (has %v)", label, f.labels)
		}
		out.Set(name, v)
	}
	if f.submitName != "" {
		out.Set(f.submitName, f.submitValue)
	}
	return out
}

func (b *browser) submit(t *testing.T, f form, byLabel map[string]string) page {
	t.Helper()
	return b.post(f.action, f.values(t, byLabel))
}

// submitFile submits a multipart form with one uploaded file.
func (b *browser) submitFile(t *testing.T, f form, byLabel map[string]string, fileLabel, filename string, data []byte) page {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, vs := range f.values(t, byLabel) {
		for _, v := range vs {
			mw.WriteField(k, v)
		}
	}
	name, ok := f.labels[fileLabel]
	if !ok {
		t.Fatalf("no field labelled %q", fileLabel)
	}
	fw, _ := mw.CreateFormFile(name, filename)
	fw.Write(data)
	mw.Close()
	req, _ := http.NewRequest("POST", b.base+f.action, &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return b.do(req)
}

// has reports whether the page's visible text contains s.
func (p page) has(t *testing.T, s string) bool {
	t.Helper()
	return strings.Contains(text(parse(t, p.body)), s)
}
