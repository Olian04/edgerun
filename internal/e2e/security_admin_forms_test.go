package e2e

// Form handling for the admin UI tests: what a browser would send for the forms
// the pages hold (adapted from internal/admin's own test rig, under sec names).

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"golang.org/x/net/html"
)

// button is one submit button: forms such as tables hold many, each carrying
// its own name and value (webui's row actions are "_act" = "row:<n>:<key>").
type secButton struct {
	label, name, value string
	disabled           bool
}

type secForm2 struct {
	action, method, enctype string
	fields                  url.Values
	buttons                 []secButton
	labels                  map[string]string   // visible label -> control name
	options                 map[string][]string // control name -> option values
	submitName, submitValue string
}

func secParse2(t *testing.T, body string) *html.Node {
	t.Helper()
	doc, err := html.Parse(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func secAttr2(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

func secHasAttr(n *html.Node, key string) bool {
	for _, a := range n.Attr {
		if a.Key == key {
			return true
		}
	}
	return false
}

func secText(n *html.Node) string {
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

func secFind(n *html.Node, pred func(*html.Node) bool) []*html.Node {
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

func secTag(name string) func(*html.Node) bool {
	return func(n *html.Node) bool { return n.Data == name }
}

// forms returns every <form> on the page with the values a browser would send
// by default.
func secForms(t *testing.T, body string) []secForm2 {
	t.Helper()
	var out []secForm2
	for _, f := range secFind(secParse2(t, body), secTag("form")) {
		fm := secForm2{action: secAttr2(f, "action"), method: strings.ToUpper(secAttr2(f, "method")), enctype: secAttr2(f, "enctype"),
			fields: url.Values{}, labels: map[string]string{}, options: map[string][]string{}}
		byID := map[string]*html.Node{}
		for _, el := range secFind(f, func(n *html.Node) bool { return secAttr2(n, "id") != "" }) {
			byID[secAttr2(el, "id")] = el
		}
		for _, l := range secFind(f, secTag("label")) {
			if target := byID[secAttr2(l, "for")]; target != nil {
				label := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(secText(l)), "*"))
				fm.labels[label] = secAttr2(target, "name")
			}
		}
		for _, in := range secFind(f, func(n *html.Node) bool {
			return n.Data == "input" || n.Data == "textarea" || n.Data == "select" || n.Data == "button"
		}) {
			name := secAttr2(in, "name")
			switch in.Data {
			case "button":
				fm.buttons = append(fm.buttons, secButton{label: secText(in), name: name, value: secAttr2(in, "value"), disabled: secHasAttr(in, "disabled")})
			case "textarea":
				if name != "" {
					fm.fields.Set(name, secText(in))
				}
			case "select":
				if name == "" {
					continue
				}
				for _, o := range secFind(in, secTag("option")) {
					if v := secAttr2(o, "value"); v != "" {
						fm.options[name] = append(fm.options[name], v)
					}
					if secHasAttr(o, "selected") {
						fm.fields.Add(name, secAttr2(o, "value"))
					}
				}
			case "input":
				typ := secAttr2(in, "type")
				switch typ {
				case "submit", "button", "file":
					continue
				case "checkbox", "radio":
					if !secHasAttr(in, "checked") {
						continue
					}
				}
				if name != "" {
					fm.fields.Add(name, secAttr2(in, "value"))
				}
			}
		}
		out = append(out, fm)
	}
	return out
}

// --- driving forms ---------------------------------------------------------------

// formWith returns the form holding an enabled button with this label, set up
// to be submitted by that button. When row is not empty the button must belong
// to that row (its value names the row's key), or the form must carry a hidden
// field with that value.
func secFormWith(t *testing.T, body, label, row string) secForm2 {
	t.Helper()
	var fallback *secForm2
	var fallbackBtn secButton
	for _, f := range secForms(t, body) {
		f := f
		for _, b := range f.buttons {
			if b.disabled || !strings.Contains(b.label, label) {
				continue
			}
			if row != "" && !strings.Contains(b.value, row) && !f.secHasValue(row) {
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
	return secForm2{}
}

func (f secForm2) secHasValue(v string) bool {
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
func (f secForm2) secValues(t *testing.T, byLabel map[string]string) url.Values {
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

func (b *secBrowser) submit(t *testing.T, f secForm2, byLabel map[string]string) secPage {
	t.Helper()
	return b.post(f.action, f.secValues(t, byLabel))
}

// submitFile submits a multipart form with one uploaded file.
func (b *secBrowser) submitFile(t *testing.T, f secForm2, byLabel map[string]string, fileLabel, filename string, data []byte) secPage {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, vs := range f.secValues(t, byLabel) {
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
	req, _ := http.NewRequest("POST", b.site.base+f.action, &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return b.do(req)
}
