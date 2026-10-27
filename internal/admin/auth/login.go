package auth

import (
	"html/template"
	"net/http"
)

// The sign-in page is ours, not webui's, so it is deliberately plain: static
// markup, no script, and a policy that allows nothing else.
var loginTmpl = template.Must(template.New("login").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sign in · edgerun</title>
<link rel="stylesheet" href="/login.css">
</head>
<body>
<main>
<h1>edgerun</h1>
<form method="post" action="/login">
{{if .Error}}<p class="error" role="alert">{{.Error}}</p>{{end}}
<input type="hidden" name="next" value="{{.Next}}">
<label>Username <input name="username" autocomplete="username" autofocus required></label>
<label>Password <input name="password" type="password" autocomplete="current-password" required></label>
<button type="submit">Sign in</button>
</form>
</main>
</body>
</html>
`))

const loginCSS = `
:root{--bg:#f6f7f9;--fg:#1c1e21;--card:#fff;--line:#d0d4da;--accent:#2f6fed;--bad:#b42318}
@media (prefers-color-scheme:dark){:root{--bg:#14161a;--fg:#e8eaed;--card:#1d2026;--line:#363b44;--accent:#6b9bff;--bad:#ff8a80}}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:grid;place-items:center;background:var(--bg);color:var(--fg);font:16px/1.5 system-ui,sans-serif}
main{width:min(22rem,calc(100vw - 2rem));background:var(--card);border:1px solid var(--line);border-radius:.75rem;padding:1.5rem}
h1{margin:0 0 1rem;font-size:1.25rem}
form{display:grid;gap:.9rem}
label{display:grid;gap:.25rem;font-size:.875rem}
input{font:inherit;padding:.55rem .65rem;border:1px solid var(--line);border-radius:.4rem;background:transparent;color:inherit}
input:focus-visible,button:focus-visible{outline:2px solid var(--accent);outline-offset:1px}
button{font:inherit;padding:.6rem;border:0;border-radius:.4rem;background:var(--accent);color:#fff;cursor:pointer}
.error{margin:0;padding:.5rem .65rem;border:1px solid var(--bad);border-radius:.4rem;color:var(--bad);font-size:.875rem}
`

// LoginCSS serves the sign-in stylesheet. It is a separate file so the page's
// policy can forbid inline styles.
func LoginCSS() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Write([]byte(loginCSS))
	})
}

func renderLogin(w http.ResponseWriter, status int, next, msg string) {
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("Content-Security-Policy", "default-src 'none'; style-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
	w.WriteHeader(status)
	loginTmpl.Execute(w, struct{ Next, Error string }{next, msg})
}
