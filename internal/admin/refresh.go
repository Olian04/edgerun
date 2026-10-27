package admin

import (
	"net/http"
	"strconv"
	"time"
)

// autoRefresh asks browsers to reload read-only pages every interval, so the
// fleet and the invocation log stay current without anyone pressing Refresh.
//
// It is done on the server, with the Refresh response header, so the pages
// need no script and the reload is just a GET of the same address (a table's
// sort, filter and page token live in the query and survive it). It is applied
// only to pages that are views and nothing else: a reload would discard a form
// someone is filling in, so pages with forms never get it, and neither does
// anything that is not a successful page render.
func autoRefresh(every time.Duration, paths map[string]bool, next http.Handler) http.Handler {
	secs := strconv.Itoa(int((every + time.Second - 1) / time.Second))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if every > 0 && r.Method == http.MethodGet && paths[r.URL.Path] {
			w = &refreshWriter{ResponseWriter: w, secs: secs}
		}
		next.ServeHTTP(w, r)
	})
}

// refreshWriter adds the header when, and only when, the page answers 200.
type refreshWriter struct {
	http.ResponseWriter
	secs string
	sent bool
}

func (w *refreshWriter) WriteHeader(code int) {
	if !w.sent {
		w.sent = true
		if code == http.StatusOK {
			w.Header().Set("Refresh", w.secs)
		}
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *refreshWriter) Write(b []byte) (int, error) {
	if !w.sent {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the real writer (flushing, deadlines).
func (w *refreshWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// livePages are the read-only views: no form on them is worth protecting.
func livePages(prefix string) map[string]bool {
	return map[string]bool{
		prefix + "/":                     true,
		prefix + string(WorkersPath):     true,
		prefix + string(InvocationsPath): true,
	}
}
