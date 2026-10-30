package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Olian04/edgerun"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

func startListener(t testing.TB, f *FakeIngress, budget time.Duration) (*httptest.Server, *Listener, *Stats) {
	t.Helper()
	url, _ := startFake(t, f)
	ing := edgerun.NewIngress(url, ingressToken, edgerun.WithHTTPClient(newHTTPClient("1", 0)))
	lim := NewLimits(ing)
	if err := lim.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := NewStats(time.Now())
	l := &Listener{Inv: NewInvoker(ing, lim, st, nil, time.Now()), Budget: budget, RunID: "lt"}
	srv := httptest.NewServer(l)
	t.Cleanup(srv.Close)
	return srv, l, st
}

func get(t testing.TB, url string, hdr map[string]string) (*http.Response, string, error) {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	return res, string(b), err
}

func TestListenerMapsLikeAFrontend(t *testing.T) {
	f := newFake()
	f.Capacity = 1
	srv, _, st := startListener(t, f, 400*time.Millisecond)

	res, body, err := get(t, srv.URL+"/status/201", map[string]string{"X-Request-Id": "abc-123"})
	if err != nil || res.StatusCode != 201 || body != "status 201\n" || res.Header.Get("X-Request-Id") != "abc-123" {
		t.Fatalf("%v %v %q %v", err, res, body, res.Header)
	}
	// An unusable request id is replaced; the response always carries one.
	res, _, _ = get(t, srv.URL+"/status/200", map[string]string{"X-Request-Id": strings.Repeat("x", 200)})
	if id := res.Header.Get("X-Request-Id"); id == "" || len(id) > 128 {
		t.Errorf("request id %q", id)
	}
	// A function's own 500 is forwarded as it is.
	if res, _, _ = get(t, srv.URL+"/status/500", nil); res.StatusCode != 500 {
		t.Errorf("function 500: %d", res.StatusCode)
	}
	for _, c := range []struct {
		host, path string
		status     int
		retry      bool
	}{
		{"missing.test", "/", 404, false},
		{"", "/trap", 502, false},
		{"", "/spin", 504, false},
		{"", "/head/99", 502, false}, // a head status outside 200-599 is a platform fault
	} {
		req, _ := http.NewRequest("GET", srv.URL+c.path, nil)
		if c.host != "" {
			req.Host = c.host
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != c.status {
			t.Errorf("%s%s: %d, want %d", c.host, c.path, res.StatusCode, c.status)
		}
		if strings.Contains(string(b), "no route") || strings.Contains(string(b), "guest exited") || strings.Contains(string(b), "budget") {
			t.Errorf("the platform's message reached the client: %q", b)
		}
		if res.Header.Get("X-Request-Id") == "" {
			t.Errorf("no X-Request-Id on an error response")
		}
	}
	// NO_CAPACITY: 503 with Retry-After: 1
	var wg sync.WaitGroup
	got := make(chan *http.Response, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, _, _ := get(t, srv.URL+"/sleep/200", nil)
			got <- res
		}()
	}
	wg.Wait()
	close(got)
	var ok, busy int
	for r := range got {
		switch r.StatusCode {
		case 200:
			ok++
		case 503:
			busy++
			if r.Header.Get("Retry-After") != "1" {
				t.Errorf("Retry-After %q", r.Header.Get("Retry-After"))
			}
		}
	}
	if ok != 1 || busy != 1 {
		t.Errorf("ok %d busy %d", ok, busy)
	}
	// A failure after the head: the client connection is aborted.
	res, body, err = get(t, srv.URL+"/midfail/50000", nil)
	if err == nil {
		t.Errorf("the client must see a failed transfer, got status %d with %d bytes", res.StatusCode, len(body))
	}
	sn := st.Snapshot()
	if sn.Outcomes[OutStreamFailure] != 1 || sn.Reasons["NO_CAPACITY"] != 1 || sn.Reasons["FUNCTION_NOT_FOUND"] != 1 {
		t.Errorf("stats of the listener: %+v %+v", sn.Outcomes, sn.Reasons)
	}
}

func TestListenerRequestRules(t *testing.T) {
	f := newFake()
	f.MaxBody = 1024
	f.Reqs = make(chan *ingressv1.InvokeRequest, 10)
	srv, _, st := startListener(t, f, 2*time.Second)

	req, _ := http.NewRequest("POST", srv.URL+"/status/200?q=a%20b&z=1", strings.NewReader("hello"))
	req.Host = "Mixed.Example.com:8080"
	req.Header.Set("X-Custom", "one")
	req.Header.Add("X-Custom", "two")
	req.Header.Set("X-Forwarded-For", "6.6.6.6")
	req.Header.Set("X-Real-IP", "6.6.6.6")
	req.Header.Set("Forwarded", "for=6.6.6.6")
	req.Header.Set("Keep-Alive", "timeout=5")
	req.Header.Set("Content-Type", "text/plain")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	got := <-f.Reqs
	r := got.Request
	hdr := map[string][]string{}
	for _, h := range r.Headers {
		hdr[h.Name] = append(hdr[h.Name], string(h.Value))
	}
	if r.Method != "POST" || r.Host != "Mixed.Example.com:8080" || r.Path != "/status/200" || r.RawQuery != "q=a%20b&z=1" || string(r.Body) != "hello" {
		t.Errorf("request: %+v", r)
	}
	if v := hdr["x-custom"]; len(v) != 2 || v[0] != "one" || v[1] != "two" {
		t.Errorf("repeated headers must be kept in order: %v", hdr)
	}
	if xff := hdr["x-forwarded-for"]; len(xff) != 1 || xff[0] != "127.0.0.1" {
		t.Errorf("x-forwarded-for from the connection, not the client: %v", xff)
	}
	for _, bad := range []string{"x-real-ip", "forwarded", "keep-alive", "connection", "transfer-encoding"} {
		if _, ok := hdr[bad]; ok {
			t.Errorf("%s must be dropped: %v", bad, hdr)
		}
	}
	if hdr["x-forwarded-proto"][0] != "http" || hdr["x-forwarded-host"][0] != "Mixed.Example.com:8080" {
		t.Errorf("%v", hdr)
	}
	if !strings.HasPrefix(r.RemoteAddr, "127.0.0.1:") || got.RequestId == "" {
		t.Errorf("remote_addr %q request_id %q", r.RemoteAddr, got.RequestId)
	}

	// Over the limit: 413, and the orchestrator is never called.
	seen := f.Seen.Load()
	big := strings.NewReader(strings.Repeat("x", 2048))
	res, err = http.Post(srv.URL+"/status/200", "text/plain", big)
	if err != nil || res.StatusCode != 413 {
		t.Fatalf("%v %v", res, err)
	}
	// ... also when the length is not declared
	res, err = http.Post(srv.URL+"/status/200", "text/plain", io.NopCloser(strings.NewReader(strings.Repeat("x", 4096))))
	if err != nil || res.StatusCode != 413 {
		t.Fatalf("chunked: %v %v", res, err)
	}
	if f.Seen.Load() != seen {
		t.Error("a request over the limit reached the orchestrator")
	}
	// exactly at the limit is accepted
	res, err = http.Post(srv.URL+"/status/200", "text/plain", strings.NewReader(strings.Repeat("x", 1024)))
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("at the limit: %v %v", res, err)
	}
	// OPTIONS * is answered by the frontend itself
	conn, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprint(conn, "OPTIONS * HTTP/1.1\r\nHost: x.test\r\n\r\n")
	line, _ := bufio.NewReader(conn).ReadString('\n')
	if !strings.Contains(line, " 400 ") && !strings.Contains(line, " 200 ") {
		t.Errorf("OPTIONS *: %q", line)
	}
	if f.Seen.Load() != seen+1 {
		t.Errorf("OPTIONS * must not be forwarded (seen %d -> %d)", seen, f.Seen.Load())
	}
	if st.Snapshot().Outcomes[OutRejected] < 2 {
		t.Errorf("rejections are counted: %+v", st.Snapshot().Outcomes)
	}
}

// A trace recorded by the listener can be replayed by `run`.
func TestRecordAndReplayRoundTrip(t *testing.T) {
	f := newFake()
	f.Reqs = make(chan *ingressv1.InvokeRequest, 100)
	url, _ := startFake(t, f)
	ing := edgerun.NewIngress(url, ingressToken)
	lim := NewLimits(ing)
	lim.Refresh(context.Background())
	st := NewStats(time.Now())
	tracePath := t.TempDir() + "/trace.jsonl"
	tw, err := NewTraceWriter(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(&Listener{Inv: NewInvoker(ing, lim, st, nil, time.Now()), Budget: time.Second, Trace: tw})
	for i := 0; i < 5; i++ {
		req, _ := http.NewRequest("POST", fmt.Sprintf("%s/echo?i=%d", srv.URL, i), strings.NewReader(strings.Repeat("b", 10*(i+1))))
		req.Host = "rec.test"
		res, _ := http.DefaultClient.Do(req)
		res.Body.Close()
		time.Sleep(50 * time.Millisecond)
	}
	srv.Close()
	tw.Close()
	for len(f.Reqs) > 0 {
		<-f.Reqs
	}
	code, rep, out := tm(t, "-scenario", writeFile(t, "r.yaml", "replay: {file: "+tracePath+"}\n"), "-url", url, "-token", ingressToken)
	if code != 0 || rep.Totals.Sent != 5 || rep.Totals.OK != 5 || rep.Totals.BytesReceived != 10+20+30+40+50 {
		t.Fatalf("%d %+v\n%s", code, rep.Totals, out)
	}
	between(t, "replay load seconds", rep.LoadSeconds, 0.15, 0.6)
	first := <-f.Reqs
	if first.Request.Host != "rec.test" || first.Request.Path != "/echo" || first.Request.RawQuery != "i=0" || first.Request.Method != "POST" {
		t.Errorf("replayed request %+v", first.Request)
	}
}
