package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

// TestWorkerKillUnderLoadRepro is the minimal reproduction of what a SIGKILLed
// worker does to the invocations in flight on it and to the requests that
// arrive while it dies: three workers, one function that sleeps a random time
// and echoes, twelve callers in a steady stream, one worker killed after 1.5 s.
// Half of the callers use ingress.v1 directly, half the real frontend over HTTP.
// It logs what each caller was told, and fails only if a caller was handed a
// successful response that is not the right one.
//
// What it has shown, in about two runs of five: a caller that cancelled nothing
// is told "cancelled" by the orchestrator (no ErrorInfo; the invocation record
// has status 0 and no error reason), because connect-go reports a "connection
// reset by peer" from the worker as CodeCanceled and the orchestrator's
// translate() takes CodeCanceled at its word. The frontend then answers such a
// request with nothing, and net/http turns that into "200 OK" with an empty
// body (the "http 200 EMPTY" line). See FINDINGS.md.
func TestWorkerKillUnderLoadRepro(t *testing.T) {
	s := newStack(t, StackOpts{Workers: 3, Concurrency: 8})
	s.Deploy(t, "slow", multiHost, env.guest)
	warm(t, s, multiHost)
	feBase, _, stopFE := startFrontend(t, s)
	defer stopFE()
	var mu sync.Mutex
	tally := map[string]int{}
	var wg sync.WaitGroup
	end := time.Now().Add(5 * time.Second)
	for u := 0; u < 12; u++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; time.Now().Before(end); i++ {
				id := fmt.Sprintf("repro-%d-%d", u, i)
				want := id + "\nbody of " + id
				k := "ok"
				if u%2 == 0 { // ingress.v1
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					inv, err := s.Ing.Invoke(ctx, &ingressv1.InvokeRequest{RequestId: id, Request: &commonv1.HttpRequest{Method: "POST", Host: multiHost, Path: "/jitter/300",
						Headers: []*commonv1.Header{{Name: "x-request-id", Value: []byte(id)}}, Body: []byte("body of " + id)}})
					if err != nil {
						k = "ingress.v1 " + fmt.Sprintf("%v/%v", connect.CodeOf(err), edgerun.Reason(err))
					} else if b, err := io.ReadAll(inv.Body); err != nil || string(b) != want {
						k = fmt.Sprintf("ingress.v1 WRONG %q %v", b, err)
					}
					cancel()
				} else { // the frontend
					req, _ := http.NewRequest("POST", feBase+"/jitter/300", strings.NewReader("body of "+id))
					req.Host = multiHost
					req.Header.Set("X-Request-Id", id)
					resp, err := http.DefaultClient.Do(req)
					if err != nil {
						k = "http transport error"
					} else {
						b, rerr := io.ReadAll(resp.Body)
						resp.Body.Close()
						switch {
						case rerr != nil:
							k = "http body error"
						case resp.StatusCode == 200 && string(b) == want:
						case resp.StatusCode == 200 && len(b) == 0:
							k = "http 200 EMPTY (no invocation id)"
						case resp.StatusCode == 200:
							k = fmt.Sprintf("http WRONG %q", b)
						default:
							k = fmt.Sprintf("http %d", resp.StatusCode)
						}
					}
				}
				mu.Lock()
				tally[k]++
				mu.Unlock()
			}
		}()
	}
	time.Sleep(1500 * time.Millisecond)
	syscall.Kill(s.WorkPIDs[0], syscall.SIGKILL)
	wg.Wait()
	t.Logf("what callers were told: %v", tally)
	for k := range tally {
		if strings.Contains(k, "WRONG") {
			t.Errorf("a caller was handed a wrong response: %s", k)
		}
		if strings.Contains(k, "EMPTY") {
			// This was the bug: a worker dying mid-call was passed on as "cancelled", which a
			// frontend reads as "nobody is waiting", and net/http turns into an empty 200.
			t.Errorf("a waiting caller was answered 200 with nothing (%d times): %s", tally[k], k)
		}
	}
}
