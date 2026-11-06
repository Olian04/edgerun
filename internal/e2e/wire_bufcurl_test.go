package e2e

// Part 2: a client that has nothing to do with Go or with this repository's
// client library, `buf curl`, against a real orchestrator and a real worker: one
// unary and one streaming RPC, over Connect, gRPC and gRPC-Web.

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func bufCurl(t *testing.T, args ...string) (string, error) {
	t.Helper()
	full := append([]string{"tool", "-modfile=tools/go.mod", "buf", "curl", "--schema", "proto"}, args...)
	cmd := exec.Command("go", full...)
	cmd.Dir = "../.." // the repository root, where buf.yaml and tools/go.mod are
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	done := make(chan error, 1)
	go func() { done <- cmd.Run() }()
	select {
	case err := <-done:
		if err != nil {
			return out.String(), &bufCurlError{err, errb.String()}
		}
		return out.String(), nil
	case <-time.After(240 * time.Second):
		cmd.Process.Kill()
		t.Fatalf("buf curl %v did not finish", args)
		return "", nil
	}
}

type bufCurlError struct {
	err    error
	stderr string
}

func (e *bufCurlError) Error() string { return e.err.Error() + ": " + e.stderr }

func jsonDocs(t *testing.T, s string) []map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(s))
	var out []map[string]any
	for {
		var m map[string]any
		if err := dec.Decode(&m); err == io.EOF {
			return out
		} else if err != nil {
			t.Fatalf("buf curl printed something that is not JSON: %v\n%s", err, s)
		}
		out = append(out, m)
	}
}

func TestWireBufCurl(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go command")
	}
	// Does the tool run at all here (offline, from the module cache)?
	if _, err := bufCurl(t, "--help"); err != nil {
		t.Skipf("buf curl cannot run in this environment: %v", err)
	}
	e := newCEnv(t)
	hdr := func(token string) []string { return []string{"-H", "Authorization: Bearer " + token} }
	call := func(proto string, token string, url string, data string, extra ...string) []map[string]any {
		t.Helper()
		args := append([]string{"--protocol", proto, "--http2-prior-knowledge", "-d", data}, hdr(token)...)
		args = append(args, extra...)
		args = append(args, url)
		out, err := bufCurl(t, args...)
		if err != nil {
			t.Fatalf("buf curl %s %s: %v\n%s", proto, url, err, out)
		}
		return jsonDocs(t, out)
	}

	for _, proto := range []string{"connect", "grpc", "grpcweb"} {
		t.Run(proto, func(t *testing.T) {
			// unary, against the orchestrator
			docs := call(proto, ingressToken, e.orch.base+"/edgerun.ingress.v1.IngressService/Describe", `{}`)
			if len(docs) != 1 || docs[0]["maxRequestBodyBytes"] == nil {
				t.Errorf("Describe: %v", docs)
			}
			impl, _ := docs[0]["implementation"].(map[string]any)
			if impl["name"] != "edgerun-orchestrator" {
				t.Errorf("Describe: implementation %v", impl)
			}
			// server-streaming, against the orchestrator: a head, then the body
			docs = call(proto, ingressToken, e.orch.base+"/edgerun.ingress.v1.IngressService/Invoke",
				`{"requestId":"buf","request":{"method":"POST","host":"compat.test","path":"/echo","body":"`+base64.StdEncoding.EncodeToString([]byte("through buf curl"))+`"}}`)
			var body []byte
			status := float64(0)
			for _, d := range docs {
				if h, ok := d["head"].(map[string]any); ok {
					status, _ = h["http"].(map[string]any)["status"].(float64)
				}
				if c, ok := d["chunk"].(string); ok {
					b, _ := base64.StdEncoding.DecodeString(c)
					body = append(body, b...)
				}
			}
			if status != 200 || string(body) != "through buf curl" {
				t.Errorf("Invoke: status %v body %q from %v", status, body, docs)
			}
			// unary and streaming against the worker
			docs = call(proto, executionToken, e.worker.base+"/edgerun.worker.v1.ExecutionService/Describe", `{}`)
			if len(docs) != 1 || docs[0]["info"] == nil {
				t.Errorf("worker Describe: %v", docs)
			}
			docs = call(proto, executionToken, e.worker.base+"/edgerun.worker.v1.ExecutionService/Execute",
				`{"invocationId":"buf","artifactDigest":"`+e.digest+`","abi":"wagi-1","request":{"method":"GET","host":"x","path":"/status","rawQuery":"code=418"}}`)
			if len(docs) < 2 || docs[0]["head"] == nil || docs[len(docs)-1]["summary"] == nil {
				t.Errorf("Execute: %v", docs)
			} else if st := docs[0]["head"].(map[string]any)["status"]; st != float64(418) {
				t.Errorf("Execute: status %v", st)
			}
		})
	}

	t.Run("an error is printed with its code and details", func(t *testing.T) {
		args := append([]string{"--protocol", "connect", "-d", `{"request":{"method":"GET","host":"nowhere.test","path":"/"}}`}, hdr(ingressToken)...)
		out, err := bufCurl(t, append(args, e.orch.base+"/edgerun.ingress.v1.IngressService/Invoke")...)
		if err == nil {
			t.Fatalf("an unrouted host succeeded: %s", out)
		}
		msg := err.Error() + out
		if !strings.Contains(msg, "not_found") || !strings.Contains(msg, "edgerun.common.v1.ErrorInfo") || !strings.Contains(msg, "ERROR_REASON_FUNCTION_NOT_FOUND") {
			t.Errorf("the error as buf curl shows it: %s", msg)
		}
	})
}
