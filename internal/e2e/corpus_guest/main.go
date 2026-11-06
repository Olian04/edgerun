// Command corpus_guest is the wagi-1 guest of the HTTP corpus tests
// (internal/e2e/corpus_*_test.go). It does three things, selected by PATH_INFO:
//
//	/raw          writes the request body to stdout unchanged, so that a test
//	              decides every byte of the guest's response;
//	/sleep?ms=N   sleeps, then answers;
//	anything else dumps what the guest was given: every entry of the
//	              environment block exactly as the runtime wrote it (duplicates
//	              included, which os.Environ would hide), and the body.
//
// Build: GOOS=wasip1 GOARCH=wasm go build ./internal/e2e/corpus_guest
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

func lookup(env []string, name string) string {
	for _, e := range env {
		if k, v, ok := strings.Cut(e, "="); ok && k == name {
			return v
		}
	}
	return ""
}

func main() {
	body, _ := io.ReadAll(os.Stdin)
	env := rawEnviron()
	path := lookup(env, "PATH_INFO")
	switch {
	case path == "/raw":
		os.Stdout.Write(body)
	case path == "/sleep":
		ms, _ := strconv.Atoi(strings.TrimPrefix(lookup(env, "QUERY_STRING"), "ms="))
		time.Sleep(time.Duration(ms) * time.Millisecond)
		fmt.Print("Content-Type: text/plain\n\nslept\n")
	default:
		var b strings.Builder
		b.WriteString("Content-Type: text/x-corpus-dump\n\n")
		for _, e := range env {
			fmt.Fprintf(&b, "E %s\n", strconv.Quote(e))
		}
		sum := sha256.Sum256(body)
		fmt.Fprintf(&b, "BODY_LEN=%d\nBODY_SHA=%s\n", len(body), hex.EncodeToString(sum[:]))
		if len(body) <= 8192 {
			fmt.Fprintf(&b, "BODY=%s\n", strconv.Quote(string(body)))
		} else {
			fmt.Fprintf(&b, "BODY=%s\n", strconv.Quote(string(body[:64])))
		}
		os.Stdout.WriteString(b.String())
	}
}
