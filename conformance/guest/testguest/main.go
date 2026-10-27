// Command testguest is a wagi-1 guest whose behaviour is selected by the
// request path. It exists to exercise workers: every edge a worker must handle
// (limits, traps, malformed output) is one path here.
//
// Build: GOOS=wasip1 GOARCH=wasm go build ./conformance/guest/testguest
package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {
	q, _ := url.ParseQuery(os.Getenv("QUERY_STRING"))
	num := func(key string, def int) int {
		if v, err := strconv.Atoi(q.Get(key)); err == nil {
			return v
		}
		return def
	}
	body, _ := io.ReadAll(os.Stdin)

	switch os.Getenv("PATH_INFO") {
	case "/echo":
		ct := os.Getenv("CONTENT_TYPE")
		if ct == "" {
			ct = "text/plain"
		}
		fmt.Printf("Content-Type: %s\n\n", ct)
		os.Stdout.Write(body)
	case "/env":
		env := os.Environ()
		sort.Strings(env)
		fmt.Printf("Content-Type: text/plain\n\n%s\n", strings.Join(env, "\n"))
	case "/status":
		fmt.Printf("Status: %d Test\nX-Test: 1\nSet-Cookie: a=1\nSet-Cookie: b=2\nContent-Type: text/plain\n\nstatus\n", num("code", 200))
	case "/stderr":
		fmt.Fprintln(os.Stderr, "log line one")
		fmt.Fprintln(os.Stderr, "log line two")
		fmt.Print("Content-Type: text/plain\n\nok\n")
	case "/sleep":
		time.Sleep(time.Duration(num("ms", 100)) * time.Millisecond)
		fmt.Print("Content-Type: text/plain\n\nslept\n")
	case "/random":
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("Content-Type: text/plain\n\n%s\n", hex.EncodeToString(b))
	case "/time":
		fmt.Printf("Content-Type: text/plain\n\n%d\n", time.Now().Unix())
	case "/spin":
		for {
		}
	case "/memhog":
		b := make([]byte, num("mb", 100)<<20)
		for i := 0; i < len(b); i += 4096 {
			b[i] = 1
		}
		fmt.Printf("Content-Type: text/plain\n\nallocated %d\n", len(b))
	case "/exit":
		fmt.Fprintln(os.Stderr, "exiting on request")
		os.Exit(num("code", 1))
	case "/panic":
		panic("boom")
	case "/big":
		fmt.Print("Content-Type: application/octet-stream\n\n")
		chunk := []byte(strings.Repeat("x", 1024))
		for n := num("bytes", 1<<20); n > 0; n -= len(chunk) {
			os.Stdout.Write(chunk)
		}
	case "/noseparator":
		// ABI violation: headers never terminated.
		fmt.Print("Content-Type: text/plain")
	case "/empty":
		// ABI violation: no output at all.
	default:
		fmt.Print("Status: 404 Not Found\nContent-Type: text/plain\n\nunknown path\n")
	}
}
