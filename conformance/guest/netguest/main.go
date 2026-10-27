// Command netguest is a wagi-2 guest whose behaviour is selected by the request
// path. It is the network counterpart of testguest: every edge of the
// edgerun_http host module (docs/protocol.md, "Guest ABI wagi-2") is one path.
//
// It is a module of its own because it imports edgerun_http, which a wagi-1
// module may not: the same bytes deployed as wagi-1 must be refused.
//
// Build: GOOS=wasip1 GOARCH=wasm go build ./conformance/guest/netguest
package main

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"unsafe"
)

// addr is the address of b in linear memory, which is what the host module
// takes. wasm32 addresses are 32 bits; Go's collector does not move objects.
func addr(b []byte) uint32 {
	if len(b) == 0 {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(&b[0])))
}

func doFetch(req []byte) int32 {
	code := fetch(addr(req), uint32(len(req)))
	runtime.KeepAlive(req)
	return code
}

func read(dst []byte) int32 {
	n := fetchRead(addr(dst), uint32(len(dst)))
	runtime.KeepAlive(dst)
	return n
}

// call performs fetch on the request text and reads the whole response.
func call(req []byte) (code int32, resp []byte) {
	code = doFetch(req)
	if code < 0 {
		return code, nil
	}
	// A small buffer, so that the cursor semantics of fetch_read are used.
	buf := make([]byte, 100)
	for {
		n := read(buf)
		if n <= 0 {
			return code, resp
		}
		resp = append(resp, buf[:n]...)
	}
}

func main() {
	q, _ := url.ParseQuery(os.Getenv("QUERY_STRING"))
	body, _ := io.ReadAll(os.Stdin)
	header := "Content-Type: application/octet-stream\n\n"

	switch os.Getenv("PATH_INFO") {
	case "/noop":
		fmt.Print("Content-Type: text/plain\n\nno network used\n")
	case "/fetch":
		// The body is the request text; the answer is "code=N\n" and the response.
		code, resp := call(body)
		fmt.Printf("%scode=%d\n", header, code)
		os.Stdout.Write(resp)
	case "/fetch-many":
		n, _ := strconv.Atoi(q.Get("n"))
		var codes []string
		for i := 0; i < n; i++ {
			code, _ := call(body)
			codes = append(codes, strconv.Itoa(int(code)))
		}
		fmt.Printf("%s%s\n", header, strings.Join(codes, " "))
	case "/read-without-fetch":
		fmt.Printf("%sread=%d\n", header, read(make([]byte, 16)))
	case "/fetch-discards":
		// A new fetch discards what the last one left unread.
		first := doFetch(body)
		read(make([]byte, 4)) // read only a little
		second := doFetch(body)
		var all []byte
		big := make([]byte, 4096)
		for {
			n := read(big)
			if n <= 0 {
				break
			}
			all = append(all, big[:n]...)
		}
		fmt.Printf("%sfirst=%d second=%d read=%d\n", header, first, second, len(all))
	case "/fetch-fail-discards":
		// A fetch that fails leaves nothing to read, not what an earlier one left.
		first := doFetch(body)
		second := doFetch([]byte("GET http://deny.test/\n\n"))
		fmt.Printf("%sfirst=%d second=%d read=%d\n", header, first, second, read(make([]byte, 4096)))
	case "/bad-pointer":
		fmt.Printf("%scode=%d\n", header, fetch(0xFFFFFF00, 100))
	default:
		fmt.Print("Status: 404 Not Found\n\nunknown path\n")
	}
}
