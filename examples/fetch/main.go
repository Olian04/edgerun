// Command fetch is a wagi-2 function: it makes an outbound HTTP request, which
// the function may do only if its operator has listed the destination.
//
// Build:  GOOS=wasip1 GOARCH=wasm go build -o fetch.wasm ./examples/fetch
// Deploy: edgerunctl create fetch
//
//	edgerunctl deploy fetch fetch.wasm --abi wagi-2 --activate
//	edgerunctl allow fetch example.com        # the network is closed until you say where
//	edgerunctl route fetch.localhost / fetch
//
// Try:    curl -H 'Host: fetch.localhost' 'http://localhost:8000/?url=https://example.com/'
//
// The request is the text "METHOD URL", header lines, a blank line, a body; the
// answer is "Status: N", header lines, a blank line, the body. See docs/protocol.md,
// "Guest ABI wagi-2".
package main

import (
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
)

func main() {
	q, _ := url.ParseQuery(os.Getenv("QUERY_STRING"))
	target := q.Get("url")
	if target == "" {
		fmt.Print("Status: 400 Bad Request\nContent-Type: text/plain\n\nadd ?url=https://example.com/\n")
		return
	}

	code, resp := get(target)
	fmt.Print("Content-Type: text/plain\n")
	switch code {
	case codeDenied:
		fmt.Printf("Status: 403 Forbidden\n\n%s is not on this function's allow list\n", target)
	case codeFailed:
		fmt.Printf("Status: 502 Bad Gateway\n\ncould not reach %s\n", target)
	case codeTooLarge:
		fmt.Printf("Status: 502 Bad Gateway\n\nthe response from %s was too large\n", target)
	default:
		if code < 0 {
			fmt.Printf("Status: 500 Internal Server Error\n\nfetch failed with %d\n", code)
			return
		}
		head, body, _ := strings.Cut(string(resp), "\n\n")
		fmt.Printf("\n%s answered:\n%s\n\n%d bytes of body\n", target, strings.ReplaceAll(head, "\n", "\n  "), len(body))
	}
}

// Results of fetch that are not a size (docs/protocol.md).
const (
	codeDenied   = -1
	codeFailed   = -2
	codeTooLarge = -3
)

// get performs "GET target" and reads the whole response.
func get(target string) (int32, []byte) {
	req := []byte("GET " + target + "\nAccept: */*\n\n")
	code := fetch(addr(req), uint32(len(req)))
	runtime.KeepAlive(req)
	if code < 0 {
		return code, nil
	}
	var resp []byte
	buf := make([]byte, 4096)
	for {
		n := fetchRead(addr(buf), uint32(len(buf)))
		if n <= 0 {
			return code, resp
		}
		resp = append(resp, buf[:n]...)
	}
}
