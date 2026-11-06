// Command hello is a minimal edgerun function: a wagi-1 guest that greets.
//
// Build:  GOOS=wasip1 GOARCH=wasm go build -o hello.wasm ./examples/hello
// Deploy: edgerunctl deploy hello hello.wasm --activate
//
// A wagi-1 guest gets the request in environment variables and on stdin, and
// writes a CGI-style response to stdout: headers, a blank line, then the body.
// See docs/protocol.md.
package main

import (
	"fmt"
	"net/url"
	"os"
)

func main() {
	query, _ := url.ParseQuery(os.Getenv("QUERY_STRING"))
	name := query.Get("name")
	if name == "" {
		name = "world"
	}
	// GREETING is an environment variable set on the function.
	greeting := os.Getenv("GREETING")
	if greeting == "" {
		greeting = "Hello"
	}

	fmt.Print("Content-Type: text/plain; charset=utf-8\n\n")
	fmt.Printf("%s, %s!\n", greeting, name)
	fmt.Printf("You asked for %s %s", os.Getenv("REQUEST_METHOD"), os.Getenv("PATH_INFO"))
	if ip := os.Getenv("HTTP_X_FORWARDED_FOR"); ip != "" {
		fmt.Printf(" from %s", ip)
	}
	fmt.Println(".")
}
