// A wagi-1 guest for the tests. version is stamped at build time so that one
// source yields modules with different digests.
package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

var version = "v1"

func main() {
	fmt.Print("Content-Type: text/plain\n\n")
	if os.Getenv("PATH_INFO") == "/env" {
		// Every variable name the guest can see, to prove what it is shown.
		var names []string
		for _, kv := range os.Environ() {
			names = append(names, strings.SplitN(kv, "=", 2)[0])
		}
		sort.Strings(names)
		fmt.Println(strings.Join(names, ","))
		return
	}
	fmt.Printf("version=%s greeting=%s secret=%s path=%s\n", version, os.Getenv("GREETING"), os.Getenv("API_KEY"), os.Getenv("PATH_INFO"))
}
