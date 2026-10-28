// A wagi-1 guest for the tests. version is stamped at build time so that one
// source yields modules with different digests.
package main

import (
	"fmt"
	"os"
)

var version = "v1"

func main() {
	fmt.Print("Content-Type: text/plain\n\n")
	fmt.Printf("version=%s greeting=%s secret=%s path=%s\n", version, os.Getenv("GREETING"), os.Getenv("API_KEY"), os.Getenv("PATH_INFO"))
}
