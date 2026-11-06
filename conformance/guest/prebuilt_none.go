//go:build !prebuilt

package guest

// Without the prebuilt tag the guests are built on demand, with the Go
// toolchain that runs the tests.
var prebuiltTest, prebuiltNet []byte
