//go:build prebuilt

package guest

import _ "embed"

// Built with `make conformance`, which compiles the guests before the binary
// that carries them, so that the binary needs no Go toolchain to run.

//go:embed prebuilt/testguest.wasm
var prebuiltTest []byte

//go:embed prebuilt/netguest.wasm
var prebuiltNet []byte
