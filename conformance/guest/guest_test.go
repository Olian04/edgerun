package guest

import (
	"bytes"
	"testing"
)

func TestBuildProducesWasmAndCaches(t *testing.T) {
	a := Build(t)
	if !bytes.HasPrefix(a, []byte("\x00asm")) {
		t.Fatalf("not a wasm module: % x", a[:8])
	}
	b := Build(t) // cached path
	if !bytes.Equal(a, b) {
		t.Error("cached build differs from first build")
	}
}
