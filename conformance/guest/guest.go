// Package guest builds the conformance test guest on demand, so no binaries
// are committed. It needs a Go toolchain at test time, which `go test`
// already implies.
package guest

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

//go:embed testguest/main.go
var source []byte

// Build returns the wasip1 module for the test guest. Builds are cached by
// source hash under the user cache directory and are safe to run from
// concurrently executing test binaries.
func Build(t testing.TB) []byte {
	t.Helper()
	sum := sha256.Sum256(source)
	name := "testguest-" + hex.EncodeToString(sum[:6]) + ".wasm"

	cacheRoot, err := os.UserCacheDir()
	if err != nil {
		cacheRoot = os.TempDir()
	}
	dir := filepath.Join(cacheRoot, "edgerun-conformance")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("guest: %v", err)
	}
	cached := filepath.Join(dir, name)
	if b, err := os.ReadFile(cached); err == nil {
		return b
	}

	work := t.TempDir()
	must(t, os.WriteFile(filepath.Join(work, "main.go"), source, 0o644))
	must(t, os.WriteFile(filepath.Join(work, "go.mod"), []byte("module testguest\n\ngo 1.21\n"), 0o644))
	out := filepath.Join(work, "out.wasm")
	cmd := exec.Command("go", "build", "-trimpath", "-o", out, ".")
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0", "GOFLAGS=-mod=mod")
	if msg, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building test guest: %v\n%s", err, msg)
	}
	b, err := os.ReadFile(out)
	must(t, err)

	// Publish atomically: another test binary may be doing the same.
	tmp, err := os.CreateTemp(dir, "tmp-*")
	must(t, err)
	_, werr := tmp.Write(b)
	must(t, werr)
	must(t, tmp.Close())
	must(t, os.Rename(tmp.Name(), cached))
	return b
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("guest: %v", err)
	}
}
