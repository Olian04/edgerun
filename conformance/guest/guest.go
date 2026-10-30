// Package guest builds the conformance test guest on demand, so no binaries
// are committed. It needs a Go toolchain at test time, which `go test`
// already implies.
package guest

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

//go:embed testguest/*.go
var testguest embed.FS

//go:embed netguest/*.go
var netguest embed.FS

// Build returns the wasip1 module for the test guest (a wagi-1 module). Builds
// are cached by source hash under the user cache directory and are safe to run
// from concurrently executing test binaries.
func Build(t testing.TB) []byte {
	t.Helper()
	if len(prebuiltTest) > 0 {
		return prebuiltTest
	}
	return build(t, testguest, "testguest")
}

// BuildNet returns the wasip1 module for the network test guest, a wagi-2
// module that imports edgerun_http.
func BuildNet(t testing.TB) []byte {
	t.Helper()
	if len(prebuiltNet) > 0 {
		return prebuiltNet
	}
	return build(t, netguest, "netguest")
}

func build(t testing.TB, src embed.FS, dir string) []byte {
	t.Helper()
	files, err := fs.ReadDir(src, dir)
	must(t, err)
	h := sha256.New()
	sources := map[string][]byte{}
	for _, f := range files {
		b, err := src.ReadFile(dir + "/" + f.Name())
		must(t, err)
		sources[f.Name()] = b
		h.Write([]byte(f.Name()))
		h.Write(b)
	}
	name := dir + "-" + hex.EncodeToString(h.Sum(nil)[:6]) + ".wasm"

	cacheRoot, err := os.UserCacheDir()
	if err != nil {
		cacheRoot = os.TempDir()
	}
	cacheDir := filepath.Join(cacheRoot, "edgerun-conformance")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatalf("guest: %v", err)
	}
	cached := filepath.Join(cacheDir, name)
	if b, err := os.ReadFile(cached); err == nil {
		return b
	}

	work := t.TempDir()
	for n, b := range sources {
		must(t, os.WriteFile(filepath.Join(work, n), b, 0o644))
	}
	must(t, os.WriteFile(filepath.Join(work, "go.mod"), []byte("module "+dir+"\n\ngo 1.21\n"), 0o644))
	out := filepath.Join(work, "out.wasm")
	cmd := exec.Command("go", "build", "-trimpath", "-o", out, ".")
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0", "GOFLAGS=-mod=mod")
	if msg, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building test guest: %v\n%s\n(the worker suite needs a Go toolchain to build its guests, or an edgerun-conformance built with `make conformance`, which carries them)", err, msg)
	}
	b, err := os.ReadFile(out)
	must(t, err)

	// Publish atomically: another test binary may be doing the same.
	tmp, err := os.CreateTemp(cacheDir, "tmp-*")
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
