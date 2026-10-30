//go:build wasip1

package main

import "unsafe"

//go:wasmimport wasi_snapshot_preview1 environ_sizes_get
func environSizesGet(count, size uint32) uint32

//go:wasmimport wasi_snapshot_preview1 environ_get
func environGet(environ, buf uint32) uint32

// rawEnviron reads the environment block the runtime wrote, entry by entry.
// The Go runtime's own copy (os.Environ) drops duplicate names.
func rawEnviron() []string {
	var count, size uint32
	if environSizesGet(uint32(uintptr(unsafe.Pointer(&count))), uint32(uintptr(unsafe.Pointer(&size)))) != 0 || count == 0 {
		return nil
	}
	ptrs := make([]uint32, count)
	buf := make([]byte, size)
	if environGet(uint32(uintptr(unsafe.Pointer(&ptrs[0]))), uint32(uintptr(unsafe.Pointer(&buf[0])))) != 0 {
		return nil
	}
	base := uint32(uintptr(unsafe.Pointer(&buf[0])))
	out := make([]string, 0, count)
	for _, p := range ptrs {
		off := p - base
		end := off
		for buf[end] != 0 {
			end++
		}
		out = append(out, string(buf[off:end]))
	}
	return out
}
