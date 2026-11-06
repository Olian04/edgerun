//go:build wasip1

package main

import "unsafe"

//go:wasmimport edgerun_http fetch
func fetch(req, n uint32) int32

//go:wasmimport edgerun_http fetch_read
func fetchRead(dst, n uint32) int32

// addr is the address of b in linear memory, which is what the host module
// takes. wasm32 addresses are 32 bits.
func addr(b []byte) uint32 { return uint32(uintptr(unsafe.Pointer(&b[0]))) }
