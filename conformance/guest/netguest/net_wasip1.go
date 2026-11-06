//go:build wasip1

package main

//go:wasmimport edgerun_http fetch
func fetch(req, n uint32) int32

//go:wasmimport edgerun_http fetch_read
func fetchRead(dst, n uint32) int32
