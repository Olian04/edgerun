//go:build !wasip1

package main

// The host module exists only for wasip1; these let the package compile on the
// machine that builds the repository.

func fetch(req, n uint32) int32     { panic("this example runs only as a wasip1 module") }
func fetchRead(dst, n uint32) int32 { panic("this example runs only as a wasip1 module") }
func addr(b []byte) uint32          { panic("this example runs only as a wasip1 module") }
