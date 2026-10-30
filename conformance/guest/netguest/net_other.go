//go:build !wasip1

package main

// The host module exists only for wasip1; these let the package compile, and
// be vetted, on the machine that builds the repository.

func fetch(req, n uint32) int32 { panic("netguest runs only as a wasip1 module") }

func fetchRead(dst, n uint32) int32 { panic("netguest runs only as a wasip1 module") }
