//go:build !wasip1

package main

import "os"

// The guest runs only as a wasip1 module; this lets the package compile, and be
// vetted, on the machine that builds the repository.
func rawEnviron() []string { return os.Environ() }
