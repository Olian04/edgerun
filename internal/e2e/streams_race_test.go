//go:build race

package e2e

// stRaceBuild is true under -race, where everything is several times slower: the
// sweeps of the stream and fault tests then take fewer, coarser samples.
const stRaceBuild = true
