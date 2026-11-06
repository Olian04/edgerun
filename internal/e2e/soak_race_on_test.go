//go:build race

package e2e

// Under the race detector wazero compiles a guest an order of magnitude slower,
// so the soak makes less work: fewer clients, slower worker churn.
const soakRace = true
