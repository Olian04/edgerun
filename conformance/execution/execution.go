// Package executionconf is the conformance suite for the Execution platform service
// (worker.v1 ExecutionService). Host: worker. Client: orchestrator. Run judges a worker; RunClient judges an orchestrator as the client, against scripted workers.
//
// The suite is black-box: it judges an implementation that is already running,
// plays every other component itself using the generated protocol code and
// nothing else, and looks only at what crosses the wire.
package executionconf

import (
	"testing"

	"github.com/Olian04/edgerun/conformance/internal/orchsuite"
	"github.com/Olian04/edgerun/conformance/internal/workersuite"
)

// Target is the worker under test and how to reach the suite; Env says how to
// configure it.
type (
	Target = workersuite.Target
	Env    = workersuite.Env
)

// Run judges the worker as the host of Execution. It plays the orchestrator
// that calls it, and the one the worker registers with.
func Run(t *testing.T, target Target) { workersuite.Execution(t, target) }

// ClientTarget is the orchestrator under test and how to reach the suite. The
// orchestrator must be new and empty, and used by nothing else.
type (
	ClientTarget = orchsuite.Target
	ClientEnv    = orchsuite.Env
)

// RunClient judges the orchestrator as the client of Execution: what it asks of a
// worker, which worker it picks, how it retries and what it reports of the
// answers. It plays the workers.
func RunClient(t *testing.T, target ClientTarget) { orchsuite.ExecutionClient(t, target) }
