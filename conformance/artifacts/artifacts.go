// Package artifactsconf is the conformance suite for the Artifacts platform service
// (worker.v1 ArtifactService). Host: orchestrator. Client: worker. Run judges the host; RunClient judges a worker as the client, against a scripted orchestrator.
//
// The suite is black-box: it judges an implementation that is already running,
// plays every other component itself using the generated protocol code and
// nothing else, and looks only at what crosses the wire.
package artifactsconf

import (
	"testing"

	"github.com/Olian04/edgerun/conformance/internal/orchsuite"
	"github.com/Olian04/edgerun/conformance/internal/workersuite"
)

// Target is the orchestrator under test and how to reach the suite; Env says how
// to configure it. The orchestrator must be new and empty, and used by nothing else.
type (
	Target = orchsuite.Target
	Env    = orchsuite.Env
)

// Run judges the orchestrator as the host of Artifacts.
func Run(t *testing.T, target Target) { orchsuite.Artifacts(t, target) }

// ClientTarget is the worker under test and how to reach the suite.
type (
	ClientTarget = workersuite.Target
	ClientEnv    = workersuite.Env
)

// RunClient judges a worker as the client of Artifacts: it fetches a module by
// digest, never runs bytes that do not hash to it, and does not remember bad
// bytes. The suite plays the orchestrator that serves them.
func RunClient(t *testing.T, target ClientTarget) { workersuite.ArtifactsClient(t, target) }
