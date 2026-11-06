// Package routingconf is the conformance suite for the Routing platform service
// (control.v1 RouteService). Host: orchestrator. Client: operator tools.
//
// The suite is black-box: it judges an implementation that is already running,
// plays every other component itself using the generated protocol code and
// nothing else, and looks only at what crosses the wire.
package routingconf

import (
	"testing"

	"github.com/Olian04/edgerun/conformance/internal/orchsuite"
)

// Target is the orchestrator under test and how to reach the suite; Env says how
// to configure it. The orchestrator must be new and empty, and used by nothing else.
type (
	Target = orchsuite.Target
	Env    = orchsuite.Env
)

// Run judges the orchestrator as the host of Routing.
func Run(t *testing.T, target Target) { orchsuite.Routing(t, target) }
