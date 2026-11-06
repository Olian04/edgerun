// Package ingressconf is the conformance suite for the Ingress platform service
// (ingress.v1 IngressService). Host: orchestrator. Client: frontend, whose duties are judged by the Edge suite (conformance/edge), which plays the orchestrator behind it.
//
// The suite is black-box: it judges an implementation that is already running,
// plays every other component itself using the generated protocol code and
// nothing else, and looks only at what crosses the wire.
package ingressconf

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

// Run judges the orchestrator as the host of Ingress.
func Run(t *testing.T, target Target) { orchsuite.Ingress(t, target) }
