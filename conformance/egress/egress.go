// Package egressconf is the conformance suite for the Egress platform service
// (egress.v1 EgressService). Host: network service. Client: worker (the orchestrator only routes). Run judges a network service; RunClient judges a worker as the client, against scripted network services.
//
// The suite is black-box: it judges an implementation that is already running,
// plays every other component itself using the generated protocol code and
// nothing else, and looks only at what crosses the wire.
package egressconf

import (
	"testing"

	"github.com/Olian04/edgerun/conformance/internal/netsuite"
	"github.com/Olian04/edgerun/conformance/internal/workersuite"
)

// Target is the network service under test and how to reach the suite; Env says
// how to configure it, and Instance is a running one.
type (
	Target   = netsuite.Target
	Env      = netsuite.Env
	Instance = netsuite.Instance
)

// Run judges the network service as the host of Egress. It plays the worker that
// calls Fetch, the orchestrator the service registers with, and the web servers
// it is asked to reach.
func Run(t *testing.T, target Target) { netsuite.Egress(t, target) }

// ClientTarget is the worker under test and how to reach the suite.
type (
	ClientTarget = workersuite.Target
	ClientEnv    = workersuite.Env
)

// RunClient judges a worker as the client of Egress: a guest's outbound request
// goes to the network service its grant names, with the allow list, budget and
// credentials the protocol says, and the guest sees what the ABI says. It plays
// the network services.
func RunClient(t *testing.T, target ClientTarget) { workersuite.EgressClient(t, target) }
