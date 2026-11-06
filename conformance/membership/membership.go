// Package membershipconf is the conformance suite for the Membership platform service
// (membership.v1 MembershipService). Host: orchestrator. Clients: worker and network service (register, heartbeat, deregister) and operator (list). Run judges the host; RunWorkerClient and RunNetworkServiceClient judge a worker or a network service as a client, against a scripted orchestrator.
//
// The suite is black-box: it judges an implementation that is already running,
// plays every other component itself using the generated protocol code and
// nothing else, and looks only at what crosses the wire.
package membershipconf

import (
	"testing"

	"github.com/Olian04/edgerun/conformance/internal/netsuite"
	"github.com/Olian04/edgerun/conformance/internal/orchsuite"
	"github.com/Olian04/edgerun/conformance/internal/workersuite"
)

// Target is the orchestrator under test and how to reach the suite; Env says how
// to configure it. The orchestrator must be new and empty, and used by nothing else.
type (
	Target = orchsuite.Target
	Env    = orchsuite.Env
)

// Run judges the orchestrator as the host of Membership.
func Run(t *testing.T, target Target) { orchsuite.Membership(t, target) }

// WorkerTarget is the worker under test and how to reach the suite.
type (
	WorkerTarget = workersuite.Target
	WorkerEnv    = workersuite.Env
)

// RunWorkerClient judges a worker as a client of Membership: what it declares,
// its heartbeats and the load they report, registering again when forgotten, and
// draining and deregistering on shutdown (the suite stops it: it runs last).
func RunWorkerClient(t *testing.T, target WorkerTarget) { workersuite.MembershipClient(t, target) }

// NetworkServiceTarget is the network service under test and how to reach the suite.
type (
	NetworkServiceTarget   = netsuite.Target
	NetworkServiceEnv      = netsuite.Env
	NetworkServiceInstance = netsuite.Instance
)

// RunNetworkServiceClient judges a network service as a client of Membership: it
// keeps trying to register, declares what it carries in canonical form,
// heartbeats, registers again when forgotten, and on shutdown drains and
// deregisters.
func RunNetworkServiceClient(t *testing.T, target NetworkServiceTarget) {
	netsuite.MembershipClient(t, target)
}
