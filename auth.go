package edgerun

import (
	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/internal/authn"
)

// A token carries exactly one scope. These are the scopes of the protocols;
// docs/protocol.md says which seam takes which.
const (
	// ScopeIngress: a frontend calling an orchestrator's IngressService.
	ScopeIngress = "ingress"
	// ScopeWorker: a worker calling an orchestrator's FleetService and ArtifactService.
	ScopeWorker = "worker"
	// ScopeAdmin: an operator calling an orchestrator's control services.
	ScopeAdmin = "admin"
	// ScopeNetwork: a network service calling an orchestrator's RegistryService.
	ScopeNetwork = "network"
	// ScopeExecute: an orchestrator calling a worker's ExecutionService.
	ScopeExecute = "execute"
	// ScopeEgress: a worker calling a network service's EgressService.
	ScopeEgress = "egress"
)

// Verifier maps the bearer tokens a server accepts to their scopes.
type Verifier = authn.Verifier

// Authenticate returns an interceptor for a handler that admits the given
// scopes: a missing or unknown token fails with UNAUTHENTICATED, a known token
// of another scope with PERMISSION_DENIED. It is for implementing a role:
//
//	path, h := egressv1connect.NewEgressServiceHandler(svc,
//		connect.WithInterceptors(edgerun.Authenticate(edgerun.Verifier{secret: edgerun.ScopeEgress}, edgerun.ScopeEgress)))
func Authenticate(v Verifier, scopes ...string) connect.Interceptor {
	return authn.Server(v, scopes...)
}
