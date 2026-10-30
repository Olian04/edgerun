// Package edgerun is the client library of the edgerun protocols: one typed
// client per seam, so that a program of your own can talk to an edgerun
// orchestrator, worker or network service in the roles the protocols define,
// and extend the system without touching it.
//
// edgerun is a set of roles that know each other only through versioned
// protocols (proto/edgerun, specified in docs/protocol.md). A role is one side
// of a seam; the library gives you the other side:
//
//	you are        you talk to        with
//	an operator    an orchestrator    NewControl   (control.v1)
//	a frontend     an orchestrator    NewIngress   (ingress.v1)
//	a worker       an orchestrator    NewFleet, NewArtifacts   (worker.v1)
//	a scheduler    a worker           NewExecution (worker.v1)
//	a network svc  an orchestrator    NewRegistry  (egress.v1)
//	a worker       a network service  NewEgress    (egress.v1)
//
// Each client is the protocol and nothing more: one method for every RPC, named
// as the RPC, taking and returning the generated messages (the packages under
// gen/ — import them for the message types). The library adds only what every
// caller would otherwise write for itself: the bearer token, the unwrapping of
// Connect's envelopes, iterators over paged lists, streamed responses as
// readers, and the verification of artifacts the protocol obliges a client to
// do. It has no retries, no caches and no workflow of its own; those are policy,
// and policy belongs to the program using it.
//
//	ctl := edgerun.NewControl("http://orchestrator:8080", adminToken)
//	fn, err := ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "hello"})
//	...
//	ing := edgerun.NewIngress("http://orchestrator:8080", ingressToken)
//	res, err := ing.Invoke(ctx, &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: "x.test", Path: "/"}})
//	if edgerun.IsReason(err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND) { ... }
//
// A token has one scope, so a client needs the token of its role: see the
// Scope constants. Platform errors carry a reason (Reason, IsReason); branch on that
// and never on messages. Control errors are plain Connect codes (connect.CodeOf). Responses stream: see Response.
//
// To implement a role instead of calling one, use the generated handlers in
// gen/ with Authenticate for the tokens and NewError for the errors, and judge
// the result with the conformance suite (cmd/edgerun-conformance).
package edgerun
