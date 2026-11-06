package control_test

import (
	"testing"

	"connectrpc.com/connect"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

// A plan that fails because the platform does is an error of the call, not a
// plan that is not valid: a tool told "step 2 is invalid" would stop trying to
// apply a plan that is fine. No black-box suite can break the database, so the
// test does.
func TestAPlatformFailureIsAnErrorAndNotAFailureOfThePlan(t *testing.T) {
	r := newRig(t, nil)
	f, err := r.ctl.CreateFunction(ctx, &controlv1.CreateFunctionRequest{Name: "fine"})
	if err != nil {
		t.Fatal(err)
	}
	// Break one table: the statements of the second step then fail in ways the
	// protocol has no word for.
	r.breakTable(t, "env_vars")

	steps := []*controlv1.Step{
		{Request: &controlv1.Step_SetLabels{SetLabels: &controlv1.SetLabelsRequest{FunctionId: f.Function.Id, Labels: map[string]string{"a": "b"}}}},
		{Request: &controlv1.Step_SetEnv{SetEnv: &controlv1.SetEnvRequest{FunctionId: f.Function.Id, Entry: &controlv1.EnvEntry{Name: "X", Value: "1"}}}},
	}
	v, err := r.ctl.Validate(ctx, &controlv1.ValidateRequest{Steps: steps})
	if err == nil || code(err) != connect.CodeInternal {
		t.Errorf("Validate: %v, %v: want an internal error, not a failure of the plan", v, err)
	}
	a, err := r.ctl.Apply(ctx, &controlv1.ApplyRequest{Steps: steps})
	if err == nil || code(err) != connect.CodeInternal {
		t.Errorf("Apply: %v, %v: want an internal error, not a failure of the plan", a, err)
	}
	if err != nil && (len(err.Error()) > 200 || containsAny(err.Error(), "env_vars", "relation", "pgx", "SQLSTATE")) {
		t.Errorf("database details reached the client: %v", err)
	}
	// And the first step of the failed apply was undone with the rest.
	g, err := r.ctl.GetFunction(ctx, &controlv1.GetFunctionRequest{FunctionId: f.Function.Id})
	if err != nil || len(g.Function.Labels) != 0 {
		t.Errorf("a step of a plan that errored was kept: %v %v", g, err)
	}
}

func containsAny(s string, subs ...string) bool {
	for _, x := range subs {
		for i := 0; i+len(x) <= len(s); i++ {
			if s[i:i+len(x)] == x {
				return true
			}
		}
	}
	return false
}
