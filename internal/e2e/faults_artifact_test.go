package e2e

// The worker's artifact stream (worker -> orchestrator, GetArtifact) cut at many
// places, through the fault proxy: the invocation fails by its budget with a typed
// INTERNAL (the fault is in the path to the orchestrator, not in the function),
// runs nothing, frees its slot, and the same module runs once the link is whole.

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

const ftArtifactMatch = "ArtifactService/GetArtifact"

// function deploys module as a new function and returns the host that routes to it.
func (s *stack) ftFunction(tag string, module []byte) string {
	fid := s.createFunction("ft-" + tag)
	s.uploadVersion(fid, module, nil, true)
	host := tag + ".test"
	s.route(host, "/", fid)
	return host
}

// ftWithSection appends a custom section to a wasm module: the same program with another digest.
func ftWithSection(module []byte, tag string) []byte {
	m := append([]byte(nil), module...)
	m = append(m, 0x00, byte(len(tag)+2), byte(len(tag)))
	m = append(m, tag...)
	return append(m, 'v')
}

// ftBigWasm is the tiny module with n bytes of incompressible padding in a custom
// section: a module that takes many chunks on the wire and costs nothing to run.
func ftBigWasm(n int) []byte {
	pad := make([]byte, n)
	rand.New(rand.NewSource(7)).Read(pad)
	content := append(binary.AppendUvarint(nil, 3), "pad"...)
	content = append(content, pad...)
	m := stTinyWasm("big")
	m = append(m, 0x00)
	m = binary.AppendUvarint(m, uint64(len(content)))
	return append(m, content...)
}

func TestFaults_ArtifactStreamBreaks(t *testing.T) {
	r := ftNewFleetRig(t, true)
	s := r.s
	tiny := stTinyWasm("base")
	px := r.px
	var seq int
	fresh := func(module []byte) (host string) {
		seq++
		return s.ftFunction(fmt.Sprintf("art%d", seq), ftWithSection(module, fmt.Sprintf("c%d", seq)))
	}
	callTiny := func(host string, budget time.Duration) result {
		return s.call(req{host: host, path: "/", budget: budget})
	}

	// what an artifact exchange is on the wire
	px.Capture()
	px.Arm(ftFault{Match: ftArtifactMatch})
	if res := callTiny(fresh(tiny), 10*time.Second); res.err != nil || string(res.body) != "ok\n" {
		t.Fatalf("a tiny module: %v %q", res.err, res.body)
	}
	c2s, s2c := px.Captured()
	reqBytes, respBytes := len(c2s), len(s2c)
	t.Logf("a GetArtifact of a %d-byte module is %d bytes out and %d bytes back", len(tiny), reqBytes, respBytes)
	if reqBytes == 0 || respBytes == 0 {
		t.Fatal("the proxy saw no artifact exchange")
	}

	type fault struct {
		name string
		f    ftFault
		ok   bool // the cut is after everything that matters: success is allowed
	}
	var faults []fault
	for _, n := range ftSteps(respBytes, 80, 4) {
		faults = append(faults, fault{fmt.Sprintf("s2c reset@%d", n), ftFault{Match: ftArtifactMatch, S2C: ftDir{ftReset, n}}, int(n) >= respBytes-8})
	}
	for _, n := range ftSteps(respBytes, 0, 11) {
		faults = append(faults, fault{fmt.Sprintf("s2c close@%d", n), ftFault{Match: ftArtifactMatch, S2C: ftDir{ftClose, n}}, int(n) >= respBytes-8})
		faults = append(faults, fault{fmt.Sprintf("s2c half-close@%d", n), ftFault{Match: ftArtifactMatch, S2C: ftDir{ftHalfClose, n}}, int(n) >= respBytes-8})
	}
	for _, n := range ftSteps(reqBytes, 32, 7) {
		faults = append(faults, fault{fmt.Sprintf("c2s reset@%d", n), ftFault{Match: ftArtifactMatch, C2S: ftDir{ftReset, n}}, int(n) >= reqBytes-12})
	}

	base := stSnapshot()
	for _, ft := range faults {
		host := fresh(tiny)
		fired := px.Fired.Load()
		px.Arm(ft.f)
		res := callTiny(host, 5*time.Second)
		px.Disarm()
		if px.Fired.Load() == fired {
			if !ft.ok {
				t.Errorf("%s: the fault did not go off", ft.name)
			}
			continue
		}
		switch {
		case res.err == nil && !ft.ok:
			t.Errorf("%s: the invocation succeeded although the artifact stream was cut (%q)", ft.name, res.body)
		case res.err != nil:
			if got := reason(res.err); got != commonv1.ErrorReason_ERROR_REASON_INTERNAL {
				t.Errorf("%s: %v (reason %v), want a typed INTERNAL", ft.name, res.err, got)
			}
		}
		if res.elapsed > 5*time.Second+1500*time.Millisecond {
			t.Errorf("%s: took %v", ft.name, res.elapsed)
		}
		// the same module, the link whole: it runs (the cut bytes were not cached)
		if again := callTiny(host, 10*time.Second); again.err != nil || string(again.body) != "ok\n" {
			t.Errorf("%s: the same module afterwards: %v %q", ft.name, again.err, again.body)
		}
	}
	t.Logf("%d cuts of the artifact stream", len(faults))
	s.ftWaitIdle(t, "after the cut artifact streams", 10*time.Second)
	if cur, ok := stSettle(base, 12, 8, 10*time.Second); !ok {
		t.Errorf("after %d cut streams: %v, before %v", len(faults), cur, base)
	}

	// a module of several megabytes (a stream of many chunks), cut in the middle
	px.Capture()
	px.Arm(ftFault{Match: ftArtifactMatch})
	big := ftBigWasm(3 << 20)
	if o := callTiny(fresh(big), 30*time.Second); o.err != nil || string(o.body) != "ok\n" {
		t.Fatalf("the big module: %v %q", o.err, o.body)
	}
	_, bigResp := px.Captured()
	t.Logf("the %d-byte module is %d bytes on the wire", len(big), len(bigResp))
	for _, frac := range []int{4, 2} {
		n := int64(len(bigResp) / frac)
		host := fresh(big)
		fired := px.Fired.Load()
		px.Arm(ftFault{Match: ftArtifactMatch, S2C: ftDir{ftReset, n}})
		res := callTiny(host, 30*time.Second)
		px.Disarm()
		if px.Fired.Load() == fired {
			t.Errorf("big module cut at %d: the fault did not go off", n)
		}
		if res.err == nil || reason(res.err) != commonv1.ErrorReason_ERROR_REASON_INTERNAL {
			t.Errorf("big module cut at %d: %v, want a typed INTERNAL", n, res.err)
		}
		if again := callTiny(host, 30*time.Second); again.err != nil || string(again.body) != "ok\n" {
			t.Errorf("big module cut at %d, then served whole: %v %q", n, again.err, again.body)
		}
	}

	// (d) a black hole in the middle of a stream: the budget ends it and the slot is freed
	host := fresh(tiny)
	px.Arm(ftFault{Match: ftArtifactMatch, S2C: ftDir{ftBlackHole, 100}})
	res := callTiny(host, time.Second)
	px.Disarm()
	if got := reason(res.err); got != commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED {
		t.Errorf("a black-holed artifact stream: %v, want DEADLINE_EXCEEDED", res.err)
	}
	if res.elapsed > time.Second+1500*time.Millisecond {
		t.Errorf("a black-holed artifact stream held the invocation for %v (budget 1 s)", res.elapsed)
	}
	s.ftWaitIdle(t, "after the black-holed artifact stream", 10*time.Second)
	if o := callTiny(fresh(tiny), 10*time.Second); o.err != nil {
		t.Errorf("another module after the black hole: %v", o.err)
	}
}
