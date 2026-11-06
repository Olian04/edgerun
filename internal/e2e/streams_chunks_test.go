package e2e

// Chunk size, and what a producer may send past the bounds it was given.
//
// SPEC AMBIGUITY (reported, not asserted): docs/protocol.md says GetArtifact
// "streams the module in chunks of at most 1 MiB" and a network service streams
// "chunks of at most 1 MiB", while artifact.proto says "Chunk size is up to the
// server". No consumer is told whether to refuse a larger chunk. These record
// what the reference worker does with a 2.5 MiB chunk on each seam.
//
// What IS specified and asserted: the worker "does not trust a service to keep to
// the bound it was given" - a service that streams more than max_response_bytes
// gets the guest -3, whatever it declared.

import (
	"strings"
	"testing"
	"time"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

func TestStreams_ArtifactChunkLargerThanTheDocumentedBound(t *testing.T) {
	r := stNewArtifactRig(t)
	big := stTinyWasm(strings.Repeat("x", 2_500_000))
	m := &stArtifactModule{bytes: big, digest: edgerun.Digest(big), pieces: [][]byte{big}}
	m.clean.Store(true)
	r.add(m)
	var o stExecObs
	eventually2(t, "the worker to answer", 30*time.Second, func() bool {
		o = r.execute(m.digest, "big", 10*time.Second)
		return o.ok() || edgerun.Reason(o.err) != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED
	})
	t.Logf("AMBIGUITY artifact chunk of %d bytes (> 1 MiB): ok=%v err=%v", len(big), o.ok(), o.err)
}

// stFetchBounds runs on the Fetch rig of TestStreams_Fetch_Worker (a worker costs a compile).
func stFetchBounds(t *testing.T, rig *stFetchRig) {
	// the guest's response limit is 4 MiB (wagi-2); send 6 MiB in 1 MiB chunks, then end cleanly
	rig.setCustom(func(o *stOut) {
		o.msg(stFetchCodec.head(200))
		chunk := make([]byte, 1<<20)
		for i := 0; i < 6; i++ {
			o.msg(stFetchCodec.chunk(chunk))
		}
		o.end(0, "", nil)
	})
	o := rig.run(stSeq{stH, stCLEAN}, "past-the-bound", 20*time.Second)
	rig.setCustom(nil)
	if o.execErr != nil || o.code != -3 {
		t.Errorf("a service that streams 6 MiB: code %d, err %v, want -3 (too large)", o.code, o.execErr)
	}

	// a single chunk of 2.5 MiB, within the bound (ambiguity: the doc says chunks are at most 1 MiB)
	rig.setCustom(func(o *stOut) {
		o.msg(stFetchCodec.head(200))
		o.msg(stFetchCodec.chunk(make([]byte, 2_500_000)))
		o.end(0, "", nil)
	})
	o = rig.run(stSeq{stH, stCLEAN}, "one-big-chunk", 20*time.Second)
	rig.setCustom(nil)
	t.Logf("AMBIGUITY fetch chunk of 2500000 bytes (> 1 MiB): code %d err %v", o.code, o.execErr)
}
