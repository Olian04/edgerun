package orchsuite

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
)

// Artifacts judges the orchestrator as the host of worker.v1 ArtifactService:
// module bytes by content digest. The suite uploads through Catalog and plays the
// worker downloading.
func Artifacts(t *testing.T, target Target) {
	h := start(t, target)
	t.Run("Authentication", h.artifactsAuthentication)
	t.Run("ContentAddress", h.artifacts)
}

func (h *harness) artifactsAuthentication(t *testing.T) {
	check.Authentication(t, h.scopes(), "worker", []string{h.env.ExecutionToken}, func(token string) error {
		ctx, cancel := ctxT(settle)
		defer cancel()
		st, err := workerv1connect.NewArtifactServiceClient(http.DefaultClient, h.url, check.Bearer(token)).GetArtifact(ctx,
			connect.NewRequest(&workerv1.GetArtifactRequest{Digest: sha([]byte("nothing"))}))
		if err != nil {
			return err
		}
		defer st.Close()
		for st.Receive() {
		}
		return st.Err()
	})
}

// artifacts checks the content address and the worker-facing download.
func (h *harness) artifacts(t *testing.T) {
	ctx, cancel := ctxT(2 * settle)
	defer cancel()
	fn := h.createFunction(t, h.name("art"))
	// Larger than one chunk, and not a multiple of it.
	module := bytes.Repeat([]byte("0123456789abcdef"), (2*(1<<20)+(512<<10))/16)
	v := h.createVersion(t, fn.GetId(), "wagi-1", module, nil, false)
	if v.GetArtifactDigest() != sha(module) {
		t.Fatalf("digest = %s, want %s", v.GetArtifactDigest(), sha(module))
	}

	download := func(digest string) ([]byte, int, error) {
		st, err := h.artifact(h.env.WorkerToken).GetArtifact(ctx, connect.NewRequest(&workerv1.GetArtifactRequest{Digest: digest}))
		if err != nil {
			return nil, 0, err
		}
		defer st.Close()
		var got []byte
		chunks := 0
		for st.Receive() {
			c := st.Msg().GetChunk()
			if len(c) > 1<<20 {
				t.Errorf("a chunk of %d bytes exceeds the 1 MiB the protocol allows", len(c))
			}
			got = append(got, c...)
			chunks++
		}
		return got, chunks, st.Err()
	}

	t.Run("download", func(t *testing.T) {
		got, chunks, err := download(v.GetArtifactDigest())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, module) {
			t.Errorf("the downloaded module differs: %d bytes, want %d", len(got), len(module))
		}
		if chunks < 3 {
			t.Errorf("%d bytes arrived in %d chunks; at most 1 MiB each needs at least 3", len(got), chunks)
		}
	})
	t.Run("unknown artifacts", func(t *testing.T) {
		for _, digest := range []string{sha([]byte("never uploaded")), "sha256:" + strings.Repeat("0", 64), "nonsense", ""} {
			_, _, err := download(digest)
			check.WantReason(t, err, commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND)
		}
	})
}
