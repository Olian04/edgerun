// Package cache holds module bytes on a worker, addressed by content digest.
//
// Artifacts are immutable, so entries never need revalidation; the only reason
// to drop one is memory pressure. Bytes are verified against their digest
// before they are cached or returned, so a corrupted or malicious artifact
// server can never get code executed.
package cache

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"sync"
	"time"

	"github.com/Olian04/edgerun"
	"github.com/Olian04/edgerun/internal/errs"
)

const digestPrefix = "sha256:"

// ErrDigestMismatch means the fetched bytes do not hash to the requested digest.
var ErrDigestMismatch = errors.New("artifact bytes do not match their digest")

// Digest returns the content address of b.
func Digest(b []byte) string {
	sum := sha256.Sum256(b)
	return digestPrefix + hex.EncodeToString(sum[:])
}

// ValidDigest reports whether s is "sha256:" followed by 64 lowercase hex digits.
func ValidDigest(s string) bool {
	if len(s) != len(digestPrefix)+64 || s[:len(digestPrefix)] != digestPrefix {
		return false
	}
	for _, c := range s[len(digestPrefix):] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// Fetcher retrieves artifact bytes from the source of truth.
type Fetcher interface {
	Fetch(ctx context.Context, digest string) ([]byte, error)
}

// Store is a bounded, verifying, de-duplicating cache of artifacts.
type Store struct {
	fetcher      Fetcher
	maxBytes     int64
	fetchTimeout time.Duration
	fmu          sync.Mutex
	fetches      map[string]*fetch
	requests     metric.Int64Counter

	mu    sync.Mutex
	items map[string]*list.Element // digest -> element of *item
	lru   *list.List               // front = most recent
	size  int64
}

type item struct {
	digest string
	data   []byte
}

// New returns a store that keeps at most maxBytes of artifacts.
func New(f Fetcher, maxBytes int64) *Store {
	return &Store{
		fetcher: f, maxBytes: maxBytes, fetchTimeout: 60 * time.Second, fetches: map[string]*fetch{},
		items: map[string]*list.Element{}, lru: list.New(), requests: newRequests(),
	}
}

// Get returns the verified bytes for digest, fetching them if needed. The
// returned slice is shared and must not be modified.
//
// Concurrent callers for the same digest share one fetch. That fetch is not
// tied to any one caller's context, so one impatient caller cannot fail the
// others; each caller still stops waiting when its own context ends.
func (s *Store) Get(ctx context.Context, digest string) ([]byte, error) {
	if !ValidDigest(digest) {
		return nil, errs.Invalid("malformed artifact digest %q", digest)
	}
	ctx, span := otel.Tracer(scope).Start(ctx, "edgerun.artifact.get", trace.WithAttributes(attribute.String("edgerun.artifact.digest", digest)))
	defer span.End()
	if b, ok := s.lookup(digest); ok {
		s.note(ctx, span, "hit")
		return b, nil
	}
	f, led := s.join(digest)
	select {
	case <-f.done:
		if f.err != nil {
			span.SetStatus(codes.Error, "artifact unavailable")
			s.note(ctx, span, "error")
			return nil, f.err
		}
		if led {
			s.note(ctx, span, "fetched")
		} else {
			s.note(ctx, span, "shared") // another invocation was already fetching it
		}
		return f.data, nil
	case <-ctx.Done():
		s.leave(digest, f)
		return nil, ctx.Err()
	}
}

// fetch is one fetch of one digest, shared by every caller that wants it while
// it runs.
type fetch struct {
	done    chan struct{}
	data    []byte
	err     error
	waiters int
	cancel  context.CancelFunc
}

// join returns the fetch of digest, starting it if there is none, and counts the
// caller as waiting for it. led says whether this call started it.
func (s *Store) join(digest string) (f *fetch, led bool) {
	s.fmu.Lock()
	defer s.fmu.Unlock()
	if f, ok := s.fetches[digest]; ok {
		f.waiters++
		return f, false
	}
	// The fetch belongs to the callers that wait for it, not to the first of them: an impatient one
	// must not fail the others. It ends when it finishes, at the timeout, or when the last of them
	// has given up (a fetch that nobody waits for is abandoned, not left to block the digest).
	fctx, cancel := context.WithTimeout(context.Background(), s.fetchTimeout)
	f = &fetch{done: make(chan struct{}), waiters: 1, cancel: cancel}
	s.fetches[digest] = f
	go func() {
		defer cancel()
		// A caller may have filled the cache between its lookup and here.
		if b, ok := s.lookup(digest); ok {
			f.data = b
		} else if b, err := s.fetcher.Fetch(fctx, digest); err != nil {
			f.err = err
		} else if Digest(b) != digest {
			f.err = fmt.Errorf("%w: wanted %s", ErrDigestMismatch, digest)
		} else {
			s.store(digest, b)
			f.data = b
		}
		s.fmu.Lock()
		if s.fetches[digest] == f {
			delete(s.fetches, digest)
		}
		s.fmu.Unlock()
		close(f.done)
	}()
	return f, true
}

// leave takes a caller out of the waiters of f; the last to leave abandons the
// fetch, and the digest is free to be fetched afresh.
func (s *Store) leave(digest string, f *fetch) {
	s.fmu.Lock()
	defer s.fmu.Unlock()
	f.waiters--
	if f.waiters == 0 {
		f.cancel()
		if s.fetches[digest] == f {
			delete(s.fetches, digest)
		}
	}
}

func (s *Store) lookup(digest string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.items[digest]
	if !ok {
		return nil, false
	}
	s.lru.MoveToFront(el)
	return el.Value.(*item).data, true
}

func (s *Store) store(digest string, b []byte) {
	if int64(len(b)) > s.maxBytes {
		return // too big to keep; still usable by the caller that fetched it
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[digest]; ok {
		return
	}
	s.items[digest] = s.lru.PushFront(&item{digest: digest, data: b})
	s.size += int64(len(b))
	for s.size > s.maxBytes {
		el := s.lru.Back()
		it := el.Value.(*item)
		s.lru.Remove(el)
		delete(s.items, it.digest)
		s.size -= int64(len(it.data))
	}
}

// ArtifactFetcher fetches artifacts through the worker.v1 ArtifactService. The
// client verifies and bounds what it returns; the store does not rely on that.
type ArtifactFetcher struct {
	Client *edgerun.Artifacts
}

// Fetch implements Fetcher. Errors from the service, including their
// ErrorInfo detail, are returned unchanged.
func (f ArtifactFetcher) Fetch(ctx context.Context, digest string) ([]byte, error) {
	return f.Client.GetArtifact(ctx, digest)
}
