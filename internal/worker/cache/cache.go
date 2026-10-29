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

	"golang.org/x/sync/singleflight"

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
	group        singleflight.Group
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
		fetcher: f, maxBytes: maxBytes, fetchTimeout: 60 * time.Second,
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
	led := false
	ch := s.group.DoChan(digest, func() (any, error) {
		led = true
		// A caller may have filled the cache between our lookup and here.
		if b, ok := s.lookup(digest); ok {
			return b, nil
		}
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.fetchTimeout)
		defer cancel()
		b, err := s.fetcher.Fetch(fctx, digest)
		if err != nil {
			return nil, err
		}
		if Digest(b) != digest {
			return nil, fmt.Errorf("%w: wanted %s", ErrDigestMismatch, digest)
		}
		s.store(digest, b)
		return b, nil
	})
	select {
	case r := <-ch:
		if r.Err != nil {
			span.SetStatus(codes.Error, "artifact unavailable")
			s.note(ctx, span, "error")
			return nil, r.Err
		}
		if led {
			s.note(ctx, span, "fetched")
		} else {
			s.note(ctx, span, "shared") // another invocation was already fetching it
		}
		return r.Val.([]byte), nil
	case <-ctx.Done():
		return nil, ctx.Err()
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
