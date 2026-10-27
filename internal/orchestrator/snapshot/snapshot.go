// Package snapshot keeps the invoke path off the database. It holds the routing
// table and the set of ready workers in memory and refreshes them in the
// background.
//
// If a refresh fails the previous view is kept: serving slightly stale routes
// is better than serving nothing because the database blinked. The cost of
// staleness is bounded by the refresh interval, and writes made through this
// same process call Refresh themselves so an operator sees their own change at
// once.
package snapshot

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Olian04/edgerun/internal/orchestrator/router"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

// Source is what the cache reads. *store.Store implements it.
type Source interface {
	LoadRouting(ctx context.Context) ([]store.RouteEntry, error)
	ReadyWorkers(ctx context.Context, aliveWithin time.Duration) ([]store.Worker, error)
}

// Cache is safe for concurrent use.
type Cache struct {
	src         Source
	aliveWithin time.Duration
	log         *slog.Logger

	routes  atomic.Pointer[router.Table]
	workers atomic.Pointer[[]store.Worker]
}

// New returns an empty cache; call Refresh before serving.
func New(src Source, aliveWithin time.Duration, log *slog.Logger) *Cache {
	c := &Cache{src: src, aliveWithin: aliveWithin, log: log}
	c.routes.Store(router.New(nil))
	empty := []store.Worker{}
	c.workers.Store(&empty)
	return c
}

// Refresh reloads routes and workers. Each is replaced only if its load
// succeeded; the first error is returned.
func (c *Cache) Refresh(ctx context.Context) error {
	var first error
	if entries, err := c.src.LoadRouting(ctx); err != nil {
		first = err
	} else {
		c.routes.Store(router.New(entries))
	}
	if ws, err := c.src.ReadyWorkers(ctx, c.aliveWithin); err != nil {
		if first == nil {
			first = err
		}
	} else {
		c.workers.Store(&ws)
	}
	return first
}

// Run refreshes every interval until ctx is done.
func (c *Cache) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rctx, cancel := context.WithTimeout(ctx, every*4)
			if err := c.Refresh(rctx); err != nil && ctx.Err() == nil {
				c.log.Warn("snapshot refresh failed; serving the previous view", "error", err)
			}
			cancel()
		}
	}
}

// Route returns the route for a request, or nil.
func (c *Cache) Route(host, path string) *store.RouteEntry {
	return c.routes.Load().Match(host, path)
}

// Workers returns the ready workers. Callers must not modify the slice.
func (c *Cache) Workers() []store.Worker { return *c.workers.Load() }
