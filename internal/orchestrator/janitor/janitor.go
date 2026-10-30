// Package janitor keeps the database from growing without bound: it deletes
// old invocation records and workers that stopped heartbeating long ago.
package janitor

import (
	"context"
	"log/slog"
	"time"
)

// Store is what the janitor needs. *store.Store implements it.
type Store interface {
	DeleteInvocationsBefore(ctx context.Context, t time.Time, limit int) (int, error)
	PruneWorkers(ctx context.Context, olderThan time.Duration) (int, error)
	PruneNetworkServices(ctx context.Context, olderThan time.Duration) (int, error)
}

// Config configures a Janitor.
type Config struct {
	Store Store
	// InvocationRetention is how long invocation records are kept.
	InvocationRetention time.Duration
	// WorkerGrace is how long a silent worker is kept in the table, so that a
	// dead one stays visible to operators for a while. Default 1 hour.
	WorkerGrace time.Duration
	// Batch bounds one delete statement so a large backlog never holds a long
	// lock. Default 5000.
	Batch int
	Log   *slog.Logger
}

// Once runs one cleanup pass and returns how many records, and how many workers and network services, it removed.
func Once(ctx context.Context, c Config) (invocations, workers int, err error) {
	if c.Batch == 0 {
		c.Batch = 5000
	}
	if c.WorkerGrace == 0 {
		c.WorkerGrace = time.Hour
	}
	cutoff := time.Now().Add(-c.InvocationRetention)
	for {
		n, err := c.Store.DeleteInvocationsBefore(ctx, cutoff, c.Batch)
		invocations += n
		if err != nil {
			return invocations, workers, err
		}
		if n < c.Batch {
			break
		}
	}
	workers, err = c.Store.PruneWorkers(ctx, c.WorkerGrace)
	if err != nil {
		return invocations, workers, err
	}
	// Network services are kept as long as workers, and counted with them.
	services, err := c.Store.PruneNetworkServices(ctx, c.WorkerGrace)
	return invocations, workers + services, err
}

// Run calls Once every interval until ctx is done. A failed pass is logged and
// retried at the next tick.
func Run(ctx context.Context, c Config, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		inv, w, err := Once(ctx, c)
		switch {
		case err != nil && ctx.Err() == nil:
			c.Log.Error("janitor pass failed", "error", err)
		case inv+w > 0:
			c.Log.Info("janitor removed old data", "invocations", inv, "workers", w)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
