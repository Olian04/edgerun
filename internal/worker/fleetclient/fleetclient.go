// Package fleetclient keeps a worker registered with an orchestrator.
//
// Workers dial the orchestrator, never the reverse, so a worker can sit behind
// NAT and an orchestrator needs no inventory of workers.
package fleetclient

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"connectrpc.com/connect"

	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/gen/edgerun/worker/v1/workerv1connect"
)

// Source reports the load a heartbeat carries.
type Source interface {
	State() workerv1.WorkerState
	InFlight() uint32
}

// Config configures a Client.
type Config struct {
	// Endpoint is the base URL at which the orchestrator reaches this worker.
	Endpoint string
	Info     *workerv1.WorkerInfo
	Fleet    workerv1connect.FleetServiceClient
	Source   Source
	Log      *slog.Logger
	// RetryMin and RetryMax bound the exponential backoff between failed
	// registrations. Defaults: 500ms and 30s.
	RetryMin, RetryMax time.Duration
}

// Client registers and heartbeats.
type Client struct {
	cfg  Config
	poke chan struct{}

	mu       sync.Mutex
	workerID string
}

// New returns a client. Call Run to start it.
func New(cfg Config) *Client {
	if cfg.RetryMin == 0 {
		cfg.RetryMin = 500 * time.Millisecond
	}
	if cfg.RetryMax == 0 {
		cfg.RetryMax = 30 * time.Second
	}
	return &Client{cfg: cfg, poke: make(chan struct{}, 1)}
}

// Poke asks for a heartbeat right now instead of at the next interval, e.g.
// right after the worker starts draining so it stops receiving work promptly.
func (c *Client) Poke() {
	select {
	case c.poke <- struct{}{}:
	default: // one is already pending
	}
}

// Run registers, then heartbeats until ctx is done, re-registering whenever
// the orchestrator no longer knows this worker. It returns ctx.Err().
func (c *Client) Run(ctx context.Context) error {
	for {
		id, interval, err := c.register(ctx)
		if err != nil {
			return err // only on ctx done
		}
		c.setID(id)
		if err := c.heartbeats(ctx, id, interval); err != nil {
			return err
		}
		c.cfg.Log.Info("orchestrator no longer knows this worker; re-registering", "worker_id", id)
	}
}

// Deregister tells the orchestrator this worker is leaving. Best effort.
func (c *Client) Deregister(ctx context.Context) error {
	c.mu.Lock()
	id := c.workerID
	c.mu.Unlock()
	if id == "" {
		return nil
	}
	_, err := c.cfg.Fleet.Deregister(ctx, connect.NewRequest(&workerv1.DeregisterRequest{WorkerId: id}))
	return err
}

func (c *Client) setID(id string) {
	c.mu.Lock()
	c.workerID = id
	c.mu.Unlock()
}

func (c *Client) register(ctx context.Context) (string, time.Duration, error) {
	delay := c.cfg.RetryMin
	for {
		resp, err := c.cfg.Fleet.Register(ctx, connect.NewRequest(&workerv1.RegisterRequest{
			Endpoint: c.cfg.Endpoint, Info: c.cfg.Info,
		}))
		if err == nil {
			interval := resp.Msg.GetHeartbeatInterval().AsDuration()
			if interval <= 0 {
				interval = 5 * time.Second
			}
			c.cfg.Log.Info("registered", "worker_id", resp.Msg.GetWorkerId(), "heartbeat_interval", interval)
			return resp.Msg.GetWorkerId(), interval, nil
		}
		if ctx.Err() != nil {
			return "", 0, ctx.Err()
		}
		c.cfg.Log.Warn("registration failed; retrying", "error", err, "in", delay)
		// Jitter keeps a restarted fleet from stampeding the orchestrator.
		wait := delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return "", 0, ctx.Err()
		}
		delay = min(delay*2, c.cfg.RetryMax)
	}
}

// heartbeats returns nil when the worker must re-register, or ctx.Err().
func (c *Client) heartbeats(ctx context.Context, id string, interval time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		case <-c.poke:
		}
		resp, err := c.cfg.Fleet.Heartbeat(ctx, connect.NewRequest(&workerv1.HeartbeatRequest{
			WorkerId: id, State: c.cfg.Source.State(), InFlight: c.cfg.Source.InFlight(),
		}))
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Transient: keep trying. When the orchestrator is back it will
			// tell us to re-register if it forgot us.
			c.cfg.Log.Warn("heartbeat failed", "error", err)
			continue
		}
		if resp.Msg.GetReregister() {
			return nil
		}
	}
}
