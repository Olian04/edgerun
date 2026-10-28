// Package registry keeps a network service registered with an orchestrator.
//
// Services dial the orchestrator, never the reverse, so a service can sit
// inside a private network and the orchestrator needs no inventory of them.
package registry

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
)

// Config configures a Client.
type Config struct {
	// Endpoint is the base URL at which workers reach this service.
	Endpoint string
	Info     *egressv1.ServiceInfo
	Registry *edgerun.Registry
	// State reports READY, or DRAINING while the service is shutting down.
	State func() egressv1.ServiceState
	Log   *slog.Logger
	// RetryMin and RetryMax bound the exponential backoff between failed
	// registrations. Defaults: 500ms and 30s.
	RetryMin, RetryMax time.Duration
}

// Client registers and heartbeats.
type Client struct {
	cfg  Config
	poke chan struct{}

	mu        sync.Mutex
	serviceID string
}

// New returns a client. Call Run to start it.
func New(cfg Config) *Client {
	if cfg.RetryMin == 0 {
		cfg.RetryMin = 500 * time.Millisecond
	}
	if cfg.RetryMax == 0 {
		cfg.RetryMax = 30 * time.Second
	}
	if cfg.State == nil {
		cfg.State = func() egressv1.ServiceState { return egressv1.ServiceState_SERVICE_STATE_READY }
	}
	return &Client{cfg: cfg, poke: make(chan struct{}, 1)}
}

// Poke asks for a heartbeat right now, e.g. when the service starts draining.
func (c *Client) Poke() {
	select {
	case c.poke <- struct{}{}:
	default:
	}
}

// HeartbeatNow sends one heartbeat, with the current state, and waits for the
// answer: a service that is about to stop uses it to be sure the orchestrator
// has heard that it is draining before it goes. Best effort.
func (c *Client) HeartbeatNow(ctx context.Context) error {
	id := c.ServiceID()
	if id == "" {
		return nil
	}
	_, err := c.cfg.Registry.Heartbeat(ctx, &egressv1.HeartbeatRequest{ServiceId: id, State: c.cfg.State()})
	return err
}

// ServiceID returns the id the orchestrator assigned, or "" before the first
// registration has completed.
func (c *Client) ServiceID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.serviceID
}

// Run registers, then heartbeats until ctx is done, registering again whenever
// the orchestrator no longer knows this service. It returns ctx.Err().
func (c *Client) Run(ctx context.Context) error {
	for {
		id, interval, err := c.register(ctx)
		if err != nil {
			return err
		}
		c.mu.Lock()
		c.serviceID = id
		c.mu.Unlock()
		if err := c.heartbeats(ctx, id, interval); err != nil {
			return err
		}
		c.cfg.Log.Info("the orchestrator no longer knows this service; registering again", "service_id", id)
	}
}

// Deregister tells the orchestrator this service is leaving. Best effort.
func (c *Client) Deregister(ctx context.Context) error {
	id := c.ServiceID()
	if id == "" {
		return nil
	}
	_, err := c.cfg.Registry.Deregister(ctx, &egressv1.DeregisterRequest{ServiceId: id})
	return err
}

func (c *Client) register(ctx context.Context) (string, time.Duration, error) {
	delay := c.cfg.RetryMin
	for {
		resp, err := c.cfg.Registry.Register(ctx, &egressv1.RegisterRequest{Endpoint: c.cfg.Endpoint, Info: c.cfg.Info})
		if err == nil {
			interval := resp.GetHeartbeatInterval().AsDuration()
			if interval <= 0 {
				interval = 5 * time.Second
			}
			c.cfg.Log.Info("registered", "service_id", resp.GetServiceId(), "heartbeat_interval", interval, "covers", c.cfg.Info.GetCovers())
			return resp.GetServiceId(), interval, nil
		}
		if ctx.Err() != nil {
			return "", 0, ctx.Err()
		}
		if connect.CodeOf(err) == connect.CodeInvalidArgument {
			// The orchestrator refused what this service declared: retrying
			// the same thing changes nothing, but the operator must see it.
			c.cfg.Log.Error("the orchestrator refused this service's registration", "error", err)
		} else {
			c.cfg.Log.Warn("registration failed; retrying", "error", err, "in", delay)
		}
		wait := delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return "", 0, ctx.Err()
		}
		delay = min(delay*2, c.cfg.RetryMax)
	}
}

// heartbeats returns nil when the service must register again, or ctx.Err().
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
		resp, err := c.cfg.Registry.Heartbeat(ctx, &egressv1.HeartbeatRequest{ServiceId: id, State: c.cfg.State()})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.cfg.Log.Warn("heartbeat failed", "error", err)
			continue
		}
		if resp.GetReregister() {
			if c.cfg.State() == egressv1.ServiceState_SERVICE_STATE_DRAINING {
				// Leaving: registering again would list us as ready, and
				// something would be sent to us that we are trying not to take.
				c.cfg.Log.Info("the orchestrator does not know this draining instance; not registering again")
				continue
			}
			return nil
		}
	}
}
