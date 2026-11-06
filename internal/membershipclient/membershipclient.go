// Package membershipclient keeps a worker or a network service registered with
// an orchestrator: it registers, heartbeats, registers again whenever it is
// forgotten, and leaves on request. The two kinds of member live the same life,
// so there is one client for both (membership.v1).
//
// Members dial the orchestrator, never the reverse, so a member can sit behind
// NAT or inside a private network and an orchestrator needs no inventory.
package membershipclient

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	membershipv1 "github.com/Olian04/edgerun/gen/edgerun/membership/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

// Config configures a Client. Exactly one of Worker and NetworkService says
// what the member is.
type Config struct {
	// Endpoint is the base URL at which the member is reached.
	Endpoint       string
	Worker         *workerv1.WorkerInfo
	NetworkService *egressv1.ServiceInfo
	Membership     *edgerun.Membership
	// State reports READY, or DRAINING while the member is shutting down.
	// Default: always READY.
	State func() membershipv1.MemberState
	// InFlight is the load a worker's heartbeat carries. May be nil.
	InFlight func() uint32
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
	memberID string
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
		cfg.State = func() membershipv1.MemberState { return membershipv1.MemberState_MEMBER_STATE_READY }
	}
	if cfg.InFlight == nil {
		cfg.InFlight = func() uint32 { return 0 }
	}
	return &Client{cfg: cfg, poke: make(chan struct{}, 1)}
}

func (c *Client) registration() *membershipv1.RegisterRequest {
	r := &membershipv1.RegisterRequest{Endpoint: c.cfg.Endpoint}
	if c.cfg.Worker != nil {
		r.Info = &membershipv1.RegisterRequest_Worker{Worker: c.cfg.Worker}
	} else {
		r.Info = &membershipv1.RegisterRequest_NetworkService{NetworkService: c.cfg.NetworkService}
	}
	return r
}

func (c *Client) heartbeat(id string) *membershipv1.HeartbeatRequest {
	return &membershipv1.HeartbeatRequest{MemberId: id, State: c.cfg.State(), InFlight: c.cfg.InFlight()}
}

// Poke asks for a heartbeat right now instead of at the next interval, e.g.
// right after the member starts draining so it stops being given work promptly.
func (c *Client) Poke() {
	select {
	case c.poke <- struct{}{}:
	default: // one is already pending
	}
}

// HeartbeatNow sends one heartbeat, with the current state, and waits for the
// answer: a member that is about to stop uses it to be sure the orchestrator
// has heard that it is draining before it goes. Best effort.
func (c *Client) HeartbeatNow(ctx context.Context) error {
	id := c.MemberID()
	if id == "" {
		return nil
	}
	_, err := c.cfg.Membership.Heartbeat(ctx, c.heartbeat(id))
	return err
}

// MemberID returns the id the orchestrator assigned, or "" before the first
// registration has completed.
func (c *Client) MemberID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.memberID
}

// Run registers, then heartbeats until ctx is done, registering again whenever
// the orchestrator no longer knows this member. It returns ctx.Err().
func (c *Client) Run(ctx context.Context) error {
	for {
		id, interval, err := c.register(ctx)
		if err != nil {
			return err
		}
		c.mu.Lock()
		c.memberID = id
		c.mu.Unlock()
		if err := c.heartbeats(ctx, id, interval); err != nil {
			return err
		}
		c.cfg.Log.Info("the orchestrator no longer knows this member; registering again", "member_id", id)
	}
}

// Deregister tells the orchestrator this member is leaving. Best effort.
func (c *Client) Deregister(ctx context.Context) error {
	id := c.MemberID()
	if id == "" {
		return nil
	}
	_, err := c.cfg.Membership.Deregister(ctx, &membershipv1.DeregisterRequest{MemberId: id})
	return err
}

func (c *Client) register(ctx context.Context) (string, time.Duration, error) {
	delay := c.cfg.RetryMin
	for {
		cctx, cancel := context.WithTimeout(ctx, callTimeout(0))
		resp, err := c.cfg.Membership.Register(cctx, c.registration())
		cancel()
		if err == nil {
			interval := resp.GetHeartbeatInterval().AsDuration()
			if interval <= 0 {
				interval = 5 * time.Second
			}
			c.cfg.Log.Info("registered", "member_id", resp.GetMemberId(), "heartbeat_interval", interval)
			return resp.GetMemberId(), interval, nil
		}
		if ctx.Err() != nil {
			return "", 0, ctx.Err()
		}
		if connect.CodeOf(err) == connect.CodeInvalidArgument {
			// The orchestrator refused what this member declared: retrying the
			// same thing changes nothing, but the operator must see it.
			c.cfg.Log.Error("the orchestrator refused this member's registration", "error", err)
		} else {
			c.cfg.Log.Warn("registration failed; retrying", "error", err, "in", delay)
		}
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

// heartbeats returns nil when the member must register again, or ctx.Err().
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
		cctx, cancel := context.WithTimeout(ctx, callTimeout(interval))
		resp, err := c.cfg.Membership.Heartbeat(cctx, c.heartbeat(id))
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Transient: keep trying. When the orchestrator is back it will
			// tell us to register again if it forgot us.
			c.cfg.Log.Warn("heartbeat failed", "error", err)
			continue
		}
		if resp.GetReregister() {
			if c.cfg.State() == membershipv1.MemberState_MEMBER_STATE_DRAINING {
				// Leaving: registering again would list us as ready, and
				// something would be sent to us that we are trying not to take.
				c.cfg.Log.Info("the orchestrator does not know this draining instance; not registering again")
				continue
			}
			return nil
		}
	}
}

// callTimeout bounds one call to the orchestrator: a call on a connection that has
// gone silent (a mapping that expired, a hung peer) must end, or the loop that
// makes it would wait forever and the member would never be alive again.
func callTimeout(interval time.Duration) time.Duration {
	return min(max(interval, 2*time.Second), 15*time.Second)
}
