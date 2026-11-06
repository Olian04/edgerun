// Package lifecycle is the registration loop of a network service: register
// (until it succeeds), heartbeat, register again when the orchestrator has
// forgotten us, and, when asked to stop, report DRAINING, wait, and deregister.
// docs/protocol.md specifies the rules; the edgerun library offers only the
// three calls.
package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
)

// Registrar is what the loop needs of the library's Registry (an interface so
// that tests can substitute a fake).
type Registrar interface {
	Register(context.Context, *egressv1.RegisterRequest) (*egressv1.RegisterResponse, error)
	Heartbeat(context.Context, *egressv1.HeartbeatRequest) (*egressv1.HeartbeatResponse, error)
	Deregister(context.Context, *egressv1.DeregisterRequest) (*egressv1.DeregisterResponse, error)
}

var _ Registrar = (*edgerun.Registry)(nil)

// Loop keeps one service registered.
type Loop struct {
	Reg      Registrar
	Endpoint string
	Info     *egressv1.ServiceInfo
	Log      *slog.Logger
	// InFlight reports the Fetch calls still running (for draining).
	InFlight func() int
	// DrainTimeout bounds how long Drain waits for them.
	DrainTimeout time.Duration
	// MinRetry and MaxRetry bound the backoff between failed registrations.
	MinRetry, MaxRetry time.Duration

	mu       sync.Mutex
	id       string
	interval time.Duration
	draining bool
	done     chan struct{}
	// Refused is called when the orchestrator refuses the registration itself
	// (bad token, bad coverage): the operator must hear about it.
	Refused func(error)
}

func (l *Loop) log() *slog.Logger {
	if l.Log == nil {
		return slog.Default()
	}
	return l.Log
}

// ID is the current service id ("" before the first registration).
func (l *Loop) ID() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.id
}

// Start runs the loop until ctx is cancelled. Cancelling ctx does not
// deregister: Drain does.
func (l *Loop) Start(ctx context.Context) {
	if l.MinRetry == 0 {
		l.MinRetry = 500 * time.Millisecond
	}
	if l.MaxRetry == 0 {
		l.MaxRetry = 15 * time.Second
	}
	l.done = make(chan struct{})
	go func() {
		defer close(l.done)
		l.run(ctx)
	}()
}

func (l *Loop) run(ctx context.Context) {
	for ctx.Err() == nil {
		if l.state() == egressv1.ServiceState_SERVICE_STATE_DRAINING {
			return // asked to stop: never announce ourselves again
		}
		if !l.register(ctx) {
			return
		}
		l.heartbeats(ctx)
	}
}

// register keeps trying until it succeeds or ctx ends.
func (l *Loop) register(ctx context.Context) bool {
	delay := l.MinRetry
	for {
		res, err := l.Reg.Register(ctx, &egressv1.RegisterRequest{Endpoint: l.Endpoint, Info: l.Info})
		if err == nil {
			iv := res.GetHeartbeatInterval().AsDuration()
			if iv <= 0 {
				iv = 5 * time.Second
			}
			l.mu.Lock()
			l.id, l.interval = res.GetServiceId(), iv
			l.mu.Unlock()
			l.log().Info("registered", "service_id", res.GetServiceId(), "heartbeat", iv)
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		switch connect.CodeOf(err) {
		case connect.CodeInvalidArgument, connect.CodeUnauthenticated, connect.CodePermissionDenied:
			// Not a registry that is not up yet: the orchestrator said no. That is
			// reported loudly every time (and to Refused), and retried slowly, since
			// an operator may fix the token or the orchestrator's configuration
			// without restarting us.
			l.log().Error("registration refused", "code", connect.CodeOf(err).String(), "err", err)
			if l.Refused != nil {
				l.Refused(err)
			}
			delay = l.MaxRetry
		default:
			l.log().Warn("registration failed, retrying", "err", err, "in", delay)
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(delay):
		}
		if delay *= 2; delay > l.MaxRetry {
			delay = l.MaxRetry
		}
	}
}

func (l *Loop) state() egressv1.ServiceState {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.draining {
		return egressv1.ServiceState_SERVICE_STATE_DRAINING
	}
	return egressv1.ServiceState_SERVICE_STATE_READY
}

// heartbeats beats until ctx ends or the orchestrator asks us to register again.
func (l *Loop) heartbeats(ctx context.Context) {
	l.mu.Lock()
	iv := l.interval
	l.mu.Unlock()
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		hctx, cancel := context.WithTimeout(ctx, iv*2)
		res, err := l.Reg.Heartbeat(hctx, &egressv1.HeartbeatRequest{ServiceId: l.ID(), State: l.state()})
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			l.log().Warn("heartbeat failed", "err", err)
			continue
		}
		if res.GetReregister() {
			l.log().Warn("the orchestrator does not know this service: registering again")
			return
		}
	}
}

// Drain is what asking to stop does: report DRAINING at once (nothing new is
// routed here), wait until nothing is in flight (and for one heartbeat interval at
// least, so the orchestrator has certainly seen it), then deregister. stop ends
// the loop's goroutine.
func (l *Loop) Drain(stop context.CancelFunc) {
	l.mu.Lock()
	l.draining = true
	id, iv := l.id, l.interval
	l.mu.Unlock()
	if iv <= 0 {
		iv = time.Second
	}
	defer func() {
		stop()
		if l.done != nil {
			<-l.done
		}
	}()
	if id == "" {
		return // never registered: nothing to tell
	}
	ctx, cancel := context.WithTimeout(context.Background(), l.DrainTimeout+5*time.Second)
	defer cancel()

	// Tell it now, not at the next tick: the beat that matters is this one.
	if res, err := l.Reg.Heartbeat(ctx, &egressv1.HeartbeatRequest{ServiceId: id, State: egressv1.ServiceState_SERVICE_STATE_DRAINING}); err != nil {
		l.log().Warn("could not report draining", "err", err)
	} else if res.GetReregister() {
		return // it has forgotten us already: nothing to deregister
	}
	grace := time.NewTimer(iv)
	defer grace.Stop()
	deadline := time.NewTimer(l.DrainTimeout)
	defer deadline.Stop()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	graced := false
	for {
		select {
		case <-grace.C:
			graced = true
		case <-deadline.C:
			l.log().Warn("drain timeout: leaving with requests in flight", "inflight", l.inflight())
			l.deregister(ctx, id)
			return
		case <-poll.C:
		}
		if graced && l.inflight() == 0 {
			l.deregister(ctx, id)
			return
		}
	}
}

func (l *Loop) inflight() int {
	if l.InFlight == nil {
		return 0
	}
	return l.InFlight()
}

func (l *Loop) deregister(ctx context.Context, id string) {
	if _, err := l.Reg.Deregister(ctx, &egressv1.DeregisterRequest{ServiceId: id}); err != nil && !errors.Is(err, context.Canceled) {
		l.log().Warn("deregister failed", "err", err)
		return
	}
	l.log().Info("deregistered", "service_id", id)
}
