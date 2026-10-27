package auth

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// limiter bounds failed sign-ins per client address. bcrypt already makes each
// guess slow; this makes a sustained guess campaign from one address stop.
type limiter struct {
	mu     sync.Mutex
	max    int
	window time.Duration
	now    func() time.Time
	fails  map[string][]time.Time
}

func newLimiter(max int, window time.Duration, now func() time.Time) *limiter {
	return &limiter{max: max, window: window, now: now, fails: map[string][]time.Time{}}
}

func (l *limiter) prune(ip string) []time.Time {
	cutoff := l.now().Add(-l.window)
	kept := l.fails[ip][:0]
	for _, t := range l.fails[ip] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	if len(kept) == 0 {
		delete(l.fails, ip)
		return nil
	}
	l.fails[ip] = kept
	return kept
}

func (l *limiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.prune(ip)) < l.max
}

func (l *limiter) fail(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prune(ip)
	l.fails[ip] = append(l.fails[ip], l.now())
}

func (l *limiter) reset(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.fails, ip)
}

// clientIP is the connection's peer. Forwarding headers are deliberately not
// believed: they are client-controlled, and trusting them would let an attacker
// pick a fresh "address" for every guess.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
