package e2e

// A small TCP fault proxy. It sits between two components and, on command,
// breaks the connection between them in the ways a network does: a reset (RST)
// or a close (FIN) after exactly N bytes of either direction, a black hole (the
// connection stays open and nothing is forwarded), a half close, added latency,
// a slow drip. It does not parse HTTP: faults are placed by byte offset, so a
// sweep of N goes through the HTTP/1.1 framing, the Connect envelope and the
// protobuf message alike.
//
// A fault goes off when a byte beyond its offset is about to be forwarded (so a
// fault at offset N means: N bytes get through, the next one does not).

import (
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type ftAction int

const (
	ftNone      ftAction = iota
	ftReset              // RST: both ends see "connection reset by peer"
	ftClose              // FIN on both ends
	ftHalfClose          // FIN towards the receiver of this direction, whose bytes are dropped from here on; the other direction goes on
	ftBlackHole          // from here on this direction is read and thrown away; the connection stays open
)

func (a ftAction) String() string {
	return [...]string{"none", "reset", "close", "half-close", "black-hole"}[a]
}

// ftDir is a fault in one direction: after After bytes (counted from the first
// byte of the request that claimed the fault) do Action.
type ftDir struct {
	Action ftAction
	After  int64
}

// ftFault is a one-shot fault: the next connection that carries a request
// matching Match (or any request, if Match is empty) claims it.
type ftFault struct {
	Match string // a substring of the chunk that starts the request (a procedure name)
	C2S   ftDir  // client to server (the request)
	S2C   ftDir  // server to client (the response)
}

type ftProxy struct {
	t      testing.TB
	ln     net.Listener
	target string
	addr   string

	mu       sync.Mutex
	armed    *ftFault
	frozen   bool          // every connection: both directions are read and thrown away
	latency  time.Duration // added before every forwarded chunk
	dripN    int           // if > 0: forward at most dripN bytes at a time, dripWait apart
	dripWait time.Duration
	refuse   bool // new connections end at once with a reset; nothing is dialled
	conns    map[*ftConn]struct{}
	cap      *[2][]byte

	Accepted atomic.Int64
	C2S, S2C atomic.Int64 // bytes forwarded in total
	Claimed  atomic.Int64 // faults claimed
	Fired    atomic.Int64 // faults that went off
}

type ftConn struct {
	p      *ftProxy
	c, s   net.Conn
	mu     sync.Mutex
	fault  *ftFault
	count  [2]int64 // bytes seen since the claim: [0] c2s, [1] s2c
	hole   [2]bool
	closed bool
	done   sync.WaitGroup
}

// ftListen starts a proxy to target on a loopback port.
func ftListen(t testing.TB, target string) *ftProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &ftProxy{t: t, ln: ln, addr: ln.Addr().String(), target: target, conns: map[*ftConn]struct{}{}}
	go p.serve(ln)
	t.Cleanup(p.Close)
	return p
}

func (p *ftProxy) Addr() string { return p.addr }
func (p *ftProxy) URL() string  { return "http://" + p.addr }

func (p *ftProxy) Close() {
	p.ln.Close()
	p.KillAll()
}

// Arm sets a one-shot fault for the next matching request.
func (p *ftProxy) Arm(f ftFault) {
	p.mu.Lock()
	p.armed = &f
	p.mu.Unlock()
}

// Disarm drops a fault that nothing claimed.
func (p *ftProxy) Disarm() {
	p.mu.Lock()
	p.armed = nil
	p.mu.Unlock()
}

// Freeze is a black hole on every connection, those open and those to come.
func (p *ftProxy) Freeze(on bool) {
	p.mu.Lock()
	p.frozen = on
	p.mu.Unlock()
}

func (p *ftProxy) SetLatency(d time.Duration) {
	p.mu.Lock()
	p.latency = d
	p.mu.Unlock()
}

func (p *ftProxy) SetDrip(n int, wait time.Duration) {
	p.mu.Lock()
	p.dripN, p.dripWait = n, wait
	p.mu.Unlock()
}

// Refuse makes new connections end at once with a reset, nothing dialled.
func (p *ftProxy) Refuse(on bool) {
	p.mu.Lock()
	p.refuse = on
	p.mu.Unlock()
}

// KillAll resets every open connection.
func (p *ftProxy) KillAll() {
	p.mu.Lock()
	var cs []*ftConn
	for c := range p.conns {
		cs = append(cs, c)
	}
	p.mu.Unlock()
	for _, c := range cs {
		c.reset()
	}
}

func (p *ftProxy) Open() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

// Capture starts (or restarts) recording what is forwarded on the connection that claims the next fault
// (arm a fault with no action to record one exchange), per direction.
func (p *ftProxy) Capture() {
	p.mu.Lock()
	p.cap = &[2][]byte{}
	p.mu.Unlock()
}

// Captured returns what was recorded since Capture: [0] request bytes, [1] response bytes.
func (p *ftProxy) Captured() (c2s, s2c []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cap == nil {
		return nil, nil
	}
	return append([]byte(nil), p.cap[0]...), append([]byte(nil), p.cap[1]...)
}

func (c *ftConn) capture(dir int, b []byte) {
	c.mu.Lock()
	claimed := c.fault != nil
	c.mu.Unlock()
	if !claimed {
		return
	}
	p := c.p
	p.mu.Lock()
	if p.cap != nil {
		p.cap[dir] = append(p.cap[dir], b...)
	}
	p.mu.Unlock()
}

// Pause stops listening (a dial to the proxy is refused) and resets what is open.
func (p *ftProxy) Pause() {
	p.mu.Lock()
	ln := p.ln
	p.mu.Unlock()
	ln.Close()
	p.KillAll()
}

// Resume listens again on the address the proxy had.
func (p *ftProxy) Resume() {
	addr := p.Addr()
	var ln net.Listener
	var err error
	for i := 0; i < 100; i++ {
		if ln, err = net.Listen("tcp", addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		p.t.Fatalf("proxy cannot listen on %s again: %v", addr, err)
	}
	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()
	go p.serve(ln)
}

func (p *ftProxy) serve(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		p.Accepted.Add(1)
		p.mu.Lock()
		refuse := p.refuse
		p.mu.Unlock()
		if refuse {
			if tc, ok := c.(*net.TCPConn); ok {
				tc.SetLinger(0)
			}
			c.Close()
			continue
		}
		s, err := net.DialTimeout("tcp", p.target, 2*time.Second)
		if err != nil {
			c.Close()
			continue
		}
		fc := &ftConn{p: p, c: c, s: s}
		p.mu.Lock()
		p.conns[fc] = struct{}{}
		p.mu.Unlock()
		fc.done.Add(2)
		go fc.pump(0, c, s)
		go fc.pump(1, s, c)
		go func() {
			fc.done.Wait()
			fc.close()
			p.mu.Lock()
			delete(p.conns, fc)
			p.mu.Unlock()
		}()
	}
}

func (c *ftConn) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()
	c.c.Close()
	c.s.Close()
}

func (c *ftConn) reset() {
	for _, x := range []net.Conn{c.c, c.s} {
		if tc, ok := x.(*net.TCPConn); ok {
			tc.SetLinger(0)
		}
	}
	c.close()
}

// claim lets this connection take the armed fault if the bytes it is about to
// forward are the request the fault is for.
func (c *ftConn) claim(dir int, chunk []byte) {
	if dir != 0 {
		return
	}
	c.p.mu.Lock()
	defer c.p.mu.Unlock()
	f := c.p.armed
	if f == nil || (f.Match != "" && !strings.Contains(string(chunk), f.Match)) {
		return
	}
	c.p.armed = nil
	c.p.Claimed.Add(1)
	c.mu.Lock()
	c.fault = f
	c.count = [2]int64{}
	c.mu.Unlock()
}

// send forwards b, with the proxy's latency and drip.
func (c *ftConn) send(dir int, dst net.Conn, b []byte) bool {
	c.p.mu.Lock()
	frozen, lat, dn, dw := c.p.frozen, c.p.latency, c.p.dripN, c.p.dripWait
	c.p.mu.Unlock()
	c.mu.Lock()
	hole := c.hole[dir]
	c.mu.Unlock()
	if frozen || hole || len(b) == 0 {
		return true
	}
	// accounted before it is written: whoever sees the bytes arrive finds them counted
	if dir == 0 {
		c.p.C2S.Add(int64(len(b)))
		c.capture(0, b)
	} else {
		c.p.S2C.Add(int64(len(b)))
		c.capture(1, b)
	}
	if lat > 0 {
		time.Sleep(lat)
	}
	if dn > 0 {
		for off := 0; off < len(b); off += dn {
			if _, err := dst.Write(b[off:min(off+dn, len(b))]); err != nil {
				return false
			}
			time.Sleep(dw)
		}
	} else if _, err := dst.Write(b); err != nil {
		return false
	}
	return true
}

// pump copies src to dst (dir 0: client to server, 1: server to client).
func (c *ftConn) pump(dir int, src, dst net.Conn) {
	defer c.done.Done()
	buf := make([]byte, 16<<10)
	for {
		n, err := src.Read(buf)
		chunk := buf[:n]
		if n > 0 {
			c.claim(dir, chunk)
			for len(chunk) > 0 {
				c.mu.Lock()
				f := c.fault
				var d ftDir
				if f != nil {
					d = f.C2S
					if dir == 1 {
						d = f.S2C
					}
				}
				active := f != nil && d.Action != ftNone && !c.hole[dir]
				seen := c.count[dir]
				c.mu.Unlock()
				if active && seen+int64(len(chunk)) > d.After {
					// the fault is inside this chunk: let the bytes before it through
					take := max(d.After-seen, 0)
					if !c.send(dir, dst, chunk[:take]) {
						return
					}
					chunk = chunk[take:]
					c.mu.Lock()
					c.count[dir] += take
					c.mu.Unlock()
					if c.fire(dir, d, dst) {
						return
					}
					continue // a hole: the rest of the chunk is dropped by send
				}
				if !c.send(dir, dst, chunk) {
					return
				}
				c.mu.Lock()
				if c.fault != nil {
					c.count[dir] += int64(len(chunk))
				}
				c.mu.Unlock()
				chunk = nil
			}
		}
		if err != nil {
			if tc, ok := dst.(*net.TCPConn); ok {
				tc.CloseWrite()
			}
			return
		}
	}
}

// fire carries out the fault of direction dir; it reports whether the pump ends.
func (c *ftConn) fire(dir int, d ftDir, dst net.Conn) bool {
	c.p.Fired.Add(1)
	switch d.Action {
	case ftReset:
		c.reset()
		return true
	case ftClose:
		c.close()
		return true
	case ftHalfClose:
		if tc, ok := dst.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}
	c.mu.Lock()
	c.hole[dir] = true // what follows in this direction is dropped
	c.mu.Unlock()
	return false
}
