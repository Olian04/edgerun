package main

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// Verification: proof that a response is the one for the request that was sent.
//
// Everything above this was measured from status and timing only. With a
// stamped body (every body starts with its own request id and the run id) and a
// function that echoes it, the content of a response says whose it is: a
// response that carries another request's stamp, or is truncated, or is
// anything but the echo, is a mismatch. With several ingress instances on one
// orchestrator this is the check that a response was delivered to the instance
// that made the request.

type verifyMode uint8

const (
	verifyNone       verifyMode = iota
	verifyEcho                  // the response body is the request body
	verifyEchoWithID            // the response body is "<request id>\n" and then the request body
)

// stampSlack is how much longer than the largest body the padding pool is, so
// that two requests of the same size rarely carry the same padding.
const stampSlack = 251

// stampOf is the beginning of every stamped body.
func stampOf(requestID, runID string) []byte {
	return []byte("tm1 " + requestID + " " + runID + "\n")
}

// A Result's Verify field.
const (
	VerifyOK       = "ok"
	VerifyBody     = "body_mismatch"
	VerifyHeader   = "header_mismatch"
	VerifyBodyHdr  = "body_and_header_mismatch"
	maxDetailBytes = 96
	maxHeadBytes   = 512
)

// echoCheck compares a response body, as it streams in, with the bytes it must
// be, without buffering it.
type echoCheck struct {
	prefix, body []byte // the expected response is prefix then body
	off          int    // received so far
	diff         int    // offset of the first difference, -1 if none yet
	sample       []byte // what was received from the first difference on
	head         []byte // the first bytes received, to find a stamp that is not ours
}

func newEchoCheck(m verifyMode, requestID string, body []byte) *echoCheck {
	c := &echoCheck{body: body, diff: -1}
	if m == verifyEchoWithID {
		c.prefix = []byte(requestID + "\n")
	}
	return c
}

func (c *echoCheck) total() int { return len(c.prefix) + len(c.body) }

// firstDiff is the index in p of the first byte that is not the one expected at
// that position of the response, or -1.
func (c *echoCheck) firstDiff(p []byte) int {
	for pos := 0; pos < len(p); {
		o := c.off + pos
		var seg []byte
		switch {
		case o < len(c.prefix):
			seg = c.prefix[o:]
		case o-len(c.prefix) < len(c.body):
			seg = c.body[o-len(c.prefix):]
		default:
			return pos // more than there should be
		}
		n := min(len(seg), len(p)-pos)
		if !bytes.Equal(seg[:n], p[pos:pos+n]) {
			for i := 0; i < n; i++ {
				if seg[i] != p[pos+i] {
					return pos + i
				}
			}
		}
		pos += n
	}
	return -1
}

// Write implements io.Writer; it never fails.
func (c *echoCheck) Write(p []byte) (int, error) {
	if c.diff < 0 {
		if i := c.firstDiff(p); i >= 0 {
			c.diff = c.off + i
			c.sample = append([]byte(nil), p[i:min(len(p), i+maxDetailBytes)]...)
		}
	}
	if room := maxHeadBytes - len(c.head); room > 0 {
		c.head = append(c.head, p[:min(len(p), room)]...)
	}
	c.off += len(p)
	return len(p), nil
}

var foreignStamp = regexp.MustCompile(`tm1 (\S+) (\S+)\n`)

// finish is "" when the response was exactly what it should be, else what was wrong.
func (c *echoCheck) finish(ownID string) string {
	if c.diff < 0 && c.off == c.total() {
		return ""
	}
	var b strings.Builder
	switch {
	case c.diff >= 0:
		fmt.Fprintf(&b, "differs at byte %d of %d expected, %d received", c.diff, c.total(), c.off)
		if len(c.sample) > 0 {
			fmt.Fprintf(&b, ", received from there %q", c.sample)
		}
	case c.off < c.total():
		fmt.Fprintf(&b, "truncated: %d of %d bytes", c.off, c.total())
	}
	if m := foreignStamp.FindSubmatch(c.head); m != nil && string(m[1]) != ownID {
		fmt.Fprintf(&b, "; the body carries the stamp of request %s (run %s)", m[1], m[2])
	}
	if c.off == 0 {
		b.WriteString("; empty body")
	}
	return b.String()
}

// checkHeader verifies that the response head carries the request id in the
// named header (exactly once). It returns what is wrong, or "".
func checkHeader(hs []*commonv1.Header, name, requestID string) string {
	var vals []string
	for _, h := range hs {
		if strings.EqualFold(h.GetName(), name) {
			vals = append(vals, h.GetValue())
		}
	}
	switch {
	case len(vals) == 0:
		return fmt.Sprintf("header %s absent", name)
	case len(vals) != 1 || vals[0] != requestID:
		return fmt.Sprintf("header %s is %q, want %q", name, vals, requestID)
	}
	return ""
}
