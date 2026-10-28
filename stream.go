package edgerun

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"connectrpc.com/connect"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// call strips the Connect envelope from a unary call.
func call[Q, R any](ctx context.Context, rpc func(context.Context, *connect.Request[Q]) (*connect.Response[R], error), req *Q) (*R, error) {
	res, err := rpc(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

// Response is a streamed HTTP response: the status line and headers have
// arrived, the body arrives as it is read.
//
// The body must be read to its end or closed; closing early cancels the call.
// The context given to the call must outlive the body: cancelling it ends the
// response, wherever it has got to.
// A failure after the head (a worker dying mid-body) is returned by Read, never
// as io.EOF, so a body that ends cleanly is a body that is complete.
type Response struct {
	// Head is the status and headers as the protocol carries them.
	Head *commonv1.HttpResponseHead
	Body io.ReadCloser
}

// Status is the HTTP status code.
func (r *Response) Status() int { return int(r.Head.GetStatus()) }

// Header returns the response headers.
func (r *Response) Header() http.Header {
	h := http.Header{}
	for _, kv := range r.Head.GetHeaders() {
		h.Add(kv.GetName(), kv.GetValue())
	}
	return h
}

// ReadAll reads the whole body and closes it.
func (r *Response) ReadAll() ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}

// source is what every server stream of the protocols is: Connect's
// ServerStreamForClient.
type source interface {
	Receive() bool
	Err() error
	Close() error
}

// body turns the chunks of a stream into an io.Reader.
type body struct {
	src    source
	chunk  func() ([]byte, error) // the bytes of the current message (nil if it carries none), or a protocol violation
	buf    []byte
	err    error // sticky; io.EOF once the stream has ended cleanly
	closed bool
}

func (b *body) Read(p []byte) (int, error) {
	if b.closed {
		return 0, http.ErrBodyReadAfterClose
	}
	for len(b.buf) == 0 {
		if b.err != nil {
			return 0, b.err
		}
		if !b.src.Receive() {
			if b.err = b.src.Err(); b.err == nil {
				b.err = io.EOF
			}
			continue
		}
		if b.buf, b.err = b.chunk(); b.err != nil {
			b.src.Close()
		}
	}
	n := copy(p, b.buf)
	b.buf = b.buf[n:]
	return n, nil
}

func (b *body) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	return b.src.Close()
}

// openStream reads a stream up to its head. A failure before the head is the
// call's error (typed, see Reason); a stream that does not begin with a head is
// a protocol violation. headOf returns the head of a message that is one, and
// chunkOf the payload of a later message, which a seam may also use to notice
// parts of its own (a summary). A later message that is a head again is a
// protocol violation.
func openStream[M any](st *connect.ServerStreamForClient[M], headOf func(*M) *commonv1.HttpResponseHead, chunkOf func(*M) []byte) (*Response, *M, error) {
	if !st.Receive() {
		err := st.Err()
		st.Close()
		if err == nil {
			err = errNoHead
		}
		return nil, nil, err
	}
	first := st.Msg()
	head := headOf(first)
	if head == nil {
		st.Close()
		return nil, nil, errNoHead
	}
	return &Response{Head: head, Body: &body{src: st, chunk: func() ([]byte, error) {
		m := st.Msg()
		if headOf(m) != nil {
			return nil, errSecondHead
		}
		return chunkOf(m), nil
	}}}, first, nil
}

// ErrProtocol is what a call fails with when the peer answered in a way the
// protocol does not allow (a response stream with no head, say). It is the
// peer's fault, not the caller's: a gateway maps it to 502.
var ErrProtocol = errors.New("edgerun: the peer violated the protocol")

var errSecondHead = fmt.Errorf("%w: a second response head", ErrProtocol)

var errNoHead = fmt.Errorf("%w: the stream did not begin with a response head", ErrProtocol)
