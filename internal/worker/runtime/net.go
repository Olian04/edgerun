package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/sys"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	"github.com/Olian04/edgerun/internal/egress/policy"
	"github.com/Olian04/edgerun/internal/wagi"
)

// ABI2 is the guest ABI with outbound HTTP. See docs/protocol.md.
const ABI2 = wagi.ABI2

// The results of a guest's fetch call that are not a size.
const (
	netDenied   int32 = -1
	netFailed   int32 = -2
	netTooLarge int32 = -3
	netInvalid  int32 = -4
	netLimit    int32 = -5
)

const (
	// maxFetches bounds the requests one invocation may make.
	maxFetches = 16
	// maxFetchRequest and maxFetchResponse bound one request and one response.
	maxFetchRequest  = 1 << 20
	maxFetchResponse = 4 << 20
)

// Network makes an outbound HTTP request on a guest's behalf. The runtime
// knows nothing of how: the worker supplies an implementation that speaks to a
// network service, and a worker without one offers no wagi-2.
type Network interface {
	Fetch(ctx context.Context, req NetRequest) (*NetResponse, error)
}

// Grant is one destination a function may reach, and the network service that
// carries it.
type Grant struct {
	// Allow is the allow-list entry, in the grammar of docs/protocol.md.
	Allow string
	// Service is the base URL of the network service.
	Service string
}

// NetRequest is one outbound request.
type NetRequest struct {
	Method, URL string
	Headers     []*commonv1.Header
	Body        []byte
	// Service is the network service to make the request through: the one that
	// carries the grant matching the URL.
	Service string
	// Allow is the grants' entries for that service; the service enforces them.
	Allow []string
	// MaxResponseBytes bounds the response body.
	MaxResponseBytes uint64
	InvocationID     string
}

// NetResponse is the answer to a NetRequest.
type NetResponse struct {
	Status  uint32
	Headers []*commonv1.Header
	Body    []byte
}

// NetErrorKind says why a request did not produce a response.
type NetErrorKind int

const (
	NetDenied   NetErrorKind = iota + 1 // the destination is not allowed
	NetFailed                           // it could not be reached, or the exchange broke
	NetTooLarge                         // the response exceeded the limit
	NetInvalid                          // the request was malformed
)

// NetError is a Network's failure.
type NetError struct {
	Kind NetErrorKind
	Msg  string
}

func (e *NetError) Error() string { return e.Msg }

// netState is one invocation's side of the host module, carried in the
// context the guest runs under.
type netState struct {
	net     Network
	grants  []parsedGrant
	id      string
	maxResp uint64

	fetches int
	pending []byte
}

type parsedGrant struct {
	Grant
	rule policy.Rule
}

// parseGrants reads the grants, leaving out any whose entry it cannot parse:
// a grant the worker cannot read lets nothing through.
func parseGrants(gs []Grant) []parsedGrant {
	out := make([]parsedGrant, 0, len(gs))
	for _, g := range gs {
		if r, err := policy.ParseRule(g.Allow); err == nil && g.Service != "" {
			out = append(out, parsedGrant{g, r})
		}
	}
	return out
}

// route picks the grant for a request URL and returns the service to use and
// the entries that service carries for this function. ok is false if no grant
// allows the URL, or the URL is not one the ABI carries.
func (st *netState) route(rawURL string) (service string, allow []string, ok bool, invalid bool) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return "", nil, false, true
	}
	port := policy.DefaultPort(u.Scheme)
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return "", nil, false, true
		}
		port = n
	}
	for _, g := range st.grants {
		if (policy.List{g.rule}).Allows(u.Scheme, u.Hostname(), port) {
			service = g.Service
			break
		}
	}
	if service == "" {
		return "", nil, false, false
	}
	for _, g := range st.grants {
		if g.Service == service {
			allow = append(allow, g.Allow)
		}
	}
	return service, allow, true, false
}

type netKey struct{}

// instantiateHTTP registers the edgerun_http host module in a runtime.
func instantiateHTTP(ctx context.Context, rt wazero.Runtime) error {
	i32 := []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}
	_, err := rt.NewHostModuleBuilder(wagi.HTTPModule).
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(hostFetch), i32, []api.ValueType{api.ValueTypeI32}).Export("fetch").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(hostFetchRead), i32, []api.ValueType{api.ValueTypeI32}).Export("fetch_read").
		Instantiate(ctx)
	return err
}

func result(stack []uint64, v int32) { stack[0] = api.EncodeI32(v) }

// hostFetch is fetch(req_ptr, req_len) -> size or a negative error.
func hostFetch(ctx context.Context, mod api.Module, stack []uint64) {
	st, _ := ctx.Value(netKey{}).(*netState)
	if st == nil {
		result(stack, netFailed)
		return
	}
	st.pending = nil
	if st.fetches >= maxFetches {
		result(stack, netLimit)
		return
	}
	st.fetches++

	ptr, n := api.DecodeU32(stack[0]), api.DecodeU32(stack[1])
	if n > maxFetchRequest {
		result(stack, netInvalid)
		return
	}
	raw, ok := mod.Memory().Read(ptr, n)
	if !ok {
		result(stack, netInvalid)
		return
	}
	req, err := ParseFetchRequest(append([]byte(nil), raw...))
	if err != nil {
		result(stack, netInvalid)
		return
	}
	// Closed by default: nothing is asked for unless a grant allows it.
	service, allow, ok, invalid := st.route(req.URL)
	switch {
	case invalid:
		result(stack, netInvalid)
		return
	case !ok:
		result(stack, netDenied)
		return
	}
	req.Service, req.Allow, req.MaxResponseBytes, req.InvocationID = service, allow, min(st.maxResp, maxFetchResponse), st.id

	resp, err := st.net.Fetch(ctx, *req)
	if err != nil {
		// The budget ran out while the request was in flight. The guest must not
		// be allowed to carry on and finish as if it had merely been refused.
		switch cerr := ctx.Err(); {
		case errors.Is(cerr, context.DeadlineExceeded):
			mod.CloseWithExitCode(ctx, sys.ExitCodeDeadlineExceeded)
		case cerr != nil:
			mod.CloseWithExitCode(ctx, sys.ExitCodeContextCanceled)
		}
		var ne *NetError
		switch {
		case errors.As(err, &ne) && ne.Kind == NetDenied:
			result(stack, netDenied)
		case errors.As(err, &ne) && ne.Kind == NetTooLarge:
			result(stack, netTooLarge)
		case errors.As(err, &ne) && ne.Kind == NetInvalid:
			result(stack, netInvalid)
		default:
			result(stack, netFailed)
		}
		return
	}
	text := FormatFetchResponse(resp)
	if uint64(len(text)) > min(st.maxResp, maxFetchResponse) {
		result(stack, netTooLarge)
		return
	}
	st.pending = text
	result(stack, int32(len(text)))
}

// hostFetchRead is fetch_read(dst_ptr, dst_len) -> bytes copied.
func hostFetchRead(ctx context.Context, mod api.Module, stack []uint64) {
	st, _ := ctx.Value(netKey{}).(*netState)
	if st == nil || len(st.pending) == 0 {
		result(stack, 0)
		return
	}
	ptr, n := api.DecodeU32(stack[0]), api.DecodeU32(stack[1])
	m := min(int(n), len(st.pending))
	if !mod.Memory().Write(ptr, st.pending[:m]) {
		result(stack, netInvalid)
		return
	}
	st.pending = st.pending[m:]
	result(stack, int32(m))
}

// ParseFetchRequest parses the text a guest passes to fetch: a request line
// "METHOD URL", header lines, a blank line, then the body. The blank line may
// be left out when there is no body. Header names are kept as written.
func ParseFetchRequest(b []byte) (*NetRequest, error) {
	line, rest, _ := bytes.Cut(b, []byte("\n"))
	line = bytes.TrimSuffix(line, []byte("\r"))
	method, url, ok := bytes.Cut(line, []byte(" "))
	if !ok || len(method) == 0 || len(bytes.TrimSpace(url)) == 0 {
		return nil, fmt.Errorf("the first line must be \"METHOD URL\"")
	}
	req := &NetRequest{Method: string(method), URL: string(bytes.TrimSpace(url))}
	for len(rest) > 0 {
		var l []byte
		l, rest, _ = bytes.Cut(rest, []byte("\n"))
		l = bytes.TrimSuffix(l, []byte("\r"))
		if len(l) == 0 {
			req.Body = rest
			return req, nil
		}
		name, value, ok := bytes.Cut(l, []byte(":"))
		if !ok || len(bytes.TrimSpace(name)) == 0 {
			return nil, fmt.Errorf("malformed header line %q", truncate(l))
		}
		req.Headers = append(req.Headers, &commonv1.Header{Name: string(bytes.TrimSpace(name)), Value: strings.TrimSpace(string(value))})
	}
	return req, nil
}

// FormatFetchResponse renders a response as the text fetch_read returns:
// "Status: N", header lines, a blank line, the body.
func FormatFetchResponse(r *NetResponse) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "Status: %d\n", r.Status)
	for _, h := range r.Headers {
		fmt.Fprintf(&b, "%s: %s\n", h.GetName(), h.GetValue())
	}
	b.WriteByte('\n')
	b.Write(r.Body)
	return b.Bytes()
}
