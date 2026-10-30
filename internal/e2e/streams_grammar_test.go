package e2e

// The grammar of the server-streaming seams, from docs/protocol.md, and what a
// consumer owes for each sequence of symbols a producer can send.
//
// The grammar
//
//	Invoke   (orchestrator -> frontend)   head chunk*                      clean close
//	Fetch    (network service -> worker)  head chunk*                      clean close
//	Execute  (worker -> orchestrator)     head chunk* summary              clean close
//	GetArtifact (orchestrator -> worker)  chunk*  (the bytes hash to the digest)  clean close
//
//   - "exactly one head, zero or more chunks" (ingress.proto, egress.proto), and for
//     Execute "then exactly one summary. A stream that ends without a summary is a
//     failed invocation" (worker.proto, protocol.md "Invocation semantics" 6).
//   - an error may end any stream at any point; before the head it is the call's
//     error, after the head it terminates the stream with the error (the
//     frontend then aborts the client's connection).
//   - everything else is a violation: a second head, a chunk (or a summary)
//     before the head, a second summary, anything after the summary, a stream
//     that ends before the head, a stream that ends without its summary, a
//     message that is not cut off cleanly (reset mid-message), a stream with
//     no end.
//   - a message with no part set is NOT classified: the spec does not say whether
//     it is a violation or a tolerated unknown part (forward compatibility), so a
//     consumer may do either. It is reported as an ambiguity, never as a defect.
//   - what follows a summary other than a clean close (an error, a reset, a stall)
//     is likewise unspecified: either the summary ended the invocation or the
//     close does. Ambiguity.
//
// Oracle: a valid sequence is accepted and its bytes arrive intact; a violating
// one ends in a typed failure (the documented HTTP status at the frontend) and
// never in a success; a stream that stops ends by its deadline.

import (
	"fmt"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

// non-terminal symbols
const (
	stH  = "H"  // the response head (status 200)
	stHB = "HB" // a head whose status is outside 200-599
	stC  = "C"  // a chunk with bytes
	stE  = "E"  // an empty chunk
	stN  = "N"  // a message with no part set
	stS  = "S"  // the summary (Execute only)
)

// terminal symbols
const (
	stCLEAN = "CLEAN" // the end-stream frame with no error: a clean close
	stTRUNC = "TRUNC" // the body just ends: no end-stream frame
	stRESET = "RESET" // the connection dies in the middle of a message
	stSTALL = "STALL" // nothing more is sent, nothing is closed
)

type stErrKind struct {
	code   connect.Code
	reason commonv1.ErrorReason
	typed  bool
}

// stErrs are the terminals that end a stream with an error: ERR:<name>.
var stErrs = map[string]stErrKind{
	"ERR:INTERNAL":           {connect.CodeInternal, commonv1.ErrorReason_ERROR_REASON_INTERNAL, true},
	"ERR:NO_CAPACITY":        {connect.CodeResourceExhausted, commonv1.ErrorReason_ERROR_REASON_NO_CAPACITY, true},
	"ERR:TRAPPED":            {connect.CodeAborted, commonv1.ErrorReason_ERROR_REASON_FUNCTION_TRAPPED, true},
	"ERR:DEADLINE":           {connect.CodeDeadlineExceeded, commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, true},
	"ERR:NOT_FOUND":          {connect.CodeNotFound, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND, true},
	"ERR:ARTIFACT_NOT_FOUND": {connect.CodeNotFound, commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND, true},
	"ERR:EGRESS_FAILED":      {connect.CodeUnavailable, commonv1.ErrorReason_ERROR_REASON_EGRESS_FAILED, true},
	"ERR:EGRESS_DENIED":      {connect.CodePermissionDenied, commonv1.ErrorReason_ERROR_REASON_EGRESS_DENIED, true},
	"ERR:TOO_LARGE":          {connect.CodeResourceExhausted, commonv1.ErrorReason_ERROR_REASON_RESPONSE_TOO_LARGE, true},
	"ERR:R999":               {connect.CodeUnknown, commonv1.ErrorReason(999), true},
	"ERR:unavailable":        {connect.CodeUnavailable, 0, false},
	"ERR:internal":           {connect.CodeInternal, 0, false},
	"ERR:resource_exhausted": {connect.CodeResourceExhausted, 0, false},
	"ERR:cancelled":          {connect.CodeCanceled, 0, false},
	"ERR:invalid_argument":   {connect.CodeInvalidArgument, 0, false},
}

// stSecret is in the message of every error the producers send: an
// operator-facing message that no frontend may let through to a client.
const stSecret = "SECRET-OPERATOR-DETAIL"

// stChunk is the content of the chunk at position i of a sequence.
func stChunk(i int) []byte { return []byte(fmt.Sprintf("<chunk %d of the response>", i)) }

// stCodec marshals the messages of one seam.
type stCodec struct {
	head    func(status uint32) []byte
	chunk   func(b []byte) []byte
	summary func() []byte // nil: the seam has no summary
	nopart  func() []byte
}

func stMust(m proto.Message) []byte {
	b, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

var stHeadHdr = []*commonv1.Header{{Name: "x-grammar", Value: []byte("yes")}}

var stInvokeCodec = stCodec{
	head: func(st uint32) []byte {
		return stMust(&ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Head{Head: &ingressv1.InvokeHead{
			Http: &commonv1.HttpResponseHead{Status: st, Headers: stHeadHdr}, InvocationId: "inv-from-producer"}}})
	},
	chunk: func(b []byte) []byte {
		return stMust(&ingressv1.InvokeResponse{Part: &ingressv1.InvokeResponse_Chunk{Chunk: b}})
	},
	nopart: func() []byte { return stMust(&ingressv1.InvokeResponse{}) },
}

var stExecuteCodec = stCodec{
	head: func(st uint32) []byte {
		return stMust(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Head{Head: &commonv1.HttpResponseHead{Status: st, Headers: stHeadHdr}}})
	},
	chunk: func(b []byte) []byte {
		return stMust(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Chunk{Chunk: b}})
	},
	summary: func() []byte {
		return stMust(&workerv1.ExecuteResponse{Part: &workerv1.ExecuteResponse_Summary{Summary: &workerv1.ExecutionSummary{DurationMicros: 1234, PeakMemoryBytes: 1 << 20}}})
	},
	nopart: func() []byte { return stMust(&workerv1.ExecuteResponse{}) },
}

var stFetchCodec = stCodec{
	head: func(st uint32) []byte {
		return stMust(&egressv1.FetchResponse{Part: &egressv1.FetchResponse_Head{Head: &commonv1.HttpResponseHead{Status: st, Headers: stHeadHdr}}})
	},
	chunk: func(b []byte) []byte {
		return stMust(&egressv1.FetchResponse{Part: &egressv1.FetchResponse_Chunk{Chunk: b}})
	},
	nopart: func() []byte { return stMust(&egressv1.FetchResponse{}) },
}

// stPlay is the producer's whole behaviour for one sequence. Chunk i is the
// i-th symbol of the sequence, so every chunk has distinct content.
func stPlay(o *stOut, c stCodec, seq stSeq) {
	for i, s := range seq.prefix() {
		switch s {
		case stH:
			o.msg(c.head(200))
		case stHB:
			o.msg(c.head(700))
		case stC:
			o.msg(c.chunk(stChunk(i)))
		case stE:
			o.msg(c.chunk(nil))
		case stN:
			o.msg(c.nopart())
		case stS:
			o.msg(c.summary())
		default:
			panic("unknown symbol " + s)
		}
	}
	switch t := seq.term(); {
	case t == stCLEAN:
		o.end(0, "", nil)
	case t == stTRUNC:
	case t == stRESET:
		o.reset()
	case t == stSTALL:
		o.stall()
	case strings.HasPrefix(t, "ERR:"):
		k := stErrs[t]
		var info *commonv1.ErrorInfo
		if k.typed {
			info = &commonv1.ErrorInfo{Reason: k.reason, Message: stSecret}
		}
		o.end(k.code, stSecret, info)
	default:
		panic("unknown terminal " + t)
	}
}

// ---- what a consumer owes ----------------------------------------------------------

type stKind int

const (
	stOK         stKind = iota // accepted, bytes intact
	stFailBefore               // fails, and nothing of the response was delivered as valid
	stFailAfter                // the head was delivered, then the stream fails
	stEither                   // accepted with its bytes intact, or failed: the spec does not say which
)

func (k stKind) String() string {
	return [...]string{"valid", "failure before the head", "failure after the head", "either"}[k]
}

type stExpect struct {
	kind stKind
	// body is what a consumer that accepts the stream delivers: the chunks, concatenated.
	body []byte
	// sent is every byte of chunk the producer sent before the failure; what a
	// consumer delivers before failing must be a prefix of it.
	sent []byte
	// violation names what is wrong with the stream ("" for a valid one or one that merely errors).
	violation string
	// ambiguity names the unspecified part of the sequence, if the verdict depends on it.
	ambiguity string
	// reasons the failure may carry.
	reasons []commonv1.ErrorReason
	// stalled is set when the stream stops: the consumer has to end it by its deadline.
	stalled bool
	// badHead is set when the head's status is outside 200-599.
	badHead bool
	// term is the terminal of the sequence.
	term string
}

// stAnalyze judges a sequence against the grammar. summary says whether the
// seam's streams end with a summary (Execute).
func stAnalyze(summary bool, seq stSeq) []stExpect {
	out := []stExpect{stAnalyzeOne(summary, seq, false)}
	if slices.Contains(seq.prefix(), stN) {
		out = append(out, stAnalyzeOne(summary, seq, true))
	}
	return out
}

func stAnalyzeOne(summary bool, seq stSeq, nStrict bool) stExpect {
	var e stExpect
	head, sum, ambiguous := false, false, ""
	fail := func(why string) {
		if e.violation == "" {
			e.violation = why
			if head {
				e.kind = stFailAfter
			} else {
				e.kind = stFailBefore
			}
		}
	}
	for i, s := range seq.prefix() {
		if e.violation != "" {
			break
		}
		switch s {
		case stH:
			if head {
				fail("a second head")
			}
			head = true
		case stHB:
			e.badHead = true
			if head {
				fail("a second head")
			} else {
				fail("a head with a status outside 200-599")
			}
		case stC, stE:
			switch {
			case !head:
				fail("a chunk before the head")
			case sum:
				fail("data after the summary")
			case s == stC:
				e.body = append(e.body, stChunk(i)...)
			}
		case stN:
			ambiguous = "a message with no part set"
			if nStrict {
				fail("a message with no part set (rejected)")
			}
		case stS:
			switch {
			case !head:
				fail("a summary before the head")
			case sum:
				fail("a second summary")
			default:
				sum = true
			}
		}
	}
	e.ambiguity = ambiguous
	t := seq.term()
	e.term = t
	e.stalled = t == stSTALL
	for i, s := range seq.prefix() {
		if s == stC {
			e.sent = append(e.sent, stChunk(i)...)
		}
	}
	internal := []commonv1.ErrorReason{commonv1.ErrorReason_ERROR_REASON_INTERNAL}
	if e.violation != "" {
		e.reasons = internal
		if k, ok := stErrs[t]; ok && k.typed {
			// a consumer that has not noticed the violation reports the error that ends the stream
			e.reasons = append(e.reasons, k.reason)
		}
		return e
	}
	// what ends a stream with no violation so far
	switch {
	case t == stCLEAN && !head:
		e.violation = "a stream that ends before the head"
		e.kind = stFailBefore
	case t == stCLEAN && summary && !sum:
		e.violation = "a stream that ends without its summary"
		e.kind = stFailAfter
	case t == stCLEAN:
		e.kind = stOK
		if ambiguous != "" {
			e.kind = stEither
		}
		return e
	default:
		// an error, a reset, no end, or a stall
		if head {
			e.kind = stFailAfter
		} else {
			e.kind = stFailBefore
		}
		if summary && sum {
			e.kind = stEither
			e.ambiguity = "what follows the summary is not a clean close (" + t + ")"
		}
		e.stalled = t == stSTALL
	}
	e.reasons = internal
	switch k, isErr := stErrs[t]; {
	case isErr && k.typed:
		e.reasons = []commonv1.ErrorReason{k.reason}
	case t == stSTALL:
		e.reasons = []commonv1.ErrorReason{commonv1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED}
	}
	return e
}
