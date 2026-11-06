package store

import (
	"fmt"
	"math"
)

// clamp32 and clamp64 turn a number a peer sent (protobuf unsigned) into what a
// column holds: the largest value it can, not a wrapped negative one.
func clamp32(v uint32) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(v)
}

func clamp64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

func errAlready(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrAlreadyExists, fmt.Sprintf(format, args...))
}
