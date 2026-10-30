package store

import "fmt"

func errAlready(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrAlreadyExists, fmt.Sprintf(format, args...))
}
