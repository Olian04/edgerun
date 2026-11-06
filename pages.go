package edgerun

import (
	"context"
	"iter"
)

// pages turns a paged list call into an iterator that fetches pages as it is
// consumed. An error ends the iteration after being yielded once.
func pages[T any](ctx context.Context, fetch func(ctx context.Context, token string) (items []T, next string, err error)) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		token := ""
		for {
			items, next, err := fetch(ctx, token)
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			for _, it := range items {
				if !yield(it, nil) {
					return
				}
			}
			if next == "" {
				return
			}
			token = next
		}
	}
}
