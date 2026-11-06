// Package store is the only code that talks to PostgreSQL. It returns the
// control.v1 resource messages directly: those messages are the specified
// model, so a second set of domain structs would only be a place for drift.
package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Olian04/edgerun/internal/orchestrator/secrets"
)

// Errors the services translate into protocol errors. They are wrapped with
// detail, so test with errors.Is.
var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrInvalid       = errors.New("invalid")
)

func notFound(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrNotFound, fmt.Sprintf(format, args...))
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// Store is safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
	box  *secrets.Box
}

// New returns a store. box seals secret environment values.
func New(pool *pgxpool.Pool, box *secrets.Box) *Store { return &Store{pool: pool, box: box} }

// PostgreSQL error classes the store maps to its own errors.
const (
	pgUniqueViolation = "23505"
	pgFKViolation     = "23503"
	pgCheckViolation  = "23514"
)

func pgCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// parseID returns the canonical form of a UUID, or ErrNotFound: nothing can
// have an id that is not a UUID.
func parseID(kind, id string) (string, error) {
	u, err := uuid.Parse(id)
	if err != nil {
		return "", notFound("%s %q", kind, id)
	}
	return u.String(), nil
}

// querier is what the store runs statements through: the pool, or, inside
// Atomic and DryRun, their transaction.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type txKey struct{}

// q returns where statements for ctx run.
func (s *Store) q(ctx context.Context) querier {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return s.pool
}

// Atomic runs f with everything it does to the store taking place in one
// transaction, committed if f returns nil and rolled back if it returns an
// error: all of it or none.
func (s *Store) Atomic(ctx context.Context, f func(ctx context.Context) error) error {
	return s.inTx(ctx, func(tx pgx.Tx) error { return f(context.WithValue(ctx, txKey{}, tx)) })
}

// DryRun runs f with everything it does to the store taking place in a
// transaction that is rolled back whatever f returns. f therefore sees the real
// state, meets the real constraints and conflicts, and returns what it would
// have returned, while nothing is left behind: that is what validate-only means.
func (s *Store) DryRun(ctx context.Context, f func(ctx context.Context) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	return f(context.WithValue(ctx, txKey{}, tx))
}

// inTx runs f in a transaction, committing if it returns nil. Inside a dry run
// it joins the dry run's transaction, which is never committed.
func (s *Store) inTx(ctx context.Context, f func(tx pgx.Tx) error) error {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return f(tx)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // no-op after a successful commit
	if err := f(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// --- pagination ------------------------------------------------------------
//
// Tokens are opaque to clients: the fields of the last row returned, so the
// next page is a keyset query (stable under concurrent writes, unlike OFFSET).

const (
	defaultPage = 50
	maxPage     = 500
)

func pageSize(n uint32) int {
	switch {
	case n == 0:
		return defaultPage
	case n > maxPage:
		return maxPage
	}
	return int(n)
}

func encodeToken(parts ...string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strings.Join(parts, "\x00")))
}

// decodeToken returns the n fields of a token, or nil for the empty token.
func decodeToken(tok string, n int) ([]string, error) {
	if tok == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return nil, invalid("malformed page token")
	}
	parts := strings.Split(string(b), "\x00")
	if len(parts) != n {
		return nil, invalid("malformed page token")
	}
	return parts, nil
}
