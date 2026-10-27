// Package migrate applies SQL migrations to PostgreSQL.
//
// It is safe to call from several orchestrator instances at once: a session
// advisory lock serialises them, and each migration runs in its own
// transaction together with its bookkeeping row, so a failed migration leaves
// no trace and is retried on the next start.
package migrate

import (
	"context"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
)

// lockKey is the advisory lock that serialises migrators ("edru").
const lockKey int64 = 0x65647275

var filename = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// Migration is one SQL file.
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// Load reads and validates the migrations in fsys.
func Load(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}
	var out []Migration
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := filename.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("migration file %q: want NNNN_name.sql", e.Name())
		}
		v, _ := strconv.Atoi(m[1])
		b, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, Migration{Version: v, Name: m[2], SQL: string(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	for i, m := range out {
		if m.Version != i+1 {
			return nil, fmt.Errorf("migrations must be numbered 0001.. without gaps or duplicates: found %04d at position %d", m.Version, i+1)
		}
	}
	return out, nil
}

// Up applies every pending migration and returns the versions it applied.
func Up(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) ([]int, error) {
	migrations, err := Load(fsys)
	if err != nil {
		return nil, err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
		return nil, fmt.Errorf("taking migration lock: %w", err)
	}
	// Unlock explicitly: the connection goes back to the pool, not away.
	defer conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, lockKey)

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    integer     PRIMARY KEY,
		name       text        NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	applied := map[int]bool{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for v := range applied {
		if v > len(migrations) {
			return nil, fmt.Errorf("database is at schema version %d but this binary only knows %d: refusing to run an older orchestrator against a newer schema", v, len(migrations))
		}
	}

	var done []int
	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return done, err
		}
		if _, err := tx.Exec(ctx, m.SQL); err != nil {
			tx.Rollback(ctx)
			return done, fmt.Errorf("migration %04d_%s: %w", m.Version, m.Name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.Version, m.Name); err != nil {
			tx.Rollback(ctx)
			return done, err
		}
		if err := tx.Commit(ctx); err != nil {
			return done, err
		}
		done = append(done, m.Version)
	}
	return done, nil
}
