package store

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/internal/wagi"
)

const (
	maxEnvVars       = 64
	maxEnvValueBytes = 32 << 10
)

// envAAD binds a sealed value to its slot.
func envAAD(functionID, name string) []byte { return []byte(functionID + "/" + name) }

// SetEnv creates or replaces one environment variable of a function. Secret
// values are sealed before they reach the database.
func (s *Store) SetEnv(ctx context.Context, functionID string, e *controlv1.EnvEntry) error {
	fid, err := parseID("function", functionID)
	if err != nil {
		return err
	}
	name := e.GetName()
	switch {
	case !wagi.ValidEnvName(name):
		return invalid("environment variable name %q: want [A-Za-z_][A-Za-z0-9_]*", name)
	case wagi.ReservedEnvName(name):
		return invalid("environment variable %q is reserved by the %s ABI", name, wagi.ABI)
	case len(e.GetValue()) > maxEnvValueBytes:
		return invalid("environment variable %q is larger than %d bytes", name, maxEnvValueBytes)
	case strings.ContainsRune(e.GetValue(), 0):
		return invalid("environment variable %q contains NUL", name)
	}
	stored := []byte(e.GetValue())
	if e.GetSecret() {
		stored = s.box.Seal(stored, envAAD(fid, name))
	}

	return s.inTx(ctx, func(tx pgx.Tx) error {
		// Serialise env writes per function so the count limit cannot be raced.
		var locked string
		if err := tx.QueryRow(ctx, `SELECT id::text FROM functions WHERE id = $1 FOR UPDATE`, fid).Scan(&locked); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return notFound("function %s", fid)
			}
			return err
		}
		var count int
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT count(*), coalesce(bool_or(name = $2), false) FROM env_vars WHERE function_id = $1`, fid, name).Scan(&count, &exists); err != nil {
			return err
		}
		if !exists && count >= maxEnvVars {
			return invalid("a function may have at most %d environment variables", maxEnvVars)
		}
		_, err := tx.Exec(ctx, `INSERT INTO env_vars (function_id, name, value, secret) VALUES ($1, $2, $3, $4)
			ON CONFLICT (function_id, name) DO UPDATE SET value = EXCLUDED.value, secret = EXCLUDED.secret`,
			fid, name, stored, e.GetSecret())
		return err
	})
}

// DeleteEnv removes one environment variable.
func (s *Store) DeleteEnv(ctx context.Context, functionID, name string) error {
	fid, err := parseID("function", functionID)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM env_vars WHERE function_id = $1 AND name = $2`, fid, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return notFound("environment variable %q of function %s", name, fid)
	}
	return nil
}

// ListEnv returns a function's variables by name. Secret values are blank:
// they are write-only.
func (s *Store) ListEnv(ctx context.Context, functionID string) ([]*controlv1.EnvEntry, error) {
	fid, err := parseID("function", functionID)
	if err != nil {
		return nil, err
	}
	if _, err := s.GetFunction(ctx, fid); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT name, value, secret FROM env_vars WHERE function_id = $1 ORDER BY name`, fid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*controlv1.EnvEntry
	for rows.Next() {
		var e controlv1.EnvEntry
		var v []byte
		if err := rows.Scan(&e.Name, &v, &e.Secret); err != nil {
			return nil, err
		}
		if !e.Secret {
			e.Value = string(v)
		}
		out = append(out, &e)
	}
	return out, rows.Err()
}
