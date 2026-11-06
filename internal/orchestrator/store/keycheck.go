package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

var keyCheckAAD = []byte("edgerun/secret-key-check")

// ErrWrongSecretKey is what CheckSecretKey says when the key cannot open what the database holds.
var ErrWrongSecretKey = errors.New("the secret key does not open the secrets of this database: every orchestrator on one database needs the same --secret-key")

// CheckSecretKey makes sure the key this store was given is the one the database's secrets were
// sealed with, and records that on first use. An orchestrator with another key would start, and
// then fail on every function that has a secret, on its own instance only.
func (s *Store) CheckSecretKey(ctx context.Context) error {
	var stored []byte
	err := s.pool.QueryRow(ctx, `SELECT value FROM orchestrator_meta WHERE key = 'secret-key-check'`).Scan(&stored)
	switch {
	case err == nil:
		if _, err := s.box.Open(stored, keyCheckAAD); err != nil {
			return ErrWrongSecretKey
		}
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return err
	}
	// First start with this check: a database that already holds secrets proves the key before the key is
	// recorded as the right one.
	var fid, name string
	var sealed []byte
	err = s.pool.QueryRow(ctx, `SELECT function_id::text, name, value FROM env_vars WHERE secret LIMIT 1`).Scan(&fid, &name, &sealed)
	switch {
	case err == nil:
		if _, err := s.box.Open(sealed, envAAD(fid, name)); err != nil {
			return ErrWrongSecretKey
		}
	case !errors.Is(err, pgx.ErrNoRows):
		return err
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO orchestrator_meta (key, value) VALUES ('secret-key-check', $1) ON CONFLICT (key) DO NOTHING`,
		s.box.Seal([]byte("edgerun"), keyCheckAAD)); err != nil {
		return fmt.Errorf("recording the secret key check: %w", err)
	}
	// Two instances may have raced to record it: whoever lost checks the winner's.
	return s.CheckSecretKey(ctx)
}
