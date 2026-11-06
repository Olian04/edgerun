// Package orchtest builds orchestrator pieces on a real PostgreSQL for tests.
package orchtest

import (
	"bytes"
	"context"
	"testing"

	"github.com/Olian04/edgerun/internal/orchestrator/migrate"
	"github.com/Olian04/edgerun/internal/orchestrator/secrets"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
	"github.com/Olian04/edgerun/internal/testpg"
	"github.com/Olian04/edgerun/migrations"
)

// Key is the secrets key tests use.
var Key = bytes.Repeat([]byte{9}, secrets.KeySize)

// Store returns a store on a fresh, migrated database.
func Store(t testing.TB) *store.Store {
	t.Helper()
	pool := testpg.New(t)
	if _, err := migrate.Up(context.Background(), pool, migrations.FS); err != nil {
		t.Fatal(err)
	}
	box, err := secrets.NewBox(Key)
	if err != nil {
		t.Fatal(err)
	}
	return store.New(pool, box)
}
