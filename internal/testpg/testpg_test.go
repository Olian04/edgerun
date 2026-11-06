package testpg

import (
	"context"
	"testing"
)

func TestMain(m *testing.M) { Main(m) }

func TestIsolatedDatabases(t *testing.T) {
	ctx := context.Background()
	a, b := New(t), New(t)
	if _, err := a.Exec(ctx, `CREATE TABLE only_in_a (id int)`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := b.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE tablename = 'only_in_a'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("databases are not isolated: n=%d err=%v", n, err)
	}
	if err := a.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE tablename = 'only_in_a'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("table missing in its own database: n=%d err=%v", n, err)
	}
}
