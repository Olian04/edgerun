package migrate_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/Olian04/edgerun/internal/orchestrator/migrate"
	"github.com/Olian04/edgerun/internal/testpg"
	"github.com/Olian04/edgerun/migrations"
)

func TestMain(m *testing.M) { testpg.Main(m) }

func fs(files map[string]string) fstest.MapFS {
	out := fstest.MapFS{}
	for n, c := range files {
		out[n] = &fstest.MapFile{Data: []byte(c)}
	}
	return out
}

func TestLoadValidation(t *testing.T) {
	for name, tc := range map[string]struct {
		files map[string]string
		want  string
	}{
		"bad name":  {map[string]string{"init.sql": "x"}, "NNNN_name.sql"},
		"gap":       {map[string]string{"0001_a.sql": "x", "0003_c.sql": "x"}, "without gaps"},
		"not first": {map[string]string{"0002_b.sql": "x"}, "without gaps"},
		"duplicate": {map[string]string{"0001_a.sql": "x", "0001_b.sql": "x"}, "without gaps"},
	} {
		if _, err := migrate.Load(fs(tc.files)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want containing %q", name, err, tc.want)
		}
	}
	if ms, err := migrate.Load(fs(map[string]string{"0002_b.sql": "x", "0001_a.sql": "y"})); err != nil || ms[0].Version != 1 {
		t.Errorf("valid set: %v %v", ms, err)
	}
}

func TestShippedMigrationsApplyAndAreIdempotent(t *testing.T) {
	pool := testpg.New(t)
	ctx := context.Background()
	got, err := migrate.Up(ctx, pool, migrations.FS)
	if err != nil || len(got) == 0 {
		t.Fatalf("first Up: %v %v", got, err)
	}
	again, err := migrate.Up(ctx, pool, migrations.FS)
	if err != nil || len(again) != 0 {
		t.Fatalf("second Up must be a no-op: %v %v", again, err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename IN
		('functions','function_versions','artifacts','routes','env_vars','workers','invocations')`).Scan(&n); err != nil || n != 7 {
		t.Fatalf("expected 7 tables, got %d (%v)", n, err)
	}
}

func TestConcurrentInstancesApplyEachMigrationOnce(t *testing.T) {
	pool := testpg.New(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	appliedBy := make(chan int, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := migrate.Up(ctx, pool, migrations.FS)
			errs <- err
			appliedBy <- len(got)
		}()
	}
	wg.Wait()
	close(errs)
	close(appliedBy)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	total := 0
	for n := range appliedBy {
		total += n
	}
	ms, _ := migrate.Load(migrations.FS)
	if total != len(ms) {
		t.Errorf("migrations applied %d times across 8 racing instances, want exactly %d", total, len(ms))
	}
}

func TestFailedMigrationLeavesNoTrace(t *testing.T) {
	pool := testpg.New(t)
	ctx := context.Background()
	bad := fs(map[string]string{
		"0001_ok.sql":  `CREATE TABLE a (id int);`,
		"0002_bad.sql": `CREATE TABLE b (id int); SELECT * FROM does_not_exist;`,
	})
	got, err := migrate.Up(ctx, pool, bad)
	if err == nil || !strings.Contains(err.Error(), "0002_bad") {
		t.Fatalf("err = %v", err)
	}
	if len(got) != 1 || got[0] != 1 {
		t.Errorf("applied = %v, want [1]", got)
	}
	var n int
	pool.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE tablename = 'b'`).Scan(&n)
	if n != 0 {
		t.Error("table b from the failed migration survived: not atomic")
	}
	pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&n)
	if n != 1 {
		t.Errorf("schema_migrations has %d rows, want 1", n)
	}

	// Fixed and retried: only the failed one is applied.
	good := fs(map[string]string{"0001_ok.sql": `CREATE TABLE a (id int);`, "0002_bad.sql": `CREATE TABLE b (id int);`})
	if got, err := migrate.Up(ctx, pool, good); err != nil || len(got) != 1 || got[0] != 2 {
		t.Errorf("retry: %v %v", got, err)
	}
}

func TestRefusesToRunAgainstANewerSchema(t *testing.T) {
	pool := testpg.New(t)
	ctx := context.Background()
	newer := fs(map[string]string{"0001_a.sql": `CREATE TABLE a (id int);`, "0002_b.sql": `CREATE TABLE b (id int);`})
	if _, err := migrate.Up(ctx, pool, newer); err != nil {
		t.Fatal(err)
	}
	older := fs(map[string]string{"0001_a.sql": `CREATE TABLE a (id int);`})
	if _, err := migrate.Up(ctx, pool, older); err == nil || !strings.Contains(err.Error(), "newer schema") {
		t.Errorf("an older binary must refuse a newer schema, got %v", err)
	}
}
