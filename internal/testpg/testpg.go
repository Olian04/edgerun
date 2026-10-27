// Package testpg gives tests a real PostgreSQL. Each test gets its own empty
// database.
//
// By default it starts a private cluster that listens only on a unix socket in
// a temp directory, from the PostgreSQL binaries on this machine. When running
// as root (as in some CI containers) it runs them as the "postgres" OS user,
// because initdb refuses to run as root. To use an existing server instead,
// set EDGERUN_TEST_DATABASE_URL to a superuser connection string; a database
// is created on it per test. Set EDGERUN_REQUIRE_PG=1 to make an unavailable
// PostgreSQL fail tests instead of skipping them (CI should).
//
// Packages must call Main from TestMain so the cluster is stopped:
//
//	func TestMain(m *testing.M) { testpg.Main(m) }
package testpg

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

var srv struct {
	once     sync.Once
	err      error
	adminURL string // superuser DSN for database "postgres"
	dir      string
	binDir   string
	asUser   string // non-empty: run postgres tools as this OS user
	counter  atomic.Int64
	mainRan  bool
}

// Main runs the package's tests and then stops any cluster it started.
func Main(m *testing.M) {
	srv.mainRan = true
	code := m.Run()
	stop()
	os.Exit(code)
}

// New returns a pool connected to a fresh, empty database that is dropped when
// the test ends.
func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool, _ := open(t)
	return pool
}

// DSN returns the connection string of a fresh, empty database, for code that
// takes a DSN rather than a pool.
func DSN(t testing.TB) string {
	t.Helper()
	_, dsn := open(t)
	return dsn
}

func open(t testing.TB) (*pgxpool.Pool, string) {
	t.Helper()
	if !srv.mainRan {
		t.Fatal("testpg: call testpg.Main(m) from TestMain, or the cluster is never stopped")
	}
	srv.once.Do(func() { srv.err = start() })
	if srv.err != nil {
		if os.Getenv("EDGERUN_REQUIRE_PG") == "1" {
			t.Fatalf("PostgreSQL is required but unavailable: %v", srv.err)
		}
		t.Skipf("PostgreSQL unavailable (set EDGERUN_REQUIRE_PG=1 to fail instead): %v", srv.err)
	}

	ctx := context.Background()
	admin, err := pgxpool.New(ctx, srv.adminURL)
	if err != nil {
		t.Fatalf("testpg: %v", err)
	}
	defer admin.Close()
	name := fmt.Sprintf("t_%d_%d", os.Getpid(), srv.counter.Add(1))
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("testpg: creating database: %v", err)
	}
	u, _ := url.Parse(srv.adminURL)
	u.Path = "/" + name
	dsn := u.String()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("testpg: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		a, err := pgxpool.New(ctx, srv.adminURL)
		if err != nil {
			return
		}
		defer a.Close()
		a.Exec(ctx, `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`)
	})
	return pool, dsn
}

func start() error {
	if external := os.Getenv("EDGERUN_TEST_DATABASE_URL"); external != "" {
		srv.adminURL = external
		return nil
	}
	bin, err := binDir()
	if err != nil {
		return err
	}
	srv.binDir = bin

	dir, err := os.MkdirTemp("", "edgerun-pg-")
	if err != nil {
		return err
	}
	srv.dir = dir
	if os.Geteuid() == 0 {
		u, err := user.Lookup("postgres")
		if err != nil {
			return fmt.Errorf("running as root and no %q OS user to run initdb as: %w", "postgres", err)
		}
		uid, _ := strconv.Atoi(u.Uid)
		gid, _ := strconv.Atoi(u.Gid)
		if err := os.Chown(dir, uid, gid); err != nil {
			return err
		}
		srv.asUser = "postgres"
	}

	data := filepath.Join(dir, "data")
	if out, err := run("initdb", "-D", data, "-A", "trust", "-U", "postgres", "--no-sync", "-E", "UTF8"); err != nil {
		return fmt.Errorf("initdb: %v\n%s", err, out)
	}
	opts := strings.Join([]string{
		"-c listen_addresses=''", "-c unix_socket_directories=" + dir,
		"-c fsync=off", "-c synchronous_commit=off", "-c full_page_writes=off",
		"-c max_connections=300", "-c shared_buffers=64MB",
	}, " ")
	if out, err := run("pg_ctl", "-D", data, "-l", filepath.Join(dir, "log"), "-o", opts, "-w", "-t", "60", "start"); err != nil {
		log, _ := os.ReadFile(filepath.Join(dir, "log"))
		return fmt.Errorf("pg_ctl start: %v\n%s\n%s", err, out, log)
	}
	srv.adminURL = (&url.URL{Scheme: "postgres", User: url.User("postgres"), Path: "/postgres",
		RawQuery: url.Values{"host": {dir}}.Encode()}).String()
	return nil
}

func stop() {
	if srv.dir == "" {
		return
	}
	run("pg_ctl", "-D", filepath.Join(srv.dir, "data"), "-m", "immediate", "-w", "stop")
	os.RemoveAll(srv.dir)
}

// run executes a PostgreSQL tool, as the postgres OS user when needed.
func run(tool string, args ...string) (string, error) {
	cmd := exec.Command(filepath.Join(srv.binDir, tool), args...)
	if srv.asUser != "" {
		u, _ := user.Lookup(srv.asUser)
		uid, _ := strconv.Atoi(u.Uid)
		gid, _ := strconv.Atoi(u.Gid)
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}}
		cmd.Env = append(os.Environ(), "HOME="+u.HomeDir)
	}
	cmd.Dir = os.TempDir()
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func binDir() (string, error) {
	if p, err := exec.LookPath("initdb"); err == nil {
		return filepath.Dir(p), nil
	}
	matches, _ := filepath.Glob("/usr/lib/postgresql/*/bin/initdb")
	if len(matches) > 0 {
		return filepath.Dir(matches[len(matches)-1]), nil
	}
	return "", errors.New("no PostgreSQL server binaries found (initdb not on PATH or in /usr/lib/postgresql/*/bin)")
}
