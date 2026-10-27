package store_test

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
)

// --- routes ----------------------------------------------------------------

func TestRoutes(t *testing.T) {
	s := newStore(t)
	fid := mustFn(t, s, "fn")

	r, err := s.CreateRoute(ctx, "API.Example.COM", "/v1/", fid)
	if err != nil {
		t.Fatal(err)
	}
	if r.HostPattern != "api.example.com" || r.PathPrefix != "/v1" || r.FunctionId != fid {
		t.Errorf("not normalised: %+v", r)
	}
	if _, err := s.CreateRoute(ctx, "api.example.com", "/v1", fid); !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("duplicate (normalised) route: %v", err)
	}
	if rr, err := s.CreateRoute(ctx, "*", "/", fid); err != nil || rr.PathPrefix != "/" {
		t.Errorf("root route: %+v %v", rr, err)
	}
	if _, err := s.CreateRoute(ctx, "*.example.com", "/x", fid); err != nil {
		t.Error(err)
	}
	if _, err := s.CreateRoute(ctx, "h", "/x", "00000000-0000-0000-0000-000000000000"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("unknown function: %v", err)
	}
	for _, c := range [][2]string{{"", "/"}, {"Bad Host", "/"}, {"a.*.b", "/"}, {"h", "x"}, {"h", ""}, {"h", "/a//b"}, {"h", "/a?b"}, {"h", "/a b"}} {
		if _, err := s.CreateRoute(ctx, c[0], c[1], fid); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("route %q %q: err = %v, want ErrInvalid", c[0], c[1], err)
		}
	}

	if err := s.DeleteRoute(ctx, r.Id); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteRoute(ctx, r.Id); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("double delete: %v", err)
	}
}

func TestListRoutesPaginatesInOrder(t *testing.T) {
	s := newStore(t)
	fid := mustFn(t, s, "fn")
	for _, hp := range [][2]string{{"b.com", "/z"}, {"a.com", "/b"}, {"a.com", "/a"}, {"*", "/"}, {"b.com", "/a"}} {
		if _, err := s.CreateRoute(ctx, hp[0], hp[1], fid); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	tok := ""
	for {
		rs, next, err := s.ListRoutes(ctx, 2, tok)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rs {
			got = append(got, r.HostPattern+r.PathPrefix)
		}
		if next == "" {
			break
		}
		tok = next
	}
	if want := "[*/ a.com/a a.com/b b.com/a b.com/z]"; fmt.Sprint(got) != want {
		t.Errorf("got %v, want %s", got, want)
	}
}

func TestDeletingAFunctionDeletesItsRoutes(t *testing.T) {
	s := newStore(t)
	fid := mustFn(t, s, "fn")
	s.CreateRoute(ctx, "h.com", "/", fid)
	s.DeleteFunction(ctx, fid)
	if rs, _, _ := s.ListRoutes(ctx, 0, ""); len(rs) != 0 {
		t.Errorf("orphaned routes: %v", rs)
	}
}

// --- environment ----------------------------------------------------------

func TestEnvSecretsAreWriteOnlyAndSealedAtRest(t *testing.T) {
	s := newStore(t)
	fid := mustFn(t, s, "fn")
	if err := s.SetEnv(ctx, fid, &controlv1.EnvEntry{Name: "PLAIN", Value: "visible"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetEnv(ctx, fid, &controlv1.EnvEntry{Name: "TOKEN", Value: "hunter2", Secret: true}); err != nil {
		t.Fatal(err)
	}
	entries, err := s.ListEnv(ctx, fid)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Name != "PLAIN" || entries[0].Value != "visible" ||
		entries[1].Name != "TOKEN" || entries[1].Value != "" || !entries[1].Secret {
		t.Errorf("entries = %v", entries)
	}

	// Replace and delete.
	s.SetEnv(ctx, fid, &controlv1.EnvEntry{Name: "PLAIN", Value: "v2"})
	if e, _ := s.ListEnv(ctx, fid); e[0].Value != "v2" {
		t.Error("replace failed")
	}
	if err := s.DeleteEnv(ctx, fid, "PLAIN"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteEnv(ctx, fid, "PLAIN"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("double delete: %v", err)
	}
}

func TestEnvValidation(t *testing.T) {
	s := newStore(t)
	fid := mustFn(t, s, "fn")
	for name, e := range map[string]*controlv1.EnvEntry{
		"empty name":    {Name: "", Value: "x"},
		"bad chars":     {Name: "A-B", Value: "x"},
		"digit first":   {Name: "1A", Value: "x"},
		"reserved":      {Name: "REQUEST_METHOD", Value: "x"},
		"header prefix": {Name: "HTTP_HOST", Value: "x"},
		"NUL":           {Name: "A", Value: "a\x00b"},
		"too big":       {Name: "A", Value: strings.Repeat("x", 32<<10+1)},
	} {
		if err := s.SetEnv(ctx, fid, e); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
	if err := s.SetEnv(ctx, "00000000-0000-0000-0000-000000000000", &controlv1.EnvEntry{Name: "A"}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("unknown function: %v", err)
	}
	for i := range 64 {
		if err := s.SetEnv(ctx, fid, &controlv1.EnvEntry{Name: fmt.Sprint("V", i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SetEnv(ctx, fid, &controlv1.EnvEntry{Name: "ONE_TOO_MANY"}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("65th variable: %v", err)
	}
	if err := s.SetEnv(ctx, fid, &controlv1.EnvEntry{Name: "V0", Value: "replacing is fine at the limit"}); err != nil {
		t.Errorf("replacing at the limit: %v", err)
	}
}

// --- routing snapshot -----------------------------------------------------

func TestLoadRouting(t *testing.T) {
	s := newStore(t)
	live := mustFn(t, s, "live")
	undeployed := mustFn(t, s, "undeployed")
	v, err := s.CreateVersion(ctx, store.NewVersion{FunctionID: live, Module: []byte("code"), ABI: "wagi-1", Activate: true,
		Limits: &commonv1.Limits{MemoryLimitBytes: 1 << 26}})
	if err != nil {
		t.Fatal(err)
	}
	s.SetEnv(ctx, live, &controlv1.EnvEntry{Name: "PLAIN", Value: "p"})
	s.SetEnv(ctx, live, &controlv1.EnvEntry{Name: "SECRET", Value: "decrypted-for-workers", Secret: true})
	s.CreateRoute(ctx, "a.com", "/", live)
	s.CreateRoute(ctx, "a.com", "/also", live)
	s.CreateRoute(ctx, "b.com", "/", undeployed)

	entries, err := s.LoadRouting(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("entries = %d", len(entries))
	}
	byKey := map[string]store.RouteEntry{}
	for _, e := range entries {
		byKey[e.HostPattern+e.PathPrefix] = e
	}
	e := byKey["a.com/"]
	if e.FunctionName != "live" || e.Version == nil || e.Version.ID != v.Id || e.Version.Digest != v.ArtifactDigest ||
		e.Version.ABI != "wagi-1" || e.Version.Limits.MemoryLimitBytes != 1<<26 {
		t.Errorf("a.com/ = %+v", e)
	}
	env := fmt.Sprint(e.Env[0].Name, "=", e.Env[0].Value, " ", e.Env[1].Name, "=", e.Env[1].Value)
	if env != "PLAIN=p SECRET=decrypted-for-workers" {
		t.Errorf("env = %q", env)
	}
	if len(byKey["a.com/also"].Env) != 2 {
		t.Error("every route of a function shares its environment")
	}
	if byKey["b.com/"].Version != nil {
		t.Error("a function with no active version must have a nil Version")
	}
}

func TestSecretsAreUnreadableAtRestAndBoundToTheirSlot(t *testing.T) {
	// Use the store's pool through a second handle to peek at raw rows.
	pool := poolOf(t)
	s := storeOn(t, pool)
	a, b := mustFn(t, s, "a"), mustFn(t, s, "b")
	for _, f := range []string{a, b} {
		s.SetEnv(ctx, f, &controlv1.EnvEntry{Name: "TOKEN", Value: "super-secret-value-" + f, Secret: true})
		s.CreateRoute(ctx, f+".com", "/", f)
	}
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT value FROM env_vars WHERE function_id = $1`, a).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("super-secret")) {
		t.Fatal("secret stored in the clear")
	}

	// Move a's ciphertext into b's row: it must not open there.
	if _, err := pool.Exec(ctx, `UPDATE env_vars SET value = $1 WHERE function_id = $2`, raw, b); err != nil {
		t.Fatal(err)
	}
	entries, err := s.LoadRouting(ctx)
	if err != nil {
		t.Fatalf("one corrupt secret must not fail the whole snapshot: %v", err)
	}
	for _, e := range entries {
		switch e.FunctionID {
		case a:
			if e.EnvErr != nil || len(e.Env) != 1 || e.Env[0].Value != "super-secret-value-"+a {
				t.Errorf("a is unaffected: %+v", e)
			}
		case b:
			if e.EnvErr == nil {
				t.Error("a swapped ciphertext opened in the wrong slot")
			}
			if len(e.Env) != 0 {
				t.Error("a function with a bad secret must get no environment at all")
			}
		}
	}
}

// --- workers ---------------------------------------------------------------

func workerInfo(abis ...string) *workerv1.WorkerInfo {
	return &workerv1.WorkerInfo{
		Implementation: &commonv1.Implementation{Name: "test", Protocols: []string{"edgerun.worker.v1"}},
		Abis:           abis, MaxConcurrency: 4, Labels: map[string]string{"region": "eu"},
	}
}

func TestWorkerRegistry(t *testing.T) {
	s := newStore(t)
	id1, err := s.RegisterWorker(ctx, "http://10.0.0.1:9000", workerInfo("wagi-1"))
	if err != nil {
		t.Fatal(err)
	}
	if known, err := s.Heartbeat(ctx, id1, workerv1.WorkerState_WORKER_STATE_READY, 2); err != nil || !known {
		t.Fatalf("heartbeat: %v %v", known, err)
	}
	ws, _ := s.ListWorkers(ctx, time.Minute)
	if len(ws) != 1 || ws[0].Id != id1 || ws[0].InFlight != 2 || !ws[0].Alive || ws[0].Info.Labels["region"] != "eu" ||
		ws[0].Info.Implementation.Name != "test" {
		t.Errorf("workers = %v", ws)
	}

	// Registering the same endpoint again replaces the entry with a fresh id.
	id2, err := s.RegisterWorker(ctx, "http://10.0.0.1:9000", workerInfo("wagi-1"))
	if err != nil || id2 == id1 {
		t.Fatalf("re-register: %q %v", id2, err)
	}
	if known, _ := s.Heartbeat(ctx, id1, workerv1.WorkerState_WORKER_STATE_READY, 0); known {
		t.Error("the old id must be unknown after re-registration, so the old process re-registers")
	}
	if ws, _ = s.ListWorkers(ctx, time.Minute); len(ws) != 1 {
		t.Errorf("same endpoint must not create a second entry: %d", len(ws))
	}

	if err := s.DeregisterWorker(ctx, id2); err != nil {
		t.Fatal(err)
	}
	if err := s.DeregisterWorker(ctx, id2); err != nil {
		t.Errorf("deregister must be idempotent: %v", err)
	}
	if err := s.DeregisterWorker(ctx, "not-a-uuid"); err != nil {
		t.Errorf("deregister of garbage: %v", err)
	}
	if known, err := s.Heartbeat(ctx, "not-a-uuid", workerv1.WorkerState_WORKER_STATE_READY, 0); known || err != nil {
		t.Errorf("heartbeat of garbage: %v %v", known, err)
	}
}

func TestRegisterWorkerValidation(t *testing.T) {
	s := newStore(t)
	for name, c := range map[string]struct {
		ep   string
		info *workerv1.WorkerInfo
	}{
		"no scheme":      {"10.0.0.1:9000", workerInfo("wagi-1")},
		"ftp":            {"ftp://h", workerInfo("wagi-1")},
		"nil info":       {"http://h", nil},
		"no concurrency": {"http://h", &workerv1.WorkerInfo{Abis: []string{"wagi-1"}}},
		"no abis":        {"http://h", &workerv1.WorkerInfo{MaxConcurrency: 1}},
	} {
		if _, err := s.RegisterWorker(ctx, c.ep, c.info); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestReadyWorkersFiltersByStateAndLiveness(t *testing.T) {
	s := newStore(t)
	ready, _ := s.RegisterWorker(ctx, "http://ready:1", workerInfo("wagi-1", "wagi-2"))
	draining, _ := s.RegisterWorker(ctx, "http://draining:1", workerInfo("wagi-1"))
	s.Heartbeat(ctx, draining, workerv1.WorkerState_WORKER_STATE_DRAINING, 0)
	s.RegisterWorker(ctx, "http://stale:1", workerInfo("wagi-1"))

	// Let every worker go stale, then keep only `ready` alive.
	time.Sleep(500 * time.Millisecond)
	s.Heartbeat(ctx, ready, workerv1.WorkerState_WORKER_STATE_READY, 1)

	got, err := s.ReadyWorkers(ctx, 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != ready || got[0].Endpoint != "http://ready:1" || got[0].InFlight != 1 ||
		got[0].MaxConcurrency != 4 || !got[0].Supports("wagi-1") || !got[0].Supports("wagi-2") || got[0].Supports("v8-1") {
		t.Errorf("ready = %+v, want only the live READY worker with both ABIs", got)
	}
	if all, _ := s.ReadyWorkers(ctx, time.Minute); len(all) != 2 {
		t.Errorf("with a generous window both READY workers are listed, draining excluded: %d", len(all))
	}

	ws, _ := s.ListWorkers(ctx, 300*time.Millisecond)
	alive := 0
	for _, w := range ws {
		if w.Alive {
			alive++
		}
	}
	if alive != 1 || len(ws) != 3 {
		t.Errorf("ListWorkers: %d alive of %d, want 1 of 3", alive, len(ws))
	}
	if n, err := s.PruneWorkers(ctx, 300*time.Millisecond); err != nil || n != 2 {
		t.Errorf("PruneWorkers = %d, %v; want the 2 stale ones", n, err)
	}
}

// --- invocations -----------------------------------------------------------

func inv(id string, at time.Time, fn string) *controlv1.Invocation {
	return &controlv1.Invocation{Id: id, RequestId: "req-" + id, FunctionId: fn, Method: "GET", Path: "/p", Status: 200,
		DurationMicros: 1234, PeakMemoryBytes: 1 << 20, StartTime: timestamppb.New(at)}
}

func TestInvocationsStoredListedAndPaged(t *testing.T) {
	s := newStore(t)
	f1, f2 := mustFn(t, s, "one"), mustFn(t, s, "two")
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	id := func(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", n) }
	var batch []*controlv1.Invocation
	for i := range 7 {
		fn := f1
		if i%2 == 1 {
			fn = f2
		}
		batch = append(batch, inv(id(i), base.Add(time.Duration(i)*time.Second), fn))
	}
	// Two records at the same instant: ordering must still be total.
	batch = append(batch, inv(id(100), base.Add(6*time.Second), f1))
	if err := s.InsertInvocations(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertInvocations(ctx, batch); err != nil {
		t.Fatalf("re-inserting the same ids must be harmless: %v", err)
	}

	var got []string
	tok := ""
	for {
		vs, next, err := s.ListInvocations(ctx, "", 3, tok)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range vs {
			got = append(got, v.Id[len(v.Id)-3:])
		}
		if next == "" {
			break
		}
		tok = next
	}
	if want := "[100 006 005 004 003 002 001 000]"; fmt.Sprint(got) != want {
		t.Errorf("got %v, want %s (newest first, ties broken by id, no duplicates across pages)", got, want)
	}

	vs, _, _ := s.ListInvocations(ctx, f2, 0, "")
	if len(vs) != 3 {
		t.Errorf("filter by function: %d records", len(vs))
	}
	for _, v := range vs {
		if v.FunctionId != f2 || v.Status != 200 || v.DurationMicros != 1234 || v.PeakMemoryBytes != 1<<20 || v.Method != "GET" {
			t.Errorf("record = %+v", v)
		}
	}
	if vs, _, err := s.ListInvocations(ctx, "not-a-uuid", 0, ""); err != nil || len(vs) != 0 {
		t.Errorf("unknown function: %v %v", vs, err)
	}
	for _, bad := range []string{"%%%", "AAAA"} {
		if _, _, err := s.ListInvocations(ctx, "", 0, bad); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("forged token %q: %v", bad, err)
		}
	}
}

func TestInvocationsOutliveTheirFunctionAndNullableIDs(t *testing.T) {
	s := newStore(t)
	fid := mustFn(t, s, "gone")
	now := time.Now()
	s.InsertInvocations(ctx, []*controlv1.Invocation{
		inv("00000000-0000-0000-0000-000000000001", now, fid),
		inv("00000000-0000-0000-0000-000000000002", now, ""), // e.g. a request no route matched
	})
	s.DeleteFunction(ctx, fid)
	vs, _, err := s.ListInvocations(ctx, "", 0, "")
	if err != nil || len(vs) != 2 {
		t.Fatalf("records must outlive their function: %v %v", vs, err)
	}
}

func TestInvocationFieldsAreTruncatedNotRejected(t *testing.T) {
	s := newStore(t)
	v := inv("00000000-0000-0000-0000-000000000001", time.Now(), "")
	v.Path = "/" + strings.Repeat("p", 10_000)
	v.Stderr = append(bytes.Repeat([]byte("old"), 30_000), []byte("THE-TAIL")...)
	if err := s.InsertInvocations(ctx, []*controlv1.Invocation{v}); err != nil {
		t.Fatal(err)
	}
	vs, _, _ := s.ListInvocations(ctx, "", 0, "")
	if len(vs[0].Path) != 2048 || len(vs[0].Stderr) != 64<<10 || !bytes.HasSuffix(vs[0].Stderr, []byte("THE-TAIL")) {
		t.Errorf("path %d bytes, stderr %d bytes (must keep the tail)", len(vs[0].Path), len(vs[0].Stderr))
	}
}

func TestInvocationRetention(t *testing.T) {
	s := newStore(t)
	base := time.Now().Add(-48 * time.Hour)
	var batch []*controlv1.Invocation
	for i := range 10 {
		batch = append(batch, inv(fmt.Sprintf("00000000-0000-0000-0000-%012d", i), base.Add(time.Duration(i)*time.Hour), ""))
	}
	s.InsertInvocations(ctx, batch)
	cutoff := base.Add(5*time.Hour + time.Minute) // records 0..5 are older
	n, err := s.DeleteInvocationsBefore(ctx, cutoff, 4)
	if err != nil || n != 4 {
		t.Fatalf("first batch: %d %v (limit 4)", n, err)
	}
	n, _ = s.DeleteInvocationsBefore(ctx, cutoff, 4)
	if n != 2 {
		t.Errorf("second batch: %d, want the remaining 2", n)
	}
	if vs, _, _ := s.ListInvocations(ctx, "", 0, ""); len(vs) != 4 {
		t.Errorf("%d records left, want 4", len(vs))
	}
}
