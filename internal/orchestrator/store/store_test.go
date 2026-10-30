package store_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	"github.com/Olian04/edgerun/internal/orchestrator/migrate"
	"github.com/Olian04/edgerun/internal/orchestrator/secrets"
	"github.com/Olian04/edgerun/internal/orchestrator/store"
	"github.com/Olian04/edgerun/internal/testpg"
	"github.com/Olian04/edgerun/migrations"
)

func TestMain(m *testing.M) { testpg.Main(m) }

var ctx = context.Background()

func newStore(t *testing.T) *store.Store {
	t.Helper()
	pool := testpg.New(t)
	if _, err := migrate.Up(ctx, pool, migrations.FS); err != nil {
		t.Fatal(err)
	}
	box, err := secrets.NewBox(bytes.Repeat([]byte{1}, secrets.KeySize))
	if err != nil {
		t.Fatal(err)
	}
	return store.New(pool, box)
}

func mustFn(t *testing.T, s *store.Store, name string) string {
	t.Helper()
	f, err := s.CreateFunction(ctx, name, nil)
	if err != nil {
		t.Fatal(err)
	}
	return f.Id
}

func mustVersion(t *testing.T, s *store.Store, fid, content string, activate bool) string {
	t.Helper()
	v, err := s.CreateVersion(ctx, store.NewVersion{FunctionID: fid, Module: []byte(content), ABI: "wagi-1", Activate: activate})
	if err != nil {
		t.Fatal(err)
	}
	return v.Id
}

func TestFunctionLifecycle(t *testing.T) {
	s := newStore(t)
	f, err := s.CreateFunction(ctx, "hello", nil)
	if err != nil || f.Name != "hello" || f.Id == "" || f.ActiveVersionId != "" || f.CreateTime == nil {
		t.Fatalf("create: %+v %v", f, err)
	}
	got, err := s.GetFunction(ctx, f.Id)
	if err != nil || got.Id != f.Id {
		t.Fatalf("get: %v", err)
	}
	if _, err := s.CreateFunction(ctx, "hello", nil); !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("duplicate name: %v", err)
	}
	if err := s.DeleteFunction(ctx, f.Id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetFunction(ctx, f.Id); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("get after delete: %v", err)
	}
	if err := s.DeleteFunction(ctx, f.Id); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("double delete: %v", err)
	}
}

func TestFunctionNameValidation(t *testing.T) {
	s := newStore(t)
	for _, bad := range []string{"", "Hello", "-a", "a-", "a_b", "a b", "a.b", fmt.Sprintf("%064d", 1)} {
		if _, err := s.CreateFunction(ctx, bad, nil); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("name %q: err = %v, want ErrInvalid", bad, err)
		}
	}
	for _, good := range []string{"a", "a1", "my-func-2", fmt.Sprintf("%063d", 1)} {
		if _, err := s.CreateFunction(ctx, good, nil); err != nil {
			t.Errorf("name %q: %v", good, err)
		}
	}
}

func TestNonUUIDIDsAreNotFoundNotErrors(t *testing.T) {
	s := newStore(t)
	for name, f := range map[string]func() error{
		"get":      func() error { _, err := s.GetFunction(ctx, "nope"); return err },
		"delete":   func() error { return s.DeleteFunction(ctx, "nope") },
		"activate": func() error { _, err := s.ActivateVersion(ctx, "nope", "nope"); return err },
		"versions": func() error { _, _, err := s.ListVersions(ctx, "nope", 0, "", store.LabelFilter{}); return err },
	} {
		if err := f(); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestListFunctionsPaginates(t *testing.T) {
	s := newStore(t)
	for _, n := range []string{"e", "b", "d", "a", "c"} {
		mustFn(t, s, n)
	}
	var seen []string
	token := ""
	for pages := 0; ; pages++ {
		fs, next, err := s.ListFunctions(ctx, 2, token, store.LabelFilter{}, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range fs {
			seen = append(seen, f.Name)
		}
		if next == "" {
			if pages != 2 {
				t.Errorf("expected 3 pages, got %d", pages+1)
			}
			break
		}
		token = next
	}
	if fmt.Sprint(seen) != "[a b c d e]" {
		t.Errorf("order/coverage: %v", seen)
	}
	if _, _, err := s.ListFunctions(ctx, 2, "garbage!!", store.LabelFilter{}, ""); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("tampered token: %v", err)
	}
}

func TestVersionsAreNumberedAndImmutable(t *testing.T) {
	s := newStore(t)
	fid := mustFn(t, s, "fn")
	v1, err := s.CreateVersion(ctx, store.NewVersion{FunctionID: fid, Module: []byte("one"), ABI: "wagi-1",
		Limits: &commonv1.Limits{MemoryLimitBytes: 1 << 26, MaxResponseBytes: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	if v1.Number != 1 || v1.ArtifactDigest != store.Digest([]byte("one")) || v1.SizeBytes != 3 ||
		v1.Limits.MemoryLimitBytes != 1<<26 || v1.Limits.MaxResponseBytes != 4096 || v1.Abi != "wagi-1" {
		t.Errorf("v1 = %+v", v1)
	}
	v2, _ := s.CreateVersion(ctx, store.NewVersion{FunctionID: fid, Module: []byte("two"), ABI: "wagi-1"})
	if v2.Number != 2 {
		t.Errorf("v2 number = %d", v2.Number)
	}
	f, _ := s.GetFunction(ctx, fid)
	if f.ActiveVersionId != "" {
		t.Error("creating a version must not deploy it unless asked")
	}

	// Identical module bytes share one artifact.
	v3, err := s.CreateVersion(ctx, store.NewVersion{FunctionID: fid, Module: []byte("one"), ABI: "wagi-1", Activate: true})
	if err != nil || v3.ArtifactDigest != v1.ArtifactDigest || v3.Number != 3 {
		t.Fatalf("v3: %+v %v", v3, err)
	}
	if f, _ = s.GetFunction(ctx, fid); f.ActiveVersionId != v3.Id {
		t.Error("Activate:true must deploy in the same transaction")
	}
	b, err := s.GetArtifact(ctx, v1.ArtifactDigest)
	if err != nil || string(b) != "one" {
		t.Errorf("artifact: %q %v", b, err)
	}
}

func TestCreateVersionValidation(t *testing.T) {
	s := newStore(t)
	fid := mustFn(t, s, "fn")
	for name, nv := range map[string]store.NewVersion{
		"empty module": {FunctionID: fid, ABI: "wagi-1"},
		"no abi":       {FunctionID: fid, Module: []byte("x")},
		"huge limit":   {FunctionID: fid, Module: []byte("x"), ABI: "a", Limits: &commonv1.Limits{MemoryLimitBytes: 1 << 63}},
	} {
		if _, err := s.CreateVersion(ctx, nv); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := s.CreateVersion(ctx, store.NewVersion{FunctionID: "00000000-0000-0000-0000-000000000000", Module: []byte("x"), ABI: "a"}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("unknown function: %v", err)
	}
}

func TestConcurrentVersionNumbersAreGaplessAndUnique(t *testing.T) {
	s := newStore(t)
	fid := mustFn(t, s, "fn")
	const n = 12
	var wg sync.WaitGroup
	nums := make(chan int64, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := s.CreateVersion(ctx, store.NewVersion{FunctionID: fid, Module: []byte(fmt.Sprint("m", i)), ABI: "wagi-1"})
			if err != nil {
				t.Error(err)
				return
			}
			nums <- v.Number
		}()
	}
	wg.Wait()
	close(nums)
	seen := map[int64]bool{}
	for k := range nums {
		if seen[k] {
			t.Errorf("version number %d assigned twice", k)
		}
		seen[k] = true
	}
	for k := int64(1); k <= n; k++ {
		if !seen[k] {
			t.Errorf("version number %d missing: numbers must be gapless", k)
		}
	}
}

func TestListVersionsNewestFirst(t *testing.T) {
	s := newStore(t)
	fid := mustFn(t, s, "fn")
	for i := range 5 {
		mustVersion(t, s, fid, fmt.Sprint("m", i), false)
	}
	var got []int64
	tok := ""
	for {
		vs, next, err := s.ListVersions(ctx, fid, 2, tok, store.LabelFilter{})
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range vs {
			got = append(got, v.Number)
		}
		if next == "" {
			break
		}
		tok = next
	}
	if fmt.Sprint(got) != "[5 4 3 2 1]" {
		t.Errorf("got %v", got)
	}
}

func TestActivateAndRollback(t *testing.T) {
	s := newStore(t)
	fid := mustFn(t, s, "fn")
	v1 := mustVersion(t, s, fid, "one", true)
	v2 := mustVersion(t, s, fid, "two", true)
	if f, _ := s.GetFunction(ctx, fid); f.ActiveVersionId != v2 {
		t.Fatal("v2 should be active")
	}
	f, err := s.ActivateVersion(ctx, fid, v1) // rollback is just another pointer move
	if err != nil || f.ActiveVersionId != v1 {
		t.Fatalf("rollback: %+v %v", f, err)
	}
}

func TestCannotActivateAnotherFunctionsVersion(t *testing.T) {
	s := newStore(t)
	a, b := mustFn(t, s, "a"), mustFn(t, s, "b")
	vb := mustVersion(t, s, b, "b-code", false)
	if _, err := s.ActivateVersion(ctx, a, vb); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("activating b's version on a: %v (the schema must forbid it)", err)
	}
	if f, _ := s.GetFunction(ctx, a); f.ActiveVersionId != "" {
		t.Error("a must still have no active version")
	}
	if _, err := s.ActivateVersion(ctx, "00000000-0000-0000-0000-000000000000", vb); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("unknown function: %v", err)
	}
}

func TestDeleteFunctionCascadesButKeepsArtifacts(t *testing.T) {
	s := newStore(t)
	fid := mustFn(t, s, "fn")
	v := mustVersion(t, s, fid, "code", true)
	vs, _, _ := s.ListVersions(ctx, fid, 0, "", store.LabelFilter{})
	digest := vs[0].ArtifactDigest
	_ = v
	if err := s.DeleteFunction(ctx, fid); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetArtifact(ctx, digest); err != nil {
		t.Errorf("artifacts are content-addressed and may be shared; deleting a function must not delete them: %v", err)
	}
}

func TestGetArtifactUnknown(t *testing.T) {
	s := newStore(t)
	if _, err := s.GetArtifact(ctx, "sha256:"+fmt.Sprintf("%064d", 0)); !errors.Is(err, store.ErrNotFound) {
		t.Error(err)
	}
	if _, err := s.GetArtifact(ctx, "garbage"); !errors.Is(err, store.ErrNotFound) {
		t.Error(err)
	}
}

// poolOf and storeOn let a test look at raw rows next to the store.
func poolOf(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testpg.New(t)
	if _, err := migrate.Up(ctx, pool, migrations.FS); err != nil {
		t.Fatal(err)
	}
	return pool
}

func storeOn(t *testing.T, pool *pgxpool.Pool) *store.Store {
	t.Helper()
	box, err := secrets.NewBox(bytes.Repeat([]byte{1}, secrets.KeySize))
	if err != nil {
		t.Fatal(err)
	}
	return store.New(pool, box)
}
