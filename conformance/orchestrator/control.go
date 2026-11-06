package orchestratorconf

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

const unknownID = "00000000-0000-4000-8000-000000000000"

func (h *harness) controlFunctions(t *testing.T) {
	ctx, cancel := ctxT(settle)
	defer cancel()

	name := h.name("cf")
	fn := h.createFunction(t, name)
	if fn.GetId() == "" || fn.GetName() != name {
		t.Fatalf("CreateFunction returned %+v", fn)
	}
	if fn.GetActiveVersionId() != "" {
		t.Error("a new function has no active version")
	}
	if fn.GetCreateTime() == nil || fn.GetCreateTime().AsTime().IsZero() {
		t.Error("create_time is not set")
	}

	got, err := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fn.GetId()}))
	if err != nil || got.Msg.GetFunction().GetName() != name || got.Msg.GetFunction().GetId() != fn.GetId() {
		t.Errorf("GetFunction = %v, %v", got, err)
	}

	t.Run("names", func(t *testing.T) {
		long := strings.Repeat("a", 63)
		for _, bad := range []string{"", "Upper", "-lead", "trail-", "has space", "under_score", "dot.ted", strings.Repeat("a", 64)} {
			_, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: bad}))
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("name %q: %v, want invalid_argument", bad, err)
			}
		}
		// The longest legal name; unique per run because a clash is a different error.
		legal := (h.run + long)[:63]
		h.createFunction(t, legal)
		h.createFunction(t, "z"+h.run+"-1")
	})

	t.Run("names are unique", func(t *testing.T) {
		_, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: name}))
		check.WantCode(t, err, connect.CodeAlreadyExists)
	})

	t.Run("unknown functions are not found", func(t *testing.T) {
		for _, id := range []string{unknownID, "not-a-uuid", ""} {
			_, err := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: id}))
			check.WantCode(t, err, connect.CodeNotFound)
			_, err = h.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: id}))
			check.WantCode(t, err, connect.CodeNotFound)
		}
	})

	t.Run("deletion", func(t *testing.T) {
		d := h.createFunction(t, h.name("cf-del"))
		if _, err := h.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: d.GetId()})); err != nil {
			t.Fatal(err)
		}
		_, err := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: d.GetId()}))
		check.WantCode(t, err, connect.CodeNotFound)
		_, err = h.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: d.GetId()}))
		check.WantCode(t, err, connect.CodeNotFound) // already gone
		// The name is free again.
		h.createFunction(t, d.GetName())
	})
}

func (h *harness) controlVersions(t *testing.T) {
	ctx, cancel := ctxT(60 * settle)
	defer cancel()
	fn := h.createFunction(t, h.name("cv"))
	other := h.createFunction(t, h.name("cv-other"))

	t.Run("validation", func(t *testing.T) {
		mk := func(id string, module []byte, abi string) error {
			_, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: id, Module: module, Abi: abi}))
			return err
		}
		check.WantCode(t, mk(fn.GetId(), nil, "wagi-1"), connect.CodeInvalidArgument)
		for _, abi := range []string{"", "Upper Case", "-x", strings.Repeat("a", 33)} {
			check.WantCode(t, mk(fn.GetId(), []byte("m"), abi), connect.CodeInvalidArgument)
		}
		check.WantCode(t, mk(unknownID, []byte("m"), "wagi-1"), connect.CodeNotFound)
		check.WantCode(t, mk("not-a-uuid", []byte("m"), "wagi-1"), connect.CodeNotFound)

		d, err := h.insp.Describe(ctx, connect.NewRequest(&controlv1.DescribeRequest{}))
		if err != nil {
			t.Fatal(err)
		}
		over := make([]byte, d.Msg.GetMaxModuleBytes()+1)
		check.WantCode(t, mk(fn.GetId(), over, "wagi-1"), connect.CodeInvalidArgument)
	})

	t.Run("numbering, content and newest first", func(t *testing.T) {
		limits := &commonv1.Limits{MemoryLimitBytes: 64 << 20, MaxResponseBytes: 1 << 20}
		m1, m2 := h.module("v1"), h.module("v2")
		v1 := h.createVersion(t, fn.GetId(), "wagi-1", m1, limits, false)
		v2 := h.createVersion(t, fn.GetId(), "wagi-1", m2, nil, false)
		v3 := h.createVersion(t, fn.GetId(), "wagi-1", m1, nil, false) // the same bytes again
		for i, v := range []*controlv1.FunctionVersion{v1, v2, v3} {
			if v.GetNumber() != int64(i+1) {
				t.Errorf("version %d has number %d", i+1, v.GetNumber())
			}
			if v.GetFunctionId() != fn.GetId() || v.GetAbi() != "wagi-1" || v.GetId() == "" || v.GetCreateTime() == nil {
				t.Errorf("version %d is incomplete: %+v", i+1, v)
			}
		}
		if v1.GetArtifactDigest() != sha(m1) || v2.GetArtifactDigest() != sha(m2) {
			t.Errorf("digests %s, %s: want the sha256 content addresses %s, %s", v1.GetArtifactDigest(), v2.GetArtifactDigest(), sha(m1), sha(m2))
		}
		if v3.GetArtifactDigest() != v1.GetArtifactDigest() {
			t.Error("identical bytes must share one digest")
		}
		if v1.GetSizeBytes() != uint64(len(m1)) {
			t.Errorf("size_bytes = %d, want %d", v1.GetSizeBytes(), len(m1))
		}
		if v1.GetLimits().GetMemoryLimitBytes() != 64<<20 || v1.GetLimits().GetMaxResponseBytes() != 1<<20 {
			t.Errorf("limits not kept: %v", v1.GetLimits())
		}

		list, err := h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: fn.GetId()}))
		if err != nil {
			t.Fatal(err)
		}
		var nums []int64
		for _, v := range list.Msg.GetVersions() {
			nums = append(nums, v.GetNumber())
		}
		if fmt.Sprint(nums) != "[3 2 1]" {
			t.Errorf("ListVersions order = %v, want newest first [3 2 1]", nums)
		}
		g, _ := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fn.GetId()}))
		if g.Msg.GetFunction().GetActiveVersionId() != "" {
			t.Error("creating a version without activate must not deploy it")
		}

		// Activation is deploy and rollback.
		for _, v := range []*controlv1.FunctionVersion{v2, v1, v3} {
			r, err := h.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: fn.GetId(), VersionId: v.GetId()}))
			if err != nil || r.Msg.GetFunction().GetActiveVersionId() != v.GetId() {
				t.Fatalf("ActivateVersion(%d) = %v, %v", v.GetNumber(), r, err)
			}
			g, _ := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fn.GetId()}))
			if g.Msg.GetFunction().GetActiveVersionId() != v.GetId() {
				t.Errorf("after activating %d the function reports %q", v.GetNumber(), g.Msg.GetFunction().GetActiveVersionId())
			}
		}
		v4 := h.createVersion(t, fn.GetId(), "wagi-1", h.module("v4"), nil, true)
		g, _ = h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fn.GetId()}))
		if g.Msg.GetFunction().GetActiveVersionId() != v4.GetId() {
			t.Error("CreateVersion with activate must deploy the new version")
		}

		// A version belongs to its function.
		foreign := h.createVersion(t, other.GetId(), "wagi-1", h.module("foreign"), nil, false)
		_, err = h.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: fn.GetId(), VersionId: foreign.GetId()}))
		check.WantCode(t, err, connect.CodeNotFound)
		for _, id := range []string{unknownID, "not-a-uuid"} {
			_, err = h.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: fn.GetId(), VersionId: id}))
			check.WantCode(t, err, connect.CodeNotFound)
		}
		_, err = h.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: unknownID, VersionId: v1.GetId()}))
		check.WantCode(t, err, connect.CodeNotFound)
		g, _ = h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fn.GetId()}))
		if g.Msg.GetFunction().GetActiveVersionId() != v4.GetId() {
			t.Error("a refused activation must change nothing")
		}
	})

	t.Run("unknown function", func(t *testing.T) {
		_, err := h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: unknownID}))
		check.WantCode(t, err, connect.CodeNotFound)
	})
}

func (h *harness) controlEnvironment(t *testing.T) {
	ctx, cancel := ctxT(2 * settle)
	defer cancel()
	fn := h.createFunction(t, h.name("ce"))
	list := func() []*controlv1.EnvEntry {
		t.Helper()
		r, err := h.fn.ListEnv(ctx, connect.NewRequest(&controlv1.ListEnvRequest{FunctionId: fn.GetId()}))
		if err != nil {
			t.Fatal(err)
		}
		return r.Msg.GetEntries()
	}
	set := func(name, value string, secret bool) error {
		_, err := h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{
			FunctionId: fn.GetId(), Entry: &controlv1.EnvEntry{Name: name, Value: value, Secret: secret}}))
		return err
	}

	t.Run("plain and secret values", func(t *testing.T) {
		h.setEnv(t, fn.GetId(), "ZETA", "last", false)
		h.setEnv(t, fn.GetId(), "ALPHA", "first", false)
		h.setEnv(t, fn.GetId(), "TOKEN", "hunter2", true)
		es := list()
		var names []string
		for _, e := range es {
			names = append(names, e.GetName())
		}
		if fmt.Sprint(names) != "[ALPHA TOKEN ZETA]" {
			t.Fatalf("ListEnv order = %v, want by name", names)
		}
		if es[0].GetValue() != "first" || es[0].GetSecret() {
			t.Errorf("a plain variable must come back as set: %+v", es[0])
		}
		if es[1].GetValue() != "" || !es[1].GetSecret() {
			t.Errorf("a secret must come back empty and marked secret: %+v", es[1])
		}

		// SetEnv replaces, and may turn a secret into a plain value and back.
		h.setEnv(t, fn.GetId(), "ALPHA", "changed", false)
		h.setEnv(t, fn.GetId(), "TOKEN", "visible now", false)
		es = list()
		if len(es) != 3 || es[0].GetValue() != "changed" || es[1].GetValue() != "visible now" || es[1].GetSecret() {
			t.Errorf("replacing a variable: %+v", es)
		}
		h.setEnv(t, fn.GetId(), "TOKEN", "hidden again", true)
		if es = list(); es[1].GetValue() != "" || !es[1].GetSecret() {
			t.Errorf("a variable turned secret must be hidden: %+v", es[1])
		}
	})

	t.Run("validation", func(t *testing.T) {
		for _, bad := range []string{"", "1A", "A-B", "A B", "A.B", "PATH_INFO", "REQUEST_METHOD", "QUERY_STRING", "GATEWAY_INTERFACE", "CONTENT_TYPE", "HTTP_HOST", "HTTP_X_ANYTHING"} {
			check.WantCode(t, set(bad, "v", false), connect.CodeInvalidArgument)
		}
		check.WantCode(t, set("OK", "has\x00nul", false), connect.CodeInvalidArgument)
		check.WantCode(t, set("OK", strings.Repeat("x", 32<<10+1), false), connect.CodeInvalidArgument)
		if err := set("OK", strings.Repeat("x", 32<<10), false); err != nil {
			t.Errorf("a 32 KiB value is allowed: %v", err)
		}
		_, err := h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: fn.GetId()}))
		check.WantCode(t, err, connect.CodeInvalidArgument)
		check.WantCode(t, func() error {
			_, err := h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: unknownID, Entry: &controlv1.EnvEntry{Name: "A", Value: "b"}}))
			return err
		}(), connect.CodeNotFound)
	})

	t.Run("at most 64 variables", func(t *testing.T) {
		many := h.createFunction(t, h.name("ce-many"))
		for i := 0; i < 64; i++ {
			h.setEnv(t, many.GetId(), fmt.Sprintf("V%02d", i), "x", false)
		}
		_, err := h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: many.GetId(), Entry: &controlv1.EnvEntry{Name: "ONE_TOO_MANY", Value: "x"}}))
		check.WantCode(t, err, connect.CodeInvalidArgument)
		h.setEnv(t, many.GetId(), "V00", "replaced", false) // replacing is not adding
	})

	t.Run("deletion", func(t *testing.T) {
		del := func(name string) error {
			_, err := h.fn.DeleteEnv(ctx, connect.NewRequest(&controlv1.DeleteEnvRequest{FunctionId: fn.GetId(), Name: name}))
			return err
		}
		if err := del("ZETA"); err != nil {
			t.Fatal(err)
		}
		check.WantCode(t, del("ZETA"), connect.CodeNotFound)
		check.WantCode(t, del("NEVER_SET"), connect.CodeNotFound)
		for _, e := range list() {
			if e.GetName() == "ZETA" {
				t.Error("a deleted variable is still listed")
			}
		}
		_, err := h.fn.ListEnv(ctx, connect.NewRequest(&controlv1.ListEnvRequest{FunctionId: unknownID}))
		check.WantCode(t, err, connect.CodeNotFound)
	})
}

func (h *harness) controlRoutes(t *testing.T) {
	ctx, cancel := ctxT(settle)
	defer cancel()
	fn := h.createFunction(t, h.name("cr"))
	create := func(host, prefix, fnID string) (*controlv1.Route, error) {
		r, err := h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: prefix, FunctionId: fnID}))
		if err != nil {
			return nil, err
		}
		return r.Msg.GetRoute(), nil
	}
	base := h.host()

	t.Run("normalisation", func(t *testing.T) {
		for _, c := range []struct{ host, prefix, wantHost, wantPrefix string }{
			{strings.ToUpper(base), "/api/", base, "/api"},
			{"a." + base, "/", "a." + base, "/"},
			{"*." + base, "/x/y/", "*." + base, "/x/y"},
		} {
			r, err := create(c.host, c.prefix, fn.GetId())
			if err != nil {
				t.Fatalf("CreateRoute(%s%s): %v", c.host, c.prefix, err)
			}
			if r.GetHostPattern() != c.wantHost || r.GetPathPrefix() != c.wantPrefix || r.GetFunctionId() != fn.GetId() || r.GetId() == "" {
				t.Errorf("CreateRoute(%s%s) = %v, want %s%s", c.host, c.prefix, r, c.wantHost, c.wantPrefix)
			}
		}
	})

	t.Run("a route is unique after normalisation", func(t *testing.T) {
		_, err := create(base, "/api", fn.GetId())
		check.WantCode(t, err, connect.CodeAlreadyExists)
		_, err = create(strings.ToUpper(base), "/api/", fn.GetId())
		check.WantCode(t, err, connect.CodeAlreadyExists)
	})

	t.Run("validation", func(t *testing.T) {
		for _, prefix := range []string{"", "api", "/a//b", "/a?x=1", "/a#f", "/a b"} {
			_, err := create(h.host(), prefix, fn.GetId())
			check.WantCode(t, err, connect.CodeInvalidArgument)
		}
		for _, host := range []string{"", "has space", "*foo.test", "a.*.test"} {
			_, err := create(host, "/", fn.GetId())
			check.WantCode(t, err, connect.CodeInvalidArgument)
		}
		_, err := create(h.host(), "/", unknownID)
		check.WantCode(t, err, connect.CodeNotFound)
		_, err = create(h.host(), "/", "not-a-uuid")
		check.WantCode(t, err, connect.CodeNotFound)
	})

	t.Run("list and delete", func(t *testing.T) {
		r, err := create(h.host(), "/listed", fn.GetId())
		if err != nil {
			t.Fatal(err)
		}
		if !h.hasRoute(t, r.GetId()) {
			t.Fatal("a created route is not listed")
		}
		if _, err := h.rt.DeleteRoute(ctx, connect.NewRequest(&controlv1.DeleteRouteRequest{RouteId: r.GetId()})); err != nil {
			t.Fatal(err)
		}
		if h.hasRoute(t, r.GetId()) {
			t.Error("a deleted route is still listed")
		}
		for _, id := range []string{r.GetId(), unknownID, "not-a-uuid"} {
			_, err := h.rt.DeleteRoute(ctx, connect.NewRequest(&controlv1.DeleteRouteRequest{RouteId: id}))
			check.WantCode(t, err, connect.CodeNotFound)
		}
	})
}

// hasRoute pages through every route looking for id.
func (h *harness) hasRoute(t testing.TB, id string) bool {
	t.Helper()
	for _, r := range h.allRoutes(t, 0) {
		if r.GetId() == id {
			return true
		}
	}
	return false
}

func (h *harness) allRoutes(t testing.TB, pageSize uint32) []*controlv1.Route {
	t.Helper()
	ctx, cancel := ctxT(2 * settle)
	defer cancel()
	var out []*controlv1.Route
	tok := ""
	for i := 0; i < 10000; i++ {
		r, err := h.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{PageSize: pageSize, PageToken: tok}))
		if err != nil {
			t.Fatalf("ListRoutes: %v", err)
		}
		out = append(out, r.Msg.GetRoutes()...)
		if tok = r.Msg.GetNextPageToken(); tok == "" {
			return out
		}
	}
	t.Fatal("ListRoutes never ended")
	return nil
}

// controlPagination checks that paging yields every item exactly once, in the
// documented order, whatever the page size.
func (h *harness) controlPagination(t *testing.T) {
	ctx, cancel := ctxT(2 * settle)
	defer cancel()

	t.Run("functions by name", func(t *testing.T) {
		prefix := "pg-" + h.run + "-"
		var want []string
		for _, s := range []string{"e", "b", "d", "a", "c"} {
			want = append(want, h.createFunction(t, prefix+s).GetName())
		}
		sort.Strings(want)
		for _, size := range []uint32{0, 1, 2, 5, 100000} {
			var all, mine []string
			tok := ""
			for pages := 0; ; pages++ {
				r, err := h.fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{PageSize: size, PageToken: tok}))
				if err != nil {
					t.Fatalf("size %d: %v", size, err)
				}
				if size == 1 && len(r.Msg.GetFunctions()) > 1 {
					t.Fatalf("a page of %d held %d items", size, len(r.Msg.GetFunctions()))
				}
				for _, f := range r.Msg.GetFunctions() {
					all = append(all, f.GetName())
					if strings.HasPrefix(f.GetName(), prefix) {
						mine = append(mine, f.GetName())
					}
				}
				if tok = r.Msg.GetNextPageToken(); tok == "" {
					break
				}
				if pages > 10000 {
					t.Fatal("never ends")
				}
			}
			if fmt.Sprint(mine) != fmt.Sprint(want) {
				t.Errorf("page size %d: got %v, want each exactly once as %v", size, mine, want)
			}
			if !sort.StringsAreSorted(all) {
				t.Errorf("page size %d: functions are not ordered by name", size)
			}
		}
	})

	t.Run("versions newest first", func(t *testing.T) {
		fn := h.createFunction(t, h.name("pg-v"))
		for i := 0; i < 5; i++ {
			h.createVersion(t, fn.GetId(), "wagi-1", h.module(fmt.Sprint(i)), nil, false)
		}
		for _, size := range []uint32{1, 2, 3, 0} {
			var nums []int64
			tok := ""
			for {
				r, err := h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: fn.GetId(), PageSize: size, PageToken: tok}))
				if err != nil {
					t.Fatal(err)
				}
				for _, v := range r.Msg.GetVersions() {
					nums = append(nums, v.GetNumber())
				}
				if tok = r.Msg.GetNextPageToken(); tok == "" {
					break
				}
			}
			if fmt.Sprint(nums) != "[5 4 3 2 1]" {
				t.Errorf("page size %d: versions %v, want [5 4 3 2 1]", size, nums)
			}
		}
	})

	t.Run("routes by host and prefix", func(t *testing.T) {
		fn := h.createFunction(t, h.name("pg-r"))
		base := "pg" + h.run + ".conformance.test"
		var want []string
		for _, host := range []string{"d", "b", "a", "c"} {
			for _, p := range []string{"/z", "/a"} {
				h.createRoute(t, host+"."+base, p, fn.GetId())
				want = append(want, host+"."+base+p)
			}
		}
		sort.Strings(want)
		var got []string
		for _, r := range h.allRoutes(t, 3) {
			if strings.HasSuffix(r.GetHostPattern(), base) {
				got = append(got, r.GetHostPattern()+r.GetPathPrefix())
			}
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("routes = %v, want each once, ordered by (host, prefix): %v", got, want)
		}
	})

	t.Run("malformed tokens", func(t *testing.T) {
		fn := h.createFunction(t, h.name("pg-t"))
		_, err := h.fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{PageToken: "!!not a token!!"}))
		check.WantCode(t, err, connect.CodeInvalidArgument)
		_, err = h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: fn.GetId(), PageToken: "!!not a token!!"}))
		check.WantCode(t, err, connect.CodeInvalidArgument)
		_, err = h.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{PageToken: "!!not a token!!"}))
		check.WantCode(t, err, connect.CodeInvalidArgument)
		_, err = h.insp.ListInvocations(ctx, connect.NewRequest(&controlv1.ListInvocationsRequest{PageToken: "!!not a token!!"}))
		check.WantCode(t, err, connect.CodeInvalidArgument)
	})
}

// artifacts checks the content address and the worker-facing download.
func (h *harness) artifacts(t *testing.T) {
	ctx, cancel := ctxT(2 * settle)
	defer cancel()
	fn := h.createFunction(t, h.name("art"))
	// Larger than one chunk, and not a multiple of it.
	module := bytes.Repeat([]byte("0123456789abcdef"), (2*(1<<20)+(512<<10))/16)
	v := h.createVersion(t, fn.GetId(), "wagi-1", module, nil, false)
	if v.GetArtifactDigest() != sha(module) {
		t.Fatalf("digest = %s, want %s", v.GetArtifactDigest(), sha(module))
	}

	download := func(digest string) ([]byte, int, error) {
		st, err := h.artifact(h.env.WorkerToken).GetArtifact(ctx, connect.NewRequest(&workerv1.GetArtifactRequest{Digest: digest}))
		if err != nil {
			return nil, 0, err
		}
		defer st.Close()
		var got []byte
		chunks := 0
		for st.Receive() {
			c := st.Msg().GetChunk()
			if len(c) > 1<<20 {
				t.Errorf("a chunk of %d bytes exceeds the 1 MiB the protocol allows", len(c))
			}
			got = append(got, c...)
			chunks++
		}
		return got, chunks, st.Err()
	}

	t.Run("download", func(t *testing.T) {
		got, chunks, err := download(v.GetArtifactDigest())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, module) {
			t.Errorf("the downloaded module differs: %d bytes, want %d", len(got), len(module))
		}
		if chunks < 3 {
			t.Errorf("%d bytes arrived in %d chunks; at most 1 MiB each needs at least 3", len(got), chunks)
		}
	})
	t.Run("unknown artifacts", func(t *testing.T) {
		for _, digest := range []string{sha([]byte("never uploaded")), "sha256:" + strings.Repeat("0", 64), "nonsense", ""} {
			_, _, err := download(digest)
			check.WantReason(t, err, commonv1.ErrorReason_ERROR_REASON_ARTIFACT_NOT_FOUND)
		}
	})
}

// controlEgress: the network allow list of a function. Closed by default;
// only the operator opens it.
func (h *harness) controlEgress(t *testing.T) {
	ctx, cancel := ctxT(settle)
	defer cancel()
	fn := h.createFunction(t, h.name("eg"))
	set := func(id string, allow ...string) (*controlv1.Function, error) {
		r, err := h.fn.SetEgress(ctx, connect.NewRequest(&controlv1.SetEgressRequest{FunctionId: id, Allow: allow}))
		if err != nil {
			return nil, err
		}
		return r.Msg.GetFunction(), nil
	}
	get := func() *controlv1.Function {
		r, err := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fn.GetId()}))
		if err != nil {
			t.Fatal(err)
		}
		return r.Msg.GetFunction()
	}

	t.Run("closed by default", func(t *testing.T) {
		if len(fn.GetEgressAllow()) != 0 || len(get().GetEgressAllow()) != 0 {
			t.Errorf("a new function must have no network: %v", get().GetEgressAllow())
		}
	})

	t.Run("canonical form", func(t *testing.T) {
		f, err := set(fn.GetId(), "B.Example.com", "a.example.com", "b.example.com", "*.CDN.example.com:8443", "192.0.2.7", "[2001:DB8::1]:8080")
		if err != nil {
			t.Fatal(err)
		}
		want := "[*.cdn.example.com:8443 192.0.2.7 [2001:db8::1]:8080 a.example.com b.example.com]"
		if fmt.Sprint(f.GetEgressAllow()) != want || fmt.Sprint(get().GetEgressAllow()) != want {
			t.Errorf("allow = %v, want %s: lower-cased, deduplicated, sorted", get().GetEgressAllow(), want)
		}
		// Replacing, not adding.
		if f, err = set(fn.GetId(), "only.example.com"); err != nil || fmt.Sprint(f.GetEgressAllow()) != "[only.example.com]" {
			t.Errorf("SetEgress must replace the list: %v, %v", f.GetEgressAllow(), err)
		}
		listed, err := h.fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{PageSize: 500}))
		if err != nil {
			t.Fatal(err)
		}
		for _, lf := range listed.Msg.GetFunctions() {
			if lf.GetId() == fn.GetId() && fmt.Sprint(lf.GetEgressAllow()) != "[only.example.com]" {
				t.Errorf("ListFunctions shows %v", lf.GetEgressAllow())
			}
		}
		// Closing the function off again.
		if f, err = set(fn.GetId()); err != nil || len(f.GetEgressAllow()) != 0 || len(get().GetEgressAllow()) != 0 {
			t.Errorf("an empty list must close the function off: %v, %v", get().GetEgressAllow(), err)
		}
	})

	t.Run("validation", func(t *testing.T) {
		good := "kept.example.com"
		if _, err := set(fn.GetId(), good); err != nil {
			t.Fatal(err)
		}
		for _, bad := range []string{"", "*", "*.", "http://example.com", "example.com/path", "user@example.com", "exa mple.com",
			"a..b", "-a.com", "example.com:0", "example.com:65536", "example.com:", "*.192.0.2.1", "::1", "bücher.example", "*example.com", "a.*.com"} {
			_, err := set(fn.GetId(), good, bad)
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("entry %q: %v, want invalid_argument", bad, err)
			}
		}
		var many []string
		for i := 0; i < 65; i++ {
			many = append(many, fmt.Sprintf("h%d.example.com", i))
		}
		_, err := set(fn.GetId(), many...)
		check.WantCode(t, err, connect.CodeInvalidArgument)
		if _, err := set(fn.GetId(), many[:64]...); err != nil {
			t.Errorf("64 entries are allowed: %v", err)
		}
		set(fn.GetId(), good)
		if got := get().GetEgressAllow(); fmt.Sprint(got) != "[kept.example.com]" {
			t.Errorf("a refused list must change nothing: %v", got)
		}
		for _, id := range []string{unknownID, "not-a-uuid"} {
			_, err := set(id, good)
			check.WantCode(t, err, connect.CodeNotFound)
		}
	})
}
