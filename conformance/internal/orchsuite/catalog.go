package orchsuite

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/Olian04/edgerun/conformance/internal/check"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	"github.com/Olian04/edgerun/gen/edgerun/control/v1/controlv1connect"
)

// Catalog judges the orchestrator as the host of control.v1 FunctionService:
// functions, versions, environment, egress allow-lists, labels, and Describe. The
// operator is the client, and the suite plays it.
func Catalog(t *testing.T, target Target) {
	h := start(t, target)
	t.Run("Describe", h.catalogDescribe)
	t.Run("Authentication", h.catalogAuthentication)
	t.Run("Functions", h.controlFunctions)
	t.Run("Versions", h.controlVersions)
	t.Run("Environment", h.controlEnvironment)
	t.Run("Egress", h.controlEgress)
	t.Run("Pagination", h.catalogPagination)
	t.Run("Labels", h.controlLabels)
	t.Run("EnvRevision", h.controlEnvRevision)
	t.Run("LabelEditing", h.controlLabelEditing)
	t.Run("Filters", h.controlFilters)
	// ValidateOnly spans Catalog and Routing: it is one rule of control.v1 ("every
	// check, nothing kept"), judged once over both services' calls, here.
	t.Run("ValidateOnly", h.validateOnly)
	t.Run("FunctionDeletion", h.functionDeletion)
}

func (h *harness) catalogDescribe(t *testing.T) {
	ctx, cancel := ctxT(settle)
	defer cancel()
	ctl, err := h.fn.Describe(ctx, connect.NewRequest(&controlv1.DescribeRequest{}))
	if err != nil {
		t.Fatalf("control Describe: %v", err)
	}
	if ctl.Msg.GetImplementation().GetName() == "" {
		t.Error("control: the implementation has no name")
	}
	if !contains(ctl.Msg.GetImplementation().GetProtocols(), "edgerun.control.v1") {
		t.Errorf("control: protocols %v do not include edgerun.control.v1", ctl.Msg.GetImplementation().GetProtocols())
	}
	if ctl.Msg.GetMaxModuleBytes() == 0 {
		t.Error("control: max_module_bytes must be reported (uploads are bounded by it)")
	}
}

func (h *harness) catalogAuthentication(t *testing.T) {
	check.Authentication(t, h.scopes(), "admin", []string{h.env.ExecutionToken}, func(token string) error {
		ctx, cancel := ctxT(settle)
		defer cancel()
		_, err := controlv1connect.NewFunctionServiceClient(http.DefaultClient, h.url, check.Bearer(token)).ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{}))
		return err
	})
}

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

		d, err := h.fn.Describe(ctx, connect.NewRequest(&controlv1.DescribeRequest{}))
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

// catalogPagination checks that paging yields every item exactly once, in the
// documented order, whatever the page size.
func (h *harness) catalogPagination(t *testing.T) {
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

	t.Run("malformed tokens", func(t *testing.T) {
		fn := h.createFunction(t, h.name("pg-t"))
		_, err := h.fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{PageToken: "!!not a token!!"}))
		check.WantCode(t, err, connect.CodeInvalidArgument)
		_, err = h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: fn.GetId(), PageToken: "!!not a token!!"}))
		check.WantCode(t, err, connect.CodeInvalidArgument)
	})
}

func lbls(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

func (h *harness) listFunctionsMatching(t testing.TB, match map[string]string) []string {
	t.Helper()
	var names []string
	token := ""
	for {
		ctx, cancel := ctxT(settle)
		r, err := h.fn.ListFunctions(ctx, connect.NewRequest(&controlv1.ListFunctionsRequest{PageSize: 2, PageToken: token, MatchLabels: match}))
		cancel()
		if err != nil {
			t.Fatalf("ListFunctions: %v", err)
		}
		for _, f := range r.Msg.GetFunctions() {
			names = append(names, f.GetName())
		}
		if token = r.Msg.GetNextPageToken(); token == "" {
			break
		}
	}
	sort.Strings(names)
	return names
}

// controlLabels: metadata a tool sets on what it manages, and finds it by.
func (h *harness) controlLabels(t *testing.T) {
	ctx, cancel := ctxT(60 * 1e9)
	defer cancel()
	with := func(kv ...string) map[string]string {
		m := lbls(kv...)
		m["conformance-run"] = h.run
		return m
	}
	create := func(name string, labels map[string]string) *controlv1.Function {
		t.Helper()
		r, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: name, Labels: labels}))
		if err != nil {
			t.Fatalf("CreateFunction(%s): %v", name, err)
		}
		return r.Msg.GetFunction()
	}
	a, b, c := h.name("lab-a"), h.name("lab-b"), h.name("lab-c")
	fa := create(a, with("owner", "gitops", "env", "prod"))
	create(b, with("owner", "gitops", "env", "dev"))
	create(c, with("owner", "human"))
	other := h.name("lab-d")
	create(other, nil) // carries none of them

	t.Run("they come back on the resource", func(t *testing.T) {
		g, err := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fa.GetId()}))
		if err != nil || fmt.Sprint(g.Msg.GetFunction().GetLabels()) != fmt.Sprint(with("owner", "gitops", "env", "prod")) {
			t.Errorf("labels: %v, %v", g.Msg.GetFunction().GetLabels(), err)
		}
	})

	t.Run("a list is filtered by every label named, exactly", func(t *testing.T) {
		for _, tc := range []struct {
			match map[string]string
			want  []string
		}{
			{with("owner", "gitops"), []string{a, b}},
			{with("owner", "gitops", "env", "prod"), []string{a}},
			{with("owner", "human", "env", "prod"), nil},
			{with("owner", "Gitops"), nil}, // values are compared exactly
			{with("env", ""), nil},         // a label with another value does not match an empty one
			{lbls("conformance-run", h.run+"-nope"), nil},
		} {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			if got := h.listFunctionsMatching(t, tc.match); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("match %v: %v, want %v", tc.match, got, want)
			}
		}
		// The empty filter matches everything this run made, labelled or not (paged two at a time).
		all := h.listFunctionsMatching(t, nil)
		found := 0
		for _, n := range all {
			if n == a || n == b || n == c || n == other {
				found++
			}
		}
		if found != 4 {
			t.Errorf("an empty filter must list every function: found %d of 4 of this run's", found)
		}
	})

	t.Run("SetLabels replaces them", func(t *testing.T) {
		r, err := h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: fa.GetId(), Labels: with("owner", "someone-else")}))
		if err != nil || fmt.Sprint(r.Msg.GetFunction().GetLabels()) != fmt.Sprint(with("owner", "someone-else")) {
			t.Fatalf("%v %v", r.Msg, err)
		}
		if got := h.listFunctionsMatching(t, with("owner", "gitops")); strings.Join(got, ",") != b {
			t.Errorf("after the change, owner=gitops matches %v", got)
		}
		r, err = h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: fa.GetId()}))
		if err != nil || len(r.Msg.GetFunction().GetLabels()) != 0 {
			t.Errorf("an empty set clears them: %v %v", r.Msg.GetFunction().GetLabels(), err)
		}
		_, err = h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: "00000000-0000-0000-0000-000000000000"}))
		checkCode(t, err, connect.CodeNotFound)
	})

	t.Run("versions carry them from creation", func(t *testing.T) {
		f := h.createFunction(t, h.name("lab-v"))
		v, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{
			FunctionId: f.GetId(), Module: h.module("lab-1"), Abi: h.abi(), Labels: with("commit", "abc")}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{
			FunctionId: f.GetId(), Module: h.module("lab-2"), Abi: h.abi(), Labels: with("commit", "def")})); err != nil {
			t.Fatal(err)
		}
		lv, err := h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: f.GetId(), MatchLabels: with("commit", "abc")}))
		if err != nil || len(lv.Msg.GetVersions()) != 1 || lv.Msg.GetVersions()[0].GetId() != v.Msg.GetVersion().GetId() ||
			lv.Msg.GetVersions()[0].GetLabels()["commit"] != "abc" {
			t.Errorf("versions matching: %v %v", lv.Msg, err)
		}
	})

	t.Run("their form is checked", func(t *testing.T) {
		long := strings.Repeat("x", 254)
		many := map[string]string{}
		for i := 0; i < 33; i++ {
			many[fmt.Sprintf("k%d", i)] = "v"
		}
		bad := map[string]map[string]string{
			"an upper-case key":     lbls("Owner", "x"),
			"an empty key":          lbls("", "x"),
			"a key starting with -": lbls("-a", "x"),
			"a key ending with /":   lbls("a/", "x"),
			"a key with a space":    lbls("a b", "x"),
			"a 64-character key":    lbls(strings.Repeat("k", 64), "x"),
			"a value over 253":      lbls("a", long),
			"a newline in a value":  lbls("a", "x\ny"),
			"a non-ASCII value":     lbls("a", "café"),
			"more than 32 labels":   many,
		}
		for name, l := range bad {
			_, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: h.name("lab-bad"), Labels: l}))
			checkCode(t, err, connect.CodeInvalidArgument, name)
			f := h.createFunction(t, h.name("lab-bad2"))
			_, err = h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: f.GetId(), Labels: l}))
			checkCode(t, err, connect.CodeInvalidArgument, name+" (SetLabels)")
		}
		edge := map[string]string{"managed-by.example.com/tool": "", "a": strings.Repeat("x", 253), strings.Repeat("k", 63): "v"}
		for i := 0; i < 29; i++ {
			edge[fmt.Sprintf("k%d", i)] = "v"
		}
		if _, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: h.name("lab-edge"), Labels: edge})); err != nil {
			t.Errorf("32 labels, a 63-character key, a prefixed key, an empty value and 253 characters are all allowed: %v", err)
		}
	})
}

func checkCode(t *testing.T, err error, want connect.Code, what ...string) {
	t.Helper()
	if err == nil || connect.CodeOf(err) != want {
		t.Errorf("%s: %v, want %v", strings.Join(what, " "), err, want)
	}
}

// controlEnvRevision: a write-only secret can still be compared.
func (h *harness) controlEnvRevision(t *testing.T) {
	ctx, cancel := ctxT(60 * 1e9)
	defer cancel()
	f1, f2 := h.createFunction(t, h.name("rev-a")), h.createFunction(t, h.name("rev-b"))
	set := func(fid, name, value string, secret bool) *controlv1.EnvEntry {
		t.Helper()
		r, err := h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: name, Value: value, Secret: secret}}))
		if err != nil {
			t.Fatalf("SetEnv(%s): %v", name, err)
		}
		return r.Msg.GetEntry()
	}
	listed := func(fid, name string) *controlv1.EnvEntry {
		t.Helper()
		r, err := h.fn.ListEnv(ctx, connect.NewRequest(&controlv1.ListEnvRequest{FunctionId: fid}))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range r.Msg.GetEntries() {
			if e.GetName() == name {
				return e
			}
		}
		t.Fatalf("%s is not listed", name)
		return nil
	}

	s := set(f1.GetId(), "API_KEY", "hunter2-"+h.run, true)
	if s.GetValue() != "" || !s.GetSecret() || s.GetRevision() == "" {
		t.Fatalf("a secret comes back with no value and with a revision: %+v", s)
	}
	if strings.Contains(s.GetRevision(), "hunter2") {
		t.Error("a revision must not contain the value")
	}
	if got := listed(f1.GetId(), "API_KEY"); got.GetRevision() != s.GetRevision() || got.GetValue() != "" {
		t.Errorf("the revision a write returns is the one a read shows: %q, %q (value %q)", s.GetRevision(), got.GetRevision(), got.GetValue())
	}
	if again := set(f1.GetId(), "API_KEY", "hunter2-"+h.run, true); again.GetRevision() != s.GetRevision() {
		t.Error("setting the value that is already there changes nothing, its revision included")
	}
	for what, other := range map[string]*controlv1.EnvEntry{
		"another value":                      set(f1.GetId(), "API_KEY", "hunter3-"+h.run, true),
		"the same value, not secret":         set(f1.GetId(), "API_KEY", "hunter2-"+h.run, false),
		"the same value in another function": set(f2.GetId(), "API_KEY", "hunter2-"+h.run, true),
		"the same value under another name":  set(f1.GetId(), "OTHER_KEY", "hunter2-"+h.run, true),
	} {
		if other.GetRevision() == s.GetRevision() {
			t.Errorf("%s must have another revision", what)
		}
	}
	p := set(f1.GetId(), "PLAIN", "visible", false)
	if p.GetValue() != "visible" || p.GetRevision() == "" || listed(f1.GetId(), "PLAIN").GetRevision() != p.GetRevision() {
		t.Errorf("a plain entry has a revision too: %+v", p)
	}
	if r := set(f2.GetId(), "FORGED", "v", false); r.GetRevision() == "" {
		t.Error("no revision")
	}
	forged, err := h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: f2.GetId(), Entry: &controlv1.EnvEntry{Name: "FORGED2", Value: "v", Revision: "forged"}}))
	if err != nil || forged.Msg.GetEntry().GetRevision() == "forged" {
		t.Errorf("the revision is output only: %v %v", forged.Msg, err)
	}
}

// validateOnly: every check, nothing kept.
func (h *harness) validateOnly(t *testing.T) {
	ctx, cancel := ctxT(120 * 1e9)
	defer cancel()
	f := h.createFunction(t, h.name("vo"))
	fid := f.GetId()
	module := h.module("vo")
	abi := h.abi()
	v := h.createVersion(t, fid, abi, module, nil, false)
	other := h.createFunction(t, h.name("vo-other"))
	ov := h.createVersion(t, other.GetId(), abi, h.module("vo-other"), nil, false)
	host := h.host()
	rt := h.createRoute(t, host, "/", fid)
	h.setEnv(t, fid, "E", "1", false)
	missing := "00000000-0000-0000-0000-000000000000"

	// What one function and the routes of this run look like, for before and after.
	snapshot := func() string {
		var sb strings.Builder
		g, err := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fid}))
		if err != nil {
			fmt.Fprintf(&sb, "function: %v|", connect.CodeOf(err))
		} else {
			fmt.Fprintf(&sb, "F %s active=%q egress=%v labels=%v|", g.Msg.GetFunction().GetName(), g.Msg.GetFunction().GetActiveVersionId(), g.Msg.GetFunction().GetEgressAllow(), g.Msg.GetFunction().GetLabels())
		}
		if vs, err := h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: fid})); err == nil {
			for _, x := range vs.Msg.GetVersions() {
				fmt.Fprintf(&sb, "V%d %s|", x.GetNumber(), x.GetArtifactDigest())
			}
		}
		if es, err := h.fn.ListEnv(ctx, connect.NewRequest(&controlv1.ListEnvRequest{FunctionId: fid})); err == nil {
			for _, e := range es.Msg.GetEntries() {
				fmt.Fprintf(&sb, "E %s=%s %s|", e.GetName(), e.GetValue(), e.GetRevision())
			}
		}
		if rs, err := h.rt.ListRoutes(ctx, connect.NewRequest(&controlv1.ListRoutesRequest{PageSize: 500})); err == nil {
			var routes []string
			for _, r := range rs.Msg.GetRoutes() {
				if strings.HasSuffix(r.GetHostPattern(), h.run+".conformance.test") {
					routes = append(routes, r.GetHostPattern()+r.GetPathPrefix())
				}
			}
			sort.Strings(routes)
			fmt.Fprintf(&sb, "R %v|", routes)
		}
		if _, err := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: other.GetId()})); err != nil {
			sb.WriteString("other gone|")
		}
		return sb.String()
	}

	freshName := h.name("vo-fresh")
	type op struct {
		name string
		call func(vo bool) (any, error)
	}
	ops := []op{
		{"create a function", func(vo bool) (any, error) {
			return h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: freshName, Labels: lbls("a", "b"), ValidateOnly: vo}))
		}},
		{"create a function that exists", func(vo bool) (any, error) {
			return h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: f.GetName(), ValidateOnly: vo}))
		}},
		{"create a function with a bad name", func(vo bool) (any, error) {
			return h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: "Bad Name", ValidateOnly: vo}))
		}},
		{"create a function with bad labels", func(vo bool) (any, error) {
			return h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: h.name("vo-l"), Labels: lbls("BAD", "x"), ValidateOnly: vo}))
		}},
		{"create a version", func(vo bool) (any, error) {
			return h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: fid, Module: h.module("vo-new"), Abi: abi, Activate: true, ValidateOnly: vo}))
		}},
		{"create a version of nothing", func(vo bool) (any, error) {
			return h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: missing, Module: h.module("vo-x"), Abi: abi, ValidateOnly: vo}))
		}},
		{"create a version with a bad abi", func(vo bool) (any, error) {
			return h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: fid, Module: h.module("vo-y"), Abi: "NOT AN ABI", ValidateOnly: vo}))
		}},
		{"activate a version", func(vo bool) (any, error) {
			return h.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: v.GetId(), ValidateOnly: vo}))
		}},
		{"activate another function's version", func(vo bool) (any, error) {
			return h.fn.ActivateVersion(ctx, connect.NewRequest(&controlv1.ActivateVersionRequest{FunctionId: fid, VersionId: ov.GetId(), ValidateOnly: vo}))
		}},
		{"set an env entry", func(vo bool) (any, error) {
			return h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "NEW", Value: "v", Secret: true}, ValidateOnly: vo}))
		}},
		{"set a reserved env name", func(vo bool) (any, error) {
			return h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: fid, Entry: &controlv1.EnvEntry{Name: "PATH_INFO", Value: "v"}, ValidateOnly: vo}))
		}},
		{"delete an env entry", func(vo bool) (any, error) {
			return h.fn.DeleteEnv(ctx, connect.NewRequest(&controlv1.DeleteEnvRequest{FunctionId: fid, Name: "E", ValidateOnly: vo}))
		}},
		{"delete an env entry that is not there", func(vo bool) (any, error) {
			return h.fn.DeleteEnv(ctx, connect.NewRequest(&controlv1.DeleteEnvRequest{FunctionId: fid, Name: "NOPE", ValidateOnly: vo}))
		}},
		{"set egress", func(vo bool) (any, error) {
			return h.fn.SetEgress(ctx, connect.NewRequest(&controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{"API.example.com"}, ValidateOnly: vo}))
		}},
		{"set a bad egress entry", func(vo bool) (any, error) {
			return h.fn.SetEgress(ctx, connect.NewRequest(&controlv1.SetEgressRequest{FunctionId: fid, Allow: []string{"a.example.com:99999"}, ValidateOnly: vo}))
		}},
		{"set labels", func(vo bool) (any, error) {
			return h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: fid, Labels: lbls("n", "m"), ValidateOnly: vo}))
		}},
		{"set bad labels", func(vo bool) (any, error) {
			return h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: fid, Labels: lbls("N", "m"), ValidateOnly: vo}))
		}},
		{"create a route", func(vo bool) (any, error) {
			return h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: h.host(), PathPrefix: "/p", FunctionId: fid, ValidateOnly: vo}))
		}},
		{"create a route that exists", func(vo bool) (any, error) {
			return h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: host, PathPrefix: "/", FunctionId: fid, ValidateOnly: vo}))
		}},
		{"create a route to nothing", func(vo bool) (any, error) {
			return h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: h.host(), PathPrefix: "/", FunctionId: missing, ValidateOnly: vo}))
		}},
		{"delete a route", func(vo bool) (any, error) {
			return h.rt.DeleteRoute(ctx, connect.NewRequest(&controlv1.DeleteRouteRequest{RouteId: rt.GetId(), ValidateOnly: vo}))
		}},
		{"delete a route that is not there", func(vo bool) (any, error) {
			return h.rt.DeleteRoute(ctx, connect.NewRequest(&controlv1.DeleteRouteRequest{RouteId: missing, ValidateOnly: vo}))
		}},
		{"delete a function", func(vo bool) (any, error) {
			return h.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: other.GetId(), ValidateOnly: vo}))
		}},
		{"delete a function that is not there", func(vo bool) (any, error) {
			return h.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: missing, ValidateOnly: vo}))
		}},
	}
	for _, o := range ops {
		t.Run(o.name, func(t *testing.T) {
			before := snapshot()
			_, dryErr := o.call(true)
			if after := snapshot(); after != before {
				t.Fatalf("validate-only changed something:\n before %s\n after  %s", before, after)
			}
			if dryErr != nil {
				// The error is the one the call itself gives (the real call fails too, so it changes nothing).
				_, realErr := o.call(false)
				if connect.CodeOf(realErr) != connect.CodeOf(dryErr) {
					t.Errorf("validate-only answered %v, the call answers %v", dryErr, realErr)
				}
			}
		})
	}

	// The order of checks is part of the protocol (docs/protocol.md, "Validate-only"): the form of a
	// request first, then the existence of what it names, then conflicts. A consumer plans on it.
	t.Run("the form of a request is checked before what it names is looked up", func(t *testing.T) {
		nothing := missing // well formed, and nothing has it
		type step struct {
			name       string
			malformed  func(vo bool) error
			wellFormed func(vo bool) error
		}
		steps := []step{
			{"CreateVersion",
				func(vo bool) error {
					_, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: nothing, Module: h.module("o1"), Abi: "NOT AN ABI", ValidateOnly: vo}))
					return err
				},
				func(vo bool) error {
					_, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: nothing, Module: h.module("o1"), Abi: abi, ValidateOnly: vo}))
					return err
				}},
			{"SetEnv",
				func(vo bool) error {
					_, err := h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: nothing, Entry: &controlv1.EnvEntry{Name: "PATH_INFO", Value: "x"}, ValidateOnly: vo}))
					return err
				},
				func(vo bool) error {
					_, err := h.fn.SetEnv(ctx, connect.NewRequest(&controlv1.SetEnvRequest{FunctionId: nothing, Entry: &controlv1.EnvEntry{Name: "FINE", Value: "x"}, ValidateOnly: vo}))
					return err
				}},
			{"SetEgress",
				func(vo bool) error {
					_, err := h.fn.SetEgress(ctx, connect.NewRequest(&controlv1.SetEgressRequest{FunctionId: nothing, Allow: []string{"a.example.com:99999"}, ValidateOnly: vo}))
					return err
				},
				func(vo bool) error {
					_, err := h.fn.SetEgress(ctx, connect.NewRequest(&controlv1.SetEgressRequest{FunctionId: nothing, Allow: []string{"a.example.com"}, ValidateOnly: vo}))
					return err
				}},
			{"SetLabels",
				func(vo bool) error {
					_, err := h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: nothing, Labels: lbls("BAD", "x"), ValidateOnly: vo}))
					return err
				},
				func(vo bool) error {
					_, err := h.fn.SetLabels(ctx, connect.NewRequest(&controlv1.SetLabelsRequest{FunctionId: nothing, Labels: lbls("fine", "x"), ValidateOnly: vo}))
					return err
				}},
			{"CreateRoute",
				func(vo bool) error {
					_, err := h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: h.host(), PathPrefix: "no-leading-slash", FunctionId: nothing, ValidateOnly: vo}))
					return err
				},
				func(vo bool) error {
					_, err := h.rt.CreateRoute(ctx, connect.NewRequest(&controlv1.CreateRouteRequest{HostPattern: h.host(), PathPrefix: "/", FunctionId: nothing, ValidateOnly: vo}))
					return err
				}},
		}
		for _, st := range steps {
			for _, vo := range []bool{true, false} {
				checkCode(t, st.malformed(vo), connect.CodeInvalidArgument, st.name, fmt.Sprintf("malformed, naming nothing (validate_only=%v)", vo))
				checkCode(t, st.wellFormed(vo), connect.CodeNotFound, st.name, fmt.Sprintf("well formed, naming nothing (validate_only=%v)", vo))
			}
		}
	})

	t.Run("a validate-only create names no id, and reserves nothing", func(t *testing.T) {
		name := h.name("vo-twice")
		r, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: name, ValidateOnly: true}))
		if err != nil || r.Msg.GetFunction().GetId() != "" || r.Msg.GetFunction().GetName() != name {
			t.Errorf("%v %v", r.Msg, err)
		}
		if _, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: name})); err != nil {
			t.Errorf("the real create after a validate-only one: %v", err)
		}
		rv, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: fid, Module: h.module("vo-ver"), Abi: abi, ValidateOnly: true}))
		if err != nil || rv.Msg.GetVersion().GetId() != "" || rv.Msg.GetVersion().GetArtifactDigest() == "" {
			t.Errorf("a validate-only version says what it would be, with no id: %v %v", rv.Msg, err)
		}
		real, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: fid, Module: h.module("vo-ver2"), Abi: abi}))
		if err != nil || real.Msg.GetVersion().GetNumber() != 2 {
			t.Errorf("the number a dry run would have taken is not used up: %v %v", real.Msg.GetVersion(), err)
		}
	})
}

// controlLabelEditing: a tool changes its own labels without disturbing another's.
func (h *harness) controlLabelEditing(t *testing.T) {
	ctx, cancel := ctxT(120 * 1e9)
	defer cancel()
	missing := "00000000-0000-0000-0000-000000000000"

	t.Run("PatchLabels changes some and leaves the rest", func(t *testing.T) {
		f, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: h.name("patch"), Labels: lbls("a", "1", "b", "2", "c", "3")}))
		if err != nil {
			t.Fatal(err)
		}
		fid := f.Msg.GetFunction().GetId()
		patch := func(set map[string]string, remove ...string) (*controlv1.Function, error) {
			r, err := h.fn.PatchLabels(ctx, connect.NewRequest(&controlv1.PatchLabelsRequest{FunctionId: fid, Set: set, Remove: remove}))
			if err != nil {
				return nil, err
			}
			return r.Msg.GetFunction(), nil
		}
		got, err := patch(lbls("b", "20", "d", "4"), "c")
		if err != nil || fmt.Sprint(got.GetLabels()) != fmt.Sprint(lbls("a", "1", "b", "20", "d", "4")) {
			t.Fatalf("set b and d, remove c: %v %v", got.GetLabels(), err)
		}
		g, _ := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fid}))
		if fmt.Sprint(g.Msg.GetFunction().GetLabels()) != fmt.Sprint(got.GetLabels()) {
			t.Errorf("the patch is stored: %v", g.Msg.GetFunction().GetLabels())
		}
		if got, err := patch(nil, "never-there"); err != nil || len(got.GetLabels()) != 3 {
			t.Errorf("removing a key that is not there is not an error and changes nothing: %v %v", got.GetLabels(), err)
		}
		if got, err := patch(nil); err != nil || len(got.GetLabels()) != 3 {
			t.Errorf("an empty patch changes nothing: %v %v", got.GetLabels(), err)
		}
		if got, err := patch(nil, "a", "b", "d"); err != nil || len(got.GetLabels()) != 0 {
			t.Errorf("removing every key clears them: %v %v", got.GetLabels(), err)
		}

		// Refused, and nothing changed.
		patch(lbls("keep", "me"))
		_, err = patch(lbls("x", "1"), "x")
		checkCode(t, err, connect.CodeInvalidArgument, "a key both set and removed")
		_, err = patch(lbls("BAD", "1"))
		checkCode(t, err, connect.CodeInvalidArgument, "a bad key to set")
		_, err = patch(nil, "BAD")
		checkCode(t, err, connect.CodeInvalidArgument, "a bad key to remove")
		_, err = patch(lbls("k", strings.Repeat("x", 254)))
		checkCode(t, err, connect.CodeInvalidArgument, "a bad value")
		many := map[string]string{}
		for i := 0; i < 32; i++ {
			many[fmt.Sprintf("k%d", i)] = "v"
		}
		_, err = patch(many) // with "keep" that makes 33
		checkCode(t, err, connect.CodeInvalidArgument, "a result over 32 labels")
		g, _ = h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: fid}))
		if fmt.Sprint(g.Msg.GetFunction().GetLabels()) != fmt.Sprint(lbls("keep", "me")) {
			t.Errorf("a refused patch changed something: %v", g.Msg.GetFunction().GetLabels())
		}
		_, err = h.fn.PatchLabels(ctx, connect.NewRequest(&controlv1.PatchLabelsRequest{FunctionId: missing, Set: lbls("a", "b")}))
		checkCode(t, err, connect.CodeNotFound, "a function that does not exist")
		_, err = h.fn.PatchLabels(ctx, connect.NewRequest(&controlv1.PatchLabelsRequest{FunctionId: "not-an-id"}))
		checkCode(t, err, connect.CodeNotFound, "a malformed id")
	})

	t.Run("patches by different tools do not lose each other's", func(t *testing.T) {
		f := h.createFunction(t, h.name("patch-race"))
		const writers = 16
		var wg sync.WaitGroup
		errs := make(chan error, writers)
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := h.fn.PatchLabels(ctx, connect.NewRequest(&controlv1.PatchLabelsRequest{FunctionId: f.GetId(), Set: lbls(fmt.Sprintf("tool-%d", i), "x")}))
				errs <- err
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		g, _ := h.fn.GetFunction(ctx, connect.NewRequest(&controlv1.GetFunctionRequest{FunctionId: f.GetId()}))
		if n := len(g.Msg.GetFunction().GetLabels()); n != writers {
			t.Errorf("%d of %d concurrent patches survived: %v", n, writers, g.Msg.GetFunction().GetLabels())
		}
	})
}

// controlFilters: finding what is wanted without reading everything.
func (h *harness) controlFilters(t *testing.T) {
	ctx, cancel := ctxT(120 * 1e9)
	defer cancel()
	with := func(kv ...string) map[string]string {
		m := lbls(kv...)
		m["conformance-filters"] = h.run
		return m
	}
	mk := func(name string, labels map[string]string) *controlv1.Function {
		t.Helper()
		r, err := h.fn.CreateFunction(ctx, connect.NewRequest(&controlv1.CreateFunctionRequest{Name: name, Labels: labels}))
		if err != nil {
			t.Fatal(err)
		}
		return r.Msg.GetFunction()
	}
	list := func(req *controlv1.ListFunctionsRequest) ([]string, error) {
		var names []string
		for {
			req.PageSize = 2
			r, err := h.fn.ListFunctions(ctx, connect.NewRequest(req))
			if err != nil {
				return nil, err
			}
			for _, f := range r.Msg.GetFunctions() {
				names = append(names, f.GetName())
			}
			if req.PageToken = r.Msg.GetNextPageToken(); req.PageToken == "" {
				sort.Strings(names)
				return names, nil
			}
		}
	}
	a, b, c, d := h.name("flt-a"), h.name("flt-b"), h.name("flt-c"), h.name("flt-d")
	mk(a, with("owner", "gitops", "env", "prod"))
	mk(b, with("owner", "gitops"))
	mk(c, with("env", ""))
	mk(d, with())
	names := func(ns ...string) string { sort.Strings(ns); return strings.Join(ns, ",") }

	t.Run("a key is required whatever its value", func(t *testing.T) {
		for _, tc := range []struct {
			req  *controlv1.ListFunctionsRequest
			want string
		}{
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), HasLabelKeys: []string{"owner"}}, names(a, b)},
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), HasLabelKeys: []string{"owner", "env"}}, names(a)},
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), HasLabelKeys: []string{"env"}}, names(a, c)}, // an empty value is a value
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), HasLabelKeys: []string{"nobody-has-this"}}, ""},
		} {
			got, err := list(tc.req)
			if err != nil || strings.Join(got, ",") != tc.want {
				t.Errorf("has %v: %v %v, want %s", tc.req.HasLabelKeys, got, err, tc.want)
			}
		}
	})

	t.Run("a key can be excluded: what nobody owns", func(t *testing.T) {
		for _, tc := range []struct {
			req  *controlv1.ListFunctionsRequest
			want string
		}{
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), LacksLabelKeys: []string{"owner"}}, names(c, d)},
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), LacksLabelKeys: []string{"owner", "env"}}, names(d)},
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), LacksLabelKeys: []string{"nobody-has-this"}}, names(a, b, c, d)},
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run, "owner", "gitops"), LacksLabelKeys: []string{"env"}}, names(b)},
			{&controlv1.ListFunctionsRequest{MatchLabels: lbls("conformance-filters", h.run), HasLabelKeys: []string{"env"}, LacksLabelKeys: []string{"owner"}}, names(c)},
		} {
			got, err := list(tc.req)
			if err != nil || strings.Join(got, ",") != tc.want {
				t.Errorf("match %v has %v lacks %v: %v %v, want %s", tc.req.MatchLabels, tc.req.HasLabelKeys, tc.req.LacksLabelKeys, got, err, tc.want)
			}
		}
	})

	t.Run("a filter that can only match nothing is refused", func(t *testing.T) {
		for what, req := range map[string]*controlv1.ListFunctionsRequest{
			"a key both required and excluded": {HasLabelKeys: []string{"x"}, LacksLabelKeys: []string{"x"}},
			"a key matched and excluded":       {MatchLabels: lbls("x", "1"), LacksLabelKeys: []string{"x"}},
			"a malformed key to require":       {HasLabelKeys: []string{"NOT A KEY"}},
			"a malformed key to exclude":       {LacksLabelKeys: []string{"Owner"}},
			"an empty key":                     {HasLabelKeys: []string{""}},
		} {
			_, err := list(req)
			checkCode(t, err, connect.CodeInvalidArgument, what)
		}
	})

	t.Run("a function is found by its name", func(t *testing.T) {
		for _, tc := range []struct {
			req  *controlv1.ListFunctionsRequest
			want string
		}{
			{&controlv1.ListFunctionsRequest{Name: a}, a},
			{&controlv1.ListFunctionsRequest{Name: strings.ToUpper(a)}, ""}, // names are lower case: this one names nothing
			{&controlv1.ListFunctionsRequest{Name: a[:len(a)-1]}, ""},       // exactly, not a prefix
			{&controlv1.ListFunctionsRequest{Name: h.name("flt-nobody")}, ""},
			{&controlv1.ListFunctionsRequest{Name: a, MatchLabels: lbls("owner", "gitops")}, a},
			{&controlv1.ListFunctionsRequest{Name: a, MatchLabels: lbls("owner", "human")}, ""}, // all the filters must hold
			{&controlv1.ListFunctionsRequest{Name: c, HasLabelKeys: []string{"owner"}}, ""},
		} {
			got, err := list(tc.req)
			if err != nil || strings.Join(got, ",") != tc.want {
				t.Errorf("%+v: %v %v, want %q", tc.req, got, err, tc.want)
			}
		}
	})

	t.Run("versions can be listed by label keys", func(t *testing.T) {
		f := h.createFunction(t, h.name("flt-v"))
		mkv := func(tag string, labels map[string]string) string {
			r, err := h.fn.CreateVersion(ctx, connect.NewRequest(&controlv1.CreateVersionRequest{FunctionId: f.GetId(), Module: h.module(tag), Abi: h.abi(), Labels: labels}))
			if err != nil {
				t.Fatal(err)
			}
			return r.Msg.GetVersion().GetId()
		}
		v1, v2 := mkv("fv1", lbls("commit", "abc")), mkv("fv2", nil)
		list := func(req *controlv1.ListVersionsRequest) []string {
			req.FunctionId = f.GetId()
			r, err := h.fn.ListVersions(ctx, connect.NewRequest(req))
			if err != nil {
				t.Fatal(err)
			}
			var out []string
			for _, v := range r.Msg.GetVersions() {
				out = append(out, v.GetId())
			}
			return out
		}
		if got := list(&controlv1.ListVersionsRequest{HasLabelKeys: []string{"commit"}}); fmt.Sprint(got) != fmt.Sprint([]string{v1}) {
			t.Errorf("versions with a commit: %v", got)
		}
		if got := list(&controlv1.ListVersionsRequest{LacksLabelKeys: []string{"commit"}}); fmt.Sprint(got) != fmt.Sprint([]string{v2}) {
			t.Errorf("versions without a commit: %v", got)
		}
		_, err := h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: f.GetId(), HasLabelKeys: []string{"Bad Key"}}))
		checkCode(t, err, connect.CodeInvalidArgument, "a malformed key on versions")
	})
}

// functionDeletion: a deleted function takes its configuration with it and
// leaves its history.
func (h *harness) functionDeletion(t *testing.T) {
	ctx, cancel := ctxT(60 * time.Second)
	defer cancel()
	_, f := h.stack(t, respond("bye"))
	h.setEnv(t, f.fn.GetId(), "GONE", "soon", false)
	h.createVersion(t, f.fn.GetId(), f.abi, h.module("second"), nil, false)
	h.served(t, f.host)
	h.mustInvoke(t, call{host: f.host, requestID: "del-" + h.run})
	check.Eventually(t, "the record", settle, func() bool { return len(h.records(t, f.fn.GetId(), 0)) > 0 })
	route := ""
	for _, r := range h.allRoutes(t, 0) {
		if r.GetFunctionId() == f.fn.GetId() {
			route = r.GetId()
		}
	}
	if route == "" {
		t.Fatal("the function's route is not listed")
	}

	if _, err := h.fn.DeleteFunction(ctx, connect.NewRequest(&controlv1.DeleteFunctionRequest{FunctionId: f.fn.GetId()})); err != nil {
		t.Fatal(err)
	}
	if h.hasRoute(t, route) {
		t.Error("the function's route survived it")
	}
	_, err := h.fn.ListVersions(ctx, connect.NewRequest(&controlv1.ListVersionsRequest{FunctionId: f.fn.GetId()}))
	check.WantCode(t, err, connect.CodeNotFound)
	_, err = h.fn.ListEnv(ctx, connect.NewRequest(&controlv1.ListEnvRequest{FunctionId: f.fn.GetId()}))
	check.WantCode(t, err, connect.CodeNotFound)
	check.WantReason(t, h.invoke(t, call{host: f.host}).err, commonv1.ErrorReason_ERROR_REASON_FUNCTION_NOT_FOUND)

	recs := h.records(t, f.fn.GetId(), 0)
	if len(recs) == 0 || recs[0].GetRequestId() != "del-"+h.run {
		t.Errorf("a deleted function's invocation records are kept: %v", recs)
	}
}
