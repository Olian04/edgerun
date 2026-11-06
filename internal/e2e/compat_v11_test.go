package e2e

// A "v1.1" of the protocol, built at run time from the checked-in descriptors:
// every message gains a string and a message field, every enum a value, every
// service two RPCs (one unary, one streaming) and the three streams a new
// member of their `part` oneof. dynamicpb speaks it; the real components, which
// know only v1.0, must cope with it as the compatibility rules of protobuf and
// of docs/protocol.md say.

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	v11ExtraName  = "edgerun.common.v1.V11Extra"
	v11NewEnum    = 77
	v11NoteField  = 1000
	v11ExtraField = 1001
	v11PartField  = 50
)

type v11Schema struct {
	files *protoregistry.Files
	// services lists every service of the mutated schema.
	services []protoreflect.ServiceDescriptor
}

var streamsWithParts = map[string]bool{
	"edgerun.worker.v1.ExecuteResponse": true, "edgerun.ingress.v1.InvokeResponse": true, "edgerun.egress.v1.FetchResponse": true,
}

func buildV11(t testing.TB) *v11Schema {
	t.Helper()
	local := &protoregistry.Files{}
	v := &v11Schema{files: local}
	visited := map[string]bool{}
	var visit func(fd protoreflect.FileDescriptor)
	visit = func(fd protoreflect.FileDescriptor) {
		if visited[fd.Path()] {
			return
		}
		visited[fd.Path()] = true
		imps := fd.Imports()
		for i := 0; i < imps.Len(); i++ {
			visit(imps.Get(i).FileDescriptor)
		}
		if strings.HasPrefix(fd.Path(), "edgerun/") { // every v1.1 file imports common (V11Extra)
			if c, err := protoregistry.GlobalFiles.FindFileByPath("edgerun/common/v1/common.proto"); err == nil {
				visit(c)
			}
		}
		if !strings.HasPrefix(fd.Path(), "edgerun/") {
			if err := local.RegisterFile(fd); err != nil {
				t.Fatal(err)
			}
			return
		}
		fdp := protodesc.ToFileDescriptorProto(fd)
		mutateFile(fdp)
		nf, err := protodesc.NewFile(fdp, local)
		if err != nil {
			t.Fatalf("v1.1 of %s: %v", fd.Path(), err)
		}
		if err := local.RegisterFile(nf); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < nf.Services().Len(); i++ {
			v.services = append(v.services, nf.Services().Get(i))
		}
	}
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if strings.HasPrefix(fd.Path(), "edgerun/") {
			visit(fd)
		}
		return true
	})
	return v
}

func mutateFile(f *descriptorpb.FileDescriptorProto) {
	const common = "edgerun/common/v1/common.proto"
	if f.GetName() != common {
		has := false
		for _, d := range f.Dependency {
			has = has || d == common
		}
		if !has {
			f.Dependency = append(f.Dependency, common)
		}
	}
	for _, m := range f.MessageType {
		mutateMessage(f.GetPackage(), m)
	}
	for _, e := range f.EnumType {
		e.Value = append(e.Value, &descriptorpb.EnumValueDescriptorProto{Name: proto.String(e.GetName() + "_V11_NEW"), Number: proto.Int32(v11NewEnum)})
	}
	if f.GetName() == common {
		f.MessageType = append(f.MessageType, &descriptorpb.DescriptorProto{
			Name: proto.String("V11Extra"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("text"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), JsonName: proto.String("text")},
				{Name: proto.String("nums"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(), JsonName: proto.String("nums")},
				{Name: proto.String("blob"), Number: proto.Int32(3), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(), JsonName: proto.String("blob")},
			},
		})
	}
	for _, s := range f.Service {
		s.Method = append(s.Method,
			&descriptorpb.MethodDescriptorProto{Name: proto.String("V11Ping"), InputType: proto.String("." + v11ExtraName), OutputType: proto.String("." + v11ExtraName)},
			&descriptorpb.MethodDescriptorProto{Name: proto.String("V11Watch"), InputType: proto.String("." + v11ExtraName), OutputType: proto.String("." + v11ExtraName), ServerStreaming: proto.Bool(true)})
	}
}

func mutateMessage(pkg string, m *descriptorpb.DescriptorProto) {
	if m.GetOptions().GetMapEntry() {
		return
	}
	if streamsWithParts[pkg+"."+m.GetName()] {
		// oneof members are declared consecutively: the new one goes after the last
		at := 0
		for i, f := range m.Field {
			if f.OneofIndex != nil && f.GetOneofIndex() == 0 {
				at = i + 1
			}
		}
		progress := &descriptorpb.FieldDescriptorProto{Name: proto.String("progress"), Number: proto.Int32(v11PartField), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String("." + v11ExtraName), OneofIndex: proto.Int32(0), JsonName: proto.String("progress")}
		m.Field = append(m.Field[:at], append([]*descriptorpb.FieldDescriptorProto{progress}, m.Field[at:]...)...)
	}
	m.Field = append(m.Field,
		&descriptorpb.FieldDescriptorProto{Name: proto.String("v11_note"), Number: proto.Int32(v11NoteField), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), JsonName: proto.String("v11Note")},
		&descriptorpb.FieldDescriptorProto{Name: proto.String("v11_extra"), Number: proto.Int32(v11ExtraField), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String("." + v11ExtraName), JsonName: proto.String("v11Extra")})
	for _, n := range m.NestedType {
		mutateMessage(pkg+"."+m.GetName(), n)
	}
	for _, e := range m.EnumType {
		e.Value = append(e.Value, &descriptorpb.EnumValueDescriptorProto{Name: proto.String(e.GetName() + "_V11_NEW"), Number: proto.Int32(v11NewEnum)})
	}
}

func (v *v11Schema) message(t testing.TB, name string) protoreflect.MessageDescriptor {
	t.Helper()
	d, err := v.files.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		t.Fatalf("v1.1 has no %s: %v", name, err)
	}
	return d.(protoreflect.MessageDescriptor)
}

// decorate fills in everything v1.1 added to m and to the messages in it.
func decorate(m protoreflect.Message) {
	md := m.Descriptor()
	if f := md.Fields().ByName("v11_note"); f != nil {
		m.Set(f, protoreflect.ValueOfString("v1.1 note "+unknownCanary))
	}
	if f := md.Fields().ByName("v11_extra"); f != nil {
		x := m.Mutable(f).Message()
		xd := x.Descriptor()
		x.Set(xd.Fields().ByName("text"), protoreflect.ValueOfString(unknownCanary))
		l := x.Mutable(xd.Fields().ByName("nums")).List()
		l.Append(protoreflect.ValueOfInt64(-1))
		l.Append(protoreflect.ValueOfInt64(1 << 62))
		x.Set(xd.Fields().ByName("blob"), protoreflect.ValueOfBytes([]byte{0, 0xff, 0xfe}))
	}
	m.Range(func(fd protoreflect.FieldDescriptor, val protoreflect.Value) bool {
		if fd.Name() == "v11_extra" || fd.Name() == "progress" {
			return true
		}
		switch {
		case fd.IsList() && fd.Message() != nil:
			l := val.List()
			for i := 0; i < l.Len(); i++ {
				decorate(l.Get(i).Message())
			}
		case fd.IsMap() && fd.MapValue().Message() != nil:
			val.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
				decorate(mv.Message())
				return true
			})
		case fd.Message() != nil && !fd.IsList() && !fd.IsMap():
			decorate(val.Message())
		}
		return true
	})
}

// upgrade is what a v1.1 peer sends for m: the same message, with everything
// v1.1 added filled in.
func (v *v11Schema) upgrade(t testing.TB, m proto.Message) []byte {
	t.Helper()
	d := dynamicpb.NewMessage(v.message(t, string(m.ProtoReflect().Descriptor().FullName())))
	if err := proto.Unmarshal(mustMarshal(t, m), d); err != nil {
		t.Fatal(err)
	}
	decorate(d.ProtoReflect())
	b, err := proto.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// newPart is a message of a stream type carrying only the member v1.1 added to
// its `part` oneof.
func (v *v11Schema) newPart(t testing.TB, name string) []byte {
	t.Helper()
	d := dynamicpb.NewMessage(v.message(t, name))
	x := dynamicpb.NewMessage(v.message(t, v11ExtraName))
	x.Set(x.Descriptor().Fields().ByName("text"), protoreflect.ValueOfString(unknownCanary+" progress"))
	d.Set(d.Descriptor().Fields().ByName("progress"), protoreflect.ValueOfMessage(x))
	b, err := proto.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// errorInfo is a v1.1 ErrorInfo with the given reason number.
func (v *v11Schema) errorInfo(t testing.TB, reason int32, msg, invocationID string) []byte {
	t.Helper()
	d := dynamicpb.NewMessage(v.message(t, "edgerun.common.v1.ErrorInfo"))
	f := d.Descriptor().Fields()
	d.Set(f.ByName("reason"), protoreflect.ValueOfEnum(protoreflect.EnumNumber(reason)))
	d.Set(f.ByName("message"), protoreflect.ValueOfString(msg))
	d.Set(f.ByName("invocation_id"), protoreflect.ValueOfString(invocationID))
	decorate(d)
	b, err := proto.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestV11SchemaIsWhatItClaimsToBe(t *testing.T) {
	v := buildV11(t)
	if len(v.services) != 10 {
		t.Errorf("%d services in v1.1, want 10", len(v.services))
	}
	for _, s := range v.services {
		if s.Methods().ByName("V11Ping") == nil || !s.Methods().ByName("V11Watch").IsStreamingServer() {
			t.Errorf("%s lacks its new RPCs", s.FullName())
		}
	}
	hb := v.message(t, "edgerun.worker.v1.HeartbeatRequest")
	if hb.Fields().ByName("v11_note") == nil || hb.Fields().ByName("state").Enum().Values().ByNumber(v11NewEnum) == nil {
		t.Errorf("HeartbeatRequest of v1.1 lacks its new field or enum value")
	}
	if v.message(t, "edgerun.worker.v1.ExecuteResponse").Fields().ByName("progress").ContainingOneof() == nil {
		t.Errorf("progress is not in the oneof")
	}
}
