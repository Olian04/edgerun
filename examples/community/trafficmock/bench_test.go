package main

import (
	"context"
	"io"
	"testing"

	"github.com/Olian04/edgerun"
	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	ingressv1 "github.com/Olian04/edgerun/gen/edgerun/ingress/v1"
)

// BenchmarkInvoke is the library alone against a fake orchestrator in another
// process: for `go test -bench Invoke -benchmem -memprofile ... -memprofilerate 1`.
func BenchmarkInvoke(b *testing.B) {
	for _, c := range []struct{ name, path string }{{"small", "/bytes/100"}, {"1MiB", "/bytes/1048576"}} {
		b.Run(c.name, func(b *testing.B) {
			url := fakeProc(b)
			ing := edgerun.NewIngress(url, ingressToken, edgerun.WithHTTPClient(newHTTPClient("1", 0)))
			req := &ingressv1.InvokeRequest{Request: &commonv1.HttpRequest{Method: "GET", Host: mockHost, Path: c.path}}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				buf := make([]byte, 64<<10)
				for pb.Next() {
					r, err := ing.Invoke(context.Background(), req)
					if err != nil {
						b.Error(err)
						return
					}
					for {
						_, err := r.Body.Read(buf)
						if err == io.EOF {
							break
						}
						if err != nil {
							b.Error(err)
							return
						}
					}
					r.Body.Close()
				}
			})
		})
	}
}
