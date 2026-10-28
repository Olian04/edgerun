module example.com/edgerun-gitops

go 1.27

require (
	connectrpc.com/connect v1.21.1
	github.com/Olian04/edgerun v0.0.0
	gopkg.in/yaml.v3 v3.0.1
)

require google.golang.org/protobuf v1.36.12 // indirect

replace github.com/Olian04/edgerun => ../../..
