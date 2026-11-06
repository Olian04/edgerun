module example.com/edgerun-trafficmock

go 1.27

require (
	connectrpc.com/connect v1.21.1
	github.com/HdrHistogram/hdrhistogram-go v1.1.2
	github.com/Olian04/edgerun v0.0.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	connectrpc.com/connect/v2 v2.0.0 // indirect
	connectrpc.com/otelconnect v0.12.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/Olian04/edgerun => ../../..
