module example.com/edgerun-wgservice

go 1.27

require (
	connectrpc.com/connect v1.21.1
	github.com/Olian04/edgerun v0.0.0
	golang.org/x/net v0.61.0
	golang.zx2c4.com/wireguard v0.0.0-20261024220952-b3bc704ef0ee
	google.golang.org/protobuf v1.36.12
	gvisor.dev/gvisor v0.0.0-20250503011706-39ed1f5ac29c
)

require (
	github.com/google/btree v1.1.2 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/time v0.7.0 // indirect
	golang.zx2c4.com/wintun v0.0.0-20230126152724-0fa3db229ce2 // indirect
)

replace github.com/Olian04/edgerun => ../../..
