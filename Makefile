# Codegen tools are pinned: protoc-gen-* in go.mod, buf in tools/go.mod (kept
# separate so buf's dependency graph never leaks into the product's).
BUF := go tool -modfile=tools/go.mod buf
BASE ?= main

.PHONY: all generate lint breaking check-generated proto-check test build conformance

all: proto-check test build

generate:
	$(BUF) generate

lint:
	$(BUF) lint

# Compare against the protocol as it exists on $(BASE). Until $(BASE) contains
# protos (i.e. before the first merge) there is nothing to compare against.
breaking:
	@if git ls-tree -r --name-only "origin/$(BASE)" 2>/dev/null | grep -q '^proto/'; then \
		$(BUF) breaking --against ".git#ref=refs/remotes/origin/$(BASE),subdir=proto"; \
	else \
		echo "breaking: origin/$(BASE) has no proto/ yet; skipping (no baseline)"; \
	fi

# Generated code is committed; fail if it is stale. Regenerates into a temp
# dir and diffs, so the result does not depend on git state.
check-generated:
	@tmp=$$(mktemp -d) && trap 'rm -rf "$$tmp"' EXIT && \
		$(BUF) generate -o "$$tmp" && \
		diff -r "$$tmp/gen" gen >/dev/null || \
		{ echo "gen/ is stale: run 'make generate' and commit"; exit 1; }

proto-check: lint breaking check-generated

test:
	go test ./...

build:
	go build ./...

# The conformance executable, with the test guests compiled into it so that it
# needs no Go toolchain where it runs: bin/edgerun-conformance --role ROLE --target HOST:PORT
conformance:
	mkdir -p conformance/guest/prebuilt bin
	GOOS=wasip1 GOARCH=wasm CGO_ENABLED=0 go build -trimpath -o conformance/guest/prebuilt/testguest.wasm ./conformance/guest/testguest
	GOOS=wasip1 GOARCH=wasm CGO_ENABLED=0 go build -trimpath -o conformance/guest/prebuilt/netguest.wasm ./conformance/guest/netguest
	go build -tags prebuilt -o bin/edgerun-conformance ./cmd/edgerun-conformance
