# One image recipe for every component. Pick the component with a build arg:
#
#   docker build -f deploy/Dockerfile --build-arg CMD=orchestrator -t edgerun/orchestrator .
#
# The binaries are static (no cgo), so the runtime image is distroless.
FROM golang:1.27 AS build
ARG CMD
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN test -n "$CMD" && CGO_ENABLED=0 go build -trimpath -o /out/edgerun ./cmd/$CMD

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/edgerun /edgerun
ENTRYPOINT ["/edgerun"]
