# Concord daemon image.
#
# Build-only validation; the daemon runs on edge nodes and is deployed
# manually, never restarted from CI.
#
#   docker build -t ghcr.io/podomy/concord .
#
# CGO stays enabled: container init joins namespaces through
# libcontainer's nsexec C constructor, and CGO_ENABLED=0 builds grow
# containers whose init can never join namespaces (see
# .github/workflows/release.yml). The builder is Debian-based so the
# same libc links at runtime.

FROM golang:1.26-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Tests run as non-root, exactly like local dev: the root-gated
# network tests (internal/cn) skip without privileges instead of
# failing on missing CAP_NET_ADMIN in the unprivileged build.
RUN mkdir -p /out && useradd -m builder && chown -R builder:builder /src /out
USER builder
RUN go vet ./... && go test ./... && CGO_ENABLED=1 go build -trimpath -ldflags "-s -w" -o /out/concord ./cmd/concord

FROM debian:bookworm-slim
COPY --from=builder /out/concord /concord
ENTRYPOINT ["/concord"]
CMD ["--help"]
