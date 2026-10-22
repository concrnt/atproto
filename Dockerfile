FROM golang:1.26 AS builder
WORKDIR /src
# The source tree may carry a go.work pointing at a sibling concrnt checkout
# that is not in the build context; build from go.mod alone.
ENV GOWORK=off
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# VERSION is passed by the Docker workflow (git tag / branch); fall back to git
# metadata for local builds.
ARG VERSION
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=${VERSION:-$(git describe --tags --always 2>/dev/null || echo docker)}" -o /bridge ./cmd/bridge

# Static ffmpeg/ffprobe (amd64 + arm64) for video transcoding on blob
# ingest; distroless has no package manager and these need no libc.
FROM mwader/static-ffmpeg:9.0.2 AS ffmpeg

# CA bundle: distroless ships its own, which lagged behind the eMudhra intermediate CA
# compromise (2026-10), and the golang builder image is not always current either, so
# build a fresh Debian bundle here. The CI workflow excludes this stage from the layer
# cache (no-cache-filters) so apt really runs on every build.
FROM debian:trixie-slim AS certificates
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates

FROM gcr.io/distroless/static-debian12
COPY --from=certificates /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /bridge /bridge
COPY --from=ffmpeg /ffmpeg /ffprobe /usr/local/bin/
# CONFIG_PATH may also point at a directory (every file in it is deep-merged in
# name order), e.g. a ConfigMap + Secret projected volume on Kubernetes.
ENV CONFIG_PATH=/app/config.yaml
EXPOSE 8010
VOLUME /data
ENTRYPOINT ["/bridge"]
