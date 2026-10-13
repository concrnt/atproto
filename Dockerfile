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

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bridge /bridge
# CONFIG_PATH may also point at a directory (every file in it is deep-merged in
# name order), e.g. a ConfigMap + Secret projected volume on Kubernetes.
ENV CONFIG_PATH=/app/config.yaml
EXPOSE 8010
VOLUME /data
ENTRYPOINT ["/bridge"]
