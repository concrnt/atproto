FROM golang:1.25 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=$(git describe --tags --always 2>/dev/null || echo docker)" -o /bridge ./cmd/bridge

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bridge /bridge
ENV CONFIG_PATH=/app/config.yaml
EXPOSE 8010 8011
VOLUME /data
ENTRYPOINT ["/bridge"]
