# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.25.11

FROM golang:${GO_VERSION}-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/simple-s3-cache ./cmd/simple-s3-cache \
    && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/simple-s3-cache-gateway ./cmd/simple-s3-cache-gateway

RUN mkdir -p /out/rootfs/cache/objects /out/rootfs/cache/meta /out/rootfs/etc/simple-s3-cache /out/rootfs/etc/ssl/certs \
    && cp /etc/ssl/certs/ca-certificates.crt /out/rootfs/etc/ssl/certs/ca-certificates.crt

FROM scratch AS runtime

COPY --from=build /out/simple-s3-cache /usr/local/bin/simple-s3-cache
COPY --from=build /out/simple-s3-cache-gateway /usr/local/bin/simple-s3-cache-gateway
COPY --from=build /out/rootfs/etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=65532:65532 /out/rootfs/cache /cache
COPY --from=build --chown=65532:65532 /out/rootfs/etc/simple-s3-cache /etc/simple-s3-cache

USER 65532:65532
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/simple-s3-cache"]
CMD ["-config", "/etc/simple-s3-cache/simple-s3-cache.yaml"]
