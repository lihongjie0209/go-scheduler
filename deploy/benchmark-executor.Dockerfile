FROM golang:1.25.12-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/executor ./cmd/script-executor

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tini \
    && addgroup -S executor \
    && adduser -S -G executor executor
COPY --from=build /out/executor /usr/local/bin/executor
USER executor
EXPOSE 9999
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/executor"]
