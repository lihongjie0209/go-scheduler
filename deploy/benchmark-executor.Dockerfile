FROM golang:1.25.13-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/executor ./cmd/scheduler

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tini \
    && addgroup -S executor \
    && adduser -S -G executor executor \
    && install -d -m 0700 -o executor -g executor /var/lib/go-scheduler/executor
COPY --from=build /out/executor /usr/local/bin/executor
ENV EXECUTOR_STATE_DIR=/var/lib/go-scheduler/executor
USER executor
EXPOSE 9999
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/executor"]
CMD ["executor"]
