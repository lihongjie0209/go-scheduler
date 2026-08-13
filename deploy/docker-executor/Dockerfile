FROM golang:1.25-alpine AS build
WORKDIR /src
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/docker-executor ./cmd/docker-executor

FROM alpine:3.22
RUN apk add --no-cache ca-certificates docker-cli tini
COPY --from=build /out/docker-executor /usr/local/bin/docker-executor
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/docker-executor"]
