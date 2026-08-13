FROM golang:1.25-alpine AS build
WORKDIR /src
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/script-executor ./cmd/script-executor

FROM alpine:3.22
ARG POWERSHELL_VERSION=7.6.4
ARG POWERSHELL_SHA256=2c7c18870b1ad0c107b1665b5f8733b656eb67260f6a6970aba4e7773287d8fd
RUN apk add --no-cache ca-certificates docker-cli icu-libs krb5-libs libgcc libintl libssl3 libstdc++ nodejs php83-cli python3 tini userspace-rcu zlib \
    && apk add --no-cache --virtual .powershell-download curl \
    && curl --fail --location --silent --show-error \
       "https://github.com/PowerShell/PowerShell/releases/download/v${POWERSHELL_VERSION}/powershell-${POWERSHELL_VERSION}-linux-musl-x64.tar.gz" \
       --output /tmp/powershell.tar.gz \
    && echo "${POWERSHELL_SHA256}  /tmp/powershell.tar.gz" | sha256sum -c - \
    && mkdir -p /opt/microsoft/powershell/7 \
    && tar -xzf /tmp/powershell.tar.gz -C /opt/microsoft/powershell/7 \
    && chmod 0755 /opt/microsoft/powershell/7/pwsh \
    && ln -s /opt/microsoft/powershell/7/pwsh /usr/bin/pwsh \
    && rm /tmp/powershell.tar.gz \
    && apk del .powershell-download \
    && addgroup -S executor \
    && adduser -S -G executor executor
COPY --from=build /out/script-executor /usr/local/bin/script-executor
USER executor:executor
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/script-executor"]
