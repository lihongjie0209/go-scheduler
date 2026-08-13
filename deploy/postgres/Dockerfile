ARG POSTGRES_MAJOR=16
FROM postgres:${POSTGRES_MAJOR}-bookworm AS builder

ARG POSTGRES_MAJOR
ARG PG_PARTMAN_VERSION=5.5.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       curl \
       gcc \
       make \
       "postgresql-server-dev-${POSTGRES_MAJOR}" \
    && curl --fail --location --silent --show-error \
       "https://github.com/pgpartman/pg_partman/archive/refs/tags/v${PG_PARTMAN_VERSION}.tar.gz" \
       --output /tmp/pg_partman.tar.gz \
    && mkdir /tmp/pg_partman \
    && tar --extract --gzip --file /tmp/pg_partman.tar.gz --directory /tmp/pg_partman --strip-components=1 \
    && make -C /tmp/pg_partman install \
    && rm -rf /tmp/pg_partman /tmp/pg_partman.tar.gz

FROM postgres:${POSTGRES_MAJOR}-bookworm

ARG POSTGRES_MAJOR
ARG PG_PARTMAN_VERSION=5.5.0

LABEL org.opencontainers.image.title="go-scheduler-postgres" \
      org.opencontainers.image.description="PostgreSQL with pg_partman built from pinned upstream source" \
      org.opencontainers.image.version="${POSTGRES_MAJOR}-pg_partman-${PG_PARTMAN_VERSION}" \
      org.opencontainers.image.source="https://github.com/pgpartman/pg_partman"

COPY --from=builder /usr/lib/postgresql/${POSTGRES_MAJOR}/lib/pg_partman_bgw.so /usr/lib/postgresql/${POSTGRES_MAJOR}/lib/
COPY --from=builder /usr/share/postgresql/${POSTGRES_MAJOR}/extension/pg_partman* /usr/share/postgresql/${POSTGRES_MAJOR}/extension/
