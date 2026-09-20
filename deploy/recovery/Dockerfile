FROM postgres:17.11-bookworm@sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0 AS build

ARG PGBACKREST_VERSION=2.59.1
ARG PGBACKREST_SHA256=1cd522afc33b8ff846ef88c55dc238717c9c8817a4f6ca7c9f64887de9c7402d
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates curl libbz2-dev liblz4-dev libpq-dev libssh2-1-dev libssl-dev libxml2-dev libzstd-dev meson ninja-build pkg-config \
    && curl -fsSLo /tmp/pgbackrest.tar.gz "https://github.com/pgbackrest/pgbackrest/releases/download/release%2F${PGBACKREST_VERSION}/pgbackrest-${PGBACKREST_VERSION}.tar.gz" \
    && echo "${PGBACKREST_SHA256}  /tmp/pgbackrest.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/pgbackrest.tar.gz -C /tmp \
    && meson setup /tmp/pgbackrest-build "/tmp/pgbackrest-${PGBACKREST_VERSION}" --buildtype=release \
    && ninja -C /tmp/pgbackrest-build \
    && install -m 0755 /tmp/pgbackrest-build/src/pgbackrest /usr/local/bin/pgbackrest

FROM postgres:17.11-bookworm@sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libbz2-1.0 liblz4-1 libssh2-1 libssl3 libxml2 libzstd1 openssl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/log/pgbackrest /var/lib/pgbackrest /etc/pgbackrest \
    && chown -R postgres:postgres /var/log/pgbackrest /var/lib/pgbackrest /etc/pgbackrest
COPY --from=build /usr/local/bin/pgbackrest /usr/local/bin/pgbackrest
COPY deploy/recovery/pgbackrest.conf /etc/pgbackrest/pgbackrest.conf
