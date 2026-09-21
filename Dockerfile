FROM golang:1.27.1-bookworm@sha256:69a7b9788769bec032d238959b61854e9ae87f57be9029ec04e9885fabf99195 AS duckdb-build
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates cmake curl git libcurl4-openssl-dev libssl-dev ninja-build python3 \
    && rm -rf /var/lib/apt/lists/*
ARG DUCKDB_COMMIT=800d6b748774cc958731a14df7b9cb170b50c075
ARG DUCKDB_VERSION=v2.0.0-dev84020
ARG DUCKDB_SOURCE_SHA256=226c9627c9c9379f6b1725e387339fd465e18c72a3dbeb6b65671eeb8cb29b5c
ARG DUCKDB_BUILD_JOBS=2
RUN curl -fsSLo /tmp/duckdb.tar.gz "https://github.com/duckdb/duckdb/archive/${DUCKDB_COMMIT}.tar.gz" \
    && echo "${DUCKDB_SOURCE_SHA256}  /tmp/duckdb.tar.gz" | sha256sum -c - \
    && mkdir /duckdb \
    && tar -xzf /tmp/duckdb.tar.gz -C /duckdb --strip-components=1 \
    && rm /tmp/duckdb.tar.gz
WORKDIR /duckdb
RUN CORE_EXTENSIONS='icu;json;parquet;httpfs' \
    DUCKDB_COMMIT="${DUCKDB_COMMIT}" \
    DUCKDB_VERSION="${DUCKDB_VERSION}" \
    ENABLE_EXTENSION_AUTOLOADING=0 \
    ENABLE_EXTENSION_AUTOINSTALL=0 \
    DISABLE_SHELL=1 \
    DISABLE_CPP_UNITTESTS=1 \
    CMAKE_BUILD_PARALLEL_LEVEL="${DUCKDB_BUILD_JOBS}" \
    make bundle-library

FROM golang:1.27.1-bookworm@sha256:69a7b9788769bec032d238959b61854e9ae87f57be9029ec04e9885fabf99195 AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends libcurl4-openssl-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
# The static build never imports the adapter's bundled engines for any platform.
# Keep their module checksums, but do not download those unused native archives.
RUN GOGC=off go mod download $(go list -m -f '{{if .Version}}{{.Path}}@{{.Version}}{{end}}' all \
    | sed '/^github.com\/duckdb\/duckdb-go-bindings\/lib\//d; /^$/d')
COPY . .
COPY --from=duckdb-build /duckdb/build/release/libduckdb_bundle.a /opt/duckdb/lib/libduckdb_bundle.a
ENV CGO_ENABLED=1 \
    CPPFLAGS=-DDUCKDB_STATIC_BUILD \
    CGO_LDFLAGS="-L/opt/duckdb/lib -lduckdb_bundle -lcurl -lssl -lcrypto -lstdc++ -lm -ldl -lpthread"
RUN GOGC=off go build -tags=duckdb_use_static_lib -trimpath -ldflags='-s -w' -o /out/eventglass-go ./cmd/eventglass-go

FROM build AS test
ENV EVENTGLASS_TEST_BINARY=/out/eventglass-go \
    GOGC=off
RUN go test -tags=duckdb_use_static_lib -count=1 ./...
RUN go vet -tags=duckdb_use_static_lib ./...

FROM node:24.21.0-bookworm-slim@sha256:0e0ff40c39bc087845bfb27465a0df4ea419520094bc35842ff83dd8cbe6f9b6 AS web-build
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm install --global npm@11.19.0 --ignore-scripts && npm ci --ignore-scripts
COPY web/ ./
RUN npm run build

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libcurl4 libssl3 libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --home-dir /nonexistent --shell /usr/sbin/nologin eventglass
COPY --from=build /out/eventglass-go /usr/local/bin/eventglass-go
COPY --from=web-build /web/dist /usr/share/eventglass/web
ENV EVENTGLASS_WEB_DIR=/usr/share/eventglass/web
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/eventglass-go"]
