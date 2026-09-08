# syntax=docker/dockerfile:1.7
FROM node:22.22.2-bookworm-slim@sha256:9f6d5975c7dca860947d3915877f85607946403fc55349f39b4bc3688448bb6e AS web
WORKDIR /source/web
COPY schemas/ /source/schemas/
COPY web/package.json web/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --ignore-scripts
COPY web/ ./
RUN npm run generated:check && npm run typecheck && npm run build

FROM rust:1.97.1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97 AS build
ARG EVENTGLASS_REVISION=development
ENV EVENTGLASS_REVISION=$EVENTGLASS_REVISION
WORKDIR /source
COPY Cargo.toml Cargo.lock rust-toolchain.toml build.rs ./
COPY migrations/ migrations/
COPY schemas/ schemas/
COPY src/ src/
COPY --from=web /source/web/dist web/dist
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/source/target \
    cargo build --locked --release --features embed-ui,s3 --bin eventglass && \
    cp target/release/eventglass /eventglass

FROM scratch AS artifact
COPY --from=build /eventglass /eventglass

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /eventglass /usr/local/bin/eventglass
RUN mkdir /data && chown 10001:10001 /data
USER 10001:10001
ENV EVENTGLASS_ADDR=0.0.0.0:8080 \
    EVENTGLASS_DATA_DIR=/data \
    EVENTGLASS_BASE_URL=http://127.0.0.1:8080 \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/eventglass"]
CMD ["serve"]
