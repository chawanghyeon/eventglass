# Rust implementation scope

Follow the repository working instructions and `../CONTRIBUTING.md`. This directory is the existing SQLite/Tantivy product; do not apply the independent Go/DuckDB architecture here.

Cargo, Rust tests, migrations, UI, and OpenAPI schemas are rooted here. Shared `../scripts/`, `../tools/`, `../deploy/`, and `../docs/observe/` remain repository-root resources. When a historical Rust design document names `src/`, `tests/`, `migrations/`, `web/`, or `schemas/`, interpret it relative to this directory. Do not modify the original source-design attachment to update paths.

Preserve production binary/service names, runtime data paths, durable ACK, and storage compatibility. Docker builds use the repository-root context and `rust/Dockerfile`. Verify path changes through the shared checks, including release-layout tests, rather than relying only on Cargo compilation.
