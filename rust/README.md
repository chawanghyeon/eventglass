# Eventglass Rust product

This directory contains the existing production implementation and the comparison baseline for the independent [Go implementation](../go/eventglass/README.md). Moving it does not change its SQLite/Tantivy storage format, HTTP behavior, executable name, service configuration, or remote data directory.

- `src/`, `tests/`, `migrations/`: Rust application and persistence contracts.
- `web/`, `schemas/`: product UI and generated API contract source.
- `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `build.rs`, `deny.toml`: pinned build and dependency policy.
- `Dockerfile`: release/container build; **use the repository root as its build context**.

Run Cargo directly from this directory to select its pinned toolchain. `cargo test --locked --test contracts` is a direct example. Shared commands remain at the repository root: `./scripts/check rust`, `./scripts/check web`, `./scripts/check sdk-live`, and `./scripts/build-release`.

From the repository root, build the container with `docker build -f rust/Dockerfile .`. The release script archives the requested Git commit, selects the Dockerfile from that archive, and builds the immutable Rust artifact. It also supports historical commits with the original root Dockerfile. Pre-push staging and the GitHub Deploy activation workflow remain unchanged.

See [development commands](../CONTRIBUTING.md), [operating instructions](../README.md), and [architecture](../docs/observe/architecture.md). Historical documents use crate-relative paths such as `src/` and `tests/`; these now resolve under `rust/`. The attached [source design](../docs/observe/source-design.md) remains byte-identical.
