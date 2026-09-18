# Eventglass

Eventglass is being rebuilt as a Go product using PostgreSQL, S3-compatible object storage, and an isolated DuckDB analytics child. The Go module, implementation, tests, and pinned build environment live at the repository root.

The former Rust product is no longer present on `main`. Its final Rust-only state is preserved by the repository tag `rust-version` (`ceb2ed7`) and can be checked out independently if needed.

Current status: G00 engine, storage, migration, and isolation contracts are complete for Linux ARM64. G01 SDK fixture and normalization work is next. The repository does not yet advertise a deployable Go release. See [`DESIGN.md`](DESIGN.md) for the normative contract and [`CONTRIBUTING.md`](CONTRIBUTING.md) for verification commands.

The attached source brief at [`docs/observe/source-design.md`](docs/observe/source-design.md) remains byte-identical. Corrections and the Go architecture are documented separately.
