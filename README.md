# Eventglass

Eventglass is being rebuilt as a Go product using PostgreSQL, S3-compatible object storage, and an isolated DuckDB analytics child. The Go module, implementation, tests, and pinned build environment live at the repository root.

The former Rust product is no longer present on `main`. Its final Rust-only state is preserved by the repository tag `rust-version` (`ceb2ed7`) and can be checked out independently if needed.

Current status: G00 engine/storage/isolation contracts, G01 SDK fixture/normalization contracts, and G02 packets I1–I4 (ingest policy, atomic PostgreSQL Accept, verified upload/bounded batching, and the fail-closed durable API runtime) are complete for Linux ARM64. G02 still requires the I5 crash/resource gate, so the repository does not yet advertise a deployable Go release. See [`DESIGN.md`](DESIGN.md), [`SDK-SUPPORT.md`](SDK-SUPPORT.md), and [`CONTRIBUTING.md`](CONTRIBUTING.md) for the normative contract, verified SDK scope, and commands.

The attached source brief at [`docs/observe/source-design.md`](docs/observe/source-design.md) remains byte-identical. Corrections and the Go architecture are documented separately.

Read [`ARCHITECTURE.md`](ARCHITECTURE.md) for package ownership, request/batch/file
boundaries, resource lifetimes, fencing and upgrade rules. Architecture checks,
multi-request streaming journals, shared byte permits and the ingestion schema
receipt/index policy, the I2 Accept transaction, the I3 verified upload and
rebatching path, and the I4 durable API runtime/lease primitives are implemented
foundations; conversion/publication job execution and crash recovery are still
pending.
Pure-Go checks do not rebuild DuckDB.

Implementation handoff: [`docs/implementation/README.md`](docs/implementation/README.md)
contains the complete v1 design reading map; the
[`work plan`](docs/implementation/work-plan.md) breaks remaining work into ordered
packets with files, prerequisites, negative tests and completion gates. These are
design contracts, not evidence that G02–G08 have been implemented or verified.

The handoff also includes [cross-boundary correctness contracts](docs/implementation/correctness.md)
and [fixed machine-readable examples](docs/implementation/contract-cases.json)
for restart recovery, exact arithmetic, sessions, retention and bounded queries.
