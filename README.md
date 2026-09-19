# Eventglass

Eventglass is being rebuilt as a Go product using PostgreSQL, S3-compatible object storage, and an isolated DuckDB analytics child. The Go module, implementation, tests, and pinned build environment live at the repository root.

The former Rust product is no longer present on `main`. Its final Rust-only state is preserved by the repository tag `rust-version` (`ceb2ed7`) and can be checked out independently if needed.

Current status: G00 engine/storage/isolation contracts, G01 SDK fixture/normalization contracts, all G02 packets I1–I5 (durable ingestion, bounded runtime, lease fencing, and the Linux ARM64 crash/resource gate), all G03 packets P1–P4 (publication catalog, isolated paired-Parquet conversion, fenced Prepare/Publish workers, Issue lifecycle, and publication crash evidence), and G04 packets Q1–Q4 (recoverable setup and auth, generated v1 wire contracts, typed query compilation, authorized snapshots and signed tokens, plus fenced deterministic query execution and exact aggregation) are complete. Q5 is the first incomplete packet; G04–G08 remain incomplete overall, so the repository does not yet advertise a deployable Go release. See [`DESIGN.md`](DESIGN.md), [`SDK-SUPPORT.md`](SDK-SUPPORT.md), and [`CONTRIBUTING.md`](CONTRIBUTING.md) for the normative contract, verified SDK scope, and commands.

The attached source brief at [`docs/observe/source-design.md`](docs/observe/source-design.md) remains byte-identical. Corrections and the Go architecture are documented separately.

Read [`ARCHITECTURE.md`](ARCHITECTURE.md) for package ownership, request/batch/file
boundaries, resource lifetimes, fencing and upgrade rules. Architecture checks,
multi-request streaming journals, shared byte permits and the ingestion schema
receipt/index policy, the I2 Accept transaction, the I3 verified upload and
rebatching path, and the I4 durable API runtime/lease primitives are implemented
foundations. I5 proves process-crash ACK recovery with empty local scratch; P2
adds verified staging and the isolated DuckDB 2.0 paired-Parquet child. P3 adds
durable output intents, paged Prepare metadata, ordered fenced Publish, Issue
application, and API/worker runtime execution. P4 proves Resolve-cut lifecycle,
multi-lane regression, SIGKILL recovery, scratch-independent publication, and
the initial ACK-to-visible/file-size distribution. Q1 adds hash-only
bootstrap/session authority, stable CSRF, current-grant reload, management key
lifecycle, and deterministic OpenAPI generation. Q2 adds bounded typed filter
IR, CEL and exact JSON adapters, canonical dataset hashing, mandatory snapshot
scope composition, and pinned DuckDB 2.0 semantic contracts. Q3 adds atomic
snapshot admission, revision-bound catalog manifests, monotonic retention floors,
and purpose-separated signed read/cursor tokens. Q4 adds durable fenced query
tasks, deterministic fan-in reduction, isolated native execution, bounded global
Top-K, exact integer aggregation, and histogram completion. Public query/detail
routes, operator surfaces, and backup/PITR recovery remain later gates.
Pure-Go checks do not rebuild DuckDB.

Implementation handoff: [`docs/implementation/README.md`](docs/implementation/README.md)
contains the complete v1 design reading map; the
[`work plan`](docs/implementation/work-plan.md) breaks remaining work into ordered
packets with files, prerequisites, negative tests and completion gates. These are
design contracts, not evidence that Q5–G08 have been implemented or verified.

The handoff also includes [cross-boundary correctness contracts](docs/implementation/correctness.md)
and [fixed machine-readable examples](docs/implementation/contract-cases.json)
for restart recovery, exact arithmetic, sessions, retention and bounded queries.
