# Eventglass

Eventglass is being rebuilt as a Go product using PostgreSQL, S3-compatible object storage, and an isolated DuckDB analytics child. The Go module, implementation, tests, and pinned build environment live at the repository root.

The former Rust product is no longer present on `main`. Its final Rust-only state is preserved by the repository tag `rust-version` (`ceb2ed7`) and can be checked out independently if needed.

Current status: G00–G06 have implemented baselines and scoped executable evidence. G05 connects the complete operator surface; M1–M4 implement compaction, fail-closed retention/GC, verified Range/block caching, and coordinated PostgreSQL/WAL/S3 recovery. Physical GC remains frozen whenever signed recovery evidence is absent or older than 24 hours. R1 passes a Linux ARM64 CPU1/512MiB/swap0 two-minute containment gate at the target logical 100 logs/s+5 errors/s mix, including actual DuckDB conversion/query children, bounded OOM, cancellation join, permit drain and scratch reclamation. R2–R4 (scaling/cost/release) remain incomplete. R1 is not the 30-minute end-to-end SLO, multi-worker scaling, provider cost, or production-HA evidence owned by later packets, and no deployable production release is claimed. [Capability coverage](api/capabilities.json) distinguishes registered APIs from UI and pending features; [work plan](docs/implementation/work-plan.md) owns packet status. See [DESIGN.md](DESIGN.md), [SDK-SUPPORT.md](SDK-SUPPORT.md), and [CONTRIBUTING.md](CONTRIBUTING.md) for behavior, verified SDK scope and checks.

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
Top-K, exact integer aggregation, and histogram completion. Q5 exposes strict
rows, aggregate, detail, snapshot and async-job routes; sync and background work
share durable claims/fences and one native-child gate, while result decoding is
bounded and token-authorized. U1 adds the generated-contract React operator
shell, login/setup, logs/detail and Explore rows+histogram, plus isolated
project/Issue component fixtures,
with URL/server/form state separated and exact Int64 display. U2 adds received-time
Live polling over fresh snapshots, signed per-lane resume checkpoints, bounded
SSE admission, and a reconnecting deduplicating operator view. A1 adds
revisioned Issue/threshold rules, complete-window rule-principal queries,
cooldowns, encrypted destination secrets, and durable delivery outbox creation.
A2 adds generation- and lease-fenced delivery claims, exact-body HMAC signing,
bounded retries, DNS/IP revalidation with pinned connections, and authorized
delivery inspection/manual retry. U3 connects project/key and Issue/occurrence
state, adds SDK outcome and alert/system inspection, and proves an SDK envelope
through durable ACK, publication, safe detail rendering and diagnostics in
Chromium. M1 adds pressure-aware bounded compaction reservations, paired-file
identity verification in the pinned DuckDB 2.0 child, fenced output intents,
and an atomic generation swap that preserves old readers without blocking
unrelated publication. M2 adds received-time retention rewrites and full expiry,
snapshot- and backup-interlocked mark/delete/confirm GC, journal retirement summaries,
an eight-day recovery grace, and late-PUT resweeps. Physical deletion requires an unexpired verified backup horizon; it does not
interpret an empty backup inventory as safety. M3's block-cache gate and M4's
coordinated PostgreSQL/WAL/S3 recovery gate pass on Linux ARM64; sustained
resource and provider release evidence remain open.
Pure-Go checks do not rebuild DuckDB.

Implementation handoff: [`docs/implementation/README.md`](docs/implementation/README.md)
contains the complete v1 design reading map; the
[`work plan`](docs/implementation/work-plan.md) breaks remaining work into ordered
packets with files, prerequisites, negative tests and completion gates. These are
design contracts, not evidence that R1–R4 have been implemented or verified.

The handoff also includes [cross-boundary correctness contracts](docs/implementation/correctness.md)
and [fixed machine-readable examples](docs/implementation/contract-cases.json)
for restart recovery, exact arithmetic, sessions, retention and bounded queries.
