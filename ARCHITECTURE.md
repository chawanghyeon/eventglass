# Eventglass architecture and change ownership

This is the active Go architecture. DESIGN.md defines product behavior and
invariants; this document assigns implementation ownership. CONTRIBUTING.md
defines executable checks. README.md reports completed gates. The original
source brief is immutable historical input, not a competing architecture.

[Implementation contracts](docs/implementation/README.md) specialize this map
into schema, concrete transactions, APIs, runtime protocols and testable packets.
They describe planned work, not completed capabilities.
Cross-boundary correctness contracts make prepared jobs restart-complete, separate
object provenance from current worker authority, and bound metadata/merge work
as well as record processing. Reuse standard big-integer arithmetic only for
bounded aggregate finalization; native DuckDB still owns scans and grouping.

## Deployment and authority

Use one Go module and release image with API, worker and scheduler roles.
Small installations assemble roles together; separate deployments use the
same operations, formats and database. DuckDB executes only in engine-child.
ARM64 is the active target. The pinned DuckDB 2.0 bundle is mandatory.

PostgreSQL owns authorization, receipts, accepted/published lane cuts, jobs,
catalog generations, Issue state and delivery state. S3 owns sanitized
journals and immutable analytics/payload bundles. Local spools, cache and
native spill are disposable and never sufficient to authorize an ACK.

## Package boundaries

| Package | Owns | Permitted internal dependencies |
|---|---|---|
| app | Role assembly, process budgets, task lifetimes, shutdown | All production packages |
| api | HTTP validation, wire decoding, current auth snapshot, response mapping | ingest, sdk, query, alerts, control, model, resource |
| ingest | Normalization/scrub and concrete ingestion workflow | sdk, model, resource, control, storage |
| sdk | SDK wire parsing/version adapters | None |
| model | Canonical values, identities/topology DTOs | None |
| control | Explicit pgx transactions, migrations, claims, catalog | model, issues |
| issues | Pure grouping/lifecycle calculations | model |
| query | Shared scope/plans, CEL lowering, orchestration/merge | model, control, engine, storage, resource |
| engine | Native execution and task protocol | model, storage |
| storage | Journal codec, S3, verified I/O and local cache | model, resource |
| maintenance | Concrete compaction, retention and recovery flows | control, storage, engine, model, resource |
| alerts | Evaluation and delivery workflows | query, control, model, resource |
| resource | Shared byte permits and drain admission | None |
| testkit | Non-durable receivers and test helpers | Test-only consumers |

The architecture test parses all production imports, rejects undeclared
packages/dependencies and prevents testkit or SQL/native drivers leaking into
pure packages. Add packages only when they have a concrete owner. Do not add
generic Service/Repository/Manager layers or package-global mutable clients.

ingest owns the upload-before-Accept workflow; control.Accept owns its entire
SQL transaction. control.Publish owns its complete SQL transaction. Handlers
never assemble these transactions or choose commit policy. Storage performs
I/O and does not decide whether an object is accepted, published or collectible.
Project/key revisions belong to ingest.Command, never canonical records.
Runtime startup/shutdown belongs to app; operations do not spawn detached tasks.

## Units and identity

- NormalizedRequest: one validated and scrubbed HTTP request, one acceptance ID.
- JournalBatch: whole requests for one tenant/lane, potentially multiple projects.
- Receipt: one request's global record range, content hash and accepted selection.
- Publication: one or more contiguous accepted batches with verified output refs.
- Bundle: analytics/payload files; physical file boundaries need not match requests.

Project IDs are installation-wide unique, enforced in SQL. Composite scope FKs
still prevent a valid project or journal from being attached to another tenant.
Record IDs identify candidate occurrences using project/acceptance/item/record.
Source IDs are separate dedupe keys. Accept selects the first committed candidate;
duplicates never reach publication. After dedupe expiry a new acceptance has a
new occurrence ID. An internal retry retains its acceptance ID and all bytes.

Each journal records a batch header, repeated request headers, records and
diagnostics, request checksum footers, and a final request count. Record ranges
are positions within the journal, including well-defined empty ranges.
No raw transport headers or authentication values enter this format.
Streaming replay stages work; a complete successful validation is required
before preparing or publishing output.

## State transitions and ownership

| State machine | Owner and transition | Failure/retry rule |
|---|---|---|
| Intent | workflow registers pending; verified upload records uploaded; Accept/Publish references | New attempt gets a new object key; never overwrite referenced bytes |
| Orphan | GC changes expired unreferenced intent to deleting under lock; deletes; retains tombstone | Late PUT cannot publish; resweep tombstones |
| Request | validate/auth snapshot -> spool -> upload -> Accept -> ACK | Lost reply returns matching receipt on internal retry |
| Batch | accepted -> prepared -> published at contiguous lane cut | Poison input blocks its lane visibly; no silent skip |
| Job | queued -> running conversion -> prepared (unleased) -> running publication -> completed | Each claim increments fence; live authority required on every mutation; durable outputs survive takeover |
| Snapshot | scope/cut registration -> heartbeat -> expiry/release | Pin acquisition and GC exclusion are atomic |
| Restore | quiesce old writers -> restore PG/WAL -> verify S3 refs -> new generation -> resume | Never adopt objects newer than restored PG cut |

The job authority tuple is (installation_id, storage_generation, job_id, fence,
owner). A higher per-job fence alone cannot protect a restored database from an
old worker. Restore must isolate old processes/credentials before reopening.
S3 keys include attempt identity; fencing a SQL write cannot cancel an in-flight PUT.
No S3/network work runs while a database transaction holds locks.
Use project/key -> lane -> dedupe -> job/intent -> Issue -> child-row lock order.
Installation/tenant/current-user authority rows precede that order where relevant;
see the control-plane contract for exact modes and lookup/retry behavior.

A mixed-project microbatch is revalidated under all relevant project/key locks.
If any request became stale, the attempted Accept commits nothing. Rebuild a
new journal for still-valid requests with their original acceptance IDs; return
the appropriate error to revoked requests. Scrub changes require re-normalizing
from still-owned in-memory input or rejecting; stale scrubbed bytes never ACK.
Original uploaded objects remain reclaimable orphans. Never split one request.

## Resource ownership

App injects shared ingress, working-memory, spool and native-task budgets across
handlers and colocated roles. Reservations follow live data through queue,
upload and commit; canceling an HTTP response does not free memory still held
by a background upload. Drain closes admission before waiting for owners.
The current Acceptor call is synchronous: returning means it retains no request
data. An asynchronous implementation must explicitly transfer the permit too.

Ingress reserves wire/decompressed bytes. Before parsing, the handler also
reserves 32 times actual decoded bytes plus 48 MiB for projections and scratch;
legacy decoding reserves for its second expansion. This is a conservative
admission estimate, not measured RSS. Fixed budgets may return 429 for a legal
large request; protocol hard limits and operational admission are distinct.
Journal encoding retains one line and a 1 MiB zstd window, not a second whole
canonical document. Replay verifies compressed checksum through a bounded
reader, rewinds an immutable spool and decodes lines into staging callbacks.

Combined roles do not each receive a full 512 MiB allowance. App must account
for Go heap, native memory, gateway buffers and OS overhead together. Native
memory_limit is not a process RSS ceiling. Linux cgroup failure/kill tests are
required; child-process separation alone is not proof that an OOM spares API.

## Query and frontend ownership

Rows, histogram, aggregate, related records, Live and alerts use one QueryPlan
with mandatory tenant/project/time/snapshot predicates. Backend owns ordering,
paging and exact aggregation. Query coordinator chooses single-worker or
partitioned execution without changing semantics; reducers consume one fenced
winning attempt per partition. Fixed 16 lanes limit publication concurrency per
tenant; worker count is independent. Scale decisions include PG waits and S3
latency so adding workers does not amplify dependency overload.

Frontend uses app assembly, generated API contracts/client, feature folders,
shared UI components and presentation helpers. TanStack Query owns server
state, URL owns shareable search state, component state owns forms/views.
Rows/histogram share absolute time bounds and read token. Clear cached server
state on logout/scope change. Render event text as text; fetch payload on detail.
Generate OpenAPI DTOs when G04/G05 introduce the management/query surface;
the UI/codegen pipeline is not implemented by this architecture correction.

## Compatibility and operations

Migration startup rejects unknown versions, gaps and changed checksums before
applying anything. Runtime readiness must check the supported schema range.
Journal readers reject unsupported versions; future readers dispatch explicitly
by version rather than re-normalizing old records. Use additive schema changes
for rolling upgrades and pin writer versions until all readers can consume them.
An SQL Down section is a tested development rollback, not proof that downgrading
a live product is safe.

Backup readiness means base backup plus continuous WAL plus every referenced S3
object is restorable. Retention/GC consult the oldest recoverable backup and live
snapshot pins. Expose backup/WAL age, stalled lane, oldest accepted job, job error
code, attempt/fence and dependency status. Repair tooling defaults to inspection;
replaying an ACKed batch must use original selection and versions.

Early verification is part of each gate: G02 records request/record/PUT/transaction
counts and maximum-input memory; G03 measures output file distribution and
ACK-to-publication lag; G04 records scan bytes/GET count/cold and warm latency.
G07 adds the complete sustained multiworker/cgroup workload, not the first
performance check. One-/two-/four-worker scaling, throughput and the 512 MiB
target remain unverified until their executable gates pass.
