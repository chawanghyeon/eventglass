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
| api | HTTP validation, wire decoding, current auth snapshot, public result/SSE mapping | ingest, sdk, query, alerts, control, model, resource, engine (protocol values only) |
| ingest | Normalization/scrub, Accept, conversion staging and publication workflows | sdk, model, resource, control, storage, engine (protocol only), issues |
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

`ingest/conversion.go` owns replay/selection/Issue summaries; `ingest/publication.go`
owns verified conversion output and publication orchestration. `query/worker.go`
and `query/coordinator.go` own durable query work. Process runners stay in app;
these operation packages receive small consumer interfaces, not app imports.
Maintenance shares its verified download/upload implementation in `files.go`.
`app.superviseTask` is only a join/cancel/heartbeat primitive, not a generic
job state machine. Transaction and authority rules stay in control.

Conversion's supervisor consumes one framed native output pair at a time:
app verifies the files, invokes ingest's upload/manifest consumer, removes the
consumed pair, and acknowledges the child before its next COPY. Engine owns
only native execution and the bounded frame codec, never uploads or PG policy.
Ingest reserves the shared disk allowance before downloading/staging and keeps
it through runner join and task-directory cleanup; app wires that same budget
used by query, maintenance, and cache. Cancellation cannot release the native
gate or delete files while the consumer is still using them.

Generated HTTP DTOs may be imported only by api, never by app/query/control.
`api.QueryAdapter` owns public result decoding and token/HTTP translation;
`query.Submission` owns catalog verification and durable plan submission, and
`query.Awaiter` waits for authoritative completion without running tasks.
Query owns deterministic pre-seal partitioning by file count, compressed bytes
and serialized manifest size. Oversized metadata groups split before task IDs
are sealed; neither app dispatch nor worker retry replaces a sealed partition.
Query may conservatively prune first-page constant-true row scans after full
catalog verification. Control derives complete requested-project coverage in
the authorized catalog read; the proof is planning-only and not serialized into
worker manifests. Query reconstructs the exact cursor-free operation and proves
the limit-plus-one bound from complete file scope/time/cut metadata. It retains
all equal-time candidates and uses the same path for submission and takeover.
`app.DurableQuerySyncExecutor` is an optional colocated helper, not a requirement
for sync/detail/Live. API-only and combined roles use the same durable protocol.
The api export interface accepts engine protocol values; app injects the
shared-budget child runner. No native driver or process launching moves into api.
Frontend guards reject shared/api dependencies on features/app. The executable
route inventory is `api/implemented-routes.json`; OpenAPI includes future routes
and is not a capability claim.
`api/capabilities.json` distinguishes registered APIs, connected/read-only UI,
and missing surfaces. Architecture checks enforce route coverage and existing
test references; they do not certify the assertions or runtime gate results.

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
Conversion, query execution, and public-result export in one runtime share one
cancellation-aware native-child gate, so sync help cannot overlap the
background worker's isolated DuckDB process.
Combined-role children reserve 192 MiB from the same working budget used by
decoding; API-only exports reserve 64 MiB. Admission waits are cancelable and
permits remain owned until the native child has exited, not just until timeout.
Interactive/Live and alert submission, plus coordinator takeover, reserve64MiB
from that same working budget before loading catalog metadata. Query owns the
reservation through joined HEAD verification, plan sealing and failure cleanup;
app injects the budget and API maps local admission failure to retryable429/503.
The16MiB catalog serialization ceiling is checked page-by-page before HEADs,
in addition to the final exact plan/manifest ceilings. The64MiB reservation is
a conservative admission estimate, not a measured worst-case RSS guarantee.
Publication and delivery have joined loops separate from native work, so a
long query cannot block their dispatch. App dispatches retention, compaction and
GC only after an empty foreground sweep and through one measured spare-time
budget. A fixed61-bucket rolling60s history credits actual idle intervals, not
startup age, dependency-error backoff or a shared native helper's occupied time.
Maintenance debits all claim/execution/join time, including failures; admission
requires M<=I/4, equivalent to20% of spare lane wall time I+M. This is measured
dispatch capacity, not a claim about native CPU utilization or tenant fairness.
Credit expiring during a granted slice is excluded, cancellation reserves100ms for
join, and an overrun remains charged and explicitly fails comparison evidence.
Foreground work is rechecked before another maintenance step; maintenance
backoff never sleeps ready foreground work. Control rechecks foreground pressure
inside each compaction/retention claim, without consuming a denied claim's
fence/attempt or releasing its reserved inputs. GC's backup/snapshot interlocks
are unchanged. Candidate selection excludes lanes with active maintenance so
an unreservable lane cannot hide other eligible work; reservation still resolves
concurrent schedulers through the existing lane lock and unique constraint.
Control ranks each eligible partition's bounded input prefix by expected file
reduction, then rewrite bytes, rather than letting lexical kind/lane order
starve another partition. The existing32MiB target, minimum8 small inputs,
128-input/256MiB ceilings and deterministic within-partition order remain;
at most128 candidate rows cross from PostgreSQL into Go. This is a selection
heuristic, not a larger maintenance grant or a cross-tenant fairness guarantee.
Bounded operation logs report counts, failures and duration
without raw error text.
The same fixed role labels expose call/work/failure and elapsed/work-time
counters on the private metrics endpoint. A bounded native query burst keeps
its ready-work slots, but an empty claim is checked only once per scheduling
sweep; other progress or the idle interval starts a fresh sweep. These are app
dispatch rules, not replacements for control's durable claims or query's work.
App also owns native process groups: TERM, escalation to KILL after100ms, leader
join and descendant reaping precede permit/scratch release. Linux uses a
subreaper and waits only on the task's private group, never another command's
children; Darwin waits for the terminated group to disappear while init reaps
orphan PIDs. A successful leader which
leaves descendants is a failed child contract, not successful engine output.
Query and maintenance reserve input/output/spill against the shared disk budget
and release only after child completion and successful scratch removal. This
conservative reservation is not a proof of filesystem hard limits or RSS.
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
The reusable Logs/Explore workspace belongs to `features/search`; Live transport
belongs to `shared/search`. Cursor pages use `shared/query/useCursorPage`, carry
AbortSignal and reset when scope changes. Project-scoped operator pages select
one project explicitly; they do not silently use the first 100 projects as an
all-project scope. Keys are loaded on expansion, not for every project card.
Rows/histogram share absolute time bounds and read token. Clear cached server
state on logout/scope change. Render event text as text; fetch payload on detail.
Generate OpenAPI DTOs when G04/G05 introduce the management/query surface;
U1 implements the generated-contract React/Vite pipeline and its deterministic
typecheck/test/build gate. Live and later operator features extend the same
state boundaries rather than adding a second frontend store.

`web/src/shared/search/session.ts` owns snapshot/job lifetimes for one mounted
dataset. It retains only resource identities/tokens, never a duplicate record
store. Sort/projection belong in operation query keys, not dataset identity.
Rows, histogram and heartbeat reuse the snapshot; teardown releases it and
cancels owned jobs, including late submission responses. A mount-specific key
prevents cached tokens from surviving their owner. Detail has its own bounded
owner when opened without a parent snapshot. Failed newly-created snapshots
are released by the API; existing caller-owned snapshots are not.
Session revocation atomically releases that session's pins, including pins whose
late response the browser never received. Other browser sessions keep theirs.

Live fixes its received-time lower bound in the signed checkpoint. Ordinary
current-cut streams use sequence positions, not a moving 15-minute filter.
Each incomplete drain retains a captured upper cut and immediately reads its
next bounded page; new publications cannot starve later lanes in that drain.
An unchanged authorized cut skips query/native/S3 work. Authorization still
runs on every poll and after result export. Per-write deadlines are cleared
between writes; they do not limit the heartbeat interval.
Conservative per-lane minimum sequence pruning avoids scanning completed files;
partial batches and compacted files spanning the boundary remain eligible.

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

## Change recipes

| Change | Edit owners | Required negative evidence |
|---|---|---|
| SDK item/normalization | sdk adapter, ingest normalizer, canonical model | Pinned capture, scrub/size/unsupported behavior; no HTTP or PG in sdk |
| Query operator | query validator/compiler, engine semantic tests, generated API if wire changes | Missing/null/type truth table, scope escape, exact distributed oracle |
| Native task | Concrete ingest/query/maintenance workflow, app runner, control authority | Cancel joins child, disk admission, expired fence, restart-complete output |
| Operator screen | API contract/route/capability entry, feature queries/forms | Current auth/CSRF/revision, next page, scope cancellation, real browser flow |
| Storage backend | storage adapter and backend profile | Byte checksum, Range, late PUT/delete, coordinated restore; no relaxed verification |

Avoid broad package churn for a feature. A new package needs a concrete owner,
dependency rule and tests; interfaces belong to their consumers. Do not duplicate
wire DTOs, weaken invariants for throughput, or infer completion from file counts.
