# Go implementation handoff

Status: v1 implementation contract, subject to its executable acceptance gates;
implementation and release gates are not complete. Product baseline inspected:
`75d9f3c`; first detailed handoff: `0aa3559`. This handoff is intended
for an implementing agent, including GPT Luna, without conversation history.
Do not start by redesigning the system or implementing every gate at once.

Current handoff: architecture/query/Live lifetime hardening, G05 including U4,
M1 compaction, M2 retention/GC, M3's bounded verified Range/block cache, and M4
coordinated PostgreSQL/WAL/S3 recovery are implemented and have scoped executable evidence. U4 passed disposable
PostgreSQL/MinIO integration and the final non-root ARM64 image browser flow. M3
passed real MinIO and pinned-DuckDB cold/warm request and byte assertions for scan,
payload, and reducer child reads. M4 passed an actual pgBackRest base+WAL restore
into two independent PGDATA volumes, full referenced-object reads, signed report
import and missing-object failure. R1's CPU1/512MiB/swap0 containment gate also
passes. R2's autoscale control, fair claims and bounded Kubernetes/KEDA baseline
also pass. The latest current-source official5m/30m/10m R3 profiles at1/2/4
workers completed on `a3a0bca-dirty` with full analytics/payload HEAD
verification and corrected combined-work backlog accounting. Each accepted
220,500 records with zero query failures/conflicts. Rows/histogram p95 were
603/1,361ms,294/513ms and289/569ms; the1-worker run also misses combined
backlog and post-load idle-backlog gates, while2/4 workers miss only histogram
p95. ACK/visibility, Go-unit limits and zero-OOM checks pass. The independent
native oracle passed, but its10M cgroup peak was4KiB above the configured limit.
R3 remains incomplete; reports and run-specific S3, resource and cost evidence
are in quality.md. Independent fixed-work capacity passes on current source
1cf3538: three fresh128-cycle installations per1/2/4-worker profile measured
median124.9/230.5/401.2 records/s, with speedup1/1.846/3.212 and efficiency
1/0.923/0.803. This is bounded publication-capacity evidence, not sustained
mixed-load SLO or steady-state maximum capacity.
The current-source official rerun includes S3 LIST accounting and records
run-specific projected monthly costs of$1,142.59/$589.90/$644.28 at1/2/4
workers. These are not directly comparable bills or a cost improvement claim.
A subsequent
conversion audit reproduced missing shared disk admission, all-partition file
accumulation before upload and a symlink output-validation gap. Their bounded
streaming correction now passes the actual10,000-day ACK/publication boundary
under CPU1/512MiB/swap0, unit/race/native contracts, integration, resource,
browser/crash and the complete10k/100k/1m/10m native oracle. This scoped
correction does not complete sustained-load service validation. A subsequent
query-owned first-page pruning path has unit/native/PG/S3/browser evidence,
retains all catalog HEAD and authority checks, and conservatively falls back for
cursors/filters/insufficient proof. The matched short service profile improves
rows p95=893→312ms and passes the backlog target, but histogram995ms still misses
500ms. Subsequent histogram state reuse reached344/848ms; a diagnostic after
maximum-maintenance hardening regressed to516/1,324ms. Repeating that runtime
with job-boundary diagnostics gives341/949ms, exposing run-to-run variation.
Control now ranks actual bounded compaction prefixes instead of lexical
error/log priority. Its matched20s/300s/90s candidate measures315/714ms with
fewer HEADs; histogram still misses500ms, while RSS/full-GET bytes and post-load
latencies increase. A later joined-cleanup/accounting correction measures416/949ms;
its replacement-worker maintenance counters now pass the independently recomputed
post-load gate, but histogram still fails. Compiler work is outside worker startup
and measured phases. Bounded staging of verified warm aggregate inputs then
measures295/566ms in the same short service profile; histogram still fails500ms.
The isolated same-input native scan improves111.6→62.3ms, with higher RSS, and
service S3 byte costs do not uniformly improve. At that earlier checkpoint, the
clean02c98f2 official one-worker run passed the full native oracle, exact220,500
public records, maintenance accounting and OOM checks, but rows/histogram p95
591/1,737ms failed. It exited1 before2/4 workers. Maximum queried files reached
2,117 and planning/transport overhead grew; retained measurements distinguish
this from the short diagnostic. The latest official1/2/4 profiles are recorded
above; query SLOs fail at every worker count, with the1-worker backlog/drain
gates also failing. The next maintenance-local four-read
bound preserves full SHA verification, ordered inputs and joined cancellation.
Real S3 download microbenchmarks improve, with higher RSS; the same short service
profile changes295/566→303/548ms and still fails histogram500ms. Native maximum-
pair direct/dispatcher, actual S3 failure/retry, full integration including10,000
event days, child contracts and browser checks pass. The following six-cohort selector reduces a real
mixed-size PG/S3/native rewrite149.0→132.0ms and GET2.48MB→92KB, but may leave
more quiet files. Its short-service306/558ms is not better than303/548ms and still
fails histogram500ms; correctness/native/browser checks pass, not R3 completion.
Separately, typed sizing, bounded native
writer fan-in and app-owned retry admission pass two maximum-pair actual-worker
resource executions, with zero OOM or budget overrun but no memory headroom.
These local results are not full-service SLO or release claims. R3
also now has a real one-worker conversion fairness regression: an app-owned
cursor prevents an older queue taking consecutive equally occupied turns after
completion. A separate query cursor/capacity filter passes exact results,
cross-tenant denial and concurrent task caps. Actual1/2/4-process conversion and
query queued-work fixtures now pass three repetitions, recording claim-count
skew and requiring participation by every process; these are not continuous-load
or elapsed-time fairness measurements. Earlier official R3 profiles are
historical only: later1/2/4-worker candidate runs included the paired-payload
HEAD regression and are excluded from current-source SLO evidence. The subsequent
full-verification1/2/4-worker official rerun includes current cost accounting,
but misses query SLOs at every worker count and the1-worker backlog/drain gates.
R3 remains the first incomplete packet. R4 remains open for AWS restore,
rollback, and release evidence; the clean `9c50906` ARM64 image now pins its
Debian package snapshot and verifies the DuckDB 2.0 bundle hash, but its exact
image scan still reports47 HIGH findings. The single-node Garage S3/recovery
contract passes locally.
Physical GC remains
fail-closed whenever the imported rehearsal horizon is missing or stale. Project, key, Issue, retained-occurrence and SDK-outcome
screens now use implemented backend routes, and the ARM64 browser gate exercises
the actual ingest/publication/query runtime. Check `api/implemented-routes.json` before wiring a feature.
Reuse `query.Submission`/`query.Awaiter` for the existing session-authorized
query path. A1 has a separate revision-bound rule principal and creates durable
delivery rows without network I/O. A2 claims and sends those rows with fenced
leases, exact-body signatures, bounded retries, redirect/DNS/IP defenses, and
fail-closed credential handling. Keep HTTP DTOs and result mapping in api.

Query worker/coordinator workflows now belong to query; conversion/publication
workflows belong to ingest. App owns joined role loops and native process runners.
Use `api/capabilities.json` for route/UI coverage, not gate names alone. Production
UI is served by the final image; browser tests no longer depend on a Vite server.

## Reading and authority

1. Read [AGENTS.md](../../AGENTS.md), [DESIGN.md](../../DESIGN.md), and
   [ARCHITECTURE.md](../../ARCHITECTURE.md) in full once.
2. Read this index, [work plan](work-plan.md), and the selected packet's linked
   contracts before editing. Read [CONTRIBUTING.md](../../CONTRIBUTING.md) for
   commands. Use the actual source tree, not proposed paths, to assess progress.
3. Implement the first incomplete packet and its tests. Preserve all earlier
   gates. Commit and push according to the repository workflow.

DESIGN defines behavior; ARCHITECTURE defines ownership. These files specialize
those contracts into implementation decisions. They are normative for new work,
not a report that the described files, commands or endpoints already exist.
If an executable experiment disproves an assumption, record the failing case
and revise the relevant design explicitly; do not quietly weaken correctness.
Historical `docs/observe` material cannot override these contracts.

| Contract | Implementing owner | Contains |
|---|---|---|
| [Control plane](control-plane.md) | control, issues | Schema additions, locks, identities, transaction boundaries |
| [Ingest and publication](ingest-publication.md) | ingest, storage, engine, control | ACK, retries, selection, prepare/publish, Issue transitions |
| [Query execution](query.md) | query, engine, control | Filter grammar, snapshots, tasks, exact merge, Live |
| [API and UI](api-ui.md) | api, web, alerts | DTOs, routes, authorization, UI state, alert delivery |
| [Operations](operations.md) | app, maintenance | Runtime, compaction, retention, backup/restore, resource limits |
| [Work plan](work-plan.md) | Current implementing agent | Ordered small packets, files, tests, release evidence |
| [Worked examples](examples.md) | Tests in each packet | Hand-calculated vectors for selection, filters, groups, paging and recovery |
| [Machine-readable cases](contract-cases.json) | Q1/Q4/M2 tests | Fixed exact arithmetic, rounding, string and retention expectations |
| [Cross-boundary correctness](correctness.md) | Owners named by C01–C09 | Restart-complete preparation, byte verification, exact arithmetic, sessions, bounded planning and recovery |

## Decisions closed by this handoff

- Empty/unsupported-only requests take the same journal/receipt/lane path as
  other requests. Empty publication advances the cut without creating files.
- v1 conversion is one job per accepted batch. Prepared jobs release compute
  and become durable `prepared` state; publication claims them separately.
  Native conversion never waits with a lease for a missing predecessor.
- A conversion file may be smaller than the 32–64 MiB steady-state target.
  Compaction reaches that target; ingestion does not wait for a file to fill.
- Receipt identity and source-event equivalence use different hashes. The
  latter cannot include acceptance ID or arrival time.
- Snapshot identity binds the dataset/filter, not rows versus histogram. Each
  cursor additionally binds its operation and sort. This permits real shared
  snapshots between UI panels without allowing query substitution.
- Retention uses a per-snapshot received-time floor. Physical retirement waits
  for readers, jobs and backup protection. Changing retention cannot resurrect
  already expired data.
- PG jobs remain conversion-specific. Query tasks and maintenance tasks have
  separate, explicit state tables; there is no generic workflow framework.
- Management authentication is implemented before public search is enabled,
  although the login UI arrives later. Ingest keys never authorize reads.
- Documentation specifies all v1 behavior; real-engine resource limits, target
  performance and external-backend recovery remain executable release gates.
- Integer sums/averages use exact bounded partial state; query manifests and
  merge fan-in are bounded too. Retention's persisted floor cannot be undone by
  changing policy. Session reload derives CSRF without rotating another tab.

## Boundary API map

Names below are planned concrete operations, not new abstract repositories.
Public DTOs stay in api; reusable scope/plan/manifest value types live in model
where an engine/control/query import cycle would otherwise arise. Pure types
must not carry SQL, HTTP, process handles or storage clients.

| Operation | Input -> output | Owns |
|---|---|---|
| ingest.Accept | Command -> ReceiptResult/error | Batching, sanitized spool, upload, bounded retry |
| control.Accept | VerifiedBatch + authorization snapshots -> receipts | Entire SQL acceptance transaction |
| control.ClaimConversion / Prepare / ClaimPublication / Publish | Authority + immutable manifest | Leases, selection, catalog and Issue transaction |
| issues.Group / ApplyOccurrence | Canonical error/current state -> pure result | Stable group bytes and lifecycle decisions |
| query.BuildPlan / Execute | Authorized scope + validated specification -> complete result/job | One validator, planner, scheduler and reducer |
| control.CreateSnapshot / RenewSnapshot / ReleaseSnapshot | Principal + dataset -> registered cuts/generations | Reader registration and current authorization |
| engine.Execute | Internal task + supervisor-owned paths -> result manifest | One isolated native task; no public query parsing |
| maintenance.Compact / Retain / Collect / VerifyRestore | Scoped durable task -> outcome | Concrete lifecycle workflows |
| alerts.Evaluate / Deliver | Evaluation/delivery lease -> outcome | Complete-window queries and at-least-once sends |
| app.Run | Validated config + process context -> shutdown result | All goroutines, clients, pools, budgets |

Use error codes, not string matching: invalid_input, unauthorized, forbidden,
revision_conflict, admission_limited, dependency_unavailable, commit_unknown,
lease_lost, generation_changed, corrupt_object, unsupported_format,
resource_exhausted, query_timeout, snapshot_expired, idempotency_conflict.
Internal errors retain wrapped causes for sanitized diagnostics; responses
never include SQL, keys, raw records, URLs containing credentials or stack dumps.

## Non-goals and stop conditions

Do not add a broker, a second transactional database, a search server, generic
repository framework, SDK replacement, or speculative dynamic sharding.
Do not revive Rust, nest a Go module, run AMD64, or execute bundled DuckDB 1.5.
Do not claim a design review proves performance, durability or production HA.
External AWS/backend evidence requires a dedicated authorized test environment;
without it, finish local packets and report the exact unverified gate.

Suggested continuation instruction:

> Read docs/implementation/README.md and work-plan.md. Inspect the current tree
> and select R3 (end-to-end load, SLO and cost evidence) from the actual
> capability gaps, including its negative tests,
> relevant verification, documentation, commit and push. Preserve DESIGN.md and
> ARCHITECTURE.md contracts. Use ARM64 and the pinned DuckDB 2.0 build. Do not
> mark a gate complete from stubs, mocks alone, skipped tests or design text.
