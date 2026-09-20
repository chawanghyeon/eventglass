# Go implementation handoff

Status: v1 implementation contract, subject to its executable acceptance gates;
implementation and release gates are not complete. Product baseline inspected:
`75d9f3c`; first detailed handoff: `0aa3559`. This handoff is intended
for an implementing agent, including GPT Luna, without conversation history.
Do not start by redesigning the system or implementing every gate at once.

Current handoff: architecture/query/Live lifetime hardening, the complete G05 operator
surface implementation, M1 compaction, and M2 retention/GC are present. U4's local
unit/web evidence passes, but real PG/MinIO and final-image browser execution remains
pending while Docker is unavailable, so G05 is not closed. M3's bounded verified
Range/block cache is connected to actual scan/payload/reducer child reads; its real
MinIO and pinned-DuckDB cold/warm execution remains pending while Docker is unavailable.
M4 is the next backend implementation packet. Physical GC remains fail-closed
until M4 provides a fresh verified backup horizon. Project, key, Issue, retained-occurrence and SDK-outcome
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
> and select U4 (operator completeness) or M3 (next backend packet) from the actual
> capability gaps, including its negative tests,
> relevant verification, documentation, commit and push. Preserve DESIGN.md and
> ARCHITECTURE.md contracts. Use ARM64 and the pinned DuckDB 2.0 build. Do not
> mark a gate complete from stubs, mocks alone, skipped tests or design text.
