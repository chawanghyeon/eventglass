# Ordered implementation packets and verification

Start from the actual tree; G00/G01 are completed baselines, not instructions
to rebuild native dependencies every packet. G02 packets I1–I5 are complete;
G03 packets P1–P4, G04 packets Q1–Q5, and G05 packets U1 (foundation/component scope), U2, and A1–A2 are complete; U3 is the first pending packet. Do not mark a packet complete until
its listed tests execute successfully. Update this status and README gate status
in the implementation commit, not by making per-packet diary files.

## Working procedure for a smaller-context implementing agent

1. Read handoff index, DESIGN/ARCHITECTURE once, then just selected packet's
   linked contracts and source. Inspect dirty worktree and preserve user changes.
2. State selected packet and its exclusions. Write its failure tests first,
   implement only necessary changes, run focused checks then relevant gate checks.
   Use [worked examples](examples.md) as independent expected values.
3. Keep driver access/package imports within existing architecture guard. Don't
   make tests pass by relaxing a boundary, using in-memory durability or removing
   a negative test. Add concrete dependencies only where packet requires them.
4. Update real API/schema/docs/status and executed evidence. Commit directly
   main with Korean subject; body lists tests actually run and unresolved limits.
   Push without bypassing hooks. Do not claim deploy; no hook deploys production.
5. If a prerequisite test fails, fix it before next packet. Missing Docker/cloud
   access is an explicit incomplete gate, not a skipped-pass. A packet can be
   committed independently while its gate remains pending.

Proposed files below are intentional new files. Names may be split to keep
files cohesive (target<=500 non-generated lines), but ownership/contracts/tests
may not change without a documented design revision. Prefer explicit Go structs
and methods over generic abstractions; one SQL transaction per named operation.

## Packets G02: durable ACK

| ID / depends on | Read | Files to create/extend | Required tests and done condition |
|---|---|---|---|
| I1 / G01 — complete | control-plane G02, ingest hashes | control/migrations/0003_ingest_policy.sql; storage/journal.go; ingest/dedupe.go; model receipt/index DTOs | `TestReceiptSelectionPartition`, `TestDedupeHashIgnoresArrival`, rebatch hash/index/outcome equality, existing journal bytes unchanged; migration prefix/upgrade and tenant FK negatives |
| I2 / I1 — complete | control-plane locks, ingest Accept | control/accept.go, intents.go, receipts.go | Real PG two concurrent same source IDs, cross-lane conflict, source expiry, missing-source no dedupe, empty receipt, whole-batch rollback, seq no holes, stale tenant/key/scrub/config, partial existing receipt subset; no S3 work in tx |
| I3 / I2 — complete | ingest lifetime/intents | ingest/batcher.go, workflow.go; api/ingest.go dynamic auth/result; control/project_auth.go | Real PG+S3 verified upload before commit, duplicate-only/unsupported-only,100ms flush,1000/request/byte thresholds, canceled caller retains permit, upload failure/late PUT/revoke rebatch; exact counters and zero stale ACK |
| I4 / I3 — complete | operations runtime | app/config.go, run.go, resources.go; cmd main; control/jobs.go | Startup fail-closed schema/storage, combined budget, drain30s, lease reclaim/fence, dependency503; fixture handler stays test-only; run starts actual durable ingress |
| I5 / I4 — complete | quality.md, failure matrix below | tests/crash/ingest_test.go; tests/resource/ingest_test.go; scripts/check crash initial cases | Parent ACK oracle survives SIGKILL/restart/empty local disk; max legal and admission-rejected inputs measured; request/PUT/tx/batch counts recorded. **G02 complete only here** |

I1 can add prepared enum before G03 table exists; add nullable prepared_output_id
and FK in P1. Do not run nonexistent later web/scale tests to claim I1 passed.
I4 exposes durable ingress only for explicitly initialized test/CLI installations;
it does not advertise public bootstrap/login before Q1. Initialize test tenants
using isolated fixtures, never automatic production seed accounts.

## Packets G03: publication and Issue state

| ID / depends on | Read | Files | Required tests and done condition |
|---|---|---|---|
| P1 / I5 — complete | control-plane G03, grouping | migrations/0004_publication.sql; issues/group.go,lifecycle.go; model manifest types | Exact fingerprint bytes/slashes/in_app/empty/custom markers/chain; occurrence uniqueness, nullable first/last release; FK negatives |
| P2 / P1 — complete | ingest conversion, operations child | engine/protocol.go,convert.go; app/worker.go; storage verified output manifests | Actual pinned2.0 bulk append/paired schema/identity/hash/bounds, corrupted final journal line discards staged work, wide-date/max-size, zero selected no files, deadline/spill cleanup; no bundled engine execution |
| P3 / P2 — complete | ingest Prepare/Publish | control/prepare.go,publish.go,issues.go; ingest worker workflow | N+1 prepared first, takeover prepared fences, lost Prepare/Publish commit reply, revoked project still publishes, mismatch file pairs fail, generation stale worker, multi-lane Issue count exactly once |
| P4 / P3 — complete | grouping transitions, crash matrix | tests/crash/publication_test.go; tests/integration/issues_test.go | Resolve cut vs ACK backlog vs new accept, ignored no regress, simultaneous lane regress once, all local files removed recovery, ACK-to-visible p95/file-size distribution; **G03 complete** |

## Packets G04: secure query engine

| ID / depends on | Read | Files | Required tests and done condition |
|---|---|---|---|
| Q1 / P4 — complete | api-ui auth/routes, control G04 | migrations/0005_auth_queries.sql; api/openapi.yaml; api/generated; control/auth.go; api/session.go; app routes; codegen tool lock | Real setup race, CSRF/login limits/hash budget, last-admin, cross-tenant grants, disabled scope, key not read auth, secret once, deterministic codegen. Define all v1 DTOs/routes now; only enable implemented routes |
| Q2 / Q1 — complete | query IR/compiler | query/filter.go,cel.go,sql.go,hash.go; model plan types | AST/CEL equivalence, missing!=false, negation, wildcard literals, namespaces/dotted pointers, invalid macros/regex, 38digit integers, Unicode, SQL injection/OR escape; independent Go oracle distinct from compiler |
| Q3 / Q2 — complete | query snapshots/tokens | control/snapshots.go,catalog.go; query/snapshot.go,token.go | Reader/GC lane lock race, RR retry, dataset shared rows/histogram, token purpose/MAC/owner/revision/generation/TTL, empty authorized catalog vs missing object, cursor equal-time ties |
| Q4 / Q3 — complete | query tasks/merges, child protocol | control/query_jobs.go; query/execute.go,merge.go; engine/query.go,reduce.go; app query-worker | Disjoint partitions, same winning attempt twice ignored, coordinator restart/cancel, topK global winner outside all local topK, weighted average, overflow,20k/20k+1 groups,64MiB total cap, empty histogram/negative epoch |
| Q5 / Q4 — complete | api-ui search/detail | api/query_http.go,query_parse.go; api/public_query_service.go,query_results.go; query/submission.go; app/query_sync.go; tests/integration/query_api_test.go | Rows+hist+detail same token, compaction-compatible catalog fixture, no unauthorized trace/detail,202 polling, expired result, whole-result failure; cold/warm bytes/GET/latency report; **G04 complete** |

Use real DuckDB for compiler semantic tests in Q2/Q4; pure compiler goldens alone
cannot prove SQL NULL/decimal/regex behavior. Reuse cached ARM64 native layer.
No UI request may rely on the old fixture Config.PublicKey for management auth.

## Packets G05: operator surface

| ID / depends on | Read | Files | Required tests and done condition |
|---|---|---|---|
| U1 / Q5 — complete | api-ui routes/UI ownership | web package/tool locks, app/router/providers, generated client, shared search/format; scripts/check web | Login/log/explore component contracts; project/Issue mock components only (connected APIs and browser E2E are U3), BigInt display, URL codec,401/403/409/410 states, dataset switch cancels queries; no handwritten generated DTOs |
| U2 / U1 — complete | query Live | query/live.go,live_budget.go; api/public_live.go,query_http.go; web/features/logs | Zero-match checkpoint advances, resume within partly emitted batch, late event-time received now, reconnect duplicates deduped, slow client/revoke/resync; bounded frames/list, HTTP 32-slot/slow-write/disconnect tests; independent worker + real HTTP 205-row partial resume; sustained cgroup load remains G07 |
| A1 / Q5 — complete | api-ui alerts, control G05 | migrations/0009_alerts.sql; alerts/evaluate.go; control/alerts.go; api rules/destinations | Cut barrier concurrent Accept, pending batch not zero, delayed complete window, revision/disable/cooldown/retention-expired window, issue transition exactly one outbox row |
| A2 / A1 — complete | api-ui delivery | alerts/deliver.go,destination.go; control/deliveries.go | Local receiver only: duplicate after lost send reply, signature stable body, retries12, permanent4xx, DNS rebinding/private IPv6/redirects denied, no secret logs, credential rotation fail-closed |
| U3 / U2,A2 | UI routes/system | remaining features and Playwright flows; sdk outcomes API | Real SDK->ACK->publication->UI, error-level log not Issue, breadcrumbs not rows, frame/raw XSS, role enforcement, no external alerts; **G05 complete** |

## Packets G06: safe automatic operation

| ID / depends on | Read | Files | Required tests and done condition |
|---|---|---|---|
| M1 / U3 | operations compaction, control G06 | migrations/0010_maintenance.sql; maintenance/compact.go; control/maintenance.go | Concurrent publication does not starve swap; exact reserved inputs only; identity preserved; crash before/after swap and reader pinned old generation |
| M2 / M1 | operations retention/GC | maintenance/retain.go,gc.go; control/retention.go | Mixed-retention rewrite, snapshot floor stable, widening cannot resurrect, journal protect8days+backup horizon, current/pinned/prepared file never deleted, latePUT tombstone resweep |
| M3 / M2 | operations cache/child | storage/cache.go; integrate existing gateway | Singleflight/pin eviction, SHA corrupt last block, shortRange/changed identity, disk quotas, no default fullGET, canceled child releases pins only after exit |
| M4 / M3 | operations recovery | maintenance/recovery.go; CLI doctor/repair/restore; deploy pgBackRest config/runbook | Actual isolated PG base+WAL+S3 restore, referenced set verification, fresh generation reads old verified files, no newer-object adoption, sessions invalid, outgoing paused, missingfile unhealthy; **G06 complete** |

## Packets G07/G08: measurable release

| ID / depends on | Files/artifact | Required tests and done condition |
|---|---|---|
| R1 / M4 | tests/resource Linux cgroup harness; scripts/check resource | CPU1/512MiB/swap0 profiles, maximum inputs, sustained mixed workload, child OOM/cancel/drain, exact permits/disk reclamation; no growing backlog target |
| R2 / R1 | app autoscale metrics; deploy/kubernetes/KEDA; scripts/check scale |1/2/4 workers, same logical results, PG64 total cap, dependency slowdown no replica storm, scale-in25%, warm min1, tenant fairness |
| R3 / R2 | tests/comparison independent oracle/load/cost report; scripts/check comparison | Fixed10k/100k/1m/10m seeds,5min warmup/30min load/10min drain, required SLOs, whole-installation PG/S3 costs with dated price inputs; **G07 complete if all targets met** |
| R4 / R3 | deploy backend locks; scripts/check release; operator/upgrade guides; SBOM/notices | Authorized AWS + proven selfhost smoke/restore, ARM64 provenance, secret/license scans, schema/journal compatibility and rollback rehearsal, declared SDK matrix; **G08 complete; only then advertise release** |

R4 cannot be closed by MinIO-only tests, a docs-only runbook, mocked S3, a skipped
cloud test or an emulator. If expensive/performance targets miss, state measured
miss and revise implementation; do not relabel target as achieved by design.

## Failure-injection matrix (stable acceptance IDs)

Use test-only failpoint barriers through inherited IPC, never public HTTP/env
backdoors in release binaries. Parent controls exact transition then SIGKILL.
Parent records every fully received HTTP200+receipt before killing child.
Restart reads durable services with **new empty scratch**; compare original
ACKed selections/IDs/Issue counts and scopes exactly. Allow extra records only
from confirmed commits whose response was lost; label them unknown-reply cases,
not missing ACKs. Mocks/fake clock cover branches, not durability evidence.

| Test ID | Barrier / race | Required oracle |
|---|---|---|
| D01 | Intent committed, before PUT | No ACK/reference; orphan eventually deleted |
| D02 | PUT completed, before Accept | No ACK; no query visibility; orphan protected only until expiry |
| D03 | SQL writes before commit | Rollback no lane hole/no winner/no receipt |
| D04 | Accept committed before response | Same internal acceptance returns receipt; one selected occurrence |
| D05 | Response completed then process dies | Every ACK restored from journal/receipt without local files |
| D06 | Revoke/scrub change after upload | Whole tx rollback; valid subset rebatches; stale subset no ACK |
| D07 | GC mark before delayed PUT | Late bytes cannot be accepted/published; resweep deletes |
| D08 | Lease expires during native/upload | Old fence fails Prepare/Publish; winning attempt only |
| D09 | Prepare committed, worker dies | New publisher adopts verified prepared manifest, no lost batch |
| D10 | Publish committed before reply | Unique catalog/occurrence/transition; cut advances once |
| D11 | Resolve between Accept and Publish | Backlog does not regress; post-cut occurrence regresses once |
| D12 | Snapshot vs swap vs GC | Original snapshot exact rows/count/payload or explicit expiry |
| D13 | Query task success response repeated | Reducer counts winning partition once |
| D14 | Native OOM/SIGSEGV/hang | Bounded error/retry, API survival measured, no leaked task/pins |
| D15 | Threshold cut while batch pending | Wait publication, never send false zero-count alert |
| D16 | Webhook accepted, sender dies | Stable ID may redeliver; no exactly-once external claim |
| D17 | PITR behind latest uploaded objects | Only restored PG-authorized set; explicit RPO and generation |
| D18 | Missing/corrupt file or forbidden bucket | Non-success complete result/readiness failure as appropriate |

Run D01..D08 in G02 where applicable, D09..D11 G03, D12..D14 G04/G06/R1,
D15..D16 G05, D17..D18 G06/G08. Each test records failpoint reached and bounded
deadline; a timeout without reaching barrier fails, not a skipped race.

## Traceability and handoff checklist

### Mandatory cross-boundary cases

Read [correctness.md](correctness.md) C sections below before the named packet.
These add acceptance cases to the existing27 packets, not a parallel backlog.
Import [contract-cases.json](contract-cases.json) as fixed expected data; do not
regenerate it from production code. A pure arithmetic model checks design math,
not native SQL, PG locks, browser behavior or real storage durability.

| ID | Contract / packet | Required result |
|---|---|---|
| H01 | C01 / P1,P3,P4 | Prepare then SIGKILL/delete scratch; replacement publisher reconstructs all Issues from durable summaries, no S3 work in SQL transaction |
| H02 | C01 / I2,P3,M2 | Producer live longer than10min preserves early uploaded parts; expired/stale producer does not; prepared refs survive unleased wait |
| H03 | C02 / I3,I5,R4 | Correct Head metadata with wrong stored bytes cannot ACK; incorrect provider checksum rejected; explicit readback profile counts full GET |
| H04 | C03 / Q4 | M+1 in one partition and -1 in another returns M=10^38-1, all partition layouts identical; M+1 final returns422 |
| H05 | C03 / Q4 | avg(M,M)=M.000000000; exact half-even positive/negative ties; native and independent arbitrary-precision oracle agree |
| H06 | C04 / I1,P2,Q1,Q2,A2 | NUL and literal backslash-u are distinct through PG/Parquet/driver/UI; long SDK reason avoids B-tree-key failure; signed webhook retries preserve exact bytes |
| H07 | C05 / Q1,U1 | Reload/multiple tabs get stable CSRF; another session has different token; grant revoke invalidates scope while session can reload current grants |
| H08 | C05 / Q1 | Password reset between hash verification and login commit rejects stale credentials; auth/credential revisions invalidate old scope/session correctly |
| H09 | C06 / Q3,Q4 | Crash during planning cannot dispatch half-plan; takeover uses same snapshot; each metadata cap produces422 without silent first-page selection |
| H10 | C06 / Q3,Q4 | Concurrent submissions across2 APIs respect user/tenant snapshot/query caps atomically; expiry/cancel frees slots exactly once |
| H11 | C06 / Q4,R1 |4,096 partitions yield585 reducers with fan-in<=8; retries counted once; fixed tree preserves rowTopK/all aggregate groups/limbs and stage byte budgets |
| H12 | C07 / Q3,M2 | Widen retention between snapshots cannot reveal a hidden row; backward clock never lowers floor; tick stale>120s rejects new snapshot |
| H13 | C07 / M2,M4 | Retired published batch releases journal FK only after holds; pending batch never does; old backup still protects former journal |
| H14 | C08 / Q1 | Kill before/after marker and after final setup commit; exactly one installation/admin, token consumed once, no network under DB lock |
| H15 | C08 / Q1 | Two different bootstrap bodies cannot share attempt; setup routes reachable while readyz503; bad/missing S3 identity cannot become empty installation |
| H16 | C05 / U2 | Revocation after SSE headers emits forbidden error+close, never attempts late HTTP status; reconnect checkpoint is not misrepresented as rendering ACK |
| H17 | C06 / Q4,M3 | Partitioned worker finishes read after cancellation/release; late output rejected by SQL, no expired complete result, no local permit release before process exit |
| H18 | C09 / I5,P4,R3 | Single-record vs real SDK batches,1/2 APIs; measured journal/bundle/checksum-GET amplification and total costs, no invented large-file efficiency |

Q3 includes the60s persisted retention-floor tick in app/scheduler and initial
floor fields; M2 later adds physical rewrite/GC. Do not defer the logical floor
to G06 while enabling G04 queries with a different retention rule.

### Product coverage

| Required behavior | Contract | Closing packets |
|---|---|---|
| SDK normalization/scrub/unsupported | DESIGN2–4 + existing SDK-SUPPORT | G01 baseline, I1–I5 |
| Durable ACK/dedupe/permissions/leases | ingest-publication + control-plane | I1–I5,D01–D08 |
| Parquet/Issue/resolve semantics | ingest-publication | P1–P4,D09–D11 |
| Exact search/count/detail/paging | query + api-ui | Q1–Q5,D12–D14 |
| Live/UI/auth/alerts/SDK diagnostics | api-ui + query Live | U1–U3,A1–A2,D15–D16 |
| Compaction/retention/backup/repair | operations | M1–M4,D12,D17–D18 |
| Resource bounds/scaling/cost | operations + DESIGN20–21 | R1–R3,D14 |
| External support/release/security | operations + quality.md | R4 |

Before declaring any packet done: check no new unbounded queue/object graph,
scope omitted from SQL/FK, external I/O under transaction, detached goroutine,
retry with changed identity, JSON number precision loss, plaintext secret on
disk/log, ignored native exit/error, or successful stub for missing work. Gate
claims must point to real command output and reproducible tests in the commit.
