# Ordered implementation packets and verification

Start from the actual tree; G00/G01 are completed baselines, not instructions
to rebuild native dependencies every packet. G02 packets I1–I5 are complete;
G03 packets P1–P4, G04 packets Q1–Q5, G05 packets U1–U4 and A1–A2, M1–M4,
and R1–R2 have implementations and scoped tests. R3 remains the first incomplete
packet. Historical5m/30m/10m ARM64 profiles from a dirty source that skipped
payload HEAD verification for analytics queries remain excluded. The first-page
pruning integration contract caught the bypass; that optimization was removed
and paired catalog verification restored. The latest corrected current-source
5m/30m/10m profiles completed at1/2/4 workers on `a3a0bca-dirty`; all accepted
220,500 records with zero query failures/conflicts. Rows/histogram p95 were
603/1,361ms,294/513ms and289/569ms. The1-worker run also missed combined
backlog and post-load idle-backlog gates;2/4 workers missed only histogram p95.
ACK/visibility, Go-unit limits and zero-OOM checks passed. The independent
native oracle passed, but its10M cgroup peak was4KiB above its512MiB limit.
These are failed G07 results, not a release claim. The current-source fixed-
work capacity on1cf3538 passed three isolated128-cycle runs per worker count at
median124.9/230.5/401.2 records/s (speedup1/1.846/3.212; efficiency1/0.923/
0.803); this is publication-capacity evidence, not a replacement for R3 mixed-
load SLOs. See quality.md for complete historical metrics and the invalidation.
An audit then found those official backlog samples omitted active maintenance
tasks and their reserved inputs. Their reported slopes/max/final therefore
under-counted total durable work; the latest official profiles above use the
corrected combined-work gate.
An earlier corrected current-source one-worker official rerun measured
rows/histogram p95=689/1,638ms and total backlog slope+16.652/min (load peak852,
final0 after55.449s drain); both query gates and the load-backlog gate fail.
Histogram p95 is based on179 samples and1,750 objects/7 scan tasks at p95;
all179 samples used cached verified bytes. The full S3/WAL/resource/cost and
compaction-reservation evidence is in quality.md. Do not close R3 on this
one-worker result; pursue bounded scan/compaction efficiency without relaxing
authorization, integrity, worker or memory ceilings.
An official one-worker1024-file scan-cap experiment was rejected: it reduced
histogram p95 scan tasks7→5 but rows/histogram p95 regressed to6,332/6,614ms,
visibility p95 to841,445ms, and combined backlog slope to+623.420/min, ending
with18,638 items after600,285ms drain. ACK, accepted count, resource/OOM and
independent fixture oracle passed; query, visibility, maintenance progress,
published count and drain/post-load gates failed. The production bound remains
256. Fresh-install maintenance trajectories prevent a causal claim; the run is
strong evidence not to adopt this candidate. Full measurements are in
quality.md; R3 remains incomplete.
An official catalog metadata-fanout16 experiment was not adopted; the
production fanout remains8. Its one-worker report accepted220,500 records but
missed query/visibility/backlog targets (overall query p95 7,904ms, visibility
p95 849,387ms, backlog slope+637.215/min and18,770 outstanding after600,123ms),
with three client/decode query failures. The run had zero cgroup OOM events.
The two-worker run stopped before report serialization when the30-minute load
schedule was8.46s late; the four-worker run stopped before report serialization
on HTTP429 `admission_limited` at sequence425. These are failed/incomplete
samples, not a causal attribution. Retained one-worker evidence and exact
limits are in quality.md; R3 remains incomplete.
A separate one-worker `20s/300s/90s` catalog-page diagnostic accepted33,600
records at both256 and1,024 files/page but regressed rows/histogram p95 from
538/1,420ms to1,811/3,043ms and combined backlog slope from+84.425 to+266.422
items/min. It is not an official G07 result and independent maintenance
trajectories prevent a causal claim; the1,024 candidate was rejected and the
production limit remains256. The bounded-cursor, every-object verification and
real PostgreSQL256/257 boundary tests pass `./scripts/check integration`; exact
S3/WAL/resource evidence and ARM64 conversion results are in quality.md. R3
remains the first incomplete packet.
M2 physical GC stays frozen whenever M4's signed coordinated backup attestation is absent or older than 24 hours.
Do not mark a packet complete until
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
| U3 / U2,A2 — connected subset verified | UI routes/system | project/Issue/SDK-outcome screens and Playwright flows | Real SDK->ACK->publication->UI, error-level log not Issue, breadcrumbs not rows, frame/raw XSS, role enforcement, no external alerts; does not close missing system/user/editor surfaces |
| U4 / U3 — complete | api-ui complete operator contract; api/capabilities.json | User/membership APIs and UI, password UI, alert/destination editors, delivery retry UI, GET /v1/system and installation-admin retention API | Pure Go and frontend contracts cover authority/revision/CSRF/pagination wiring and show cuts/limits/backup degradation. Disposable PostgreSQL/MinIO integration and the final non-root ARM64 image Playwright flow pass; **G05 complete**. |

## Packets G06: safe automatic operation

| ID / depends on | Read | Files | Required tests and done condition |
|---|---|---|---|
| M1 / U3 — complete | operations compaction, control G06 | migrations/0010_maintenance.sql; maintenance/compact.go; control/maintenance.go | Concurrent publication does not starve swap; exact reserved inputs only; identity preserved; crash before/after swap and reader pinned old generation |
| M2 / M1 — complete with M4 interlock | operations retention/GC | maintenance/retain.go,gc.go; control/retention.go; migrations/0012_gc_interlock.sql | Mixed-retention rewrite, snapshot floor stable, widening cannot resurrect, journal protect8days+backup horizon, current/pinned/prepared file never deleted, latePUT tombstone resweep; unknown/stale backup health freezes deletion; stale GC attempts cannot confirm; completed producer FK cleanup and expired swap leases tested |
| M3 / M2 — complete | operations cache/child | storage/cache.go; integrated capability gateway and durable query block manifests | Pure tests cover singleflight/pin eviction, corrupt cached blocks, short/changed ranges, restart reuse and shared disk quotas. Scan, payload and reducer inputs have no default full GET; pins release after the joined child returns. Pinned-DuckDB/MinIO cold/warm assertions prove one cold Range read and no additional warm Range read. |
| M4 / M3 — complete | operations recovery | maintenance/recovery.go; CLI doctor/backup/restore; deploy/recovery; docs/operations/recovery.md | Actual isolated PG base+WAL+S3 restore verifies WAL-only rows and the restored authority's complete object set. Signed rehearsal import refreshes live GC evidence without changing its generation. Activation increments generation, invalidates sessions, leaves outgoing alerts paused, reads verified old files, never adopts newer objects, and missing files remain unhealthy; **G06 complete**. |

## Packets G07/G08: measurable release

| ID / depends on | Files/artifact | Required tests and done condition |
|---|---|---|
| R1 / M4 — complete | tests/resource Linux cgroup harness; scripts/check resource | CPU1/512MiB/swap0 profile verifies maximum input, two-minute 100 logs/s+5 errors/s logical mix through normalization and actual conversion/query children, no growing cycle backlog, cgroup OOM=0, bounded child OOM/cancel/join, exact permit drain and zero scratch residue. This is containment evidence, not R3's 30-minute end-to-end SLO. |
| R2 / R1 — complete | app autoscale metrics/control; deploy/kubernetes/KEDA; scripts/check scale | Private fixed-label backlog metrics and storage availability, EWMA prior, two-sample scale-out, dependency freeze, 300s stable scale-in capped at25%, warm min1/max20 and PG64 total bound. Conversion/query claims prefer tenants without running work. Linux ARM64 1/2/4 control harness produced the same checksum; manifests enforce non-root/read-only/CPU1/512MiB/bounded scratch and KEDA timing. This is control evidence, not R3 throughput. |
| R3 / R2 — implemented; current-source SLO gate fails | tests/comparison independent oracle/load/cost report; scripts/check comparison | The corrected5m warmup/30m load/10m drain, SIGKILL/restart, cold/warm/idle, per-role ARM64 cgroup, whole-installation PG/S3 accounting and independent native oracle are implemented. Latest current-source profiles on `a3a0bca-dirty` accepted220,500 records each with zero query failures/conflicts; rows/histogram p95 were603/1,361ms,294/513ms and289/569ms at1/2/4 workers. The1-worker run also misses combined-backlog and post-load idle-backlog gates;2/4 workers miss only histogram p95. ACK, visibility and zero-OOM checks pass. The10M oracle peak exceeded its512MiB limit by4KiB despite zero OOM; do not report it as under limit. Full S3/WAL/resource/cost and per-query evidence is in quality.md. Different submitted-envelope hashes make cross-worker deltas non-causal. Fixed-work capacity on1cf3538 passes nine isolated128-cycle installations at124.9/230.5/401.2 records/s, but is not a substitute for mixed-load SLOs. **G07 remains incomplete.** |
| R4 / R3 — external release gate open | deploy backend locks; release verification; operator/upgrade guides; SBOM/notices | Fresh local `./scripts/check recovery` passes real disposable PostgreSQL base/WAL+S3 restore and missing-object fail-closed; fresh ARM64 browser E2E, the documented pinned live SDK matrix, and changed-file secret hook pass. Actual AWS identity/restore, signed release provenance, image/dependency security and license scans, and deployment rollback rehearsal remain unverified. **G08 remains incomplete; do not advertise a release.** |

R3 remains the first incomplete packet. A further 20s/300s/90s ARM64
one-worker diagnostic recorded 646 planned files at the end of its 32-file
scan-partition run. Aligning the planner and native bounds at 128 files per
scan preserved the byte/manifest/child limits and completed all 59 searches;
rows/histogram p95 changed from 1,750/1,951ms to 854/1,068ms, while
visibility p95 was 13.605s and backlog still grew +27.34 jobs/min. This is
diagnostic evidence with different compaction trajectories, not a G07 pass.
The next same-profile pair suppressed repeated empty query claims in app's
bounded worker sweep: rows/histogram p95 changed1,580/1,834ms to766/789ms,
visibility9.953s to6.022s, and backlog slope+14.35 to+9.66 jobs/min. All59 queries
completed, but query/visibility/backlog targets still miss. Request bytes and
sampled memory increased; full metrics and limitations are in quality.md.
PostgreSQL regression coverage additionally exposed and fixed recovery rollback
on a no-work claim, including retry exhaustion and query-specific help; this
transaction policy stays in control, separate from app scheduling.
The native converter now reuses eight typed stage columns without changing the
Parquet schema. A same-limit local benchmark measured one/100-record batches
at117.043/132.552ms before and114.337/127.518ms after, but a repeat pair's ranges
overlap, so no stable speedup is claimed. A slower broad JSON rewrite was
rejected. Exact types/NULLs/large integers and real PG/S3/browser flows pass,
but this is not evidence that the end-to-end R3 targets now pass.
Detailed pinned-engine profiling subsequently identified repeated attribute-type
binding. Defining that same STRUCT once per private conversion connection reduced
local one/100-record median conversion time from113.574/127.067ms to85.072/97.945ms
under the same CPU1/512MiB limits. Output inspection and JSON semantics remain;
no memory saving or end-to-end SLO pass is implied. See quality.md for allocations,
cgroup peaks and scope of the measurement.
The following equal20s/300s/90s PG/S3 pair measured backlog slope+10.24→−3.32/min,
visibility5.235→2.775s and drain6.014→1.007s with all60 searches complete.
Rows/histogram p95 remain576/631ms, above500ms, so R3 is still incomplete.
The next256-file scan candidate passed all targets in the same20s/300s/90s
diagnostic: rows/histogram p95=362/457ms, visibility869ms, backlog slope−1.26/min,
all60 searches complete and final backlog0. It keeps the64MiB scan and1MiB task
limits; metadata-heavy groups split deterministically only before plan sealing.
At that checkpoint the official5min/30min/10min1/2/4-worker evidence was still
required; later corrected profiles are summarized at the top of this plan and
in quality.md. A separate
late-query scheduling experiment did not improve the targets and was discarded.
Completion audit also found missing R3 evidence in the current harness: the
`ColdRegexMS` request does not clear caches and still uses the last15min window;
`DatasetSHA256` identifies the independent fixture definition, not the actual
transmitted workload. Receipt totals are not a full published/query-result
oracle, and equal offered load at1/2/4 workers does not establish increasing
independent throughput or scaling efficiency. Add real cold/all-history and idle
checks, actual input provenance and native/result oracle comparisons, plus a
capacity/efficiency measurement before closing G07, even if the SLO target map
passes. Preserve existing measurements with their original scope/limitations.
The next harness increment records actual submitted envelope SHA/count/bytes
separately from the fixture-definition hash, includes retries/duplicates, and
requires the public aggregate API's full-run per-kind counts. A20s/60s/90s
ARM64 diagnostic verified8,000 logs+400 errors and all12 mixed queries, but
rows/histogram p95=624/592ms and backlog slope+2.11/min still miss. This is a
count oracle for that actual workload, not the missing four-size typed/filter/
identity oracle, cold/all-history/idle phases or official-duration capacity pass.
A second fresh diagnostic of the same product image, after aligning PG/WAL
measurement with final query work, passed its short target map at rows/histogram
236/359ms, visibility776ms and backlog slope−1.05/min. It still cannot close
R3; no product change explains the different compaction/task trajectory.

The harness now replaces every isolated worker/tmpfs after drain, verifies new
container identities and empty block caches, then compares all-run regex rows
on the same snapshot and observes idle query work. Actual quick1/2/4-worker
post-load phases pass, including cold Range128/131/116 versus warm0/0/0.
Resource evidence now requires sampled PG/S3 and Go containers plus cgroup peak,
OOM and memory/swap limits for every worker incarnation; missing evidence fails.
The4-worker short run still fails load-backlog slope(+0.132/min versus<=0.1),
even though its cold/warm/idle/resource checks pass. The runner preserves that
failure and continues those independent phases. Official-duration cold/idle,
the actual four-size native oracle and capacity/efficiency remain unverified;
neither fixed offered load nor quick cache checks close R3.
The final1-worker repeat also verified all three cgroup incarnations and cold/
warm Range117/0, but retained a load-backlog failure(+4.42/min). All three
worker counts now have actual post-load/resource collector evidence, not a
complete official R3 result. See quality.md for all successful and failed runs.

Mode A now has an actual SDK-byte → normalization → verified journal replay →
native Parquet → existing query scan/reduce oracle (`scripts/check native-oracle`),
required by the official comparison command. It uncovered a reproducible10k-row
conversion OOM: repeated parsing of the full stage JSON exceeded192MiB. Shared
path-list extraction fixes that case without changing native/cgroup limits,
schema, publication ownership or record identity. Exact scalar/NULL and large
batch regression tests cover the change. The original1m typed-attribute scans
exhausted both192MiB and the separate worker's256MiB native profiles. Row-local
list projection now removes correlated attribute materialization without
changing limits or scan partitioning, and rejects duplicate stored paths before
type selection. Exact accumulator tests cover projected numeric operands.
The fresh2026-09-22 actual10k/100k/1m/10m oracle passes all typed/time/count,
partition identity and equal-time page checks (`.tools/native-oracle.JAxTts`);
1m/10m elapsed106.241s/1,053.615s, cgroup OOM/kills0. The10m cgroup peak was
536,879,104bytes (reported8KiB above its configured512MiB maximum),
including filesystem cache;2,684 memory-limit events were recorded. This is
functional Mode A evidence, not a worst-case RSS guarantee or official G07 pass.
The subsequent cleanf415e55 official-duration1/2/4-worker runs completed with
zero final backlog, conflicts and OOM; rows/histogram p95 were378/451,
331/414 and327/404ms. Their old target maps pass, but each ACK population has
12,600 samples instead of10,800 because warmup was included. A maintenance
audit also found no enforced spare-time budget and stale claim admission.
New regressions reproduce both defects before the fix, including12 actual PG
queued/prepared/expired-lease pressure cases. Corrected official measurements,
independent capacity/efficiency and R4 remain outstanding. Preserve the old
failed profiles alongside this limited baseline; details are in quality.md.

The initial budget implementation is not a closed gate. A matched20s/300s/90s
one-worker diagnostic with100ms native TERM grace and active-lane exclusion
still has rows/histogram p95=1,441/1,856ms, load slope=+0.645/min and five
maintenance-budget overruns, despite exact33,600 published records, no OOM and
zero final backlog. Retain `.tools/comparison-report-1.xeG5dP` as failure
evidence. The bounded control loader now removes per-input SQL round trips
(128 inputs:260→5 queries, matched median95.982→13.730ms). This audit also
reproduced and fixed retention's incorrect two-input minimum; a real
ingest→publication→compaction→mixed/full-expiry native test supplements the
earlier synthetic manifest coverage. Native input verification now uses two
bounded ordered scans with per-input identity/scope checks, replacing three
queries per input and discarded input full-hash calculations. The matched
128-small-input native microbenchmark improved from1,509.122 to184.480ms,
but does not prove service SLOs or maximum-byte rewrite progress. Next work
must address cancellation/join allowance, including maximum-sized rewrite progress,
without raising resource limits or weakening identity/snapshot/GC checks.

The subsequent native/control candidate's same-profile diagnostic records
rows/histogram p95=864/1,053ms and load slope=+0.11549/min: both remain failures.
Maintenance overruns/compaction failures are0 in this run, exact33,600 public
records and final backlog0 pass, and last planned files are761. Preserve
`.tools/comparison-report-1.HMaw0M`; this is not official-duration or independent
capacity evidence. The shared download path's24MiB journal cap was reproduced
against actual MinIO bytes and corrected with explicit journal24MiB,
query-result64MiB and bundle128MiB admission. Full SHA verification, exclusive
private-file creation and joined response-close/partial cleanup are retained.
A real256-event native path produces two approximately36MiB Parquet files and
verifies mixed/full retention and pinned snapshots. The initial64-wide-event
conversion failure was reproduced at the256MiB native limit. Separating payload
fragments from scope JSON, sharing canonical metadata, and normalizing byte-sized
chunks now lets that legal batch convert at both192/256MiB without raising
limits; original engine JSON byte/NULL semantics are explicitly compared.
The integration fixture is strengthened from16 batches of16 to4 batches of64.
The32-batch/512-event compaction failure is now addressed by bounded, ordered
native partition writes and concatenation within the existing spill budget.
A stronger56-input/896-event regression uses266,090,129 actual compressed bytes
and passes at192/256MiB native memory, with complete column hashes and physical
ordering checked. It does not establish spare cgroup memory, official-duration
maintenance-budget progress or the service SLOs. See quality.md for matched
latency/allocation/RSS costs and the executed integration/resource checks.
An additional layout audit reproduced a pre-existing mismatch with DESIGN5.2:
conversion used project/service/event-time ordering but maintenance used receipt
or ID order. Both paths now share the canonical physical sort, including null
service ordering. Payload layout keys are taken once from verified retained
analytics, not reconstructed from raw JSON or persisted as new payload columns.
Clean0004b4f's20s/300s/90s diagnostic still fails rows/histogram p95 at1,444/1,750ms
and backlog slope+0.493072/min; final backlog0 and exact public counts do not
close those missed targets. The next control increment removes reservation's
remaining per-input round trips: actual PG checks show14/38/134/518→9 calls for
2/8/32/128 inputs while preserving sorted identity, complete-set atomicity,
scope/generation/size rejection and joined cancellation/concurrent retry.
R3 remains first incomplete; raw-object download success is not a substitute.

Independent capacity now has an executable `scripts/check capacity` matrix,
separate from the mixed-load SLO. Paused disposable workers receive exactly the
same number of real durable-ACK jobs across four projects, then drain with the
existing publication pipeline. A32-cycle/3,360-record diagnostic passes all
three profiles:1/2/4 workers drain192 jobs in23.203/12.301/7.400s, respectively
144.809/273.149/454.054 records/s (speedup1/1.886/3.136). Every project's public
log/error counts match, with complete PG/S3/role cgroup observations and OOM0.
This quick result is one sample per profile on a dirty harness tree. The later
clean4ced580128-cycle matrix passes all nine fresh installations with13,440
records/768 jobs each: median1/2/4-worker throughput134.533/256.474/454.039
records/s, relative speedup1/1.906/3.375 and efficiency1/.953/.844. This closes
that fixed-work measurement, not the live-ingestion/search SLO. A subsequent
audit reproduced missing conversion disk admission and16 files present before
the first upload of an8-day acknowledged batch. Bounded streaming and file
boundary corrections now pass actual10,000-day ACK/publication under CPU1/
512MiB/swap0, exact per-day identity, one outstanding pair, cancellation/retry,
shared-disk release, native contracts, integration, resource, browser and crash
checks. Matched process-only benchmarks reduce supervisor allocation bytes by
50.1–51.4%, with only0.07–2.06% latency median differences and no measured cgroup
headroom improvement; this does not establish a service throughput speedup.
All four earlier native-oracle containers passed on0bf0956, but editing its
dispatcher mid-run caused that outer command to fail. The new frozen-source
10k/100k/1m/10m rerun now completes with exit0, including the outer command.
Corrected official-duration mixed-load SLOs remain required. See quality.md
for retained evidence, memory and actual S3 transfer scope.

R4 cannot be closed by MinIO-only tests, a docs-only runbook, mocked S3, a skipped
cloud test or an emulator. If expensive/performance targets miss, state measured
miss and revise implementation; do not relabel target as achieved by design.

The clean64d6038 short mixed-load baseline still misses rows/histogram p95
(893/1,166ms) and backlog slope(+0.698678/min). The next bounded optimization
keeps full catalog integrity/authorization checks and prunes only first-page
constant-true row scans using a proven limit-plus-one threshold. Unit/race,
native contracts, real durable PG/S3 pagination/authorization/missing-object
checks and the production-image browser pass. Matched planning-only costs
decrease, but allocation counts and benchmark cgroup peak increase; do not claim
extra memory headroom or service SLO completion from that local measurement.
See query.md for the exact proof/fallback contract and quality.md for evidence.
The same20s/300s/90s real-service pair measures rows p95=893→312ms and backlog
slope+0.698678→−0.720254/min, with59 successful queries and exact published counts
on both sides. Histogram p95=1,166→995ms still misses500ms; the command correctly
exits1. Whole-installation sampled peak and full-GET bytes increase slightly.
R3 therefore stays incomplete, including corrected official-duration profiles
and maximum-size maintenance resource evidence; R4 is not released.

The next correction reproduces two physical Parquet scans for histogram gap
filling and reuses bounded aggregate state instead. Native scope/metric/empty/
maximum-bucket regressions, contracts, real PG/S3 and browser checks pass.
Matched32/256-file native latency improves20.5%/29.9%; the same short-service
profile measures histogram p95=995→848ms, rows312→344ms, no growing backlog and
zero OOM. The500ms histogram target still fails, so this is not R3 completion.
See quality.md for exact artifacts, inputs, resources and S3 accounting.

The separate maximum-pair resource runner now passes real durable ACK/native/
S3/compaction/retention over265,537,875 input bytes and266,086,088 replacement
bytes under CPU1/512MiB/swap0. It preserves pinned reads, identity, half-open
retention, revocation/retry and cleanup. Its cgroup reaches512MiB with reclaim
pressure but OOM0; no headroom claim is made. This closes that scoped resource
execution, not maximum-byte progress under the app's rolling20% dispatcher.
The latter and corrected official mixed-load SLOs remain outstanding. A later
repeat while adding the real dispatcher reproduces native-child OOM; maximum-
pair stability is reopened rather than inferred from that first scoped pass.

The subsequent typed-size/writer-bound correction caps app maintenance native
memory at96MiB and admits four4MiB write ranges at that limit. Per-operation
retry cooldown allows fresh measured idle credit after budget cancellation,
without changing the20% accounting, fences or joined cleanup. Two fresh
CPU1/512MiB/swap0 executions now pass both maximum-pair direct workflows and the
actual worker dispatcher, including canceled compaction/retention retry and
pinned reads, with OOM0 and budget overrun0. Reclaim still reaches the512MiB
limit; this is scoped eventual-progress evidence, not headroom or mixed-load
SLO completion. See quality.md for failed alternatives and exact evidence.
The same fresh20s/300s/90s one-worker diagnostic then measures rows/histogram
p95=516/1,324ms, worse than the preceding344/848ms. Both500ms query targets fail;
exact published counts, all59 queries, backlog/accounting, restart/cache checks
and OOM0 pass. R3 must now resolve both query SLOs and the maintenance/query
trajectory before corrected official-duration runs; native-only speedup and
maximum-byte progress do not close it. R4 remains incomplete.

Job-boundary diagnostics then repeat the same0eba7e8 runtime at341/949ms,
showing that the previous single-run regression is not a causal attribution.
Slow histograms use multiple scans over hundreds of files; lexical error/log
candidate selection can leave a larger log partition waiting in the same lane.
Control now compares each partition's actual32MiB/128-input/256MiB-bounded prefix,
preferring file reduction then rewrite bytes. Real PG tests reproduce the former
choice and verify limits, cancellation, active-lane exclusion and reservability.
The matched short candidate records315/714ms, HEAD59,673→51,867 and exact public
counts, but histogram still fails500ms. Selector allocations, sampled whole RSS,
full-GET bytes and post-load cold/warm latency increase. No budget, fence, native
limit or query authorization changes. R3 remains first incomplete: resolve the
remaining histogram SLO and run corrected official-duration profiles. See
quality.md for source identities, scope, costs and failures.

A two-unstarted-compaction lookahead experiment was rejected: the same short
profile measured363/956ms and backlog slope+0.227181/min, worse than the accepted
candidate. No lookahead restriction remains in the product. Its new cold worker
also logged a327ms maintenance budget overrun that the legacy post-load verdict
omitted. Post-load verification now retains/rechecks the full replacement-worker
operation counters, including startup. Admission reserves200ms for native grace
plus joined cleanup rather than100ms for grace alone; actual work and overruns
remain fully charged. Deterministic regressions reproduce both omissions before
the fix. Sustained test compilation moves before installation startup. These
are correctness/measurement repairs, not a relaxed SLO or R3 completion.
The fresh maximum-pair actual dispatcher passes146.77s with13,424ms all-attempt
maintenance against113,200ms observed idle, overrun/OOM0. The new short service
profile measures416/949ms, not a speedup; histogram alone fails. Post-load
maintenance1,639ms against8,200ms idle passes, with exact cold/warm rows and
zero idle query work. The latest corrected official profiles are summarized
near the top of this plan; histogram SLO remains unmet.

The subsequent verified warm small-input transport stays within query/storage:
cache-only<=64KiB aggregate inputs may use private native input files, with8MiB
total and an additional shared-disk reservation. Cold/corrupt/large inputs and
rows/detail keep the lazy gateway. No provider prefetch, altered plan/scope or
early pin/file release is allowed. Matched256-file native medians111.645→62.293ms
reduce loopback514→0 and Go allocation, but RSS increases. The fresh short service
pair measures416/949→295/566ms; histogram still misses500ms. Exact counts,
backlog, main/post-load budgets and OOM pass; full-GET/Range bytes and whole
installation memory increase, so no cost/memory superiority is claimed.
The subsequent frozen clean02c98f2 official run passes the full10k/100k/1m/10m
native oracle and one-worker exact220,500 public records, final backlog0,
main/post-load maintenance budgets and OOM0. Rows/histogram p95=591/1,737ms
both fail500ms; the command exits1 after post-load verification and does not
start2/4 workers. Maximum query files reach2,117; rows/histogram pre/post-job
overhead p95=468/478ms and durable-job p95=196/1,248ms identify separate costs,
not a proven single cause. R3 remains first incomplete: resolve both query SLOs
at official duration and complete the corrected profiles, then dependent R4.
Detailed identities/costs are in quality.md.

Maintenance now bounds verified input downloads at four concurrent streams,
with stable input order and joined failure/cancellation before scratch/permit
release. Same-input real-MinIO128-pair medians improve78.514→25.070ms for8KiB
files and147.551→72.595ms for256KiB files; RSS increases and request bytes stay
identical. Actual missing/corrupt objects and retries, maximum-pair native
direct/dispatcher, the full integration gate, child contracts and browser pass.
The matched short service profile measures295/566→303/548ms; histogram still
fails500ms. This is not the official-duration correction required above.
R3 remains first incomplete, including fairness/complete scaling evidence;
do not advance to a release claim on this scoped transport optimization.

Size-cohort selection now keeps quiet large replacements out of tiny-input
rewrites. Six fixed paired-byte cohorts preserve the existing minimum/target,
128-input/256MiB ceilings, lane reservations and app budget. Actual PG boundary,
quiet/progress/ranking tests and a durable-ingest/native/S3 identity oracle cover
the policy. The matched mixed-size workflow median149.044→132.002ms and GET
2,477,649→92,423B show scoped rewrite savings, not faster full-service queries:
the short service pair measures303/548→306/558ms and still fails histogram500ms.
More quiet files and selector cost2.157→2.554ms are explicit tradeoffs. R3 remains
incomplete; official-duration query/fairness/scaling and R4 work remain required.

The conversion fairness audit reproduced repeated selection of the older tenant
after its previous work prepared, completed or expired. A bounded app-owned
last-tenant cursor now rotates equally occupied tenants through the existing
control transaction. The actual one-worker12+4-job fixture changes the second
tenant's first claim from turn12 to turn2, preserves all16 IDs in both Parquet
roles and has OOM0. The standalone synthetic round-robin test is removed.
This is not a throughput claim or full fairness closure: query scheduling,
cross-worker skew and sustained service SLOs still need evidence before R3/R4.

The subsequent query audit reproduces two actual PG defects: a saturated older
query hides another runnable query, and completing a task loses the next
tenant's turn. Candidate capacity filtering plus the unchanged locked cap check
and a separate bounded app cursor repair them. Twelve concurrent claim callers
and subsequent sweeps reach exactly four tasks per query; targeted synchronous
help does not steal work. An actual single-worker PG/S3/native fixture changes
A/A/B to A/B/A while preserving exact results and cross-tenant denial. These
are correctness checks, not a measured throughput improvement; multi-worker
skew and corrected official-duration SLOs still keep R3/R4 incomplete.

The subsequent actual1/2/4-process conversion/query fixtures pass three samples
each, checking every worker's participation, exact identities, authorization,
first untouched-tenant turn and recording claim-count skew. All processes share
one bounded test cgroup; this is queued-work correctness, not per-replica
throughput or elapsed-time fairness. The frozen clean04ec2c2 official run also
completes all four native dataset oracles and exact220,500 public records, with
backlog0, restart/cache/maintenance checks and OOM0. Rows/histogram p95 improve
from02c98f2's591/1,737ms to404/874ms under the same duration/resource/load profile,
but histogram still exceeds500ms. The command exits1 before corrected2/4-worker
profiles. Keep R3 and R4 incomplete; retain the full measured tradeoffs in
quality.md rather than treating these passes as release completion.

A larger warm-cache staging experiment was not adopted. Its mixed256-file
native benchmark reduced median74.009→66.452ms and allocations14.2→6.2MB, but
the matched short-service rows/histogram p95 changed291/600→298/644ms with both
histograms still failing500ms. Retain the new benchmark and negative evidence;
the product keeps64KiB/file and8MiB/task staging. Do not treat this rejected
candidate as an implemented improvement or rerun it without a new hypothesis.

A two-grid compaction experiment was also rejected. It compacted a real eight-
pair boundary fixture that the disjoint policy leaves quiet, but selector median
cost increased2.470→3.252ms and matched short-service rows/histogram p95 changed
293/585→302/625ms, with maximum queried files589→619. Exact33,600 public records,
60 queries, backlog0, OOM0 and maintenance accounting pass in both, not the
histogram500ms target. Restore the existing product/architecture; retain the
actual-file quiet-boundary oracle, prefix-bound tests and negative measurements.
Do not repeat this candidate without new evidence or mark R3/R4 complete.

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
