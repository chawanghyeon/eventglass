# Go verification and release quality

Use [CONTRIBUTING.md](../../CONTRIBUTING.md) for exact commands and
[DESIGN.md sections 21–23](../../DESIGN.md#21-validation-and-cost-contract) for
acceptance criteria. ARM64 is the active target; AMD64 remains unverified.

## Fast feedback

`./scripts/check architecture` enforces production package dependencies without
loading the native driver. `./scripts/check unit` includes architecture checks,
Go unit tests/vet and repository layout/hash checks. `./scripts/check focused
<package> [go test flags]` runs one approved pure-Go package. `./scripts/check
journal-bench` measures streaming journal allocation/time on the current host;
allocations are not a claim about Linux peak RSS.

The unit gate discovers packages automatically, with only native engine execution
excluded and covered by the pinned ARM64 contracts gate. `./scripts/check perf`
records repeated journal and controlled metadata-latency samples with revision,
dirty state and toolchain. CI records these samples; it does not establish SLOs.
Catalog verification uses at most eight readers per page, preserves catalog order,
and joins readers on error/cancellation. The synthetic 1ms/HEAD experiment is not
a provider benchmark or proof of end-to-end search speed.

`api/capabilities.json` maps registered routes to UI coverage and test sources;
architecture checks detect missing entries and stale paths. Test-source existence
does not prove semantic coverage. U4's user/system/editor routes and frontend flows
are implemented; unit, codegen, architecture, Vitest, typecheck and production build
pass locally. On 2026-09-21, `./scripts/check integration` passed against disposable
PostgreSQL/MinIO and `./scripts/check-browser` passed the final non-root ARM64 image
Playwright flow in 2.6 seconds. These runs close U4/G05, not a release gate.
`check-browser` now uses the final non-root ARM64 image and production assets,
not a Vite development server. Release still requires R1-R4 evidence.

Use `./scripts/check sdk` for pinned offline and live SDK contracts. Use
`./scripts/check integration` for disposable PostgreSQL/MinIO schema and S3
contracts. `./scripts/check contracts` executes pinned DuckDB 2.0 on Linux ARM64.
Never run the bundled 1.5 engine to avoid the native build. Native dependencies
are isolated in a reusable Docker build layer; pure Go changes use focused checks.

## Durability and resources

Required crash tests use a parent-owned ACK oracle, IPC failpoint barriers,
SIGKILL, restart and exact restored record/receipt comparison. Panic-only and
mock storage tests do not prove durable ACK. Use real PostgreSQL transactions
and owned temporary S3 prefixes. A missing environment/skip is not a passing gate.

Measure wire buffers, decoded object graphs, projections, compression scratch,
spool/cache/spill and native allocations. Permits follow the allocation owner;
cancellation cannot free resources still used by a worker. Test drain, failed
upload, interrupted commit, expired owner and generation changes. Combined-role
budgets must account for the entire process group. Measure Linux cgroup peak and
OOM behavior; Go allocation benchmarks cannot establish the 512 MiB target.

Report accepted and published counts separately, small-file distribution, PG
lock waits and WAL growth, S3 calls/bytes, cold/warm query latency, backup/WAL age
and blocked lanes. Include all PG/S3 costs in whole-installation comparisons.

The Q5 public-query fixture was executed on Linux ARM64 with the pinned DuckDB
`v2.0.0-dev84020`, disposable PostgreSQL, and MinIO on 2026-09-20 via
`./scripts/check integration`. The same two-row catalog and read token produced
cold HEAD=3, full GET=3, 8,348 bytes, 401ms and warm HEAD=3, full GET=3, 8,348
bytes, 86ms. These are correctness-fixture observations, not sustained SLO or
production-cache claims; G07 retains the representative load/resource gate.

Query/Live hardening evidence is split deliberately: pure tests cover fixed
catchup bounds, per-lane advancement, burst/backlog resync and token rules;
real localhost HTTP tests cover pre/post-header forbidden behavior, 32-slot
admission, disconnect cleanup, idle intervals longer than write deadlines and
slow-reader termination. ARM64 `TestPublicQueryEndToEnd` executes search/detail/
aggregate with an independent worker and no API-local executor.
`TestPublicQueryEndToEndHTTPResumeWithIndependentWorker` reconnects real SSE
with issued IDs inside a 205-row batch in five consecutive replay cycles,
includes records received over 15 minutes
ago, and revokes an idle stream's project access.
Idle polls assert zero additional query jobs/S3 calls; a separate export barrier
revokes permission after result export and verifies that no row is emitted.
`TestLogoutReleasesOnlyTheRevokedSessionSnapshots` checks atomic pin cleanup
without releasing another browser session's snapshot.
The query-job integration test also blocks a status reader on a completion row
lock, then commits the result; artifact lookup must observe the winning result
instead of combining a rechecked job row with a stale outer-join NULL.
Vitest tests resource-owner
teardown/late submission/heartbeat/StrictMode, operation token reuse and actual
reconnect headers. These are not Playwright ingestion-to-browser evidence or a
multi-process sustained-load benchmark; U3/G07 still own those gates.

M3 replaces default full-object query input downloads with localhost capability
Range reads backed by a 1 GiB soft-cap block cache inside the shared disk budget.
Pure tests show one remote block read for a cold task and zero additional remote
reads for an identical warm task; they also cover singleflight, active-pin
eviction refusal, restart reuse, cached corruption reload, short/changed ranges,
and canceled waiters. Query status persists bytes served from cache. The earlier
Q5 cold/warm figures above predate this cache and remain a baseline only. On
2026-09-21 the pinned DuckDB/MinIO integration recorded cold HEAD=3, full GET=2
(5,264 bytes), Range GET=1 (3,120 bytes), 101ms and warm HEAD=3, full GET=2
(5,264 bytes), Range GET=0 (0 bytes), 94ms. Full GETs are result/export reads;
query inputs use the Range gateway. This closes M3's request/byte gate only. The
small correctness fixture did not isolate peak RSS or allocations; R1 owns Linux
cgroup memory evidence, and these numbers are not a general latency or throughput
improvement claim.

M4's `./scripts/check recovery` builds the checksum-pinned pgBackRest 2.59.1
source for Linux ARM64, writes a PostgreSQL 17 full backup and continuous WAL to
a separate TLS MinIO prefix, and restores two independent PGDATA volumes to the
same explicit LSN. On 2026-09-21 the full backup took 2 seconds and both restores
took 2 seconds in the local Colima fixture. The restored WAL-only row and one
18-byte referenced object passed; a newer unreferenced object was not adopted.
Activation changed storage generation 1 to 2, revoked sessions, and kept alerts
paused. A private HMAC-signed report refreshed the live generation-1 GC horizon;
deleting the referenced object made the second restore fail with S3 404 and stay
`verification_required`. These timings are fixture observations, not RPO/HA or
throughput claims. The operational sequence and key separation are documented in
[`docs/operations/recovery.md`](../operations/recovery.md).

R1's `./scripts/check resource` runs the linked release binary on Linux ARM64
with cgroup CPU quota 1, memory 512 MiB, swap 0, PID limit 256, read-only root and
a bounded scratch tmpfs. On 2026-09-21 the default two-minute schedule completed
24 cycles/12,600 normalized records at the logical 100 logs/s+5 errors/s input
mix. Each cycle ran isolated DuckDB conversion and rows-query children; the worst
cycle was 380 ms against a 5 s no-backlog interval. Cgroup peak memory was
190,730,240 bytes with zero cgroup OOM/OOM-kill, and scratch entries were zero
after each completed cycle. The same gate measured the largest legal synthetic
message at 349,364 bytes, classified a DuckDB native/spill exhaustion as
`resource_exhausted`, joined a canceled child before removing output/spill, and
proved drain did not release a live 192 MiB permit. This is a two-minute resource
containment fixture with local files, not the R3 5-minute warmup/30-minute full
PG/S3/API load, latency SLO, or whole-installation RSS/cost result.

R2's `./scripts/check scale` runs the autoscale/controller tests in the Linux
ARM64 build. A fixed 96-task control workload produced the same SHA-256
`9e698f81334bec1fb49ff3952bf0c600aa6b93696af3f1dab2c3bea7344c8fb0`
at 1/2/4 workers; observed harness times were 302/152/77 ms. Those sleeps test
dispatcher concurrency and logical equivalence, not Eventglass throughput.
PostgreSQL integration proves a second claim prefers a tenant with no running
conversion over an older second job from the first tenant. Pure controller tests
cover the 1 MiB/s conservative prior, EWMA alpha 0.2, two samples before scale
out, dependency error>20%/pool wait>1s freeze, warm minimum one, 300s stability
and at most25% scale-in. API1+worker20+scheduler1 uses 50 of the 56 allocatable
PG connections (8 remain reserved from the global64). The private metrics listener
exports only three fixed pool labels and gates KEDA demand on the PostgreSQL/S3
marker check. The manifests are a bounded deployment baseline, not a deployed or
proven autoscaling cluster.

R3's `./scripts/check comparison` is now executable against fresh PostgreSQL and
MinIO installations and final non-root ARM64 images. It fixes independent
10k/100k/1m/10m oracle hashes, sends the real Sentry HTTP mix, samples Docker
resources, performs a worker SIGKILL/restart, and prices measured PG/WAL and S3
requests/bytes from the dated 2026-09-21 us-east-1 fixture. The official
one-worker run used five minutes warmup, 30 minutes load, and ten minutes drain.
It accepted 220,500 records plus 35 duplicate retries with zero conflicts; ACK
p95 was 375 ms. All Go units stayed below 512 MiB with no OOM, while peak whole
installation RSS was 1,257,012,985 bytes. Rows/histogram p95 was 11,812/11,085
ms, 113 queries failed, the load backlog slope was +474.52 jobs/min, maximum
backlog was 13,310, and 13,060 remained after a 600,759 ms drain. That report
predates usable visibility samples. The harness now derives a real sample from
the newest returned row's server `received_time_us` when the nullable response
estimate is absent, and still fails closed if neither source exists. A Go1.27.1
20s/60s/90s diagnostic accepted 8,400 records with ACK p95 372ms, rows/histogram
p95 1,149/933ms, backlog slope +48.59 jobs/min and zero final backlog; it is not
an official-duration pass. Raising catalog metadata concurrency from4 to32
reduced the 32-object synthetic latency from about10.3ms to1.4ms, but the same
end-to-end diagnostic regressed rows/histogram p95 to1,244/1,133ms and was
rejected. The measured local rates project to
$384.01/month under the fixture's stated Fargate/RDS/S3 assumptions, including
$265.54/month of S3 requests; EKS, load balancer, NAT, CloudWatch, DNS, support,
tax, cross-region transfer, and multi-AZ premium are explicit exclusions. This
is measured failure evidence, not a cost or performance claim. R3 remains
incomplete and R4 cannot start as a completed dependent gate.

On 2026-09-21, further R3 diagnostics separated response latency from the
server's durable query-job elapsed time. Under a 20s/60s/90s one-worker profile,
the prior image measured rows/histogram p95 890/1,150ms, of which 798/1,052ms
were durable job time; the difference was 92/93ms. A bounded eight-task worker
round-robin and one-pass Parquet output inspection (analytics stats/identity/
projects together, payload count/identity together) retain per-file block and
whole SHA-256 evidence plus S3 full readback. The comparable quick candidate
measured rows/histogram p95 498/449ms, visibility p95 878ms, peak conversion
backlog 20 versus 68 and drain 2.007s versus 6.013s; ACK p95 was 375ms versus
372ms. S3 requests (PUT/HEAD/GET/Range) changed from 1,497/2,478/3,752/191
to 1,494/2,607/3,649/197; bytes are retained in the local report. Image reuse
is explicitly diagnostic and reports its supplied revision; official R3 still
builds fresh ARM64 images. This short result does not prove a 30-minute SLO or
isolate conversion-only attribution from schedule effects. The longer
20s/300s/90s candidate showed slope -2.27 jobs/min, peak backlog 33, 2.012s
drain, ACK p95 373ms and visibility p95 2.040s, but only four successful
queries, 18 failures and rows/histogram p95 640/744ms. In the latest short run,
two failed HTTP responses were 503 dependency_unavailable and only four durable
jobs existed, all succeeded; this disproves the hypothesis that those failures
were expired query-task leases. The harness created a new moving-window snapshot
for every query without releasing successful snapshots; the server's four-active-
snapshots-per-user cap correctly rejected subsequent submissions. Its first
release attempt also failed CSRF because DELETE omitted Origin. With Origin and
CSRF supplied, the next 20s/120s/90s isolated one-worker run completed all 24
queries with zero failures, ACK p95 374ms, visibility p95 4,725ms, rows/histogram
p95 573/681ms, backlog slope +24.63 jobs/min and final backlog zero after
6.011s. A second 20s/300s/90s diagnostic completed all 59 measured searches
with zero HTTP failures, but rows/histogram p95 grew to 3,057/3,376ms,
visibility p95 exceeded 5s, backlog slope reached +84.77 jobs/min, and drain
took 42.056s. It issued 69,185 S3 HEADs across the run, with no conflicts or
cgroup OOM. These query/visibility/load-backlog targets still fail; this is
diagnostic evidence, not a completion claim. R3 remains incomplete and no
release claim follows. Paired-file concurrent upload
and two native children per CPU1 worker were rejected; the latter doubled
average conversion duration and missed throughput/latency targets. No relaxed
verification or GC override was retained.

In an equal 20s/300s/90s one-worker follow-up, keeping every catalog HEAD but
bounding concurrent metadata checks at eight instead of four measured
rows/histogram p95 2,838/3,231ms versus 3,057/3,376ms, backlog slope +65.38
versus +84.77 jobs/min, drain 33.051s versus 42.056s, and S3 HEAD requests
62,840 versus 69,185. Both runs completed 59 searches; 30,000 logs and 1,500
errors were measured in each. The single-run difference includes variable
compaction/object counts (6,765 versus 6,844) and is not proof of proportional
end-to-end speedup. CPU1 query p95, visibility and growing backlog still miss;
the official R3 profile has not passed.

On 2026-09-21, the comparison report also began recording the API's actual
planned-object count and scanned bytes. A 20s/300s/90s one-worker run with
32-file scan partitions saw planned objects rise from91 to646 and at most
6,907,548 compressed input bytes; rows/histogram p95 was1,750/1,951ms,
visibility p95 9.008s and backlog slope +20.59 jobs/min. The first 128-file
candidate failed every query because the planner limit exceeded the native
request's still-32-file cap; it was rejected, not treated as a benchmark.
After binding the planner limit to the native cap and checking a real pinned
DuckDB 128-file scan plus 129-file rejection, the same-duration candidate
completed all59 searches with rows/histogram p95 854/1,068ms, ACK p95 375ms,
visibility p95 13.605s, backlog slope +27.34 jobs/min and no cgroup OOM.
S3 HEAD/GET/Range counts were 36,121/14,705/2,395 in the 32-file run and
27,247/15,105/2,027 in the 128-file run. Compaction differed (last planned
object count646 versus340), so this is an end-to-end same-workload comparison,
not isolated attribution to scan partition size. Query, visibility and
load-backlog targets still fail; R3 remains incomplete.

A subsequent paired 20s/300s/90s diagnostic used the same ARM64 CPU1/512MiB
profile, 33,600 accepted records and dataset SHA, comparing `fc16e34` with
empty-query-claim suppression. Both completed59 searches without query failure,
conflict or cgroup OOM. Ready queries retain their eight-slot bounded burst;
an empty query claim runs once rather than eight times per scheduling sweep.

| Measurement | Baseline | Empty-claim suppression |
|---|---:|---:|
| ACK p95 ms | 375 | 374 |
| Rows / histogram p95 ms | 1,580 / 1,834 | 766 / 789 |
| Visibility p95 ms | 9,953 | 6,022 |
| Load backlog slope jobs/min | +14.35 | +9.66 |
| Maximum backlog / drain ms | 163 / 12,016 | 68 / 6,020 |
| S3 PUT / HEAD / GET / Range requests | 6,096 / 63,549 / 13,521 / 2,148 | 6,183 / 29,956 / 15,500 / 2,045 |
| S3 PUT / GET / Range bytes | 34,365,078 / 76,385,151 / 19,126,566 | 42,211,488 / 102,270,091 / 21,318,441 |
| PG WAL bytes | 86,789,104 | 81,562,688 |
| Whole installation / worker peak sampled bytes | 798,894,324 / 75,895,930 | 814,722,579 / 82,124,472 |

Compaction trajectories again differ (last query planned1,063 versus260 files),
so the latency delta is not isolated attribution. Transfer bytes and sampled
memory increased; no blanket cost or memory improvement is claimed. Private
operation counters now survive report aggregation across the injected restart;
the candidate recorded2,257 query calls/179 claimed attempts and264.673s of
conversion work. The interrupted query attempt contributes one operation failure
but no public-query failure. These are short diagnostics, not the official
R3 pass: query latency, visibility and load-backlog growth remain out of target.

Real PostgreSQL regression tests also reproduced recovery writes rolling back
when a claim found no next task: the third expired attempt remained `running`,
and query-specific help lost another task's requeue. Control now commits these
recovery updates on benign no-work/capacity returns; stale heartbeat and late
completion remain fenced and a fourth attempt is never admitted.

`BenchmarkOpenThreadConfiguration` checks one suspected native startup cost
without changing production initialization. Five2s Linux ARM64 CPU1/512MiB
samples gave median12.567ms,5,963 Go B/op,170 allocs/op for setting threads
after open versus12.226ms,6,359 B/op,173 allocs/op for `threads=1` in the DSN.
This small startup-only difference does not explain the141ms mean conversion
work time; it was not promoted as an end-to-end optimization. Go allocations
exclude native memory, and this local benchmark performs no S3 requests.

The next conversion experiment kept the original typed stage and reused its
eight already-decoded analytics columns. Five2s `BenchmarkConvertSelectedBatch`
samples per case, on the same pinned DuckDB2.0/Linux ARM64 CPU1/512MiB profile
with tmpfs scratch and a256MiB native limit, gave these medians:

| Local native conversion | Before | Reused stage columns |
|---|---:|---:|
| One-record batch ms / batches per second | 117.043 / 8.544 | 114.337 / 8.746 |
| 100-record batch ms / batches per second | 132.552 / 7.544 | 127.518 / 7.842 |
| One-record Go B/op / allocs/op | 2,223,951 / 896 | 2,222,550 / 895 |
| 100-record Go B/op / allocs/op | 3,990,880 / 13,208 | 3,989,102 / 13,207 |

An immediate three-sample repeat pair measured110.871/125.013ms before and
110.384/124.738ms after; their ranges overlap. The initial percentage reduction
is therefore not a stable speedup claim. That pair's whole benchmark-container
`memory.peak` was111,226,880 versus104,562,688 bytes, including native allocations
and cgroup-accounted cache; this is not isolated process RSS or proof of a fixed
memory saving. The implementation removes redundant extraction and keeps the
existing types without relying on a promised latency gain.

These are local conversion rates, not durable-ACK or whole-installation
throughput. Both variants perform zero S3 requests/transfers in this benchmark.
Parquet types, paired identities, NULL versus empty service names, Unicode and
integers above2^53 are checked with the actual pinned engine. Full ARM64 native
contracts, disposable PG/S3 integration and the final-image browser flow pass.
The first broad single-struct JSON projection was rejected: its strict decoder
rejected absent optional fields; after preserving those semantics it still
regressed to242.828/253.460ms for one/100 records. That SQL was removed.
`BenchmarkConversionPhaseCosts` isolates opening/configuration, append,
materialization, paired write/inspection and close; paired write/inspection
took90–95ms in the unchanged baseline. It is a stage diagnostic, not the full
conversion workflow. This small conversion change does not close R3 or
replace a new official end-to-end run.

Detailed profiling of the same pinned engine then isolated repeated attribute
type binding: one analytics COPY spent about27ms preparing and36–40ms executing,
while paired output inspection took8–12ms. Inlining the partition constants did
not help and was discarded. The converter now defines the unchanged nine-field
attribute STRUCT once per private connection and refers to that type from
`from_json`; no output validation, sorting or JSON missing-field policy is removed.
Against `9b0ae92`, five2s samples with the identical ARM64 CPU1/512MiB/tmpfs
limits produced these medians:

| Local native conversion | Baseline | Reused attribute type |
|---|---:|---:|
| One-record ms / batches per second | 113.574 / 8.805 | 85.072 / 11.755 |
| 100-record ms / batches per second | 127.067 / 7.870 | 97.945 / 10.210 |
| One-record Go B/op / allocs/op | 2,223,966 / 899 | 2,223,358 / 916 |
| 100-record Go B/op / allocs/op | 3,990,269 / 13,212 | 3,991,281 / 13,234 |
| Benchmark-container memory.peak bytes | 108,810,240 | 109,760,512 |

This measures approximately25%/23% less local conversion time, not a reduction
in memory or whole-installation cost. Go allocation counts increase slightly;
the cgroup peak includes native memory and cache, not isolated process RSS.
Both local variants issue zero S3 requests and transfer zero S3 bytes. A fresh
DuckDB connection without the alias checks the resulting Parquet types and
38-digit negative integers, fractional numbers, false, Unicode, JSON, absent
optional values and NULL/empty arrays. Temporary profiling code is not retained.
The updated candidate passed all internal/cmd/architecture tests against the
pinned Linux ARM64 native library, real isolated PostgreSQL/MinIO integration
(32.154s), final-image Playwright (2.8s), unit and generated-contract checks.
The next equal20s/300s/90s end-to-end pair compared `9b0ae92` and `a36513e`.
Both accepted33,600 records and completed60 searches with no conflict or OOM.

| One-worker PostgreSQL/MinIO measurement | Baseline | Reused attribute type |
|---|---:|---:|
| ACK p95 ms | 374 | 380 |
| Rows / histogram p95 ms | 620 / 635 | 576 / 631 |
| Visibility p95 ms | 5,235 | 2,775 |
| Load backlog slope jobs/min / maximum | +10.24 / 61 | −3.32 / 44 |
| Final backlog / drain ms | 0 / 6,014 | 0 / 1,007 |
| Conversion work ms / claimed attempts | 265,015 / 1,871 | 226,466 / 1,856 |
| S3 PUT / HEAD / GET / Range requests | 6,223 / 24,690 / 15,650 / 2,026 | 6,206 / 23,167 / 15,849 / 1,912 |
| S3 PUT / GET / Range bytes | 42,568,775 / 103,387,988 / 21,292,850 | 44,666,878 / 108,836,307 / 21,012,185 |
| PG WAL bytes | 80,403,592 | 80,117,992 |
| Whole installation / worker peak sampled bytes | 843,453,561 / 82,051,072 | 852,156,741 / 89,967,820 |

Compaction trajectories differ (last planned files222 versus110); these are
whole-workload results, not isolated attribution or a memory/cost saving claim.
The candidate's short-run backlog and visibility targets pass, but both search
p95 targets still exceed500ms. Retained evidence is under
`.tools/comparison-report-1.a2EONz` and `.tools/comparison-report-1.0xHvAQ`.
R3 remains incomplete; this diagnostic does not replace its official profile.

The subsequent scan-bound experiment keeps the existing worker scheduler and
raises the shared planner/native file cap from128 to256, without raising the
64MiB scan target,1MiB manifest, native256MiB or8-active-Range limits. With the
same20s/300s/90s profile, CPU1/512MiB worker and dataset SHA, both variants
accepted33,600 records (30,000 logs+1,500 errors during load) and completed60
queries without conflicts, failures or cgroup OOM:

| One-worker scan-bound diagnostic | 128 files (`a36513e`) | 256 files |
|---|---:|---:|
| ACK p95 ms | 380 | 379 |
| Rows / histogram p95 ms | 576 / 631 | 362 / 457 |
| Visibility p95 ms | 2,775 | 869 |
| Load backlog slope jobs/min / maximum | −3.32 / 44 | −1.26 / 18 |
| Final backlog / drain ms | 0 / 1,007 | 0 / 1,002 |
| Native query tasks / work ms | 145 / 12,617 | 61 / 8,432 |
| S3 PUT / HEAD / GET / Range requests | 6,206 / 23,167 / 15,849 / 1,912 | 6,107 / 23,204 / 15,664 / 1,840 |
| S3 PUT / GET / Range bytes | 44,666,878 / 108,836,307 / 21,012,185 | 42,923,395 / 105,257,670 / 20,476,128 |
| PG WAL bytes | 80,117,992 | 78,309,672 |
| Whole installation / worker peak sampled bytes | 852,156,741 / 89,967,820 | 909,618,706 / 67,077,406 |

The short diagnostic's target map is all true. It is **not** the official R3
profile or a whole-installation memory saving: total sampled memory increased,
and the compaction trajectory differs (last planned files110 versus146).
Go allocations are not separately sampled by this end-to-end harness. The
unchanged dated cost model projects USD517.69 versus512.26/month; this is not
an actual AWS bill or demonstrated production saving. Candidate evidence:
`.tools/comparison-report-1.zVW4PD`, runtime image
`sha256:584f373c952fa3c01771267b942b8425dd8c297ea96d1100c9a2a056e9360aa5`.
The subsequent metadata-bound regression reproduced rejection of a group that
can fit smaller manifests. Planning now bisects such groups before sealing,
preserves stable file order/exactly-once assignment, rejects a single oversized
manifest and enforces the total metadata budget while constructing the plan.
The diagnostic did not exercise that fallback. The final source passed unit,
codegen, all Linux ARM64 native contracts/vet, isolated PostgreSQL/MinIO
integration (host11.422s and native25.156s), and final-image Playwright (3.1s).
The first integration attempt's native test compilation was killed while other
builds competed for the Colima VM's2 CPUs/4GiB; its serial rerun passed. This is
not hidden as a product test pass. A network-disabled CPU1/512MiB/swap0 run of
the real256-file/257-rejection and cancellation/exact-integer tests passed with
memory.peak68,354,048 bytes and OOM counters0 (no S3 I/O). The official profile
still must validate the final source revision.

A separate late-query-before-conversion scheduling candidate was discarded:
the same short profile measured rows/histogram602/723ms, visibility6,097ms,
backlog slope+1.61/min and whole/worker peak881,810,470/87,115,694 bytes.
S3 PUT/HEAD/GET/Range counts were6,126/25,599/15,668/1,916 and transferred
PUT/GET/Range bytes43,740,225/106,883,392/20,818,290. All60 queries completed,
but there was no demonstrated improvement. Its source changes are removed;
only measured evidence remains in `.tools/comparison-report-1.06GFpo`.

Completion audit of the current comparison harness found additional limitations.
Its historical `ColdRegexMS` field is a post-drain last15min regex request with
no verified cache reset; it is not cold-cache or all-history evidence.
`DatasetSHA256` is the frozen independent fixture-definition checksum, not a
hash of the actual transmitted envelopes. The fixed-fixture test checks its
own independent summary, not the production engine's result over all four
dataset sizes. The receipt count target alone does not prove the full published
and queried dataset. Finally, equal100+5/s offered load at1/2/4 workers cannot
demonstrate throughput capacity growth/efficiency. These checks must be added
and executed before R3 closure. Existing reports remain useful for their actual
SLO/resource workload, but an all-true target map is not by itself a complete
G07 audit. No existing evidence file has been relabeled or rewritten.

The subsequent harness now names the independent checksum
`FixtureDefinitionSHA256` and the unreset regex observation
`PostDrainLast15MinRegexMS`. `SubmittedInput` streams the exact envelope bodies
offered to the HTTP client, including admission retries and intentional
duplicates: domain prefix `eventglass-submitted-envelope-v1` plus NUL, followed
by repeated big-endian uint64 body length and body bytes. Its order is the
locked submission order, not TCP arrival order or a claim that every attempt
was accepted. Raw bodies/DSN keys are not saved or retained for the whole run.
The final public aggregate request covers the whole received-time workload and
checks both kind counts independently of receipts. These checks add required
targets; they do not complete the four-size typed/filter/identity oracle.
Ingest-cycle failure now joins all six HTTP attempts, and phase failure joins
the backlog/query monitors before teardown. Local HTTP regression tests cover
retry accounting, frame ambiguity, incomplete/duplicate/malformed aggregates,
snapshot release on invalid results, and outstanding requests after a failure.

Fresh Linux ARM64 runtime image
`e1a04b04be3253236c64ab05e09226f33d5ad380db60a708c32d13ee93514d09`
from dcdbb9d plus harness changes ran20s warmup/60s load/90s maximum drain on
the restarted4CPU/8GiB Colima host. It verified8,000 published logs+400 errors
through the real API/worker/engine in504ms. Submitted input was2,702 attempts,
8,725,120bytes, SHA256
`95227d049074bb7693d784293331d272d0fd955967464b56f84d5fb1011cb15e`.
Accepted8,400/duplicate1/conflict0; all12 mixed queries succeeded, but rows/
histogram p95=624/592ms and backlog slope+2.11/min failed the targets.
ACK p95=372ms, visibility1,019ms, final backlog0/drain2,013ms. Thus the
command correctly exited nonzero; `.tools/comparison-report-1.vIQ320`
preserves the failed report and logs. No same-host before/after speedup is claimed.

S3 PUT/HEAD/fullGET/Range requests were1,492/6,295/3,666/545; PUT/fullGET/Range
bytes8,498,788/20,762,726/5,254,700; reported WAL16,379,280bytes. Docker stats
sampled whole-installation/worker peaks547,591,549/65,682,800bytes and recorded
OOM kills0. These sampled usage figures are not a continuous RSS maximum;
each Go runtime still had enforced CPU1/512MiB/swap0. The dated model projected
USD515.78/month, not an actual bill or savings claim. The report's PG size/WAL
sample precedes the final regex/oracle requests, while S3 counters include them;
that boundary was subsequently corrected and rerun below. Verified cold cache,
all-history regex, idle, native four-size oracle and capacity/efficiency
measurements remain open along with the official-duration SLO rerun.

The same product image, rebuilt neither in Go nor native code, was then reused
for another fresh20s/60s/90s quick diagnostic of the corrected PG/WAL boundary.
Current harness code was mounted explicitly; its `Revision` identifies the
dcdbb9d product source, not an official clean-source measurement.
`.tools/comparison-report-1.Rm83yU` passed its measured target map:8,400 accepted,
8,000+400 published counts,12 complete queries, ACK p95=371ms, rows/histogram
p95=236/359ms, visibility776ms, backlog slope−1.05/min, final0/drain1,001ms.
Input provenance included2,776 attempts/8,302,217bytes, SHA256
`c43b7ecc597c6d65ba4ad92a57e3bb36d6b1acfcc9b6cedc178329e36a141302`.
The full-run count oracle took342ms. PUT/HEAD/fullGET/Range requests were
1,500/5,001/3,662/481 and PUT/fullGET/Range bytes8,529,956/20,693,560/4,776,130.
WAL16,702,584bytes now includes both final queries; sampled whole/worker peaks
557,051,803/69,300,387bytes, OOM0. The same dated model projected USD500.03.
Different compaction/task trajectories (max planned objects255 versus144) and
the tiny sample make this variation evidence, not a product speedup or savings.
Both runs and the unresolved official gate remain recorded. All harness checks
also passed on Linux ARM64 with race/count3; the runner now executes those
non-load checks before measuring, and unit/codegen/architecture checks pass.

The official run from7ee4297 was interrupted before a complete report existed;
its last samples/logs are preserved in
`.tools/eventglass-comparison.LG4DOu/workers-1/artifacts`. On resumption Colima
was stopped. Restarting its existing profile exposed4CPU/8,308,363,264 bytes,
not the prior2CPU/4,094,459,904-byte environment. The incomplete run is not a
pass, and later measurements on this host are not a same-environment comparison
with the earlier end-to-end figures. Its exited test containers/network were
removed only after preserving evidence; the native dependency cache remains.

Planning-bound regression evidence (Go1.27.1, Linux ARM64, pinned DuckDB2.0):
the unchanged7ee4297 image retained2,560 metadata-heavy files and performed
5,120 HEAD callbacks instead of rejecting the unplannable catalog. It also
mapped catalog/admission limits to retryable503 dependency failures. The new
16MiB streaming metadata counter rejects before HEADs on the overflowing page,
and query-owned64MiB planning reservations share app's working-memory budget.
Cancellation tests hold admission until the blocked reader has actually joined;
seal/error cleanup retains the permit. HTTP quota failure is terminal422,
local admission is retryable429 (503 during drain), with Retry-After1.

The safety check has a measured cost, not a speedup. With identical192-file
metadata and no network, CPU1/512MiB/swap0, GOGC100/GOMEMLIMIT352MiB,
five1s samples measured baseline ns/op24,155/24,864/24,966/70,717/60,802
and candidate405,050/409,703/434,402/347,665/373,233: median+380,084ns.
Allocation medians are50,272→50,305B/op,23→24allocs/op; derived bookkeeping
rates40,054→2,469calls/s are not query/ingest throughput. Cgroup peaks were
40,316,928/27,578,368 bytes, both OOM0; S3 requests/bytes were0/0 in both.
Raw paired evidence is `.tools/planning-metadata.cSnaMM/paired-results.txt`.
Five repeats of the planning/catalog boundary and cancellation tests under
the same cgroup limits passed, peak132,960,256 bytes/OOM0. These fixtures do
not establish worst-case full-runtime RSS or an official R3 pass.
Actual PostgreSQL/MinIO integration passed on host (13.397s) and native ARM64
(25.818s), including alert evaluation, five independent-worker Live replays,
and exhausted planning admission with no added HEAD/query job before a normal
successful query. Unit, focused race(count3), codegen, native contracts and vet
also passed. Cold/warm integration retained the same exact result with Range
requests1→0 (3,120→0bytes); this small fixture is not R3 cold-cache evidence.
The final ARM64 image passed Playwright (3.1s). The429/503 response contract
and capability coverage were updated together; regenerated Go/TypeScript
contracts pass byte-for-byte checking, and all29 frontend tests plus strict
TypeScript/production build pass. Schema/storage formats are unchanged.

### Post-load cold cache and idle evidence

The R3 runner now gracefully joins and replaces every disposable worker after
drain, keeping the same PG/S3 installation while dropping only those workers'
tmpfs caches. It checks empty cache directories, different unique container
IDs, zero query/Range counters before cold work, and retains replacement IDs
and pre-replacement logs. The same full-run received-time regex and read token
must return the identical100-row result hash on its warm repeat. Cold here means
Eventglass block caches, not provider/OS cache. Warm misses are recorded rather
than presumed zero because the scheduler may choose another worker.
An idle phase requires no new query jobs, no executed query tasks and zero final
backlog; normal maintenance HEAD work is recorded, not silently discarded.
Official idle is60s; quick mode uses10s. Missing/incomplete post-load phases or
shortened official idle fail final report validation. A failed load SLO still
runs these independent checks without changing its failure to a pass.

Actual Go1.27.1/pinned DuckDB2.0 Linux ARM64 diagnostics used the same4CPU/8GiB
Colima host, CPU1/512MiB/swap0 per Go runtime,20s warmup/60s load/90s maximum
drain and10s idle. Each accepted8,400 records, verified8,000 logs+400 errors via
the public aggregate API, and completed12 mixed queries. Product image
`e1a04b04be3253236c64ab05e09226f33d5ad380db60a708c32d13ee93514d09`
was freshly built for1 worker and reused only in quick mode for2/4; all source
changes in this increment are verification code, not product performance tuning.

| Workers / retained report | Cold/warm ms | Cold/warm Range requests | Cold/warm Range bytes | Rows/histogram p95 ms | Load backlog slope/min |
|---|---:|---:|---:|---:|---:|
|1 / `.tools/comparison-report-1.miTHDo`|348/183|128/0|1,577,033/0|411/421|−21.36|
|2 / `.tools/comparison-report-2.0ZzpqR`|278/218|131/0|1,629,211/0|312/372|−1.12|
|4 / `.tools/comparison-report-4.kJ5B02`|225/203|116/0|1,489,481/0|316/404|+0.132 (fail)|

Each cold/warm phase performed one output PUT and two full result GETs; those
are not default full-object input reads. Cold/warm HEAD counts were260/260,
267/267 and239/239. Cold/warm full GET bytes were16,056/16,056,16,060/16,060
and16,174/16,174. Cold/warm cache-byte stats were0/1,577,033,0/1,629,211 and
0/1,489,481. Idle observed no new queries, Range/fullGET/PUT calls or backlog;
maintenance performed3/4/6 HEADs. Post-load WAL was216,864/250,080/348,272bytes.
These are single-pair cache observations, not stable latency improvement or
capacity/scaling evidence. The short time window also does not prove native
search over records older than15min; the official workload must do that.

Resource verification additionally reads `memory.peak`, `memory.events`,
`memory.max` and `memory.swap.max` from each container before replacement and
at final observation. Worker1 requires three distinct incarnation observations
(initial, failure restart, cold replacement); other workers require two.
Every Go role plus PG and MinIO requires memory samples and OOM observations.
Empty/missing data, duplicate identities or wrong Go memory/swap caps fail.
These are observed cgroup peaks, including the small observation process, not
a final kernel read after kill or a sum pretending to be simultaneous RSS.
The first1-worker table row predates this extra capture and is not evidence for it.

For2 workers, observed API/scheduler peaks were145,092,608/13,074,432bytes,
workers77,643,776/82,796,544, PG206,409,728 and MinIO289,382,400; simultaneous
Docker-sampled whole usage peaked615,347,385bytes. For4 workers the corresponding
API/scheduler peaks were145,149,952/12,922,880, workers78,512,128/73,400,320/
74,530,816/74,629,120, PG237,600,768 and MinIO292,225,024; sampled whole usage
peaked653,513,454bytes. All observed OOM events/kills were zero and all Go
cgroups showed512MiB/zero swap. The4-worker command nevertheless exited nonzero
for its measured backlog slope; the failed report remains unchanged.
The pre-post-load dated monthly workload model projected USD517.72/538.91/607.42;
post-load query/idle S3 and WAL are reported separately, not annualized as steady
traffic. No real bill, cost saving or linear scaling claim follows.

The final1-worker repeat with the strengthened resource collector is preserved
in `.tools/comparison-report-1.do0Ozf`, including the original pre-SIGKILL log,
all three worker cgroup observations and the cold replacement identity. It
passed cold/warm/idle/resource checks:306/305ms, Range117/0 and1,500,366/0bytes,
same100 rows/snapshot, warm cache1,500,366bytes. Each query did HEAD238/fullGET2/
PUT1, with fullGET16,454bytes and PUT8,227bytes. Idle had HEAD3 and no new query
work; phase WAL194,704bytes. Observed cgroup peaks were API144,838,656,
scheduler13,160,448, worker78,827,520, PG192,446,464 and MinIO267,128,832bytes;
sampled whole usage575,835,994bytes, OOM events/kills0, verified Go512MiB/swap0.
The load still failed backlog slope+4.42/min despite final0/drain2.003s,
rows/histogram359/394ms, ACK374ms, visibility913ms and all12 queries succeeding.
The model projected USD508.93; this remains a failed short run, not official
capacity evidence. No threshold was relaxed to make either failure pass.
Final Linux ARM64 race/count2 checks passed for the entire non-load comparison
and report suites; unit, codegen, architecture, shell syntax and source-design
hash checks passed. No product API, schema, runtime or UI source changed.

### Actual native fixture oracle and bounded JSON projection

The2026-09-22 Mode A increment uses actual SDK envelopes with20 batched logs
and one error (the final envelope may be partial), fixed event times and
native-fixture-v1 payloads. It is distinct from the old selector-definition
checksum: integer, fractional double and numeric string selectors now have
actual different typed semantics. Missing takes precedence over null; log
attribute keys are literal, so `/a.b` does not mean `/a/b`. The independent Go
oracle calculates exact filter/time counts and length-framed occurrence IDs,
checks every converted partition's complete sorted identity hash and two
equal-time keyset pages. The real journal writer/replayer must preserve each
canonical record exactly. Retry tests cover identical and new acceptances,
including valid source-event dedupe keys and ID-less records; they do not
simulate or claim PG Accept/durable ACK behavior.

This exposed real conversion memory failures. A10,000-record regression against
the previous converter failed at181.7MiB/192MiB with a16MiB allocation request.
A separate9,996-record SDK-derived stage failed at the worker's256MiB limit,
and a CPU1/512MiB direct-child attempt was killed after its first partition.
`materializeStage` and the analytics projection now extract a fixed path list
once per row into a materialized relation, instead of independently reparsing
the full canonical JSON for each output column. The original stage table is
dropped only after its replacement exists. Native limits, output types/order,
record IDs, payload projection, fences and publication workflow are unchanged.
The same10,000-record regression then passed at192MiB in0.87s; a further
three-repeat run passed it and the scalar test with cgroup OOM/kills0. A separate
all-scalar test checks distinct optional strings, missing/NULL values, escaping,
Unicode, nanoseconds, severity and exception fields; existing tests cover exact
DECIMAL(38,0), attributes, payload pairing and cancellation cleanup.

Sequential before/after native conversion benchmarks used the same ARM64 image,
CPU1/512MiB/swap0,96MiB Go soft limit, isolated disk volume, five iterations per
sample and three samples. Median1-record time94.194→91.274ms, Go allocations
2,222,881→2,223,539B/op and918→938allocs/op;100-record time108.519→99.287ms,
3,992,240→3,991,232B/op and13,241→13,255allocs/op. Combined benchmark cgroup peaks
were110,993,408→94,494,720bytes, OOM/kills0. S3 requests and network bytes were0.
Raw samples: `.tools/conversion-paired.IzULuo/{before,after}.log`. These small
native-conversion samples are not end-to-end throughput or an R3 SLO claim.

Fresh CPU1/512MiB/no-swap containers, Go1.27.1 ARM64 and pinned DuckDB2.0 produced
the following final fixture evidence (`.tools/native-oracle.dVVCFz`):

| Records | Result | Analytics bytes | Total time | Cgroup peak bytes |
|---|---|---:|---:|---:|
|10,000|all count/type/time/identity/page checks pass|540,409|2.279s|292,061,184|
|100,000|all count/type/time/identity/page checks pass|5,318,576|14.472s|323,973,120|
|1,000,000|typed-attribute query budget exhausted|52,794,512|108.320s to failure|536,870,912|

The actual10k/100k envelope SHA256 values are respectively
`0b6690eba56008d7e4b7cffc9741f93c9cb7712b7d6a7ec5159f48b04c9ee7e8` and
`911c2e3803f3d3bd875cd213101947e75aea162ce8aebfa0ca9b6d4a11520da0`.
Their journal bytes were683,617/6,814,916; normalization and oracle setup
148/1,474ms, conversion828/6,893ms, supervisor allocations315,621,112/
2,805,729,000bytes. Allocation totals exclude native unmanaged memory;
cgroup peaks include filesystem cache. The1m run converted all records in
102.363s including normalization/journals, passed overall/error counts,
half-open time and two row pages, but failed seven attribute-filter scans at
the192MiB native query budget. Cgroup OOM/kills stayed0. The harness retained
this failure, did not print a successful oracle result, and did not proceed
to10m. Four-size completion, capacity/efficiency and official-duration targets
therefore remain open. This is not a release gate pass or a cloud comparison.
The failure does not establish behavior at the separate worker's256MiB native
limit; that profile needs a matched check before choosing a query change.
Go unit/vet, generated-contract equality, architecture/layout, full pinned ARM64
native contracts/vet, frontend tests/typecheck/build and the actual browser flow
passed. Real isolated PostgreSQL/MinIO integration passed, including native
query/Live resume (25.041s) and Chromium SDK-to-Issue UI (2.8s). The additional
valid-ID duplicate and partial-envelope checks passed ARM64 race/count3.
No API/schema changed; capability gates remain incomplete and source-design's
frozen SHA256 is unchanged.

### Row-local attribute queries under native memory bounds

The subsequent2026-09-22 matched worker-profile check also failed the same
seven typed/presence filters at256MiB (`.tools/attribute-actual-million-before-256.log`):
all1m records converted into52,794,512 analytics bytes, but the query budget
was exhausted; total108.06s, cgroup peak536,870,912bytes, OOM/kills0. This
established a real worker-path limitation, not only the stricter192MiB oracle.

The query compiler now performs scalar/presence/array/text lookups within each
bounded row list using pinned2.0 list lambdas rather than correlated UNNEST.
Namespace/path duplicates are rejected before type selection. Actual native
regressions reproduced seven previously silent duplicate-path cases (mixed
types, presence, null, array, untyped/typed group, integer metric); all now fail
with the specific stored-format error, while valid scalar/missing/null and
independent Boolean-oracle cases pass. Eight integer metrics plus two grouping
dimensions execute with exact limbs, counts, exclusions and min/max. Numeric
operands use a row projection before repeated accumulator expressions: no new
repository/workflow layer, global relation, dynamic repartitioning or higher
memory/operation limit. Mandatory BuildPlan scope, shared authorization,
Submission/Awaiter, child joining and cache lifetime are unchanged.
The operation-budget test uses eight integer metrics, two groups and1KiB
pointers:46,066 encoded bytes under the unchanged65,536-byte limit. Native
tests also verify typed/untyped double grouping normalizes negative zero,
finite sums/counts/exclusions/min/max and negative-epoch empty histogram buckets.

Sequential paired measurements used a1m-row single Parquet file with two
attributes, including a high-cardinality string; every scan returned the exact
399,000 expected matches. Three before/after pairs used the same ARM64 image,
library, CPU1/512MiB/swap0,96MiB Go soft limit,256MiB spill and disk volume.
The baseline compiler sources are byte-identical to1f175a5. No heavy checks ran
concurrently. Medians (`.tools/attribute-paired.yF01lS`) are:

| Native MiB | Scan/readback ms, before→after | Input rows/s, before→after | Process peak RSS KiB, before→after | Go allocated bytes, before→after | Go allocations, before→after |
|---|---:|---:|---:|---:|---:|
|192|7,786.850→194.973|128,422→5,128,911|268,756→75,704|1,103,344→1,105,632|811→826|
|256|7,753.628→194.442|128,972→5,142,916|334,016→75,704|1,096,952→1,095,504|793→798|

The timer covers native scan plus exact count readback, not fixture generation;
input throughput is1m divided by that median duration. Process peak RSS includes
fixture construction and earlier subtests; Go allocation counters exclude native
allocations. Median cgroup peak across both profiles was501,161,984→79,003,648
bytes, including filesystem cache; all OOM/kills0, S3/network requests/bytes0.
The smaller single-file fixture also passed before the fix: this is a narrow
performance comparison, **not** the SDK multi-file failure reproduction or an
end-to-end/competitor throughput claim. Earlier uninstrumented pairs remain in
`.tools/attribute-paired.Q6HJYE`.

The fresh four-size Mode A run (`.tools/native-oracle.JAxTts`) passed all actual
SDK normalization/journal, exact partition-identity, typed/time/count and two
equal-time page checks. It used Go1.27.1/Linux ARM64, the same pinned2.0 library,
CPU1/512MiB/swap0,96MiB Go soft limit,256MiB conversion/192MiB query memory and
256MiB spill, network denied and private disk volumes. Colima had4CPU/8GiB;
no concurrent heavy checks ran during measurement.

| Records | Analytics bytes | Total time | Supervisor allocated bytes | Cgroup peak bytes |
|---|---:|---:|---:|---:|
|10,000|540,409|1.958s|315,662,544|258,134,016|
|100,000|5,318,576|11.579s|2,805,721,088|338,571,264|
|1,000,000|52,794,512|106.241s|27,688,857,920|397,402,112|
|10,000,000|528,495,771|1,053.615s|277,314,720,816|536,879,104|

All cgroup OOM/kills were0. The10m peak includes filesystem cache and is8KiB
above the configured536,870,912-byte limit; memory.events
reported2,684 `max` events. Do not describe this as guaranteed RSS below512MiB.
Allocation totals exclude unmanaged native memory and are cumulative, not live
heap. S3 requests/transferred network bytes were0. The1m attribute queries took
259–345ms;10m used nine fixed scans and three reducers, with attribute queries
2.397–3.139s. These are local native-oracle observations, not service p95 values.
Actual1m/10m envelope SHA256 values are
`9cac00b14d958cb966c231bb64a4d0734534688db622940e4f887ad6de751677` and
`1497dc8c86ac7ed8990b081b5d74e71833a75ef3185a4e6529fdb218aca2f5ec`;
10k/100k hashes match the earlier run. The10m run retained2,196 analytics files,
wrote/replayed682,231,716 journal bytes and spent148.437s normalizing/oracle
setup and657.290s in conversion; those timers are subsets, not a partition of
total elapsed time. Containers exited0 and owned scratch volumes were removed
only after child exit. Official-duration capacity/scaling and R4 remain open.
Final Go unit/vet, generated-contract equality, architecture/layout, pinned
ARM64 native contracts/vet, query/app/control race checks and frontend29 tests,
typecheck/build passed. Real isolated PostgreSQL/MinIO integration passed on the
host(10.944s) and the native ARM64 query/Live paths(24.456s); Chromium's actual
SDK-to-Issue flow passed in2.7s. No API/schema changed, so generated contracts and
capability gate status remain unchanged. The frozen source-design hash matches.

## Release and workflow

Verification image builds now share a recipe-addressed local DuckDB dependency
cache. Import from a trusted local ARM64 build checks the native recipe and real
engine version, records the source image/library hashes, and rejects a different
recipe. Fresh Go/UI sources are still built from the root Dockerfile; this is
not quick-mode application reuse or a signed release attestation. The imported
library SHA256 and the new image's linked library both equal
`84ad753acc1390e13ce56e728d75d379bebeedec7f3df58ce071c1b373e4c79f`.
The static build keeps module checksums but excludes unused platform-bundled
engines from module downloads; both their expanded directories and zip archives
were absent in the verified ARM64 image. Recipe/argument/gate layout tests pass.
The initial full contract run passed tests/vet but its image export hit Colima
disk exhaustion. Only11 obsolete Eventglass test images were removed; source,
measurement evidence, user containers/volumes and the native cache were retained.
The repeated `scripts/check contracts` then completed including image export.
The new dependency-cache path also passed the freshly built UI's actual
`scripts/check-browser` flow (4.7s). These build checks do not close G07/G08.
`scripts/check integration` completed with real PostgreSQL/MinIO on the host
(11.976s) and the fresh Linux ARM64 native query/Live paths (25.356s).

Commit reviewed changes directly to main and push after relevant checks. Current
pre-push runs Go unit/layout/architecture checks. Historical Rust deployment
staging/activation scripts are not present and must not be reported as active.
No production deployment follows from a successful development push.

G08 requires tested schema/format compatibility, real AWS and supported self-host
restore, ARM64 image provenance, secret/license scans and operational runbooks.
Unknown schema versions/gaps/checksum drift fail before migration. Journal readers
reject unknown versions. A development Down migration does not prove safe live
downgrade. Backup restore verifies PG base/WAL plus all referenced S3 objects;
never adopt newer unreferenced objects or expose partial restoration as healthy.
