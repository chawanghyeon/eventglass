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

### Official-duration baseline and maintenance/measurement audit

The clean `f415e55` run completed all1/2/4-worker profiles with5min warmup,
30min offered load and a bounded10min drain. Its native10k/100k/1m/10m oracle
also passed (`.tools/native-oracle.15JVAC`). The pinned Go1.27.1/DuckDB2.0
ARM64 environment used Colima4CPU/8,308,363,264bytes RAM; each Go role had
CPU1/512MiB/swap0. No heavy checks ran alongside these measurements. Retained
raw reports are `.tools/comparison-report-1.CXtyDe`,
`.tools/comparison-report-2.m9Qt6Y` and `.tools/comparison-report-4.QTUXw9`.

| Workers | Legacy ACK p95 ms | Rows / histogram p95 ms | Visibility p95 ms | Load backlog slope/min | Whole-installation sampled peak bytes |
|---|---:|---:|---:|---:|---:|
|1|369|378 /451|1,470|+0.03113|2,382,721,186|
|2|373|331 /414|734|−0.00338|2,644,322,023|
|4|373|327 /404|624|−0.00282|3,030,690,822|

Every run accepted220,500 and passed the complete public count oracle with
zero final backlog, conflicts and cgroup OOM/kills. All **former** target maps
are true. The ACK values above include warmup:12,600 samples, not the10,800
load-only samples required for a30min run. They are not corrected ACK SLO
evidence. Constant105records/s offered load does not measure maximum capacity
or scaling efficiency. Runtime scratch and these disposable PG/S3 stores were
tmpfs-backed; these results do not establish larger disk-backed capacity.

| Workers | Full GET requests /bytes | Range requests /bytes | HEAD | PUT requests /bytes | PG WAL bytes | S3 retained bytes |
|---|---:|---:|---:|---:|---:|---:|
|1|105,534 /1,491,102,540|10,546 /255,685,959|139,467|40,554 /676,078,332|653,293,808|676,231,097|
|2|105,406 /1,486,680,994|16,785 /457,723,656|140,585|40,513 /673,990,517|661,884,816|674,148,727|
|4|105,361 /1,472,676,348|24,287 /757,257,100|140,709|40,501 /667,130,415|688,209,712|667,130,248|

Post-load same-snapshot cold/warm regex times were299/303,287/219 and283/202ms;
Range requests119/0,116/0 and137/0; Range bytes11,207,030/0,
11,201,221/0 and11,380,129/0. Only Eventglass block caches were reset, not
provider/OS caches. Each60s idle check saw no new query work. These are scoped
observations, not a claim of end-to-end or competitor superiority.

Audit found that the worker ran maintenance without measured spare capacity,
and a queued/prepared/expired maintenance claim could bypass pressure that
arrived after reservation. Regression-only tests on old code actually failed:
four maintenance attempts with continuously ready foreground work;12 warmup
ACK samples in a2s fake-clock test; all12 real PostgreSQL compact/retain ×
queued/prepared/expired × ingest/query-pressure cases claimed work improperly.
The isolated PG cases use metadata fixtures, not physical GC/backup evidence.

App now owns a bounded61-bucket spare-lane admission budget. Only observed
idle waits earn credit; maintenance attempt time includes failures and actual
joined cleanup. Admission reserves100ms for termination, predicts credit expiry,
records overruns and blocks subsequent work while in debt. This is wall-time
admission accounting, not a hard retrospective CPU percentage guarantee.
Control rechecks pressure in the claim transaction without another round trip.
Native runners retain permits/files until their private process group has
terminated and been reaped. Linux adopts orphan descendants as a subreaper but
waits only on the owned group. The harness excludes warmup ACK latency and
requires exact measured sample count, maintenance accounting/progress and zero
budget overruns. Physical GC's real backup interlock is unchanged.

An identical20s/300s/90s one-worker diagnostic compared the preservedf415e55
images with the first budget candidate (2s cancellation reserve). The baseline
`.tools/comparison-report-1.UqIPaJ` passed latency/completeness targets but
failed the new maintenance-accounting target. The candidate
`.tools/comparison-report-1.N3cvhL` passed accounting but **failed** both query
latency targets: rows/histogram p95 increased336/388→1,686/2,047ms. It spent
only2,979ms on maintenance despite54,400ms measured idle; its last query scanned
1,729 objects versus128 before. Both accepted33,600, measured1,800 load-only
ACKs, returned exact32,000 logs/1,600 errors and ended with zero backlog/OOM.
ACK p95 was369→368ms. No heavy checks ran during the sequential pair. The
failure is retained, not relabeled a pass or a performance improvement.

The2s reserve starved short spare intervals: a deterministic700ms foreground/
second fixture could not run even100ms maintenance within10s. The corrected
100ms TERM grace preserves unconditional KILL/join/reaping and passes that
progress-within-budget regression. Darwin can remove a terminated process from
its group before init removes its orphan PID; its test requires immediate group
absence and bounded eventual PID reaping, while Linux requires immediate reaped
PID absence. EPERM during Darwin group observation is not treated as exit.
Twenty repeated host process-group tests and app/control race tests passed.
Actual PG tests also reproduced three queued/running/prepared active-lane
starvation cases for compaction and both retention selectors: each repeatedly
selected an unreservable lane0 despite eligible lane1. Candidate selection now
excludes active lanes using the existing partial unique index, while reservation
still owns the transactional race/fence checks. No index/schema change is needed.

The subsequent100ms-grace/active-lane-fix candidate was measured under the
same20s/300s/90s one-worker profile, again with no concurrent heavy checks.
`.tools/comparison-report-1.xeG5dP` remains a **failed** diagnostic: rows/
histogram p95=1,441/1,856ms, ACK p95=369ms, visibility p95=1,947ms and
five maintenance budget overruns. Actual maintenance elapsed9,641ms is below
56,800/4ms total idle credit, but the per-attempt overruns correctly fail the
gate instead of being hidden by aggregate accounting. Load-backlog slope also
fails at+0.64474/min; final backlog nevertheless drains to0. All33,600 accepted records pass
the exact public count oracle; OOM/conflicts0 and measured ACK samples1,800.

| Diagnostic | Whole-installation sampled peak bytes | Worker observed cgroup peak bytes | Full GET requests /bytes | Range requests /bytes | HEAD | PUT requests /bytes |
|---|---:|---:|---:|---:|---:|---:|
|f415e55 baseline|860,230,776|76,742,656|15,770 /106,912,656|1,846 /20,689,654|23,294|6,129 /43,560,831|
|2s-reserve candidate|878,423,571|71,446,528|11,871 /62,929,187|2,237 /19,007,575|121,900|5,943 /31,414,429|
|100ms-grace/lane fix|841,878,599|71,532,544|12,646 /67,558,548|2,208 /19,011,180|104,873|5,972 /31,743,611|

These are cgroup/installation observations, not separate native RSS or Go
allocation measurements. The105records/s offered rate is fixed, not independent
capacity. Actual request-byte hashes differ because installations/times/retries
differ; the logical fixture definition and limits are unchanged. Fewer rewrites
reduce some S3 transfers but grow query HEAD/read work; do not label the combined
result a speedup or lower total production cost. Compaction's per-input control
queries, native inspection cost, cancellation/join slack and maximum-size
rewrite progress remain to be measured and resolved. Corrected official-duration
runs and independent capacity/efficiency still cannot be closed.

Executed verification for this fix: host ARM64 unit/vet, architecture/layout,
generated-contract equality, app/control race checks and comparison regression
tests passed. Final disposable PostgreSQL/MinIO integration passed in13.548s;
Linux ARM64 native query/Live integration passed in24.497s. Pinned native
contracts/vet passed, including the new Linux process-group tests. A separate
non-root/read-only CPU1/512MiB/swap0 run repeated20 times also killed and reaped stubborn
descendants, rejected a zero-exit leader leaving an orphan, and preserved an
unrelated command's child. The default2min resource profile passed with24
cycles/12,600 records,251ms worst cycle,141,873,152-byte cgroup peak, zero
OOM and no leftover scratch; native OOM/cancel and permit-drain checks passed.
These are containment observations, not end-to-end throughput. Actual
pgBackRest2.59.1 base+WAL restore into two private PGDATA volumes passed with
the signed verification import, generation bump and missing-S3-object
fail-closed check. The fixture's18-byte object is not a backup capacity test.
Frontend29 tests and production typecheck/build passed; Chromium's actual
SDK-to-Issue production-image flow passed in2.7s. Raw logs are retained
under `.tools/maintenance-after-*` and `.tools/maintenance-final-*`; no external
alerts or deployment occurred. Colima disk exhaustion initially interrupted
image export and a test-binary link. Fourteen obsolete Eventglass experimental
images were removed, preserving all raw reports, current before/after images,
the pinned native cache and unrelated containers/volumes. Repeated integration,
contract image export and the non-root lifetime tests then completed.

### Maintenance metadata loader and real retention regression

The next audit reproduced a functional M2 defect on committed3bdd56e:
`LoadRetention` called a compaction loader requiring at least two bundles,
although retention reserves exactly one. A real PG metadata test failed with
`compaction inputs are incomplete`. The new pinned-native end-to-end test also
reproduced this after actual durable ingestion, conversion, S3 publication and
two-input compaction; it failed before the mixed-retention rewrite. Earlier
synthetic prepared-manifest tests did not exercise this loader path.

The fix keeps transaction/fence policy in control and existing workflows in
ingest/maintenance: retention permits exactly one input, compaction two to128.
Project associations are read with the reserved metadata; every referenced
file pair is read in one bounded query. Stale authority, canceled requests and
a payload whose intent is no longer referenced remain failures. No API/schema,
authorization, durability, native engine, backup attestation or GC bypass changed.

Matched real PostgreSQL17.11 measurements used Go1.27.1 Darwin ARM64, one warmed
connection, isolated schemas and five samples per input count. No heavy build
or verification ran alongside these samples. Median values:

| Reserved inputs | Before/after SQL queries | Before/after latency ms | Before/after Go allocation bytes | Before/after allocations |
|---|---:|---:|---:|---:|
|2|8 /5|3.044 /2.014|10,104 /8,904|154 /125|
|128|260 /5|95.982 /13.730|564,456 /407,008|7,161 /3,534|

The SQL-count test enforces at most six queries independently of input count
and checks exact bundle/file/project association. This is a PG metadata-loader
micro-measurement, not native rewrite speed, whole-service throughput, capacity,
RSS or R3 completion. S3 requests/transferred bytes are0 in both measurements;
RSS was not measured. Raw samples: `.tools/maintenance-load-before.log` and
`.tools/maintenance-load-after.log`; real native reproduction:
`.tools/maintenance-retention-before.log`. Native per-input inspection cost,
cancellation-budget overruns, maximum-size rewrite progress and corrected
official-duration/capacity measurements remain open.

Executed checks for this change: ARM64 Go unit/vet and architecture/layout,
generated-contract equality, control/maintenance race checks, real host
PostgreSQL/MinIO integration(14.888s), and pinned Linux ARM64 native
query/Live/maintenance integration(25.167s). Full native contracts/vet and image
export passed with the unchanged dependency cache. The final retention test,
including a64MiB no-spill independent pair reader and an assertion of zero S3
I/O for fully expired retirement, passed three times in a non-root/read-only
CPU1/512MiB/swap0 container(0.66/0.63/0.64s). It checks exact paired identities,
the inclusive received-time floor, old snapshot catalog/file preservation,
stale fence, cancellation before work, idempotent prepared-swap retry, revoked
membership, unchanged published_seq, zero scratch/permit residue and no
physical GC. The first full run reached revocation correctly but expected the
wrong error class; the test now requires the existing unauthenticated contract
for a removed membership. No production authorization behavior was changed.
These functional timings are not paired performance measurements. Logs:
`.tools/maintenance-loader-*` and `.tools/maintenance-retention-bounded.log`.
The freshly built ARM64 production image also passed Chromium's actual
SDK-to-Issue UI flow(2.7s); no external alert or deployment occurred.

### Batched native compaction input verification

The remaining native hotspot issued three input queries per bundle and computed
full-file/block hashes which were never compared or consumed. Inputs had already
been downloaded with exact full-SHA verification. The engine now checks all
analytics inputs in one ordered scan and all payload inputs in another. Each
individual input must have a nonempty, strictly ordered, valid record-ID set
with its expected SHA and matching role counts; analytics scope is checked for
every row, not filtered before verification. The union of swapped file pairs is
not sufficient. Output inspection and its full/block SHA evidence are unchanged.
Input file128MiB and combined256MiB checks occur before opening the engine.
Native sorting remains subject to the existing task memory/spill limits; the
Go verifier keeps at most128 counters and one streaming hash, not all rows.

The pinned engine's scan provenance was tested with real Parquet files that
swap record sets and forge a physical filename column to impersonate the other
input. Neither role can bypass per-input checks. The initial test incorrectly
expected an extra filename column alone to be rejected; the actual engine uses
its scan provenance, so the final test asserts the meaningful anti-spoofing
property. Scope, NULL/zero project/batch, out-of-day bounds, malformed/duplicate
IDs, missing/empty role, swapped pairs, repeated files and canceled work all
fail closed in the focused native tests. These use real conversion outputs and
real Parquet mutations; no S3/PG mock is involved in this native-only boundary.

`BenchmarkCompactionPairedInputs` compares the unchanged01a8430 native code and
the candidate using16 canonical records per input,2/8/32/128 inputs, five samples
of three operations each. Both use Go1.27.1 Linux ARM64, the same pinned DuckDB
library, CPU1/512MiB/swap0, non-root/read-only root,256MiB private tmpfs,
GOMEMLIMIT96MiB/GOMAXPROCS1, native256MiB/spill256MiB and no network. Fixture
construction is excluded from operation timing; output cleanup is included.
No other heavy checks/builds ran during either measurement. Median results:

| Inputs / records | Before/after ms per operation | Before/after records/s | Before/after Go allocated bytes | Before/after allocations |
|---|---:|---:|---:|---:|
|2 /32|74.321 /71.144|430.6 /449.8|6,367,629 /2,164,677|1,932 /1,814|
|8 /128|140.433 /76.469|911.5 /1,674|19,096,189 /2,294,589|6,205 /5,359|
|32 /512|415.440 /98.451|1,232 /5,201|70,011,061 /2,816,757|23,296 /19,528|
|128 /2,048|1,509.122 /184.480|1,357 /11,101|273,667,941 /4,904,568|91,648 /76,172|

The largest observed process maxRSS was98,267,136 versus97,914,880bytes. This
includes fixture construction and prior samples, is not isolated task RSS or
cgroup peak, and does not establish a significant RSS reduction. Go allocations
exclude unmanaged native memory. S3 requests and network bytes were0 in both
runs. These are small-input local native rewrite measurements, not the previous
catalog-HEAD measurement, whole-service throughput, provider cost, independent
capacity, or a maximum256MiB-input proof. R3 remains incomplete, including its
maintenance cancellation/join overruns and corrected official-duration runs.
Raw samples and focused regressions: `.tools/native-compaction-before.log`,
`.tools/native-compaction-after.log`, `.tools/native-compaction-focused-final.log`.

Host ARM64 unit/vet, architecture/layout, generated-contract equality and
app/maintenance race checks passed. Real PG/MinIO integration passed(14.751s),
including the actual native query/Live/retention paths(25.251s). Full pinned
ARM64 native contracts/vet and image export passed; the additional sparse-file
boundary test passed separately and proves admission arithmetic only, not
maximum-byte Parquet execution. The default2min resource configuration completed
24 cycles/12,600 records in115.467s, worst cycle272ms, cgroup peak142,065,664bytes,
OOM0 and scratch0; native OOM/cancel/join and permit drain checks passed.
Chromium's real SDK-to-Issue production-image flow passed(2.7s). The unchanged
DuckDB dependency cache was reused. Logs: `.tools/native-compaction-unit.log`,
`integration.log`, `contracts.log`, `byte-bounds.log`, `resource.log` and
`browser.log` under the same `.tools/native-compaction-` prefix.

The next actual one-worker20s/300s/90s service diagnostic completed all post-load
phases but **failed** the rows/histogram and load-backlog targets. Compared with
the prior3bdd56e-policy run(`comparison-report-1.xeG5dP`), this candidate includes
both the bounded control-loader and native-inspection changes. Rows/histogram
p95 were864/1,053ms(previous1,441/1,856ms), ACK369ms, visibility1,248ms and
load-backlog slope+0.11549/min(previous+0.64474/min). Final backlog0 is not a
substitute for the failed load-growth target. All33,600 accepted records matched
public counts(32,000 logs/1,600 errors), measured ACK samples1,800, query samples59,
conflicts/OOM0; one phase-boundary query was canceled. Last planned files fell
from1,496 to761. Compaction recorded185 progressed calls(including prepare/swap,
not185 completed merges), no failed calls and10.966s total time; observed spare
time69.400s. There were no maintenance-budget overruns in this run, but this
does not prove all cancellation schedules or maximum-byte tasks fit the budget.

Whole-installation sampled peak was922,679,769bytes(previous841,878,599), worker
observed cgroup peak72,822,784bytes, PG WAL86,626,088bytes. S3 full GET14,194 /
78,939,273bytes, Range2,181 /19,689,492bytes, HEAD66,973, PUT6,006 /
33,415,988bytes. More rewrites reduced HEAD/query work but increased full GET/
PUT traffic compared with the previous candidate; no overall cost/RSS reduction
is claimed. Cold/warm all-run regex939/685ms, Range610/3, same snapshot/rows;
10s idle had no query work but470 full GETs from maintenance. The provider/OS
cache was not cleared. Both diagnostics use the same logical fixture/limits,
not identical submitted bytes(installation/time/retry identities differ), and
fixed105records/s is offered load, not independent capacity.

Retained failure evidence: `.tools/comparison-report-1.HMaw0M`, log
`.tools/native-compaction-comparison.log`, exact candidate code checksums
`.tools/native-compaction-source-sha256.txt`. The next checks must cover larger
maintenance inputs, including the shared downloader's then journal-sized
24MiB cap, and improve query latency/maintenance progress under the unchanged
spare-time policy. Corrected official-duration1/2/4 runs, independent capacity
and R4 provider/release evidence remain open.

### Verified-download size contracts

Actual MinIO objects of24MiB+1byte and128MiB reproduced the shared downloader's
unconditional24MiB rejection before any GET. This affected maintenance's legal
128MiB files and public/Live/alert query results with a64MiB contract, not just
journals. The caller now supplies its operation limit; storage additionally
enforces the shared128MiB format ceiling. Journal admission remains24MiB.
Full-byte SHA/size verification, private exclusive creation and fsync remain;
declared Content-Length mismatch fails before reading the body. HTTP/API schemas
and capability gates are unchanged.

Real-object regressions cover24MiB+1byte, exactly64MiB and exactly128MiB,
one GET/exact transfer counts, operation-limit rejection with no file/network
I/O, wrong size/SHA, missing objects, preservation of existing paths and
in-flight cancellation followed by retry. A controlled response-close barrier
also proves partial files remain owned until the response has finished closing;
that unit test supplements, rather than replaces, the real MinIO cancellation.
The raw-object fixtures are deliberately not presented as Parquet validation.

The separate pinned-native regression durably accepts16 batches of16 unique
events with deterministic96KiB high-entropy fields, converts and publishes each,
then compacts256 records into actual analytics/payload files of approximately
37,896,657/37,941,286bytes. It downloads both files, checks paired identities,
rewrites at the exact last-batch received-time floor and fully retires at floor+1.
It verifies the pinned old snapshot/bytes, revoked authorization, stale fence,
cancellation, idempotent swap retry and zero reserved disk/scratch after joined
work. Physical GC is not enabled and no backup attestation is synthesized.

Important boundary failures remain: the initial8-batch/64-events-per-batch
fixture failed during conversion with a16MiB allocation request at252.2/256MiB
native usage. Splitting into32 batches of16 allowed conversion but failed the
512-event compaction child. Reducing the separate download regression to16
batches does not resolve either failure. Their evidence remains in
`.tools/large-download-diagnostic.log` and
`.tools/large-download-integration-second.log`. Maximum256MiB-input rewrite
progress and fixed-profile cgroup bounds still need proof. Functional download
timings are single observations, not a speedup, RSS or service-throughput claim.
The original rejection evidence is `.tools/large-download-before.log`.

The final actual PG/MinIO suite passed in16.936s and its pinned-native
query/Live/alert/maintenance subset in32.015s. A separate Linux ARM64 run used
CPU1/512MiB/swap0, non-root/read-only root,96MiB Go soft limit and an isolated
disk-backed scratch volume. All real download and large-retention cases passed,
OOM/kill counters0; the large-retention case took7.62s and recorded52 PUTs /
178,200,443bytes,52 HEADs,142 full GETs /659,961,316bytes,0 Range GETs. Counts
include fixture creation and independent verification, not only production
rewrite work. The same cgroup also ran the24/64/128MiB raw-object cases.
Observed memory.peak was536,875,008bytes with memory.max536,870,912 and
memory.events.max837: it reached the512MiB boundary (peak one4KiB page above
the configured limit), not spare-memory evidence. This includes page cache
and is not anonymous RSS. Do not advertise a passed maximum-input resource
gate or a latency/throughput improvement from these functional checks; other
verification was active on the host. Evidence: `.tools/large-download-bounded.log`
and `.tools/large-download-integration-final.log`.
ARM64 unit/vet/architecture/layout, generated-contract byte comparison, race
checks for storage/ingest/maintenance/API/app, full pinned static native
contracts/vet and final-image Chromium(2.7s) also passed. The frozen source
design checksum is unchanged. Logs use the `.tools/large-download-` prefix.

### Wide canonical conversion memory

The64-wide-event failure above is now reproduced independently using the real
normalizer and the pinned native conversion, without PG/S3 or a child-error
wrapper obscuring the cause. Each event contains96KiB of deterministic
high-entropy text. The request passes the unchanged canonical admission rules.
The original implementation failed a16MiB native allocation at252.2/256MiB;
the container did not OOM. Combining JSON paths into one expression and then
materializing that expression in a separate statement both still failed. These
discarded candidates are not the implemented solution.

The existing engine appender now keeps scope JSON, raw JSON, envelope SDK JSON,
warnings and canonical metadata in separate bounded VARCHAR columns. Analytics
and payload share the same canonical metadata instead of storing the wide
attributes twice. Payload output canonicalizes only each required fragment,
using the same pinned engine JSON semantics instead of decoding the entire
stage four times. Normalization visits appender byte-bounded chunks in monotone
order, using a single chunk cursor; the completed per-partition temporary table
then feeds the existing sorted Parquet COPY. This
retains numeric formatting, Unicode/HTML escapes and absent/null distinctions;
a direct-string-copy candidate failed the new byte-for-byte reference test and
was corrected before acceptance. Separate columns alone passed256MiB but still
failed192MiB; byte-bounded normalization is required as well. The appender's
4MiB pre-flush accounting includes all five string columns (plus a bounded
single row when the internal stage format is larger), with a2048-row cap and
one integer chunk identifier. No stored format, public API, native memory/spill limit,
receipt-selection rule or module ownership changed.

The legal64-event native regression now passes with exactly64 raw payloads,
paired analytics/payload files9,480,152/9,483,827bytes under both192MiB and256MiB
native limits. Its fresh CPU1/512MiB/swap0 cgroup running both subtests observed
memory.peak342,695,936bytes, memory.events.max/OOM/kill0. This is one wide-input case,
not the entire maximum-input space or a worst-case worker RSS proof.

Matched local measurements used Go1.27.1, the same pinned static library,
CPU1/512MiB/swap0, GOMAXPROCS1,96MiB Go soft limit,256MiB native memory/spill,
non-root/read-only root, isolated disk-backed scratch and no network. There
were no competing builds or heavy checks during timing. Each median below is
five samples of three operations, with fixture setup excluded from timed Go
allocations. Before isb5de17e; after is the shared-metadata, byte-chunked
implementation, not the earlier candidates that still failed192MiB.

| Native conversion case | Median before → after | Go bytes/op before → after | Go allocations/op before → after |
| --- | --- | --- | --- |
|16 wide events|165.431→155.080ms|69,532,618→56,032,482|3,644→4,040|
|1 ordinary event|88.057→92.137ms|2,217,181→2,218,205|921→1,094|
|100 ordinary events|96.036→98.465ms|3,983,373→3,573,016|13,222→14,874|

The wide case's median rate was96.72→103.2records/s. Process maximum RSS across
setup and all samples was179,712,000→144,150,528bytes; fresh benchmark cgroup
peaks190,492,672→154,906,624bytes, including file cache. Go bytes exclude native
allocations. Allocation *counts increased*, and ordinary small-batch medians
were slightly slower; this is not an across-the-board performance improvement.
S3 requests and transferred/network bytes were0 in both local measurements.
No end-to-end throughput, cost, query-SLO or competitor claim follows from these
numbers. Logs and binaries use `.tools/wide-conversion-`, with `before` and
`chunked` identifying the matched pair. The512-wide-record compaction failure,
corrected official-duration R3 runs and independent capacity remain open.

Final verification also passed ARM64 unit/vet/architecture/layout and codegen,
app/ingest/maintenance race checks, full static native contracts/vet (including
all scalar/attribute/null mappings and actual mid-conversion cancellation followed
by retry), actual PG/MinIO16.823s and the native query/Live/alert/maintenance
subset30.012s. The durable fixture now uses4 batches of64 wide events rather
than16 batches of16, so its timings and S3 totals are not a matched performance
comparison with the earlier download regression.
The strengthened real pipeline also passed separately under CPU1/512MiB/swap0,
non-root/read-only root and disk scratch. It produced analytics/payload files
37,895,746/37,928,775bytes; its fixture plus verification recorded16 PUTs /
198,259,603bytes,16 HEADs,46 full GETs /699,859,633bytes,0 Range GETs.
Together with the24/64/128MiB raw-object tests, this cgroup reached exactly
536,870,912bytes and memory.events.max926, with OOM/kill0: functional success
does not establish spare-memory headroom.
The default resource gate actually ran115.445s,24 cycles/12,600 records,
worst cycle257ms, peak142,020,608bytes, OOM0 and scratch0; native OOM/cancel/drain
checks passed too. Final-image Chromium passed in2.6s. Execution logs use
`.tools/wide-conversion-chunked-`; source and matched binary checksums are in
`.tools/wide-conversion-chunked-source-sha256.txt`. The source-design checksum
is unchanged. R3/R4 gates remain incomplete.

### Wide native compaction and ordered byte-bounded output

The previous512-record failure was reproduced with32 actual native input pairs,
152,053,783 compressed bytes, CPU1/512MiB/swap0,256MiB native memory and256MiB
spill. Verification and a separate sorted-table materialization succeeded, but
the analytics COPY failed at255.8/256MiB. Merely lowering the row-group byte
target, lowering page sizes, changing COPY ordering, disabling external-file
cache or reducing read-ahead did not fix it. No pinned dependency was changed.

The engine now uses Parquet uncompressed metadata to retain a direct COPY path
for small inputs. Wide inputs materialize scalar keys and actual byte sizes,
then write ordered4MiB ranges, at most eight ranges per invocation, and concatenate
the private native Parquet files in numeric range order. A4KiB minimum row charge,
1,024-partition cap, conservative byte reservation and actual manifest checks
bound intermediate work. Reserved intermediate space is subtracted from the
request's existing spill budget, not added to it. Payload sizing uses existing
string lengths rather than re-serializing JSON. Ordinary full-pair compaction
does not repeat a payload semi-join after exact per-input identity verification;
retention still filters payload by the exact retained analytics IDs.

This path deliberately repeats some **local** scans to bound native writer
buffers. It does not introduce additional S3 downloads, parallel writers,
new storage engines, public formats or transaction authority. All original
identity/scope checks, final SHA/block/statistics checks, fenced catalog swap,
snapshot protections and fresh-backup GC interlock remain in place.

The stronger native regression converts56 input pairs/896 actual wide records:
266,090,129 compressed input bytes, below the256MiB input bound. Both192MiB
and256MiB native profiles produce exactly896 records and analytics/payload files
132,977,110/133,052,290bytes, each below128MiB. Every native column is independently
hashed before/after, and physical analytics/payload ordering is checked by file
row number. The final large-input fixture uses the existing production2GiB
spill allowance; this is not a product-limit increase or a claim that its entire
intermediate set fits256MiB spill. The full-value verifier uses the existing
192MiB query profile; an earlier64MiB verifier failed on the large result.
The4106ecd baseline was also rebuilt with this identical56-pair/2GiB-spill
fixture: it fails at191.9/192MiB and255.9/256MiB respectively, with cgroup
peak509,136,896bytes and OOM/kill0. Thus extra spill alone does not explain the
successful rewrite. See `.tools/wide-compaction-matched-regression-before.log`.

Actual mid-partition cancellation joins native work, removes spill and retries
the unchanged inputs successfully. Separate tests check exact received-time
retention boundaries, all-retained payload filtering, insufficient intermediate
spill admission/retry, numeric partition order, extra files, invalid names,
symlinks and byte/count limits. The two large-profile tests took35.02s including
fixture construction and independent verification; this is **not** compaction
latency. Their cgroup observed memory.peak536,875,008bytes and max-events9,390,
OOM/kill0. The cancellation/retention cgroup reached536,870,912bytes with
max-events2,300 and OOM/kill0. These runs do not establish spare-memory headroom.

Matched measurements use before4106ecd versus this bounded writer, Go1.27.1,
the identical pinned static library, CPU1/512MiB/swap0, GOMAXPROCS1,96MiB Go soft
limit, non-root/read-only root, isolated disk scratch and no network. No builds
or other heavy checks run during timing. Wide measurements use the same2GiB
spill allowance on both sides; the existing ordinary-input benchmark keeps its
256MiB allowance. Each median is five samples of three operations; setup is
excluded from timed Go allocations but included in process/cgroup peaks.

For16 wide pairs/256 records, latency is515.512→1,025.038ms and throughput
496.6→249.7records/s. Go bytes/op are2,472,917→2,572,546, allocations/op
10,244→11,828. Reported process max RSS is542,871,552→272,465,920bytes; fresh
cgroup peaks are536,870,912→449,191,936bytes, including file cache. The kernel's
process high-water RSS and charged cgroup counter are different observations;
neither is Go allocation accounting. Both OOM/kill counts are0; max-events
13,222→0. S3 requests/transferred bytes and network bytes are0 in both runs.
This is a memory/progress fix with a **wide-input latency regression**, not a
throughput improvement, service-SLO result or competitor comparison.

The unchanged ordinary16-record-per-input fixture also measures the metadata
admission overhead; it is not hidden by the wide-input result:

| Input pairs | Median before → after | Go bytes/op before → after | Go allocations/op before → after |
| --- | --- | --- | --- |
|2|69.719→70.842ms|2,164,856→2,169,688|1,813→1,909|
|8|75.904→79.923ms|2,294,688→2,305,152|5,359→5,467|
|32|98.715→106.477ms|2,821,392→2,853,989|19,528→19,685|
|128|186.189→213.294ms|4,913,725→5,033,397|76,172→76,521|

The ordinary cases are slower too. Their full before/after logs are
`.tools/wide-compaction-small-{before,after}.log`; all S3/network counts are0.

Raw matched logs use `.tools/wide-compaction-benchmark-{before,after}.log`;
regressions use `.tools/wide-compaction-queryprofile.log` and
`.tools/wide-compaction-focused.log`; source and binary hashes are recorded in
`.tools/wide-compaction-source-sha256.txt`.

ARM64 unit/vet/architecture/layout and generated-contract checks pass. Full
pinned-native contracts/vet pass (engine66.043s), followed by actual PostgreSQL/
MinIO17.050s and native query/Live/alert/maintenance integration35.960s.
App/ingest/maintenance race checks pass in4.312/4.171/1.759s.
The durable wide fixture now uses8 batches of64 records, not the earlier4, so
these are not matched performance comparisons. Its separate CPU1/512MiB/swap0,
non-root/read-only-root run passes in11.02s and produces analytics/payload
76,011,247/76,027,270bytes. Fixture plus verification records28 PUTs /
377,780,164bytes,28 HEADs,78 full GETs /1,363,406,089bytes and0 Range GETs.
Together with the actual24/64/128MiB verified-download regressions, its cgroup
peak is536,870,912bytes, max-events690, OOM/kill0. No physical GC is performed.
Logs use `.tools/wide-compaction-{contracts,integration,integration-bounded}.log`.
The default resource gate runs115.465s,24 cycles/12,600 records at logical105/s,
worst cycle257ms, cgroup peak142,188,544bytes, OOM0 and scratch0. Native OOM,
cancellation/join and permit-drain checks pass. Final-image Chromium passes in
2.7s. Resource/browser logs use the same `.tools/wide-compaction-` prefix.
The source-design file remains byte-identical.

Corrected official-duration service runs,
maintenance-budget progress and independent capacity/cost evidence still remain;
R3/R4 are not closed by these local results.

### Canonical physical order survives maintenance

A follow-up audit found that the existing compactor did not honor DESIGN5.2's
physical sort. Convert ordered both files by project/service(NULLS FIRST)/event
time/record ID; Compact instead used receipt/lane order for analytics and ID-only
order for payload. The09d06ad wide test checked preservation of those old orders,
not conformance to the canonical layout contract. A two-input/six-record native
regression first verifies the conversion outputs, then proves that both merged
files incorrectly order IDs `a b c d e f` rather than the independently expected
`c d e f b a`. It includes different projects, null/empty services and event times
which disagree with receipt order. Before fails in0.314s; the focused fix passes
in0.324s. Those single-test durations are not performance comparisons.

Conversion and compaction now share a fixed native sort definition. Payload's
retained typed analytics layout keys are materialized once and reused across
bounded output batches. Final COPY projects the original five payload columns;
extra layout fields occur only in private intermediates. No raw JSON decoding,
normalizer replay, public schema change, authorization change, receipt ordering
change or retention-floor change is involved. The native engine retains its
byte-bounded writer, per-input scope/identity checks and final pair verification.

The56-pair/896-record regression again passes both192/256MiB profiles, preserving
all column hashes and now asserting canonical physical order for both files.
Outputs are132,971,566/133,052,290bytes. The final-source rerun takes30.79s including
fixture/verification; cgroup peak536,875,008bytes, max-events12,075 and OOM/kill0
are not headroom proof.
Cancellation/retry and retention-boundary tests pass too. A further actual
SDK-shaped fixture uses12 pairs/192 records and57,030,740 compressed bytes,
three projects and null/empty/ASCII/Unicode services. Actual Parquet metadata
asserts that this exercises the wide partitioned path, not only small COPY.
It passes with the focused small regression in a fresh CPU1/512MiB/swap0,
non-root/read-only cgroup: peak377,630,720bytes, max/OOM/kill0.

Matched measurements compare09d06ad with this layout correction, using unchanged
fixtures and the same pinned dependency. CPU1/512MiB/swap0, Go1.27.1/GOMAXPROCS1,
96MiB Go soft limit, isolated disk scratch and network denial remain identical.
No heavy work runs alongside timings; each median uses five samples of three
operations. Wide input uses2GiB spill on both sides, ordinary cases256MiB.
For16 wide pairs/256 records, latency997.986→1,114.394ms, rate256.5→229.7records/s,
Go bytes/op2,572,522→2,576,178 and allocations/op11,828→11,848 are measured.
Process max RSS286,744,576→334,110,720bytes and cgroup peaks452,435,968→512,221,184
include fixture setup; max/OOM/kill remain0. S3/network requests and bytes are0.
The additional typed layout join has a measured cost; this is contract repair,
not a performance improvement. Service SLOs and pruning benefits are unproven.

| Ordinary input pairs | Median before → after | Go bytes/op before → after | Go allocations/op before → after |
| --- | --- | --- | --- |
|2|69.442→79.499ms|2,169,672→2,171,141|1,908→1,929|
|8|76.668→87.521ms|2,305,280→2,307,400|5,467→5,486|
|32|103.749→117.785ms|2,853,984→2,856,584|19,685→19,705|
|128|203.833→240.290ms|5,036,186→5,051,053|76,523→76,542|

These small-input cases also become slower. Their cgroup peaks are102,789,120
and104,603,648bytes, max/OOM/kill0. Logs use the `compaction-order-small-` prefix.
Evidence uses `.tools/compaction-order-{before,after,large,focused,scoped}.log`
and `.tools/compaction-order-wide-{before,after}.log`.

Final-source ARM64 validation passes unit/vet/architecture/layout/codegen, all
pinned-native engine contracts/vet (69.758s), PostgreSQL/MinIO integration
(16.385s), native query/Live/alert/maintenance integration (36.388s), and the
Chromium operator check (2.8s). A separate CPU1/512MiB/no-swap, non-root,
read-only disk-backed run verifies real512-record ingest/publication/compaction
and retention in11.43s. Its paired files are76,012,426/76,027,270bytes;
PUT28/377,780,744bytes, HEAD28, full GET78/1,363,410,786bytes and Range0 include
fixture setup and verification. With the real24/64/128MiB download checks, that
cgroup reaches536,870,912bytes/max-events1,960/OOM/kill0; no headroom is claimed.
The default resource gate passes24 cycles/12,600 logical records in115.447s,
105/s, worst cycle256ms, cgroup peak142,397,440bytes/OOM0/scratch0, including
native OOM/cancel/join and permit drain. These are scoped regression checks,
not corrected official-duration R3 capacity or release evidence. Logs use
`.tools/compaction-order-{unit,codegen,large-final,contracts,integration,integration-bounded,resource,browser}.log`.
Fresh uncached host ARM64 race checks pass app4.289s, ingest4.061s and
maintenance1.754s. The frozen source-design SHA256 remains unchanged.

A fresh service diagnostic on clean0004b4f uses20s warmup/300s load/90s drain,
one ARM64 worker and the existing CPU1/512MiB/no-swap role profiles. It **fails**
rows/histogram p95 (1,444/1,750ms) and load-backlog slope (+0.493072/min, max21).
ACK p95 is372ms with exactly1,800 load-only samples, visibility p952,012ms,
59 completed mixed queries and final backlog0. The public oracle sees exactly
32,000 logs/1,600 errors. Last/max planned objects reach1,324; conversion has
1,864 progress calls/233,469ms, compaction100 progress calls/6,697ms total attempt
time with no failures, observed spare48,300ms and no budget overruns. Progress
calls are not completed merge counts. Whole-installation sampled peak854,045,227
bytes includes PG/S3; worker cgroup peak65,536,000bytes and all cgroup OOM/kill0.
Measured S3 full GET12,922/69,386,905bytes, Range2,196/19,076,892bytes, HEAD93,292,
PUT5,973/31,939,373bytes and PG WAL91,804,264bytes include setup/query verification.
Independent empty-worker-cache cold/warm regex is1,658/1,212ms with identical
snapshot/rows, Range1,165/5 (11,356,845/42,538bytes). Ten-second idle creates no
query work but does710 full GETs for maintenance, so it is not zero-I/O idle.
Evidence: `.tools/comparison-report-1.OqImKB` and
`.tools/compaction-order-comparison.log`. This is neither a matched comparison
against earlier product revisions nor the required official-duration R3 pass.

### Bounded reservation round trips

The R3 audit also found a remaining N+1 path in `ReserveCompaction`: each input
added two metadata reads and two reservation writes. A real PostgreSQL query
tracer reproduces14/38/134/518 calls for2/8/32/128 inputs, including transaction
boundaries. Control now locks the caller-sorted input set in one bounded read,
aggregates the requested files' sizes, and performs one checked set update and
one checked `INSERT SELECT`. Every count must match the complete requested set;
lane locking, runtime generation, partition/256MiB/128-input limits and the exact
sorted newline-framed identity hash remain authoritative. The shared single-input
retention validator uses the same lock/read path. No HTTP/domain ownership,
schema, public API, native codec, S3 access or GC attestation policy changes.

The unchanged regression fails before on query count and passes after at9 calls
for every size. Independent expected hashes and exact per-input generation/hash
associations match. Actual PG negative tests pass missing/foreign-tenant/wrong-lane/
closed/cross-partition/oversize/stale-generation/duplicate-UUID-alias rejection,
zero partial rows, cancellation while an input is locked, and a joined retry
race with exactly one winning complete reservation. An initial test fixture
omitted the required `retired_at` when closing a bundle; that fixture was repaired
without weakening the database constraint, then both revisions were rerun.

Matched five-sample measurements use0004b4f versus the control-only correction,
the same Go1.27.1 Linux ARM64 build/dependencies and disposable PostgreSQL, fresh
CPU1/512MiB/no-swap non-root/read-only test cgroups, Go96MiB/GOMAXPROCS1, identical
fixtures and one warmup per size. Each sample has an unmeasured150ms pause so
fixture/reset CPU throttling does not carry into the short reservation. Builds
and other heavy checks do not overlap timing. Medians follow:

| Inputs | Calls before → after | Time before → after | Go bytes before → after | Go allocations before → after |
| --- | --- | --- | --- | --- |
|2|14→9|1.493→1.391ms|6,368→6,848|168→200|
|8|38→9|3.296→1.596ms|20,576→17,216|528→405|
|32|134→9|9.975→2.789ms|80,760→67,208|1,977→1,216|
|128|518→9|49.882→16.013ms|318,344→267,416|7,741→4,400|

Reservation rates computed from these medians are670→719,303→627,100→359 and
20.0→62.5reservations/s respectively; these are not end-to-end ingestion or
completed-compaction throughput. Tiny-input allocations increase. Process max
RSS35,008,512→35,086,336bytes and cgroup peaks34,684,928→34,148,352bytes include
fixture setup; max/OOM/kill0 on both sides. Different RSS/cgroup accounting is
not a memory-saving claim. S3 requests and bytes are0 (PG network is real).
Source/library hashes, binaries and raw logs remain in
`.tools/reservation-roundtrips.V3jMBy`; final aggregate log is
`.tools/reservation-roundtrips-final.log`. R3 service SLO/capacity remains open.

Final validation passes unit/vet/architecture/layout/codegen, real PostgreSQL/
MinIO integration23.057s, pinned-native query/Live/maintenance integration36.571s
(including the512-record large paired bundle/retention path), and Chromium2.8s.
Actual pgBackRest2.59.1 base backup plus WAL/S3 recovery passes: backup1s, two
restored clusters2s, target LSN0/5022540, verified old-generation read0.010s,
newer unreferenced object excluded, generation activation/session invalidation,
and missing referenced object fails closed. This remains isolated MinIO recovery,
not authorized AWS or selfhost-provider release evidence. Host ARM64 uncached
race checks pass control1.360s/app4.243s/maintenance1.651s; frozen source-design
hash is unchanged. Logs use `.tools/reservation-{unit,codegen,integration,recovery,browser}.log`.

The subsequent clean0bf0956 Mode A build reran all four native-fixture-v1 sizes
in separate CPU1/512MiB/no-swap ARM64 cgroups with192MiB native query memory.
All four actual test containers exit0 with OOM/kill0; typed filters, exact
partition identities, paired files, half-open boundaries and equal-time pages
pass. The outer `scripts/check` dispatcher nevertheless returns2: it was edited
to add the capacity command while its delegated native test was running, so its
shell resumed reading at an obsolete source-file offset. This is an execution
mistake, not an engine test failure, but the whole command is **not** recorded
as passing. Freeze the scripts before the next complete command run.

| Records | Native-oracle elapsed | Analytics files / bytes | Cgroup peak bytes |
| --- | --- | --- | --- |
|10,000|2.006s|4 /540,409|186,687,488|
|100,000|11.338s|26 /5,318,576|189,206,528|
|1,000,000|102.274s|221 /52,794,512|244,535,296|
|10,000,000|996.152s|2,196 /528,495,771|536,875,008|

The10m run includes normalization148.943s/conversion600.338s,682,231,716 journal
bytes and277,310,711,032 cumulative supervisor allocated bytes (not resident
memory). Its cgroup has2,149 max events and no OOM/kills; the one-page transient
peak over the limit is not headroom proof. S3 requests/network bytes are0.
Actual10m envelope SHA remains
`1497dc8c86ac7ed8990b081b5d74e71833a75ef3185a4e6529fdb218aca2f5ec`.
These are scoped observations, not a paired performance improvement or R3 SLO
pass. Container states, hashes and logs: `.tools/native-oracle.uXZ3wm` and
`.tools/reservation-native-oracle.log`.

### Independent queued-work capacity matrix

`scripts/check capacity` reuses the comparison runner's isolated ARM64 topology,
budgets and resource collectors. It does not alter runtime scheduling, Accept,
publication, query authorization, snapshots or backup/GC interlocks. Actual HTTP
ACKs preload four projects while workers are paused. Requests are sequential so
random lane coalescing cannot change the number of durable jobs between runs.
The observer verifies zero publication and the exact pending job count before
signalling resume; measured drain includes Docker resume overhead. It then
checks every project's complete log/error count through the public query API.
Preload, drain and query-verification work are distinguished; no monthly bill
is inferred from a finite queued-work experiment.

The32-cycle quick matrix on0bf0956 plus the dirty harness passes all three
profiles, each with3,360 records/192 jobs and four exact800-log/40-error public
results. Native dependencies are unchanged and no heavy build/check overlapped
the timed phases. Colima has4 CPUs/8GiB; every Go role retains CPU1/512MiB/swap0,
non-root/read-only operation and bounded scratch. PG/S3 are fresh shared tmpfs
services, not AWS or disk-backed capacity evidence.

| Workers | Drain | Records/s | Relative speedup / efficiency | Whole-installation sampled peak bytes |
| --- | --- | --- | --- | --- |
|1|23.203s|144.809|1 /1|459,397,920|
|2|12.301s|273.149|1.886 /0.943|432,165,352|
|4|7.400s|454.054|3.136 /0.784|567,839,550|

Observed worker cgroup peaks are40,824,832 bytes for1 worker,
41,586,688/41,893,888 for2 and38,637,568–40,022,016 for4; all role/provider
incarnations have resource observations, OOM/kill0. These are observed memory
peaks, not total Go allocations or unbounded capacity claims. Drain full GETs
are960 for every profile, transferring5,697,414/5,696,117/5,696,880 bytes;
PUTs384 with2,692,249/2,691,725/2,692,208 bytes; HEAD387/388/390; Range GET0.
Drain WAL is4,363,080/4,150,336/4,215,048 bytes. Public count verification adds
192 Range GETs and1,695,602/1,695,162/1,695,510 bytes, recorded separately from
drain. Maintenance remains enabled, so provider totals include its actual work.

This establishes executable measurement, not a product before/after speedup,
maximum live-ingestion throughput, linear scalability or R3 completion. The
full matrix requires128 cycles/13,440 records/768 jobs and three fresh samples
per profile on clean source. The report rejects partial project counts,
differently batched jobs, already progressing or undrained work, invalid hashes,
inconsistent rates, missing PG/S3 resources or unverified cgroup limits; its
capacity policy does not relax sustained-run restart/cold incarnation checks.
Evidence: `.tools/capacity-quick.log`, `.tools/capacity-matrix.OYQQEj`, and
`.tools/capacity-report-1.ywSL2p`, `-2.s9086K`, `-4.VbSSQW`.

Validation passes unit/vet/architecture/layout, byte-identical Go/TypeScript
code generation, shell syntax, and uncached ARM64 race checks for comparison
16.909s/report1.238s. The first sandboxed race attempt could not bind localhost;
the permitted rerun executes the real HTTP retry tests successfully. The fresh
non-root ARM64 release image also passes its actual Chromium flow in2.6s.
Frozen source-design bytes remain unchanged. Logs use
`.tools/capacity-{unit,codegen,race,browser}.log`. No API/schema or capability
coverage changed; R3/R4 remain incomplete.

The subsequent full fixed-work matrix runs clean4ced580 (immutable application
and precompiled driver images) with128 cycles/13,440 records/768 jobs and three
fresh installations for each worker count. The whole command exits0 and every
profile passes complete public counts (four projects,3,200 logs+160 errors each),
resource coverage/limits and OOM/kill0. No heavy build/check overlaps measurement.
Only unrelated, unexecuted follow-up test files were authored after the measured
images and driver were frozen; those files are not part of these measurements.

| Workers | Three drain times (s) | Median records/s | Speedup / efficiency | Whole-installation peak range (bytes) |
| --- | --- | --- | --- | --- |
|1|99.901 /105.201 /99.601|134.533|1 /1|570,306,853–619,901,351|
|2|52.903 /52.403 /52.402|256.474|1.906 /0.953|511,700,890–601,166,444|
|4|31.801 /29.503 /29.601|454.039|3.375 /0.844|629,424,519–671,892,894|

Worker cgroup peaks across all nine samples are40,222,720–43,143,168 bytes.
Every drain makes3,840 full GETs (22,794,991–22,800,636 bytes),1,536 PUTs
(10,771,074–10,774,046 bytes), no Range GET, and1,539/1,540/1,542 HEADs for
1/2/4 workers. Drain WAL spans17,118,200–17,610,064 bytes. Each public oracle
adds768 Range GETs and6,782,650–6,785,063 bytes, separate from measured drain.
The fixed-work fixture SHA is
`0ab36b2a73e517ede5432cca7149ec61efc8acc5cc6f2b90bd1edfb249457d27`;
actual submitted-byte hashes, progress and runtime/library/driver identities are
retained per sample. Evidence: `.tools/capacity-full.log`,
`.tools/capacity-matrix.I2gjkh`, and its nine referenced `capacity-report-*`
directories. This is a scoped capacity/efficiency pass, not a mixed-load SLO,
AWS result, maximum sustained rate, release or product before/after speedup.

### Conversion streaming and disk-admission correction

Three new regressions reproduce gaps on4ced580. The actual pinned ARM64 child
converts an8-day durable-ACK batch into16 local files before the first upload,
contrary to the one-outstanding-pair contract. A pure workflow regression also
shows scratch creation with no shared disk budget. An output-boundary regression
shows a matching full/block checksum does not prevent following a symlink to a
file outside the assigned output directory. Baseline logs are
`.tools/conversion-stream-before-{unit,native}.log`; the actual PG/S3/native
failure takes0.714s. These findings do not invalidate prior scoped tests, but
their coverage was insufficient to prove these wider contracts.

The correction under test keeps ingest owning upload/Prepare, app owning child
groups/permits, and engine owning COPY. Conversion uses bounded1MiB framed
messages and a per-index continue acknowledgement after verification/upload
and removal of the completed pair. Other single-result child operations retain
their existing protocol. The shared disk reservation includes journal,64MiB
bounded staging, two128MiB outputs, occurrence/manifest pages and the existing
2GiB spill allowance. Missing admission fails before scratch creation; cleanup
failure retains the reservation. Output symlinks/nonregular files or changed
sizes fail before full checksum reads. An additional framing regression caught
truncated/whitespace trailing bodies being confused with clean EOF; it is fixed
without accepting partial terminal data.

The EOF-corrected ARM64 image passes the actual8-day ACK/publication path
in0.74s. Go1.27.1 ARM64 unit/vet/architecture/layout, generated-contract parity,
focused race checks and the full pinned-native contracts gate pass. Actual child
tests cover cancellation while a consumer owns the pair, shared-gate retention,
joined cleanup, fresh retry and rejection of wrong-version/index/abort ACKs.
Pure workflow tests also cover exhausted shared-disk admission, an exact stage
write boundary and reservation retention until a cancelled runner returns.

Initial full checks were not successful: one new test fixture incorrectly used
a nonprivate temporary root; that fixture is corrected. A later contract run
hit Colima `No space left on device` during the existing wide compaction test.
Five obsolete task-generated build images were removed (not the pinned native
cache, before/after baseline, current candidate or data volumes), increasing
available space from2.9 to7.8GB. A sequential retry passes the full contract
tests and vet (`.tools/conversion-stream-contracts-retry.log`, image
`sha256:ab71afe58377d5ae3816ad10d6ce47ca94ddbb519107a83d5baa4fa219bb0ad0`).
The concurrent10,000-day integration attempt also ended without a child summary
after122.79s, and remains a failed attempt, not a publication pass; its precise
child error was not exposed by the supervisor. A bounded isolated rerun now
passes both8-day errors (0.69s) and a legal10,000-day log container (733.56s)
through real durable ACK, pinned native conversion, S3 upload, Prepare and
Publish. Every callback checks exactly two output files and the expected
single-record identity for its day; the final catalog contains exactly10,000
current bundles/rows/distinct days and scratch is empty. Normal60-second leases
are heartbeated every15seconds, not enlarged for the test. The non-root,
read-only CPU1/512MiB/swap0 container has174,440,448B memory.peak, zero
memory.max/OOM/kill events and exit0; it uses384MiB scratch and96MiB Go soft
limit. PG/MinIO are disposable external providers, not included in that worker
cgroup peak. Evidence: `.tools/conversion-stream-capped.eWuBn5` (the earlier
`.VQvEFy` attempt failed before work because PostgreSQL was not ready; the runner
now awaits readiness). The final integration gate incorporates this bounded
leg, additionally asserting release of the production shared-disk reservation
and reporting actual S3 counters. The full final integration command now passes
(`.tools/conversion-stream-integration-final.log`): general PG/S3 tests21.937s,
native maintenance/query/Live tests35.780s,8-day conversion0.73s and10,000-day
conversion741.66s. The latter has172,769,280B cgroup peak, zero max/OOM/kill
events and no residual disk reservation. Its measured provider operations are
20,001PUT/96,930,793B,20,001HEAD,40,002fullGET/193,861,586B, and0Range requests/
bytes. These include journal upload/download, bundle upload readback and
publication re-verification; content checks were not replaced by uploader
metadata. Exact record identity and all10,000 current day partitions pass.
The final host checks also pass29 frontend tests, strict TypeScript and the
production UI build. The default resource gate passes24 paced cycles/12,600
records over115.384s (the nominal2-minute profile schedules its last cycle at
115s), worst cycle257ms,141,549,568B cgroup peak, zero OOM and no scratch entries.
Native resource-exhaustion, joined cancellation and owner-held permit drain
checks pass (`.tools/conversion-stream-resource.log`). This is the scoped R1
profile, not R3 sustained service SLO evidence. The production ARM64 image's
actual SDK-to-UI browser flow passes (2.7s); the crash gate passes its durable
ACK/Prepare/Publish SIGKILL and retry checks, plus admission/canonical-limit
checks. Its five-sample ACK-to-visible p95 is125.155483ms and files span
2,777–7,256B; this small crash-harness sample is not a sustained latency claim.
Logs are `.tools/conversion-stream-{browser,crash}.log`.

The frozen-source full Mode A command now also exits0 for all four sizes; this
replaces the earlier incomplete outer-command evidence, not its historical
failure record. New evidence is `.tools/native-oracle.uvQxKQ` with image
`sha256:417d2e2d0860cd97ecad7a969ca11b895fc74199ac22d44d7238fa1685bf5986`
and product binary SHA256
`23c49b500f84fed4b0e560624092981cdd273fdb0db2d883e146e7b322624dcb`.
Every size passes normalization/journal/conversion, exact per-partition identity,
typed/null/missing/dotted-key filters, half-open time bounds, equal-time keyset
pages and retry identity contracts, with0S3/network requests and bytes.

| Records | Elapsed ms | Analytics files / bytes | Cgroup peak bytes |
| --- | --- | --- | --- |
|10,000|1,767|4 /540,409|175,992,832|
|100,000|10,412|26 /5,318,576|192,913,408|
|1,000,000|95,761|221 /52,794,512|249,573,376|
|10,000,000|955,331|2,196 /528,495,771|536,879,104|

All containers exit0 with OOMKilled=false and zero OOM/kill counters. The10m
profile has2,338 memory.max events; its kernel peak is8KiB above the configured
512MiB limit, so this is memory-pressure/reclaim evidence, not spare headroom.
Its actual SDK envelope SHA remains
`1497dc8c86ac7ed8990b081b5d74e71833a75ef3185a4e6529fdb218aca2f5ec`.
These timings are correctness-run observations, not a matched service speedup.
The streaming/disk correction has scoped local verification; corrected official
R3 sustained SLOs and R4 release evidence remain incomplete.

The matched real-process benchmark is complete: frozen4ced580 versus the
EOF-corrected candidate, same fixed staged records, CPU1/512MiB/swap0, Linux
ARM64, pinned DuckDB, native memory/spill256MiB, supervisor Go soft limit96MiB,
GOMAXPROCS1, read-only root and512MiB temporary filesystem. Each profile has
five2-second samples; no other heavy check/build ran during either measurement.
It includes child startup, native conversion, supervisor checksum verification
and output cleanup, but excludes durable ingestion, staging, PG and S3.

| Records / days | Median ms before → after | Scoped records/s before → after | Supervisor B/op before → after | allocs/op before → after |
| --- | --- | --- | --- | --- |
|1 /1|102.129 →102.053|9.792 →9.799|4,352,823 →2,114,071|205 →140|
|100 /1|106.960 →104.755|934.928 →954.607|4,353,069 →2,114,147|205 →140|
|8 /8|565.271 →558.061|14.153 →14.335|33,808,532 →16,855,842|830 →530|

Removing the repeated parent verification cuts measured supervisor allocation
bytes by50.1–51.4%; latency medians differ by only0.07–2.06%, not evidence of a
material end-to-end throughput gain. Kernel cumulative max RSS across the full
benchmark process is child99,835,904 →98,222,080B and supervisor27,598,848
→26,714,112B. Cgroup memory.peak is69,525,504 →70,684,672B (higher, not a memory
headroom improvement); both have zero memory.max events/OOM/kills and exit0.
Those kernel/cgroup accounting scopes differ and must not be summed. S3 request
counts and transferred bytes are0 on both sides (network denied). Raw samples,
compiled test binaries and container/image observations are retained in
`.tools/conversion-process-bench.lRH9ZU/{before,after}.log`; the candidate image
is `sha256:5c716a36e557b89c9b28316048af4556955684db515c3b4f2b72ae13fab67ec2`.

### Conservative first-page scan pruning

The clean64d6038 one-worker20s/300s/90s diagnostic still fails: rows/histogram
p95=893/1,166ms (server656/927ms), ACK p95=370ms, visibility p95=1,351ms,
backlog slope+0.698678/min and final backlog0. All59 queries complete and public
counts match32,000 logs+1,600 errors, including warmup. The profile retains
its failed targets; successful drain/cold/warm/idle/OOM checks do not close R3.
Evidence: `.tools/comparison-report-1.xBNSbV` and
`.tools/r3-query-baseline-64d6038.log`.

The next candidate preserves full paired catalog HEAD verification, authorization,
snapshot pins, row predicates and fixed retry partitions, but query can prove
that older files cannot affect a constant-true first page. Control derives
complete requested-project coverage in the same bounded catalog page query.
Only fully matching file row counts establish the limit-plus-one threshold;
partly matching files do not contribute, and equal-time candidates always stay.
Exact reconstruction of the existing cursor-free operation excludes cursors,
filters, detail, Live and aggregates. This is query-owned pre-seal pruning, not
a new HTTP fast path or an engine/service replacement.

Actual Go1.27.1 ARM64 unit/vet/layout, race, generated-contract parity, pinned
native contracts, PG/S3 and production-image browser checks pass. The native
integration gate27.359s includes actual durable ingestion of four two-row bundles:
both descending sorts scan one file on the first page, keep all four files for
cursor pages and return every expected row in the independently expected batch
order. All eight catalog HEADs remain on every page. A missing older object
fails before planning even though it could be pruned; membership removal denies
before S3 access. Real catalog tests distinguish intersecting versus complete
project scope. Unit tests cover boundaries, partial scope/cuts/retention,
lookahead, equal times, overflow, changed operations, deterministic retry and
300 independently generated row sets. Existing cancellation/join tests pass.
The initial revocation assertion expected403 rather than the existing missing-
membership unauthenticated result; only that test expectation was corrected.
Evidence: `.tools/rows-pruning-{unit,race,codegen,contracts,integration-final,browser}.log`.

A matched planning-only benchmark runs full-scan and pruning branches on the
same verified catalog and candidate binary, not different service workloads.
Each side uses Linux ARM64 CPU1/512MiB/swap0, GOMAXPROCS1, Go soft limit96MiB,
read-only root and denied network, five one-second samples per file count.

| Files | Median full/pruned ms | Median full/pruned B/op | Full/pruned allocs/op |
|---|---:|---:|---:|
|256|0.395199 /0.237312|434,027 /357,749|465 /1,022|
|1,024|1.636284 /0.423364|2,819,751 /1,049,587|533 /1,053|
|8,192|12.324695 /1.658564|27,572,696 /7,577,045|1,016 /1,314|

Planning latency decreases40.0–86.5% and allocated bytes17.6–72.5%, but allocation
counts increase. Whole benchmark cgroup peak also increases62,844,928→132,157,440B;
both have zero memory.max/OOM/kill events. Therefore this is not a memory-headroom
improvement. PG/S3/native execution are excluded, requests/transfers are0 on both
sides, and no service throughput/SLO or competitor advantage follows. Raw logs
and the compiled binary are retained in `.tools/rows-pruning-bench.YjkErx`.

The following real mixed-load comparison uses the same fresh-installation
20s/300s/90s one-worker profile, pinned engine, CPU1/512MiB/swap0 roles and
100 logs/s+5 errors/s offered load. No heavy checks/builds overlap either timed
run. Baseline is clean64d6038; candidate is its pruning working tree, recorded
as64d6038-dirty, runtime image
`sha256:c8648b45e7a002207ec52e7203dac68af474e123f8259a06e410b72dced170cf`.
This is a scoped optimization comparison, not an official-duration pass.

| Metric | Full-scan baseline | Pruning candidate |
|---|---:|---:|
|Rows p95 /server p95 ms|893 /656|312 /164|
|Histogram p95 /server p95 ms|1,166 /927|995 /803|
|ACK p95 /visibility p95 ms|370 /1,351|371 /821|
|Load backlog slope jobs/min; final backlog|+0.698678;0|−0.720254;0|
|Query task work /work ms|205 /28,561|132 /16,753|
|First /last /max planned files|103 /863 /872|6 /7 /663|
|Whole-installation sampled peak bytes|792,211,749|794,107,573|
|API /worker observed cgroup peak bytes|155,848,704 /71,643,136|146,874,368 /68,775,936|
|S3 HEAD requests|70,914|62,677|
|S3 PUT requests /bytes|5,999 /33,038,143|5,954 /33,411,734|
|S3 full GET requests /bytes|13,923 /76,768,475|14,281 /80,217,970|
|S3 Range requests /bytes|2,185 /19,593,114|2,082 /19,084,481|

Both complete59 measured queries with no query failures, exact32,000 log/1,600
error public counts including warmup, zero OOM and no maintenance-budget overrun.
Offered/accepted work is unchanged, so this is not a maximum-throughput increase.
Query objects combine rows and histograms; only the former uses pruning.
Compaction trajectories differ (182→202 work phases) and all I/O counters include
foreground/maintenance work, so neither total S3 deltas nor histogram changes
can be attributed solely to the proof. Some transfers and whole-installation
peak increase. The candidate passes row/ACK/visibility/backlog targets, but
histogram995ms still exceeds500ms and the overall comparison exits1. Cold/warm
all-history regex, exact same-snapshot rows,10-second idle and three worker
cgroup incarnations pass; cold/warm Range requests are562/3. R3 remains open.
Evidence: `.tools/rows-pruning-comparison.log` and
`.tools/comparison-report-1.Miw90V`; use retained directories rather than an
older official-profile convenience JSON when checking this quick diagnostic.

### Histogram empty buckets without repeated input scanning

On the476b071 product baseline, a regression reproduces two references to the
scoped source and pinned DuckDB2.0 `EXPLAIN (FORMAT JSON)` confirms two physical
Parquet scans. Completing empty buckets re-evaluated the whole source predicate
instead of reusing the aggregate. The correction retains only histogram state
in an explicit native materialized CTE: without dimensions it has at most2,000
buckets and8 metric states per bucket. No raw-row materialization, metadata-only
counting, new query service or skipped integrity/authentication checks are added.
Grouped/no-gap requests, reducer state and empty-input semantics stay unchanged.

The generated and physical-plan regressions now pass with one source scan.
Actual native tests cover negative epoch, exact half-open bounds, tenant/project/
kind/retention/lane/user predicates, missing-only populated buckets, integer
count/excluded/limb state, empty input and the2,000-bucket maximum under32MiB
native memory. The initial numeric fixture omitted its required declared integer
type; the test was corrected without relaxing product validation. Unit/vet/
architecture, race and generated-contract parity pass; full pinned-native tests
and vet pass (engine69.398s, subprocess contracts0.851s). Real PG/S3 query/Live/
alert/authority/retry checks pass27.378s and production-image Chromium passes2.7s.
Evidence: `.tools/histogram-{rescan-before-unit,rescan-before-native,native,unit,race,codegen,contracts,integration,browser}.log`.

The same final benchmark/fixture/oracle code is compiled against the frozen old
query builder and corrected builder. Each profile uses five one-second samples,
Linux ARM64 CPU1/512MiB/swap0, GOMAXPROCS1, Go soft limit96MiB, native memory/
spill256MiB, non-root/read-only and denied network. Input hashes frame each file
as BE64 byte length followed by its bytes in fixture index order. Both sides
have identical hashes, sizes and independently verified counts in every bucket:
32 files/3,200 rows/56,635B hash
`9067dec9a48e3b394acb04939145a3ba76f8dcee7ea4437bb6e657634426ba6c`;
256 files/25,600 rows/452,834B hash
`9de7c2916d63adc0313f45439fca47b5d6d7e34bddef870a3ebc33c5cc6956c6`.

| Files | Median before/after ms | Before/after B/op | Before/after allocs/op |
|---|---:|---:|---:|
|32|34.237677 /27.220350|1,114,798 /1,102,826|794 /678|
|256|91.531703 /64.120128|1,294,638 /1,282,430|1,696 /1,580|

Native operation latency falls20.5%/29.9% (about29.2→36.7 and10.9→15.6 native
operations/s respectively). These operations include native open/COPY/output
inspection, but not a process supervisor, PG, S3 or service queuing. Go B/op
excludes native allocations. Whole benchmark cgroup peaks, including fixture
setup and independent verification, are90,177,536→89,292,800B; both have zero
memory.max/OOM/kill events. This small peak difference is not broad memory-
headroom evidence. S3/network requests and bytes are0 on both sides. The earlier
exploratory baseline lacks the final hash/bucket oracle and is not substituted
for this pair. Retained final artifacts:
`.tools/histogram-before-final.vaPGhL` and `.tools/histogram-after-final.uBoGMK`.
These native measurements alone do not close the mixed-load R3 SLO or R4 release.

The subsequent freshly built1-worker20s/300s/90s diagnostic retains evidence in
`.tools/comparison-report-1.CZky1f` (`.tools/histogram-comparison.log`). Its matched
baseline is the preceding `.tools/comparison-report-1.Miw90V`, not an older
convenience report. Both use the same fixture definition and logical load;
timestamps, submitted-envelope hashes and compaction scheduling differ. This
single service pair is not a repeated statistical estimate:

| Measurement | Before | After |
|---|---:|---:|
|Rows p95 /server p95 ms|312 /164|344 /168|
|Histogram p95 /server p95 ms|995 /803|848 /655|
|ACK /visibility p95 ms|371 /821|371 /799|
|Load backlog slope per minute /final backlog|−0.720254 /0|−0.744368 /0|
|Query work /busy ms|132 /16,753|134 /14,419|
|Maximum query files /bytes|663 /7,393,897|682 /7,602,007|
|S3 HEAD requests|62,677|64,933|
|S3 PUT requests /bytes|5,954 /33,411,734|5,962 /33,018,065|
|S3 full GET requests /bytes|14,281 /80,217,970|14,274 /79,220,960|
|S3 Range GET requests /bytes|2,082 /19,084,481|2,077 /18,920,588|
|PG WAL bytes|80,312,256|79,790,152|
|Sampled whole-installation peak bytes|794,107,573|785,737,840|
|API /worker cgroup peak bytes|146,874,368 /68,775,936|145,653,760 /66,129,920|

All59 measured queries succeed and the public oracle returns32,000 logs/1,600
errors. Maintenance makes progress, with73,600ms observed spare time, zero
budget overruns and zero OOM. The expected cancellation probe accounts for the
one canceled query job, not a measured query failure. Cold/warm all-history
regex measures919/697ms, Range604/3, with equal snapshot/rows, three observed
worker incarnations and no query work during10s idle. The histogram target is
still false and the command exits1; corrected official-duration profiles and
maximum-size maintenance evidence remain required. No overall throughput or
production memory-headroom claim follows from these diagnostic samples.

### Maximum paired maintenance resource path

`./scripts/check maintenance-resource` now builds the current root ARM64 binary,
compiles the test before observation and runs real durable ingestion, native
conversion, S3 publication, compaction and mixed/full retention under one
CPU1/512MiB/swap0 cgroup. It retains only isolated disk-backed scratch, with the
existing native256MiB/spill2GiB and role-owned3.5GiB disk admission (4GiB minus
512MiB safety reserve), shared with conversion. The process is non-root,
read-only, GOMAXPROCS1 and GOMEMLIMIT96MiB. Disposable PostgreSQL/MinIO are outside
this worker cgroup; this is not a whole-installation512MiB claim.

The14-batch/896-record seeded high-entropy fixture produces265,537,875 actual
paired input bytes. The replacement has133,023,607 analytics and133,062,481
payload bytes (266,086,088 total); both input/output must fall within250–256MiB
and existing per-file128MiB checks remain active. The executed native children
use the unchanged pinned library SHA
`84ad753acc1390e13ce56e728d75d379bebeedec7f3df58ce071c1b373e4c79f`.
Compaction including actual downloads, uploads and Prepare takes6.859335s;
mixed retention takes1.248170s, fully expired metadata-only retirement0.001346s.
The full test passes19.88s. Exact paired identities, pinned old snapshots,
half-open retention, canceled/stale requests, completed retry, revoked read
authority, zero residual scratch/reservations and absence of physical GC pass.
The canceled request in this fixture is canceled before execution; it does not
replace the separately executed mid-native-cancellation resource regressions.

Actual S3 totals are PUT46/641,741,372B, HEAD46, full GET126/2,347,278,883B and
Range0/0B, including fixture creation and independent old/new pair reads.
Kernel memory.peak is536,875,008B (4KiB above the configured536,870,912B limit),
with5,513 memory.max pressure events and zero OOM/kill events. This reaches the
512MiB limit: it proves completion with
reclaim in this profile, not memory headroom. File cache is included in cgroup
usage. There is no before/after speedup or service-throughput claim.

Evidence is `.tools/maintenance-resource.AazeXE/{environment.txt,run.log,state.json}`
and `.tools/maintenance-resource-final.log`; image
`sha256:9520dc25db169f0ba00f1a29ebd68ade47d17d84cfa71ae4f4f3764505508898`.
The first runner attempt stopped before creating test services because a random
Compose project suffix contained uppercase characters; lowercasing that suffix
fixes the harness. The successful run uses actual objects, not a raw-download
substitute or fabricated manifest. It invokes existing maintenance workflows
with joined heartbeat supervision, **not** the app's rolling20% dispatcher.
Maximum-byte progress under that dispatcher still requires separate evidence;
the6.9s work duration alone cannot prove eventual admission/retry success.
R3's service histogram/official-duration gates and R4 remain incomplete.
The earlier passing `.tools/maintenance-resource.K01kbI` used separate4GiB
test budgets; it is superseded by the final shared role-budget run above.

A subsequent repeated resource/actual-dispatcher run on957d0ca-derived sources
reproduces a killed native compaction child and kernel OOM events. Therefore
the preceding single successful resource execution does not establish reliable
maximum-byte completion. Retain the failure evidence and keep this boundary
open until the repeat failure is corrected and reverified; do not increase the
512MiB cgroup or replace this fixture with smaller/compressible input.

The actual worker harness subsequently reproduced two separate defects. At the
old256MiB managed limit the direct child and dispatcher suffered three kernel
OOM kills (`.tools/maintenance-resource.szGpZO`); lowering it to192MiB alone
still suffered two (`.tools/maintenance-resource.iw5vFu`). Full-row analytics
JSON sizing fails at128MiB when requesting another16MiB at123.7MiB used
(`.tools/max-compaction-native-128.log`). Separately, the real30s lease retry
schedule can repeatedly spend fresh idle credit on canceled partial rewrites:
the four-minute synctest completes zero jobs in eight attempts before the fix
(`.tools/maintenance-dispatch-starvation-before.log`).

Typed UTF-8 sizing removes that JSON copy, retaining every scalar/nested
variable column, conservative overhead, the2x intermediate reservation and
actual file-manifest checks. App keeps all-attempt accounting and fences, but
delays only the budget-canceled operation slot for one60s window; ordinary
errors retain250ms delay and other work continues. The starvation regression
then passes. This does not manufacture idle credit or expand the20% grant.

The128MiB candidate passed both direct and actual-dispatcher tests once
(`.tools/maintenance-resource.aBFdTb`), but its fresh repeat
(`.tools/maintenance-resource.mYPbhc`) killed the direct child with OOM1 even
though the dispatcher again passed81.72s. Both outcomes are retained; a
successful dispatcher does not erase a failed direct execution. Lowering the
managed limit to64MiB alone or with two write ranges fails in the native writer
at56.7–57MiB while requesting8MiB;96MiB with eight ranges also fails. Reducing
writer fan-in to three ranges at96MiB completes native compaction in13.221s,
too long for the12s admission ceiling even before S3 I/O. Four ranges complete
the same265,497,584B/896-record native rewrite in8.371s. These two diagnostic
processes subsequently suffered OOM in their separate192MiB complete-row
verification, not in Compact; their outer commands remain failed. Simply
lowering that verifier to96MiB still reproduced OOM. The final independent
oracle hashes each named/typed column with native JSON semantics, then chains
those digests in schema order. It reads one input file and one value column at
a time, with16-ID pages, at most64 columns/8192 records and96MiB managed memory
plus an isolated2GiB spill limit. Every nested/decimal/null value is still
compared; physical sort and exact paired identities remain separate assertions.
Negative tests detect changed nulls, nested strings/integers/null structs,
decimal values, physical types, renamed and additional columns. This removes
both the complete-row JSON copy and repeated wide decoding across all inputs;
it changes no product S3 path. `.tools/max-compaction-native.ptCnRP/run.log`
passes the265,497,584B native fixture and all these assertions in32.38s, including
7.885s Compact; CPU1/512MiB/swap0 peak536,875,008B,8,480 max events, OOM0.
Actual durable workflow and full native contracts are required independently
of this diagnostic.

The final96MiB/four-range candidate passes two fresh complete
`maintenance-resource` invocations. Both use actual durable SDK ingestion and
14 paired inputs/896 records/265,537,875B, the same product binary SHA256
`e141e6204a6c40f872288b28925c48a90941c8f70e63996f3e8c770dd9dcd07e`, pinned
native library, CPU1/512MiB/swap0,96MiB Go limit,2GiB native spill and the existing
shared role disk budget. Each starts its own actual worker and proves canceled
compaction **and retention** eventually complete after fresh idle credit;
old snapshot identities, revocation, half-open retention and zero physical GC
remain mandatory. The second retention wait is expected cancellation/retry,
not excluded from the wall time or worker I/O.

| Evidence directory | Direct compaction / whole test | Actual dispatcher whole test | Idle credit / all-attempt work | Cgroup peak / max events / OOM kills |
| --- | --- | --- | --- | --- |
| `.tools/maintenance-resource.ozjNEF` |9.329s /22.57s|145.66s|112,200ms /12,956ms|536,875,008B /12,453 /0|
| `.tools/maintenance-resource.dypcOY` |9.212s /22.13s|146.03s|113,700ms /13,146ms|536,870,912B /12,319 /0|

Both report zero budget overrun; cgroup usage still reaches the configured
limit, so there is no headroom claim. Direct outputs total266,094,484 and
266,094,575B; mixed retention takes1.291/1.243s and fully expired retirement
0.802/1.392ms with no S3 work. Actual dispatcher compaction takes71.001/71.251s
including the60s retry window, with three task claims (canceled attempt,
successful Prepare, Swap). Mixed retention takes62.751/63.001s with three claims;
fully expired retirement takes257/254ms with one claim. Kernel counters include
fixture/oracle/supervisor/native descendants, not just the successful child.

Actual direct S3 totals respectively are PUT46/641,750,010B and46/641,750,102B,
HEAD46 each, full GET126/2,347,321,347B and126/2,347,321,804B, Range0.
Worker-only totals are PUT6/294,617,655B and4/285,108,489B, HEAD6/5,
full GET56/1,263,049,591B and56/1,263,018,649B, Range0; canceled attempts are
included. Separate fixture/read-oracle totals are PUT42/356,641,579B and
42/356,641,575B, HEAD42 each, GET92/1,530,580,275B and92/1,530,580,717B.
The separately created installation marker is not included in those store
counters. This is not a before/after speedup or sustained service-SLO claim.

The separate matched native sizing benchmark uses16 paired inputs/256 records,
76,028,076B and identical framed file SHA256
`d94e08e5fc2ec8c0e3971f772ae725389d5ccb1642a9f090b5073684c289d353` in every
sample on both sides. Same Go1.27.1/pinned ARM64 engine, CPU1/512MiB/swap0,
GOMAXPROCS1, Go96MiB, native256MiB/spill2GiB, non-root/read-only, private disk
volume and denied network; both binaries are compiled before timing and run
sequentially without competing builds. Each median is five samples of three
Compact+output-cleanup operations, with setup outside the timer. Every operation
checks exact count/identity. Both use eight write ranges at256MiB; this isolates
the native sizing change, **not** the96MiB actual-worker profile above.

| Metric | Previous JSON sizing | Typed sizing |
| --- | ---: | ---: |
| Median time/op |1.669056060s|1.116696239s|
| Median records/s |153.4|229.2|
| Go allocated B/op |2,576,146|2,589,845|
| Go allocations/op |11,848|11,931|
| Process maximum RSS, including setup |331,247,616B|364,822,528B|
| Whole benchmark cgroup peak |481,218,560B|517,042,176B|
| S3 requests / bytes per operation |0 /0|0 /0|

Latency improves33.1%, but Go allocations and RSS increase; do not describe this
as memory savings or whole-service throughput. Neither side has max-pressure
or OOM events. Evidence: `.tools/maintenance-sizing-bench.wh68E1`, before image
`sha256:ee287e9082686123f2853e32b3de6608b11da8267243e0a283ec627b2c7728a0`,
after image `sha256:9e29f84e45b6b21557d7c4e8f828a527e326ad0ac40abefea0e9fb8db9d2e9e8`.

Root ARM64 unit/layout/architecture/vet and generated-contract comparisons pass;
race checks pass app4.478s/maintenance1.671s/ingest4.025s. Full pinned-native
contracts/vet pass (engine173.206s, process contracts0.843s), followed by real
isolated PG/MinIO query/Live/authority/snapshot/retention integration39.722s.
The first full-contract attempt failed with Colima `No space left on device`,
not a passing check. Removing exactly eight unused old Eventglass test images
freed build space; source/evidence, containers/volumes, current comparison images
and the fixed native cache were preserved. The unmodified tests then passed,
including image export. Logs use `.tools/maintenance-96-{unit,race,codegen,
contracts-retry,integration}.log`; the failed disk run is
`.tools/maintenance-96-contracts.log`. These checks do not close R3/R4.

The final ARM64 production-image Chromium flow passes2.7s
(`.tools/maintenance-96-browser.log`). Building the subsequent fresh comparison
image again exhausted Colima's20GiB disk before measurement. Five further old,
unreferenced Eventglass test images were removed; no containers/volumes or native
cache were removed. The before/after benchmark executables and logs remain,
with executable SHA256 `eb0020f635f97a3d39fb16437e5091a8f400d9dd9e404ed8fc2d6cdc5186c060`
and `c38dd4faad9e96e1c106c042e31ece8094214b55a1ff926ccc739c17520c78ee`.
The following comparison rebuild succeeded without reusing an old application.

The fresh one-worker20s/300s/90s diagnostic is retained at
`.tools/comparison-report-1.skhb95/report.json`, revision957d0ca-dirty, with logs
`.tools/maintenance-96-comparison-retry.log`. Compare with the preceding
`.tools/comparison-report-1.CZky1f/report.json`, not an older convenience file.
Both have the same fixture-definition hash,33,600 accepted including warmup,
32,000 logs+1,600 errors through public query,59 measured successful queries,
zero conflicts and final backlog0. Actual submitted envelopes differ with
fresh installations/timing:10,798/40,179,137B versus11,072/36,933,652B; this is
the same logical profile, **not** byte-identical input like the native benchmark.
New submitted-input SHA256 is
`d3e35ce558e172b075a2a35df3a38651d6421302b65bc049560294909dba812e`.

| Service observation | Preceding profile |96MiB maintenance candidate |
| --- | ---: | ---: |
| Rows / histogram p95 |344 /848ms|516 /1,324ms|
| Rows / histogram server p95 |168 /655ms|193 /1,066ms|
| ACK / visibility p95 |371 /799ms|377 /1,764ms|
| Maximum queried files / bytes |682 /7,602,007B|849 /8,849,878B|
| Conversion work / busy time |1,867 /224,159ms|1,853 /245,468ms|
| Compaction work / busy time |198 /13,115ms|139 /9,915ms|
| Observed idle credit |73,600ms|52,200ms|
| Sampled whole-installation peak |785,737,840B|882,554,960B|

Both new query p95 targets fail500ms; the command correctly exits1. The smaller
native benchmark does not outweigh this service regression. Less idle credit,
fewer compaction work steps and more queried files are observed together; this
single pair does not establish which code/trajectory difference caused them.
All maintenance attempts remain charged (compaction9,959ms+retention108ms+GC31ms),
with zero budget overrun and cgroup OOM. Backlog slope+0.003173/min passes the
existing tolerance, max12; do not describe that as a negative slope. API/worker
cgroup peaks are159,756,288/66,146,304B, with all three worker incarnations observed.

New S3 totals are PUT5,881/31,953,883B, HEAD74,514, full GET13,617/74,407,277B,
Range2,061/18,363,553B; PG WAL79,810,888B. These include foreground and maintenance,
not just query traffic. After load, cold/warm all-history regex takes1,210/877ms,
Range795/4, preserving the same snapshot and rows. Ten seconds idle adds no
query work, but maintenance still reads548 objects/3,827,791B. Restart, cache,
public counts and resource checks pass; R3 requires both query SLOs, diagnosis
of this trajectory, and corrected official-duration profiles. R4 is not released.

### Bounded compaction selection and query boundary evidence

The diagnostic workload now retains at most4,096 per-query wall-time boundaries
and correlates them with durable jobs/tasks in one post-load PG read. It records
pre-job/job/post-job time, files/bytes, scan/reduce counts and attempts, plus
load-end partition and maintenance input counts. It adds no request-path SQL.
Clock mismatch, duplicate/missing jobs and inverted intervals fail rather than
silently clamping. These intervals include waits and result export; they are not
exclusive CPU phases or pure catalog HEAD timings.

Repeating the unchanged0eba7e8 runtime with this instrumentation gives
rows/histogram341/949ms, compared with its previous516/1,324ms. This variation
precludes attributing that earlier regression to the maintenance memory cap
from one pair alone. Slow histograms select515–726 files in three scans plus a
reducer: the three slowest HTTP/job/pre/post intervals are1,228/946/224/57,
1,092/796/251/44 and949/749/163/36ms (integer rounding). At load end, all16 lanes
have active compactions;15 reserve error inputs while log partitions hold11–24
unreserved small bundles. Lexical kind order can indefinitely prefer8 error
inputs over64 eligible logs in the same lane, independent of expected reduction.

Real PG regression tests reproduce that choice, lower-lane8 versus64/129 inputs,
and64 larger files whose32MiB prefix has only8 inputs versus16 tiny files. Control
now ranks the actual bounded prefix by file reduction then rewrite bytes, keeping
the old minimum/target/ceilings and all reservation/pressure/fence checks. Only
the winning at-most128 rows reach Go. Active queued/running/prepared lanes,
cancel, next-candidate reservability and quiet seven-input partitions are tested.
Before fails the expected choice assertions; after passes. Evidence includes
`.tools/compaction-selection-before.v2xRuc`, `.tools/compaction-selection-after.z6x2c2`
and the matched cost reruns below. This changes no native memory, execution
budget, query authority or physical GC interlock.

The same logical168-bundle catalog (8/32/128 per lane, two files each) measures
selector cost separately. Fresh UUIDs mean this is not byte-identical catalog
input. Same Go1.27.1, pinned native library, actual PG, CPU1/512MiB/swap0,
Go96MiB/GOMAXPROCS1, non-root/read-only, five samples of ten calls after warmup,
150ms between samples outside timing; no competing builds or load. The old
source is taken exactly from0eba7e8 and compiled in the same current build image.
Each call still makes two PG queries; no S3 operation occurs. Before picks8
inputs, after128, so this is the cost of the better candidate, not faster
production of the same selected result.

| Selector observation | Before | After |
| --- | ---: | ---: |
| Median ms/call |2.031350|2.229509|
| Derived calls/s |492.3|448.5|
| Median Go B/allocations per call |5,809 /180|69,153 /2,213|
| Process maximum RSS including fixtures |32,088,064B|35,209,216B|
| Cgroup peak / OOM |33,918,976B /0|34,705,408B /0|

Cost evidence: `.tools/compaction-selection-before.M6OBDT` and
`.tools/compaction-selection-after.jV9sqM`. The before command exits1 because its
separate choice assertions intentionally fail; the cost test itself passes.
An initial cost retry found the old image tag had been replaced, before starting
PG or timing; the exact committed source was then compiled as described above.

The matched service diagnostic uses one worker,20s warmup/300s load/90s drain,
the same fixture definition,100 logs+5 errors/s offered load, role CPU1/512MiB
with zero swap, and actual isolated PG/MinIO. This is not a capacity increase or
official-duration result. Before reuses the verified0eba7e8 runtime, executable
SHA256 `e141e6204a6c40f872288b28925c48a90941c8f70e63996f3e8c770dd9dcd07e`;
after is freshly built with executable SHA256
`cd5b8ce6f829269f9134c48b7779ecbeeda50162745af9326015f687ef330e72`.
Both use library SHA256
`84ad753acc1390e13ce56e728d75d379bebeedec7f3df58ce071c1b373e4c79f`.
Reports: `.tools/comparison-report-1.kawvFg/report.json` and
`.tools/comparison-report-1.p9oSqK/report.json`; logs:
`.tools/query-boundary-diagnostic.log` and `.tools/compaction-selection-comparison.log`.
Both commands correctly exit1: histogram still misses500ms. Both return32,000
logs+1,600 errors, all59 measured queries, no conflicts, final backlog0,
accounted maintenance with no overrun, and zero OOM across all incarnations.

| Service observation | Before | After |
| --- | ---: | ---: |
| Rows / histogram p95 |341 /949ms|315 /714ms|
| Rows / histogram job p95 |147 /749ms|148 /565ms|
| ACK / visibility p95 |375 /834ms|370 /769ms|
| Maximum selected files |726|626|
| Load-end error / log bundles |435 /266|331 /200|
| Compaction work steps / all-attempt ms |203 /13,312|237 /15,588|
| Conversion work ms / measured idle ms |221,456 /75,400|217,217 /79,900|
| Scheduler all-attempt ms |581|589|
| S3 HEAD |59,673|51,867|
| S3 PUT count / bytes |5,950 /33,640,765B|5,976 /35,849,052B|
| S3 full GET count / bytes |14,249 /80,517,201B|14,672 /87,137,918B|
| S3 Range count / bytes |2,021 /18,676,080B|2,018 /19,385,895B|
| PG WAL |79,980,840B|79,163,552B|
| Sampled whole-installation peak |795,491,695B|858,437,711B|
| Post-load cold / warm regex |854 /613ms|1,347 /797ms|
| Post-load cold / warm Range calls |581 /3|474 /2|

Actual submitted envelopes differ:10,913/47,140,535B with SHA256
`efe9de2622a2af94816658f9c51535d2a32e6eb849dff574aa238ef215339cf8`, versus
10,814/37,948,635B with SHA256
`7fe23a714ccb8f0b4c54f2d91843d0198c6e0cf290e517938c892e09d4a99746`.
Fresh installation IDs, timing and retries differ despite the same logical
profile. Histograms improve24.8% in this pair, but allocations/RSS/full-GET bytes
and post-load latency increase. Do not generalize this into whole-service,
memory or cost superiority. Backlog max12→18 and slope−0.2354→−0.5825/min both
pass the existing tolerance; no query work is created during the10s idle phase.
Cold/warm results preserve the same snapshot and exact rows within each run.
The remaining histogram SLO and corrected official1/2/4-worker profiles keep R3
open, and R4 remains unreleased.

Root ARM64 unit/layout/architecture/vet and byte-identical generated-contract
checks pass. Control/query/app race tests pass1.381/7.726/4.510s. The fresh
ARM64 comparison build runs the complete pinned-native `go test -count=1 ./...`
and `go vet ./...` with the static-library tag and actual child executable:
engine152.442s and process contracts0.863s pass. This executes the Dockerfile's
contract commands in the existing build image without exporting another large
test image; no alternate native library is used. Separate required-environment
PG/MinIO integration passes25.994s on ARM64; actual native search/Live,
authorization, planning/snapshot, compaction/mixed+expired retention and bounded
candidate tests pass47.869s with no skipped test in that selected native run.
The final non-root/read-only ARM64 image's actual Chromium operator flow passes
2.6s. Logs are `.tools/compaction-selection-{unit,codegen,race,contracts,
integration,browser}.log`. These correctness checks do not turn the failed
histogram target into a successful R3 gate or claim R4 completion.

### Joined cleanup and post-load maintenance accounting

A two-unstarted-compaction lookahead experiment on the same20s/300s/90s,
one-worker profile was rejected and fully removed. Its real PG selector tests
passed, but `.tools/comparison-report-1.L2Oa34/report.json` measured
rows/histogram363/956ms versus the accepted315/714ms baseline, with failing
backlog slope+0.227181/min. All33,600 public records were exact,59 queries had no
public failures and final backlog was zero; these do not override the failed
targets. Sampled whole-installation peak was789,845,112B, OOM0. Cold/warm regex
was798/671ms, Range506/2 and6,128,472/16,337B. This is rejected diagnostic
evidence, not a product improvement or reason to change the SLO.

The replacement worker logged a327ms `maintenance_budget_overrun` after load,
but the old report checked maintenance only during the main load. Consequently,
older successful cold/warm/idle flags do not establish post-load maintenance
accounting. The revised collector retains the complete replacement-worker
operation counters, including startup before the first scrape. Both the test
and final reporter require observed idle credit, all retention/compaction/GC
attempt time<=idle/4 and zero overruns. The reporter recomputes from raw counters
and rejects missing, negative, fractional or inexact JSON-integer evidence;
a stored true target cannot substitute. All seven negative regressions fail
against the old reporter (`.tools/postload-accounting-before.log`).

The old admission allowance reserved only100ms native TERM grace. A deterministic
actual-dispatcher test that holds native ownership through100ms grace plus50ms
cleanup reproduces350ms work against300ms allowance (idle1,200ms), recording an
overrun (`.tools/maintenance-join-cleanup-before.log`). Admission now reserves
200ms, including100ms cleanup/scheduling margin, with the unchanged200ms minimum
work slice. This is not permission to abandon cleanup: actual joined duration
is fully charged, slow joins still fail, and permits stay owned until completion.
The regression and the existing deliberately long cleanup/overrun test both pass.
The sustained harness also compiles its test executable before any installation
starts and reuses it for setup/load/cold/idle, removing compiler competition
with worker startup. Capacity uses its existing precompiled executable.

Fresh pinned-native ARM64 maximum-pair PG/S3 verification passes in
`.tools/maintenance-resource.IxYnyB`:14 pairs/896 records/265,537,875B,
direct compaction9.234s and full direct workflow22.24s. The actual dispatcher
completes compaction and mixed retention in three claims each (71.753/63.002s)
and full expiry in one claim; total146.77s. Observed idle113,200ms,
all maintenance attempts13,424ms and overruns0. Old pinned pair identity,
authority and half-open retention checks pass; physical GC remains frozen.
CPU1/512MiB/swap0 cgroup peak536,875,008B has OOM/kill0 but significant reclaim,
so no memory headroom is claimed. The product executable SHA is
`34f37941f753543f20a44abc8554c1fbbfb87e24809c4eaf7fc4ee71993bdaf4`;
the unchanged pinned native library SHA is
`84ad753acc1390e13ce56e728d75d379bebeedec7f3df58ce071c1b373e4c79f`.
This repairs admission/verification, not an established speedup or R3 closure.

The fresh20s/300s/90s one-worker service run is retained in
`.tools/comparison-report-1.rV0OM9/report.json`, with runner log
`.tools/maintenance-accounting-comparison.log`. Rows/histogram p95 are416/949ms
(server162/720ms), ACK374ms, visibility868ms. The500ms histogram target alone
fails; the change is not a speedup relative to315/714ms. All33,600 records are
publicly queryable exactly,59 measured queries have zero public failures,
backlog max13/slope−0.095188/min/final0. Main-load compaction187 work,
12,539ms all attempts; retention148ms and GC43ms, versus64,300ms observed idle,
with zero recorded overruns. The newly enforced post-load check passes:
compaction1,607ms+retention24ms+GC8ms=1,639ms versus8,200ms idle, overruns0.
Cold/warm regex1,009/668ms uses Range722/3,7,859,828/24,742B, same snapshot and
exact rows;10s idle adds no query work. This full fresh-worker counter snapshot
is evidence the older post-load reports did not retain.

Sampled whole-installation peak is769,727,134B, observed worker cgroup peak
70,086,656B and OOM0. Main-load S3 PUT5,947/33,613,229B, HEAD63,526,
full GET14,203/80,279,287B, Range1,990/18,523,640B; PG WAL75,997,200B.
Post-load S3/WAL are separate. Actual input provenance is10,940 envelopes,
40,937,267B, framed SHA
`0d14c4fa08176384ed5c5bcf7db02d3176da807f06377e8de13d1d3a5f0ae456`.
The native dependency is unchanged; all product images are freshly built.
Earlier timings include compiler work after worker startup; the corrected
harness removes it, so this pair cannot isolate a product-only performance
effect. Neither this short profile nor containment closes R3/R4.

ARM64 unit/layout/architecture/vet and byte-identical code generation pass.
App/control/query race checks pass4.549/1.307/7.785s. The current comparison
build image executes the full Dockerfile native test/vet commands with its
pinned library and real child: engine160.355s, child contracts0.864s. As before,
this avoids exporting another large test image; environment-required cases are
verified separately, not claimed from skipped no-environment tests. Real PG/S3
integration passes26.309s on host ARM64 and49.918s in the selected actual-native
search/Live/authority/snapshot/maintenance run, with no skips in that selected
run. The final non-root/read-only ARM64 Chromium flow passes2.9s. The comparison
harness regression suite also passes in the fresh runner (2.275/0.198s);
an initial unprivileged host attempt failed on localhost bind permissions and
is not counted as a pass. Logs use `.tools/maintenance-accounting-` with
`unit`, `codegen`, `race`, `contracts`, `integration`, `browser`, `comparison`
and `resource` suffixes. The unrelated full capacity matrix was not rerun.
Commit45d45ee contains this joined-cleanup/post-load accounting correction.
Its commit message inadvertently repeats the preceding c09e8eb selector change
and measurements; that message is not evidence for45d45ee. This section and
the following corrective documentation commit record its actual scope/results.
Published history is retained rather than rewritten. The secret-scan baseline
only tracks the moved line of the unchanged source-design checksum, not a new
credential exemption.

### Warm small aggregate input transport

The next R3 experiment isolates local HTTP overhead instead of weakening catalog
verification or increasing query concurrency. The pinned native benchmark uses
the same256 Parquet inputs/25,600 records/452,834B, framed input SHA
`9de7c2916d63adc0313f45439fca47b5d6d7e34bddef870a3ebc33c5cc6956c6`.
Its unchanged engine initially measures approximately54ms on local files versus
112ms through the actual warm block-cache gateway, despite zero source reads
and514 loopback requests per scan (`.tools/query-gateway-baseline.ECyIW4`).

Query now copies only already-cached, verified aggregate inputs<=64KiB into its
private task directory, with an8MiB/task bound and additional8MiB shared-disk
reservation. `BlockCache.ReadCached` is a non-loading/non-waiting probe: it
rechecks size/hash, pins valid bytes and never creates or interferes with a
provider flight. Cold/corrupt/large/excess inputs use the existing lazy gateway;
rows/detail are unchanged. No S3 prefetch, skipped HEADs, new query plan, changed
scope predicate, reducer or native limit is introduced. Pins survive until
native join, and cleanup precedes disk release, including cancellation/failure.
The initial prototype fetched small cold inputs; that variant was replaced
before service measurement so predicates that prune inputs cause no extra I/O.

The final comparison uses one test binary but separate fresh non-root/read-only
CPU1/512MiB/swap0 containers, GOMEMLIMIT96MiB/GOMAXPROCS1, native192MiB and
256MiB spill. Compilation precedes observation. Five samples of three iterations
include actual query-owned staging, file removal and native execution; every
bucket is checked against an independent count, including empties. Source reads
are real isolated file Range reads, not S3 or PG, and cache warming is excluded.
Evidence is `.tools/query-small-inputs-measured.mv0Kwa/{warm-gateway,warm-staged}.log`.

| Scoped warm256-file scan | Gateway | Staged warm input |
|---|---:|---:|
| Median execution |111.645ms|62.293ms|
| Reciprocal scan rate |8.96/s|16.05/s|
| Go allocation/op |4,198,773B|2,191,130B|
| Go allocations/op |20,872|7,918|
| Median observed process RSS |68,485,120B|76,677,120B|
| Maximum process HWM |83,169,280B|88,543,232B|
| Container memory.peak |86,327,296B|93,220,864B|
| Loopback requests/op |514|0|
| Source Range requests/bytes in measured loop |0/0|0/0|
| OOM/kill |0/0|0/0|

This reduces scoped median time44.2% and Go allocation, but process RSS and
container peak increase. RSS is sampled after the timed loop; HWM/peak include
fixture/cache setup and all five samples. The reciprocal rate is not ingestion
or distributed service throughput. S3 counts are zero in both local profiles.

The fresh service measurement uses the same20s/300s/90s logical105-record/s
one-worker profile and precompiled harness as the preceding correction. Product
executable SHA is `fc5231a7f2848f921245924b3ca413918b3684c8e5504761bc82234509db650a`;
the unchanged library is `84ad753acc1390e13ce56e728d75d379bebeedec7f3df58ce071c1b373e4c79f`.
Before evidence is `.tools/comparison-report-1.rV0OM9/report.json`, candidate
`.tools/comparison-report-1.VCMPUF/report.json` (runner `.tools/small-inputs-comparison.log`).

| Whole service short profile | Before | Candidate |
|---|---:|---:|
| Rows/histogram p95 |416/949ms|295/566ms|
| Histogram durable-job p95 |720ms|396ms|
| ACK/visibility p95 |374/868ms|374/735ms|
| Maximum query files |756|561|
| Compaction work steps (prepare/swap) |187|239|
| All maintenance attempts / measured idle |12,730/64,300ms|15,632/78,300ms|
| Query work time |14,848ms|8,827ms|
| Sampled whole-installation peak |769,727,134B|864,623,260B|
| Worker cgroup peak |70,086,656B|70,410,240B|
| S3 PUT count/bytes |5,947/33,613,229B|5,942/35,510,791B|
| S3 HEAD count |63,526|50,177|
| S3 full GET count/bytes |14,203/80,279,287B|14,600/86,426,496B|
| S3 Range count/bytes |1,990/18,523,640B|2,035/19,630,435B|
| PG WAL |75,997,200B|74,702,512B|

Both runs return exact33,600 public records, final backlog0 and no public query
failures or OOM. Candidate backlog max14/slope−0.359175/min passes. Main and
post-load maintenance accounting both pass with no overrun; post-load attempts
1,681ms versus8,300ms idle. Cold/warm all-history regex742/545ms, Range505/2,
6,082,486/16,585B retains identical snapshot/rows;10s idle adds no query work.
Candidate actual submitted bodies are10,893 envelopes/43,630,817B, framed SHA
`8974fe17f45e40ab5576f6729bc829174032d3d5276976f3ec8247aea4297b8c`.
UUIDs, timing, retries and compaction trajectories differ between installations;
the service pair is not identical byte input or a sole-cause attribution.
Higher RSS and full-GET/Range bytes forbid a memory/cost superiority claim.
Histogram566ms still fails500ms, so the runner exits1 and R3 remains incomplete.
Corrected official1/2/4 profiles and R4 evidence remain required.

ARM64 unit/layout/architecture/vet and byte-identical code generation pass.
App/storage/query race checks pass4.501/1.539/8.865s. Tests cover cache probes
during a real blocked provider flight, pin eviction, invalid identity, canceled
and drained cache, corrupt/truncated cached files,64KiB and8MiB boundaries,
mixed local/lazy inputs, exclusive private files and joined cancellation.
A new cancellation fixture initially supplied a non-private scratch directory;
its waiting test was diagnosed/stopped and corrected to the product's private
directory contract, then the bounded full race run passed. This was a fixture
failure, not successful product cancellation evidence from that first attempt.
Actual PG/S3 integration passes27.806s and the selected pinned-native
search/Live/authority/snapshot/maintenance suite47.861s without skips. The latter
observes the real aggregate child's local warm input and no extra Range read,
not a substituted runner result. Full static-library native tests/vet also pass
in the fresh build image with latest query/storage tests mounted read-only;
process contracts1.834s pass. The final ARM64 non-root/read-only Chromium flow
passes2.7s. Logs use `.tools/small-inputs-{unit-all,codegen,race,integration,
contracts,browser,comparison}.log`. No API schema changed. Physical GC, native
limits, authorization and transaction fences are unchanged; source-design SHA
remains the frozen original. No release is declared by these scoped results.

### Corrected official-duration warm-input verification

The frozen clean `02c98f2d8127385973428064b26d1e15ce912c16` run of
`scripts/check comparison` (Go1.27.1 ARM64, no quick/reuse overrides) completes
the four native oracles, then fails the one-worker service profile. Outer exit1
is retained in `.tools/small-inputs-official-comparison.log`;2/4 workers do not
start after that failure. No source, limits or thresholds changed during the run.
Fresh build image `sha256:1b49743661fd1f8095f7c6fc1be2a6951ab1b9556ec8ce38c2f554f35d7330b7`
contains executable `fc5231a7f2848f921245924b3ca413918b3684c8e5504761bc82234509db650a`
and the unchanged pinned library SHA above. Environment and exact source hashes
are retained in `.tools/native-oracle.aIwFfl/environment.txt`.

Native10k/100k/1m/10m profiles all pass the independent query oracle, with
elapsed1,827/10,631/96,797/965,986ms and OOM0. The10m profile produces2,196 analytics
files/528,495,771B from682,231,716 journal bytes; actual envelope SHA is
`1497dc8c86ac7ed8990b081b5d74e71833a75ef3185a4e6529fdb218aca2f5ec`.
Its supervisor cumulatively allocates272,557,818,128B; cgroup peak536,879,104B
reaches the512MiB boundary with reclaim pressure, not spare memory headroom.
These local-file oracles have S3 requests/network bytes0 and are not service
throughput evidence. Logs/container exit states are in the same directory.

The actual isolated PG/MinIO service uses5min warmup/30min measured load and
the10min drain limit. Retained evidence is
`.tools/comparison-report-1.xOwwze/report.json`, including per-query phase data,
pre/post-restart worker logs, all three worker incarnation cgroups and raw
installation resource samples. Load test duration is2,106.63s; post-load65.01s
passes. Input comprises71,760 submitted envelopes/287,013,539B, framed SHA
`19366283e385483a9cc14fd48e2bd310d36b35de5c96d86ae1bee5648d4dc757`.

| Official one-worker measurement | Result |
|---|---:|
| ACK / visibility p95 |370/1,049ms|
| Rows / histogram p95 |591/1,737ms; both fail500ms|
| Rows / histogram durable-job p95 |196/1,248ms|
| Rows / histogram pre/post-job overhead p95 |468/478ms|
| Public counts |210,000 logs +10,500 errors, exact|
| Public queries / failures |359/0|
| Maximum query files |2,117|
| Final backlog / drain |0/2,009ms|
| Main maintenance attempts / observed idle |82,330/448,000ms; no overrun|
| Sampled whole-installation peak |2,010,015,332B|
| Worker cgroup peak / OOM events / kills |213,065,728B/0/0|
| S3 PUT count/bytes |39,026/293,132,090B|
| S3 HEAD count |823,728|
| S3 full GET count/bytes |98,433/711,846,998B|
| S3 Range count/bytes |12,937/148,037,541B|
| PG WAL |692,682,648B|

The backlog slope+0.0206425/min passes the existing regression tolerance; it is
not reported as negative. The report's dated cost model projects840.84USD/month
under its explicit assumptions/exclusions, not an AWS bill or cloud execution.
Per-query Go allocations are not measured by this service profile. Planning
overhead and increasing catalog cardinality are diagnostic evidence, not proof
that one component alone caused the failed SLOs. The short295/566ms profile has
different duration/cardinality and is not a matched official before/after pair.

Fresh-worker cold/warm all-history regex takes2,814/1,997ms with identical
snapshot/rows; Range reads2,162/11 and29,498,200/92,589B. The provider/OS cache is
not cleared. Full replacement-worker maintenance attempts total2,621ms against
54,400ms observed idle, no overrun;60s idle adds zero query work. Physical GC
remains blocked without signed backup verification. Exact data, restart,
cache, containment and maintenance passes do not override the two failed query
targets. R3 and dependent R4 remain incomplete; no release is advertised.

### Bounded verified maintenance downloads

The concrete maintenance input path now runs at most four streaming downloads,
retaining the existing full-byte SHA/size checks, exclusive private paths,
input order, file128MiB/paired256MiB limits and shared disk reservation. First
failure cancels siblings; all provider bodies and cleanup join before the
workflow can remove files, release disk or start native work. No new workflow
framework, transaction policy, cache or provider prefetch is introduced.

The bounded-concurrency regression fails on the serial baseline (one active
read rather than four), then passes with stable128-pair ordering and exactly
four maximum active reads. The first test-fixture draft cleaned its temporary
directory before joining the baseline on assertion failure; that fixture was
corrected and the retained baseline repro contains only the intended failure.
Race tests additionally cover blocked provider completion after cancellation
or another read's error, disk/scratch lifetime, one-input retention, oversized
input rejection, existing-directory rejection and retry. Explicit real MinIO
tests reject missing and same-length corrupt bytes, then succeed after repair;
these are wired into `scripts/check integration`, not successful mock substitutes.

`BenchmarkMaintenanceDownloads` uses deterministic opaque bytes, not Parquet:
it isolates real S3 full reads, SHA verification, private-file writes and cleanup.
The byte-identical benchmark source is compiled before each measurement in the
same pinned ARM64 build image. Fresh before/after CPU1/512MiB/swap0 non-root,
read-only containers use GOMEMLIMIT96MiB/GOMAXPROCS1 and128MiB tmpfs, with separate
disposable MinIO installations. No other build/test overlaps timed loops.
Warmup/provider upload and independent file inspection precede timing; five
samples of three iterations include download and directory cleanup. Evidence:
`.tools/maintenance-downloads-before.3XIgF0` and
`.tools/maintenance-downloads-after.fc6eSg` contain source/binary/image hashes,
raw samples, fixture hashes and memory events.

| Pairs / bytes per file | Median latency before→after | Transfer rate before→after | S3 GET / bytes per operation (both) |
|---|---:|---:|---:|
|16 /8KiB|10.619→3.360ms|24.69→78.03MB/s|32 /262,144B|
|16 /256KiB|18.605→9.244ms|450.89→907.51MB/s|32 /8,388,608B|
|128 /8KiB|78.514→25.070ms|26.71→83.65MB/s|256 /2,097,152B|
|128 /256KiB|147.551→72.595ms|454.82→924.43MB/s|256 /67,108,864B|

128-pair fixture hashes are respectively
`da809f6dc7a5b2dd345bd09c43d4cb4ea265888dc8cec838d0ed63a6a4d8b32d` and
`b83e05c62ec21ea3aa5b91cbb0306b7ecab8c053a07951fe5191ba37dce07080`.
No measured PUT/HEAD/Range operations occur. This is download throughput, not
ingestion throughput or native compaction speedup.

| Pairs / file size | Go B/op before→after | allocs/op before→after | Median VmRSS before→after |
|---|---:|---:|---:|
|16 /8KiB|1,468,376→1,469,368|17,498→17,484|28,241,920→28,434,432B|
|16 /256KiB|2,213,837→2,214,981|17,498→17,486|28,835,840→30,089,216B|
|128 /8KiB|11,748,365→11,747,546|139,823→139,584|29,835,264→32,784,384B|
|128 /256KiB|17,712,778→17,711,490|139,832→139,587|30,404,608→35,012,608B|

Maximum VmHWM33,439,744→38,002,688B and cgroup peak98,512,896→104,194,048B
increase; OOM/kill0 on both. These process peaks include fixture setup, clients,
all subbenchmarks and samples, not just one operation. Do not claim memory saving.

Fresh actual PG/S3 maximum-pair execution
`.tools/maintenance-resource.mK8BJE` passes direct compaction/retention21.95s
and real-dispatcher146.03s over265,537,875 input bytes/896 records. Dispatcher
compaction/retention require three claims each (including cancellation/retry),
then fully expired retirement succeeds. All maintenance attempts12,894ms
against112,800ms observed idle stay within budget, no overrun. Cgroup
peak536,875,008B reaches the512MiB boundary with reclaim pressure; OOM/kill0
does not establish headroom. Pinned identities and full/mixed expiry remain exact.

The same20s/300s/90s service comparison uses baseline
`.tools/comparison-report-1.VCMPUF/report.json` and candidate
`.tools/comparison-report-1.h8LAhb/report.json`. Candidate product SHA is
`b7616c21d9973364ff43a384ee2347f1f2a7aa75efd80698f45bb12b8b20c454`, unchanged
native library SHA is recorded above. Fresh build/runtime image IDs are
`sha256:9472db30581e639eece3c58200e01f9e9bd9a2180f2080f2974f449bdccbb172` and
`sha256:881ba1cf78784aa7f12b67443112cac682e45738ebeeb51d37711aa29d5852cf`.

| Short actual service | Before | Candidate |
|---|---:|---:|
| Rows/histogram p95 |295/566ms|303/548ms|
| Histogram durable-job p95 |396ms|390ms|
| ACK/visibility p95 |374/735ms|371/750ms|
| Maximum query files |561|601|
| Completed compaction tasks / work steps |116/239|117/241|
| All maintenance attempts / observed idle |15,632/78,300ms|15,271/74,700ms|
| Sampled whole-installation peak |864,623,260B|788,207,236B|
| Worker cgroup peak |70,410,240B|71,512,064B|
| S3 PUT count/bytes |5,942/35,510,791B|5,967/35,528,788B|
| S3 HEAD count |50,177|51,132|
| S3 full GET count/bytes |14,600/86,426,496B|14,593/86,156,852B|
| S3 Range count/bytes |2,035/19,630,435B|2,027/19,500,180B|
| PG WAL |74,702,512B|75,054,136B|

Both return exact33,600 public records,60 successful searches, final backlog0,
OOM0 and passing main/post-load maintenance accounting. Candidate slope−0.207509/min
and max backlog11 pass. Cold/warm same-snapshot rows take838/612ms with
Range540/3 and6,338,487/25,222B;10s idle adds no query work. Post-load maintenance
1,746ms versus8,200ms idle has no overrun. Candidate input is10,779 envelopes/
43,052,103B with framed SHA
`f33b02bdb9b0cd70a263f34561d7db0e32507d6a789ec71daf746c3cf52054e6`.
UUIDs, retries, timing and compaction trajectories differ: the small histogram
difference is not a stable full-service speedup or sole-cause attribution.
Histogram548ms still fails500ms, so the command exits1. Official-duration R3
query correction, full remaining evidence and R4 are still incomplete.

The older warm-input table's187/239 figures are work steps, not completed
compaction tasks: corresponding retained reports contain93/116 completed tasks.
That label is corrected above without changing raw evidence or published history.

Executed checks: ARM64 unit/layout/architecture/vet; byte-identical codegen;
maintenance race1.823s; explicit bounded-container MinIO missing/corrupt/retry;
standard PG/S3 integration25.735s plus downloader0.803s; selected pinned-native
query/Live/authority/maintenance39.242s; actual10,000-event-day publication750.72s
(170,078,208B cgroup peak, OOM0, one outstanding pair and no disk residue);
native child contracts0.840s; fresh non-root/read-only Chromium2.8s; maximum-pair
resource and the failed service comparison above. Browser/child checks overlapped
the long correctness-only date-boundary test, not either performance measurement.
Logs use `.tools/maintenance-download-{unit,codegen,race,verify,resource,
integration,contracts,browser,comparison}.log`. API/schema, fences, authorization,
native limits and the signed-backup GC interlock are unchanged.

### Fixed size-cohort compaction selection

Control now selects within six paired compressed-byte cohorts, with boundaries
16KiB/64KiB/256KiB/1MiB/4MiB. This is a scheduling refinement allowed by DESIGN12,
not a data-corruption fix or a new storage layout. The existing oldest prefix,
minimum8,32MiB target,128-input/256MiB ceilings, one active task/lane, pressure,
reservation/fencing and app-owned maintenance budget remain. Quiet cohorts can
retain seven inputs each, explicitly trading more files for fewer rewrites.
There is no fairness, query-SLO or whole-system throughput claim.

Actual PG tests cover every exact size boundary, quiet7+7, ready8+8, independent
larger-cohort progress, mixed-count ranking, reservation, active lane exclusion
and the existing cancellation/limit cases. Baseline22dcdbb selects an old10MiB
pair plus eight8KiB pairs (10,551,296B); the candidate selects only the65,536B
small cohort. Equal-count large/small cohorts select83,886,080→65,536B. These
first two figures are catalog metadata, not actual S3/Parquet measurements.
Evidence: `.tools/compaction-tiers-before.XDC4vw` and
`.tools/compaction-tiers-after.qHPm1p`. Five samples of ten selector calls after
warmup use the same logical168-bundle catalog and fresh bounded ARM64 containers.
Median cost2.157140→2.554424ms increases; both select128 inputs and make two PG
queries,69,153 Go B/op and2,213 allocs/op, with S3 count/bytes0. Process maximum
RSS34,877,440→35,561,472B and cgroup34,308,096→35,295,232B include fixture setup;
OOM/kill0. This is overhead, not a selector speedup.

`TestCompactionSizeCohortNativeRewrite`, now included by the standard integration
gate, supplies the missing actual-byte evidence. Nine durable ACK/conversion/
publication batches produce one1,197,426B large pair and eight tiny pairs totaling
79,637B. Five fresh isolated fixtures then time selection through fenced swap.
The independent post-swap pinned-engine oracle reads both current Parquet roles
and compares all12 exact IDs to durable occurrences, including the quiet pair.
Baseline's only intended failure is selecting all nine instead of eight. The
first draft oracle referenced the wrong file metadata columns; it was corrected
before both retained measurements, not treated as a product failure or pass.

Both sides compile before observation in the same Go1.27.1 ARM64/pinned-native
image, replacing only control's selector source with baseline22dcdbb or candidate.
Separate fresh CPU1/512MiB/swap0 non-root/read-only containers use128MiB scratch,
GOMEMLIMIT96MiB/GOMAXPROCS1 and disposable PG/MinIO. No other test/build overlaps
timing. Fresh IDs/timestamps make this the same logical workload, not identical
file bytes. Fixture/oracle CPU time and S3 reads are outside the measured workflow.
Evidence: `.tools/compaction-tier-native-before.kTqd50` and
`.tools/compaction-tier-native-after.C85mmm`; rejected first draft is `.MPphLi`.

| Mixed-size workflow, median unless stated | Before | Cohorts |
| --- | ---: | ---: |
| Selection through swap |149.044465ms|132.001774ms|
| Go allocation / allocations |3,744,120B /15,528|3,412,336B /14,278|
| Actual S3 full GET count / bytes |20 /2,477,649B|18 /92,423B|
| Actual S3 PUT count / bytes |2 /1,200,586B|2 /12,786B|
| Actual HEAD / Range count |2 /0|2 /0|
| Parent max RSS, includes fixture/oracle |87,367,680B|87,146,496B|
| Combined cgroup peak, includes children/setup/oracle |136,417,280B|137,076,736B|

All12 IDs remain exact; OOM/kill0. Cgroup peak increases, so no memory saving is
claimed. This proves reduced repeated large-pair rewriting, not greater sustained
ingestion capacity. Test-source SHA is
`97c951e0c542855cd0b7861932a1cd7edfc9d732d1cd912da5a660f08e6af065`.
Baseline/candidate selector SHAs are `0bec2148f42225740b15b68002fdba4775a2d3dbf103645b0af9200b2892270f`
and `74652a700e8d680fc99030c0d6e5bc49f83739e6a67ba9fae35f3d994a14285c`.

The unchanged20s/300s/90s real-service profile compares prior `.h8LAhb` with
`.tools/comparison-report-1.FddXVM/report.json`. Fresh candidate binary SHA is
`2048f7984ba32f02f2b0d921d96a426d218abdf3aecf5480c3c1f62892d0b71b`, build image
`sha256:b15b4ae25515d8f9bbaa9ac3151a2c19544df13d82b6cd4d29f9a61a48096fec`;
the pinned native library is unchanged. No reused application or other load runs.

| Actual short service | Before | Cohorts |
| --- | ---: | ---: |
| Rows / histogram p95 |303 /548ms|306 /558ms|
| Histogram durable-job p95 |390ms|387ms|
| ACK / visibility p95 |371 /750ms|372 /748ms|
| Maximum query files |601|605|
| Completed compactions / work steps |117 /241|121 /251|
| All maintenance attempts / observed idle |15,271 /74,700ms|15,628 /77,200ms|
| Sampled whole-installation peak |788,207,236B|794,473,527B|
| Worker cgroup peak |71,512,064B|68,087,808B|
| S3 PUT count / bytes |5,967 /35,528,788B|5,965 /34,487,563B|
| S3 HEAD count |51,132|51,496|
| S3 full GET count / bytes |14,593 /86,156,852B|14,554 /83,776,597B|
| S3 Range count / bytes |2,027 /19,500,180B|2,040 /19,111,638B|
| PG WAL |75,054,136B|74,946,720B|

Both return33,600 exact public records,60 successful queries, final backlog0,
OOM0 and passing main/post-load maintenance accounting. Candidate max backlog12,
slope−0.238604/min; no overrun. Cold/warm same-snapshot rows818/605ms, Range550/3
and6,510,070/24,515B;10s idle adds no query work. Post-load attempts1,759ms versus
8,100ms idle pass. Actual input10,875 envelopes/44,524,233B has framed SHA
`e09c2ae1c0f4e66d2cff079145d1993582eb19c7ee42ad7e8075df3e7ac5a469`.
Histogram558ms still fails500ms and the command exits1. Variation in generated
IDs, retries and scheduling precludes a whole-service speedup/cost attribution.
Official-duration query correction, fairness/scaling and dependent R4 remain open.

Executed candidate checks: ARM64 Go1.27.1 unit/layout/architecture/vet and
byte-identical codegen; complete host PG/S3 suite30.057s; selected pinned-native
query/Live/authority/retention/snapshot/compaction suite61.263s, including the new
real size-cohort test; child-process contracts1.032s; freshly built final-image
Chromium3.0s. These correctness checks overlap each other, not either measurement.
The unchanged10,000-day conversion boundary and maximum-pair direct/dispatcher
checks were not rerun for this control-only increment; their22dcdbb evidence is
above. Logs use `.tools/compaction-tiers-{unit,codegen,integration,contracts,
browser,comparison}.log` and `.tools/compaction-tier-native-*.log`. No API/schema,
native engine/limit, ingest/query ownership, authority or backup interlock changed.

### Actual single-worker conversion tenant turns

The prior claim order preferred tenants without running jobs, then globally old
retry/creation times. Real PG tests reproduce selecting the same older tenant
after its preceding job prepares, publishes or expires; the existing test only
covered a still-running predecessor. The original failed assertions are retained
in `.tools/tenant-turns-before.log`; the cursor fix passes those transitions and
negative cursor, maximum-ID wrap, missing-ID gap, retry-time, canceled claim and
stale-generation checks. Rejected claims leave attempt counts unchanged.

App's joined native loop now retains one last-claimed conversion tenant ID.
Control's existing claim transaction rotates equally occupied tenants after it,
then applies the original per-tenant retry/age/job ordering. The cursor changes
only after a committed claim, including one whose execution later fails; no
unbounded map, extra query, schema or persistent queue state is introduced.
Running-work preference, SKIP LOCKED, generation, lease and fencing stay intact.
Restart resets this hint. It is not cross-worker fairness or query scheduling.

The new `TestConversionTenantTurnsActualWorker` runs actual durable ingest of
12 older-tenant and4 newer-tenant one-record batches, then starts the real product
worker. A private-schema trigger (maximum64 rows) records conversion claims,
excluding publication claims. After all16 jobs publish and the worker joins,
the pinned engine independently reads both S3 Parquet roles and compares all IDs
to submitted normalized records, not catalog counts. It is wired into the native
integration gate. The standalone test-local round-robin algorithm is removed
from `tests/scale`; it never exercised product code and cannot prove scheduling.

Before `.tools/tenant-native-before.KdVXaZ` has claim order
`A A A A A A A A A A A B A B B B`: tenant B first appears at turn12. After
`.tools/tenant-native-after.EBLaAa` has `A B A B A B A B A A A A A A A A`:
B first appears at turn2 and the first four available rounds alternate. Both
return all16 exact IDs in analytics and payload with no retry or OOM. Baseline
fails only the turn-order assertion after the ID oracle; candidate passes.
Elapsed2.64/2.04s and cgroup137,183,232/79,085,568B are whole correctness-test
observations including setup/child/oracle, not matched throughput/RSS benchmarks;
do not infer a speedup or memory saving from these single runs.

Both run Linux ARM64 CPU1/512MiB/swap0, non-root/read-only,128MiB tmpfs,
GOMEMLIMIT96MiB/GOMAXPROCS1 with private PG/MinIO and the unchanged pinned engine.
Test helpers are compiled from current sources; actual worker subprocesses are
the explicitly identified product binaries, never a substituted test dispatcher.
Baseline product SHA `2048f7984ba32f02f2b0d921d96a426d218abdf3aecf5480c3c1f62892d0b71b`
was exported from the retained comparison runtime after its old untagged build
image was unavailable. Candidate SHA is
`2b5f7edf9e5a7ecda820cc817a3393e859480f5bf0c4fcc9cd31c0d93dd38e27`, build image
`sha256:9f684c82ce00442595c5099341941080437936130133d5133d8d8df3389d29f9`.
Test-source SHA is `f2d23d2f0d84be2bd9987fee5ca699a90998aa1627645aa783dda9844efba6aa`.
The first fixture mistakenly reused an SDK source event ID, correctly receiving
a conflict before worker startup; `.eE7mmq`/`.TaTSLx` are rejected setup attempts,
not product failures or fairness evidence. Both retained runs use the corrected
unique-source-ID fixture. The temporary export container was removed; product
data and user containers were untouched.

ARM64 unit/layout/architecture/vet, app/control race4.495/1.354s, full host PG/S3
31.664s, selected pinned-native query/Live/authority/retention/snapshot/compaction
57.353s, child contracts1.500s and fresh production-image Chromium2.8s pass.
The new actual-worker test separately passes as above. Correctness checks may
overlap; these are not performance comparisons. Logs use
`.tools/tenant-turns-{unit,race,integration,contracts,browser}.log` and
`.tools/tenant-native-{before-fixed,after-fixed}.log`.
API/schema, native budgets, query authorization and the backup GC interlock are
unchanged. Sustained SLOs have not been rerun for this correctness change;
single-worker query fairness, replica skew and remaining official profiles are
still R3 requirements. R4 and release claims remain incomplete.

The ARM64 `scale` control/model gate and byte-identical codegen also pass. Its
first build export failed with Colima disk exhaustion before tests ran. Four
obsolete unreferenced Eventglass verification images (one with two tags) plus
the failed partial scale image were removed, then the unchanged check passed.
Source/evidence, user containers/volumes, current product images and the pinned
native cache were preserved; removed images can be rebuilt. Logs are
`.tools/tenant-turns-scale.log` (failed export), `scale-retry.log` and `codegen.log`
under the same prefix. This control/model gate does not run native workload
throughput or prove multi-worker fairness.

### Actual single-worker query turns and saturated-query progress

Actual PG regressions on the preceding claim policy reproduce two defects:
an older query already running four tasks hides another query's ready task,
and after a task completes the older tenant wins again despite another ready
tenant. `.tools/query-scheduling-before.log` retains both failures. The fix
filters full queries during candidate selection, retaining the fresh locked
count as the concurrency authority, and adds an independent app-owned last-query
tenant cursor. No extra SQL round trip, unbounded map, task limit, transaction
owner or HTTP contract is introduced. Targeted synchronous help stays confined
to its own query. These are scheduling corrections, not a throughput claim.

`.tools/query-scheduling-boundaries.log` passes real PG tests in2.140s: negative
cursor, canceled context, stale generation, maximum-ID wrap, completed-slot
reuse and12 concurrent callers plus bounded following sweeps reaching exactly
four running tasks for each of two queries. Existing expired-task recovery and
shared admission tests pass too. These use metadata-only plans, not native/S3
execution. SKIP LOCKED can defer a raced query to the next sweep; the test does
not assume every concurrent caller must win.

`TestPublicQueryEndToEndTenantTurnsActualWorker` separately submits two A queries
and one B query using actual Parquet/S3 fixtures and the ordinary authorized
Submission/Awaiter path before starting the real product worker. A private-schema
trigger bounds claim observations at16. After joining the worker, final results
must contain the exact three expected record IDs and correct project in every
query; a different tenant/session must not retrieve B's result. This is actual
query execution, not durable-ingest proof or a replacement test dispatcher.
Before `.tools/query-native-before.zjwP39` produces A/A/B and fails only the turn
assertion after the result/authorization oracle. After
`.tools/query-native-after.rZANjQ` produces A/B/A and passes. Whole correctness
test time0.69/0.48s and cgroup peak156,651,520/99,958,784B include setup and all
children; these single observations do not establish speed or RSS improvement.
Both have OOM0. This test does not measure S3 cost or sustained throughput.

Both use Go1.27.1 Linux ARM64, CPU1/512MiB/swap0, non-root/read-only,128MiB
scratch, GOMEMLIMIT96MiB/GOMAXPROCS1, isolated PG/MinIO and the unchanged pinned
DuckDB library SHA recorded above. The test helpers compile from current source;
the baseline actual product binary was preserved before rebuilding, SHA
`2b5f7edf9e5a7ecda820cc817a3393e859480f5bf0c4fcc9cd31c0d93dd38e27`.
Candidate product SHA is
`517c3d57cbd6c8af52529bd37e54fcd8df8ee5d57c335df83b793f6bdb4843e0`, build image
`sha256:b6cff67573bd6f627a2f27a809d7baa71895b0b2ac6af3adbb9271f1cefb6b2b`.
Identical native test-source SHA is
`2eb524802f290e8e712a388f257c3f70626e97a6213eed2f9e5333ddd5e39be8`.
Sustained service profiles are not rerun for this correctness-only increment.
Multi-worker skew, corrected official-duration query SLOs and R4 stay open;
physical GC still requires fresh signed coordinated-backup evidence.

Executed regression checks pass: ARM64 unit/layout/architecture/vet and
byte-identical codegen; app race4.556s and control race (cached); full host
PG/S3 integration30.189s; selected pinned-native query/Live/authorization/
snapshot/retention/compaction57.589s, including the new worker test; child
contracts1.411s; fresh production-image Chromium2.9s. Logs use
`.tools/query-scheduling-{unit-retry,race,codegen,integration,contracts,browser}.log`.
The first unit invocation failed only because the sandbox denied localhost
binds (`unit.log`); the unchanged check passed with local-network permission.
Correctness checks can overlap and their times are not paired benchmarks.
No schema/API change required regenerated contracts or capability-gate changes.

### Official-duration verification after bounded maintenance and tenant scheduling

The frozen clean `04ec2c2bdc878259f0dd2d6e8773405a7067e735` product completes
the full native oracle and one-worker official service profile, but the outer
`scripts/check comparison` correctly exits1: histogram p95 still exceeds500ms.
No quick/reused-product override or threshold change was used. The native-oracle
build image is `sha256:371b49f514d0c30d30e979f74626302761c3d9c955d67d832de95717042a40df`;
comparison build/runtime images are
`sha256:7132b6e65cdae3d37ce9cf06bffaa644e5fda16ee595043e118726dbea220a53` /
`sha256:294f02808dfe2a4f8466ae72929c82e02086c7b589041f1df1012b87315e7365`.
The product binary is `517c3d57cbd6c8af52529bd37e54fcd8df8ee5d57c335df83b793f6bdb4843e0`,
with the unchanged pinned native library. Product, comparison sources and
compiled measurement inputs stayed frozen; unrelated integration-test additions
were edited only after compilation and were neither mounted nor executed in
this measurement. No competing builds or tests ran during observation.

`.tools/native-oracle.62xXNy` retains all four successful10k/100k/1m/10m runs,
their exit states, input/source/image hashes and resource evidence. Elapsed time
is1,775/10,376/96,032/956,124ms with OOM0. The10m actual envelope SHA remains
`1497dc8c86ac7ed8990b081b5d74e71833a75ef3185a4e6529fdb218aca2f5ec`;
2,196 analytics files hold528,495,771B from682,231,716 journal bytes.
Supervisor cumulative allocations272,562,880,144B are not RSS; the kernel peak
536,875,008B reaches the512MiB boundary with reclaim, not memory headroom.
These network-denied native oracles make zero S3 requests and do not measure
service throughput.

Service evidence is `.tools/comparison-report-1.HEZiKz/report.json`, resource
samples, all three worker incarnation observations and retained runtime logs;
the outer log is `.tools/query-scheduling-official-comparison.log`.
The actual SIGKILL replacement starts at10:08:43UTC. Warmup/load/drain limits,
offered logical100 logs/s+5 errors/s, CPU1/512MiB/swap0 per Go role, Go1.27.1,
native engine and local PG/MinIO profile match the earlier clean02c98f2 run.
The table compares these official-duration observations, not the shorter
diagnostics or an isolated attribution to one of the intervening changes.

| Measurement |02c98f2|04ec2c2|
|---|---:|---:|
| Rows / histogram p95 |591/1,737ms|404/874ms|
| Rows / histogram durable-job p95 |196/1,248ms|155/646ms|
| Rows / histogram pre/post-job overhead p95 |468/478ms|251/251ms|
| ACK / visibility p95 |370/1,049ms|370/821ms|
| Public queries / failures |359/0|360/0|
| Maximum selected files |2,117|878|
| Final backlog / drain |0/2,009ms|0/1,004ms|
| Main maintenance attempts / observed idle |82,330/448,000ms|97,446/448,100ms|
| Sampled whole-installation peak |2,010,015,332B|1,920,068,483B|
| Worker cgroup peak |213,065,728B|138,022,912B|
| S3 PUT count / bytes |39,026/293,132,090B|39,294/241,854,491B|
| S3 HEAD count |823,728|573,291|
| S3 full GET count / bytes |98,433/711,846,998B|101,099/613,859,945B|
| S3 Range count / bytes |12,937/148,037,541B|12,835/129,040,927B|
| PG WAL |692,682,648B|666,788,112B|

Both runs return exactly210,000 logs+10,500 errors through public queries,
with no conflicts, OOM events, OOM kills or maintenance-budget overruns. The new
backlog maximum21 and slope+0.00537794/min pass the existing tolerance, not a
negative-slope claim. Load-end completed compactions total723; raw counters
include failed/no-work maintenance attempts. The cost model estimates715.57USD/
month under its dated assumptions, versus840.84USD; neither is a cloud bill.
Actual71,563 envelopes/293,597,622B have framed SHA
`30f18f61d89146187ebddaa085bb4d285eefa98089a3de66a17f06f46ea9f374`.
Fresh identities, retries and scheduling differ between runs; full GET and PUT
request counts increase. No broad cost/memory superiority or per-query allocation
claim follows from these single service observations.

Independent post-load checks pass in62.41s: new-worker cold/warm same-snapshot
rows take1,191/995ms, with Range793/4 and18,928,028/32,930B. Provider/OS caches
are not cleared. Full replacement-worker maintenance takes7,113ms against
49,800ms observed idle, with zero budget overruns;60s idle adds no query work.
The retained post-load compaction failure is counted, not excluded. Missing
signed backup evidence still blocks physical GC. Rows now pass500ms but
histogram874ms fails. At that checkpoint the runner stopped before2/4-worker
profiles; the later corrected1/2/4 results are in the section below. Native,
data, cache and resource passes cannot override the SLO failures at1/2 workers.

### Actual two/four-worker tenant claim observations

The existing native conversion and public-query fixtures now share only a
bounded test-process startup helper, not a new product scheduler abstraction.
For two/four replicas, it waits for each real worker's first idle query sweep,
pauses those processes, queues the finite workload, then resumes them. A cleanup
registered after all process cleanups resumes paused workers before TERM/join;
the ordinary runtime helper waits for termination before deleting scratch.
Readiness uses a context-bound one-second HTTP timeout, bounded transport and
64KiB response; each isolated-schema audit is limited to64 rows.

Conversion submits24 A+8 B real durable one-record ACK jobs. After all workers
join, every normalized record ID must appear exactly once in both current S3
Parquet roles. Query submits8 A+8 B ordinary async Submission/Awaiter searches,
using four principals per tenant within the unchanged2/user and8/tenant limits.
Every result must contain the three expected IDs and correct project, and A
cannot retrieve B's result. Each requested worker must actually claim work;
there are no retries or missing claims. Single-worker original12+4 conversion
and2+1 query fixtures retain their strict alternating/A-B-A assertions.

`.tools/multiple-tenant-workers.os3B3p/{environment.txt,run.log}` records three
repetitions of all six fixtures,18 passing executions in total. The unchanged
04ec2c2 product binary/library hashes are above; current tests compile against
comparison-build `7132b6e65cdae3d37ce9cf06bffaa644e5fda16ee595043e118726dbea220a53`.
The test binary SHA is
`d14dadf814e794a6c01ee2ea95c87174b0d7b479b213e59391e06e4abf6e4a9a`;
all three test-source hashes are retained alongside it. No heavy correctness
check overlapped the official service measurement.

| Fixture | Processes | B first claim, three samples | Maximum prefix count skew, three samples |
|---|---:|---|---|
| Conversion24+8 |2|3,3,3|3,2,3|
| Conversion24+8 |4|4,5,3|3,5,2|
| Query8+8 |2|3,3,3|2,2,2|
| Query8+8 |4|5,5,5|4,4,4|

Skew is the absolute claim-count difference while both tenants still have
unclaimed work. The first B claim is bounded by processes+1 because each fresh
worker can initially select A once. We record, rather than promise a universal
bound for, later skew. Audit sequence allocation is not a strict cross-process
commit/completion order. These finite preloaded fixtures exclude startup skew
and do not establish time fairness under continuous arrivals or starvation
bounds over arbitrary workloads.

The test driver and all its workers share one Linux ARM64 CPU1/512MiB/swap0,
non-root/read-only cgroup,128MiB scratch, GOMEMLIMIT96MiB/GOMAXPROCS1; PG/MinIO
are disposable and outside it. Peak215,158,784B, OOM events/kills0 and exit0
are correctness-test observations, not the per-replica resource/throughput
profile. No latency improvement, S3 cost or service allocation claim is made.

Additional checks pass: Go1.27.1 ARM64 unit/layout/architecture/vet and
byte-identical codegen, complete host PG/S3 integration30.797s, selected pinned
native query/Live/authority/snapshot/retention/compaction integration60.437s,
child contracts0.913s and freshly built final-image Chromium3.0s. Relevant
failure/retry/cancellation and authorization cases remain in those suites;
none are skipped. Logs use `.tools/multiple-tenant-{unit,codegen,integration,
contracts,browser}.log` and `.tools/multiple-tenant-workers-check.log`.
No product implementation, API schema, native budget or backup-GC interlock
changes in this test increment; the capability map keeps R3/R4 pending.

### Rejected larger warm-block staging experiment

The warm-input path excludes every file above64KiB even when its entire
verified1MiB cache block is already present. A measured candidate considered
those single-block aggregate inputs too, without changing the8MiB total staging
reservation, native limits, manifest order, authority or catalog HEAD checks.
Two fixed passes preserved the original<=64KiB opportunity before spending spare
staging space on larger blocks. One buffer was bounded to1MiB; no provider read
or flight was started by the probe. Cold/corrupt/multi-block/excess inputs kept
lazy verified Range reads. Pins survived joined native execution and task files
were removed before disk permits were released, including cancellation. The
candidate was subsequently rejected: its native benefit did not translate into
a measured service improvement. The product retains64KiB/8MiB staging limits;
only the reproducible mixed-size benchmark is retained as code.

The new mixed-size native benchmark contains256 real Parquet files,32 with
deterministic unselected text making them64–256KiB, and224 tiny files. Logical
rows25,600 and every histogram bucket are independently checked. Identical
4,263,106 input bytes have framed SHA
`3eed7d58548b552e755227f06585176f7bde1d533cfbb1d092a5e63ac0f6d806`.
No S3/PG is involved: the source is real isolated files, warmed through the
actual verified block cache before measurement. It deliberately keeps the
large column unselected rather than crediting staging with provider savings
that native column pruning would already provide.

Before `.tools/mixed-staging-before.X6O9ef/warm-staged-verbose.log` and after
`.tools/mixed-staging-after.eYqHKE/warm-staged.log` each run five samples of
three iterations in separate fresh Go1.27.1 Linux ARM64 CPU1/512MiB/swap0,
non-root/read-only containers,128MiB scratch, GOMEMLIMIT96MiB/GOMAXPROCS1 and
network isolation. Compilation precedes observation; no other builds/tests run
concurrently. Per-operation time and allocations include input preparation,
native execution and cleanup; RSS/HWM and cgroup peaks include fixture setup.

| Warm-staged measurement | Before | After |
|---|---:|---:|
| Median elapsed |74.009ms|66.452ms|
| Median Go allocations |14,158,864B /9,813 allocations|6,158,749B /7,924 allocations|
| Local HTTP requests / operation |66|0|
| Source reads / bytes during measured loop |0 /0|0 /0|
| Median process RSS |75,186,176B|77,197,312B|
| Process HWM |104,640,512B|105,631,744B|
| Cgroup peak |110,534,656B|111,558,656B|
| OOM events / kills |0 /0|0 /0|

An earlier non-verbose execution of the exact same baseline binary measured
71.885ms median (`warm-staged.log` in its directory), showing timing variation;
both baselines are retained, not selectively discarded. The unchanged warm
gateway control still issues514 local requests/operation with zero source reads.
These are scoped latency/allocation savings with slightly higher observed memory,
not a service-throughput, S3-cost or memory-superiority claim. The benchmark does
not replace the full sustained R3 profile.

Environment/source hashes are beside both measurements. Native library SHA
remains `84ad753acc1390e13ce56e728d75d379bebeedec7f3df58ce071c1b373e4c79f`.
Rejected candidate production binary SHA is
`f1302049e7c8803a9fba12b5a1defb53e35d4cc541fd5253254d806135e106ab`, freshly built
in ARM64 image `sha256:0c0513d2c245df32dac43f6858ee5736cb93b780d2b2dc0394ebe8c5d3d05fc2`.
The first candidate compile rejected typed-constant/int mismatches in unit tests
before execution (`.tools/mixed-staging-after.S6rNdj`); the correction and final
two-pass policy are the measured candidate, not a skipped validation.

Candidate boundary tests covered the exact1MiB file ceiling,1MiB+1 lazy fallback, the unchanged
8MiB total reservation, small-input priority despite large-first manifests,
cold-probe no-read, invalid full/block identity, canceled preparation and retry,
and second-pass failure releasing an earlier tiny pin. The joined-cancellation
unit held a full1MiB input and proved file, disk permit and cache pin remained
owned until its runner returns. Existing actual child cancellation contracts
and PG/S3 authorization/failure/retry checks were separate real executions.
Go1.27.1 ARM64 unit/layout/architecture/vet and byte-identical codegen pass;
query/storage race7.754/1.529s, full host PG/S3 integration31.237s, selected
pinned-native query/Live/authority/snapshot/retention/compaction59.530s, Linux
ARM64 query/storage unit0.469/0.075s, actual child contracts0.956s and fresh
final-image Chromium3.1s pass. Correctness checks may overlap each other, never
the native or service measurements. Logs use `.tools/mixed-staging-{unit,
codegen,race,integration,native-contracts,browser}.log`. No API/schema change
or backup-GC bypass accompanied this candidate. Its implementation, candidate-
specific tests and architectural contract edits were restored to HEAD after
measurement; `.tools/mixed-staging-rejected.diff` preserves their exact patch.

The paired real-service diagnostic uses one worker,20s warmup/300s load/90s drain
limit, per-role CPU1/512MiB/swap0 and fresh isolated PG/MinIO installations, with
no competing tests/builds. Baseline04ec2c2 binary is the unchanged788451d product;
candidate reports788451d-dirty with the binary identity above. Baseline build/
runtime image IDs are7132b6e65cda…/294f02808dfe…; the fresh candidate runtime
is `sha256:bc13a5016d2349f4f6902dbc67f006e2cd1abf3afdf729cb6823054c14d4740b`.
This explicitly uses diagnostic image reuse, not official completion evidence.
Reports are `.tools/comparison-report-1.uHEzNW` and `.tools/comparison-report-1.EYcDw9`;
logs are `.tools/mixed-staging-service-{before,after}.log`.

| Real service measurement | Baseline | Rejected candidate |
|---|---:|---:|
| Rows / histogram p95 |291/600ms|298/644ms|
| Rows / histogram durable-job p95 |157/423ms|145/479ms|
| Rows / histogram pre/post-job overhead p95 |174/177ms|174/177ms|
| ACK / visibility p95 |371/721ms|372/751ms|
| Maximum selected files |613|635|
| Final backlog / drain |0/1,002ms|0/1,005ms|
| Sampled whole-installation peak |809,280,469B|805,411,223B|
| Worker cgroup peak |68,308,992B|67,506,176B|
| S3 PUT count / bytes |5,963/34,258,231B|5,965/34,165,676B|
| S3 HEAD count |51,972|51,360|
| S3 full GET count / bytes |14,492/82,957,868B|14,463/82,576,977B|
| S3 Range count / bytes |2,050/19,128,710B|1,995/18,674,965B|
| PG WAL |74,917,016B|74,932,648B|

Both return the exact33,600 public records with60 successful queries, zero
conflicts/OOM/budget overruns, and passing backlog/maintenance/resource checks.
Backlog slopes are−0.058382/−0.698678 per minute, maxima12/18. Main maintenance
all-attempt time15,282/15,171ms against77,200/76,800ms observed idle passes.
Cold/warm same-snapshot regex takes860/613→884/635ms; Range575/3→601/3 and
6,736,690/24,599→6,935,671/24,630B. Ten-second idle adds zero query work, and
post-load maintenance1,670/1,661ms against8,300ms idle passes in both runs.
Both commands exit1 because histogram still exceeds500ms. Per-query service
allocations are not measured; lower sampled memory and some S3 counts are not
attributed as improvements. Fresh identities, retries and scheduling differ:
baseline10,771 envelopes/41,369,661B SHA
`178b8f999e6eed7b0f6492a01092a3a183e892e7f8a01c6808d551b64caeaa3a`,
candidate10,978/44,850,672B SHA
`ccd11ac430ee0efbf07786defce4f66d919923a3d5e95dd6ab1629bef206416c`.
This single pair does not prove the transport change alone caused regression,
but it provides no basis to ship it as the required service optimization.
R3 remains open; its latest official404/874ms result and pending2/4 profiles
are unchanged. No R4 release or physical-GC override follows from this experiment.
After restoration, unit/layout/architecture/vet, byte-identical codegen and
pinned ARM64 query tests pass again (`mixed-staging-retained-{unit,codegen,native}.log`).
The retained change adds only the mixed-size benchmark and its documentation;
no production source differs from788451d. The unused legacy fixture wrapper was
removed after measurement without changing generated inputs or execution.

### Rejected overlapping compaction size boundaries

The disjoint selector strands seven pairs immediately below a boundary plus one
at it, even though their paired sizes differ by only2 bytes. Real PostgreSQL
regressions fail at all five original boundaries on the unchanged baseline
(`.tools/compaction-tiers-before.gsGLcT`). A candidate evaluated the original
grid and one fixed half-shifted grid, ranks bounded prefixes separately by grid,
and returned exactly one winner. It introduced no input duplication, new
transaction, task limit, pressure exception or maintenance-budget change.
Eleven candidate boundaries passed, with quiet distant cohorts, seven-input non-readiness,
oldest-ID order,128-input cap,32MiB target, minimum-eight above target,8MiB file
exclusion and normal fenced reservation covered. The standard integration
selection now includes both native cohort tests.

The same CPU1/512MiB/swap0 catalog-cost fixture still uses two PG statements per
selection, zero S3 traffic and at most128 returned rows. Five-sample median time
increases2.470→3.252ms; median Go allocations fall69,153→42,497B and2,213→932
allocations after returning the pair sum as bigint. This is not a net service
speedup. Evidence: `.tools/compaction-tiers-{before.gsGLcT,after.B5jKej}`.

Actual durable ingestion, S3 publication and pinned-native compaction reproduce
the boundary with seven pairs totaling69,679B below16KiB each and one28,673B pair.
The old selector returns no work in all five repetitions; the candidate compacts
all eight in all five and independently verifies all eight persisted record IDs
in both current Parquet roles. Median selection-through-swap time is136.251ms,
Go allocation3,491,632B/14,230 allocations,18 full GETs/about130,040B (including
replacement verification),2 PUTs/about31,688B,2 HEADs and zero Range requests.
There is no baseline completion latency to divide by: this is restored progress,
not a speedup claim. Fresh IDs/timestamps make logical, not byte-identical, inputs.

The separate quiet1.20MB pair plus eight tiny pairs control still selects only
the tiny files. Median workflow time129.580→133.000ms is not an improvement;
both use18 GETs/about92,424B,2 PUTs/about12,787B,2 HEADs and zero Range requests.
Native runs compile before measurement and have no overlapping heavy work:
`.tools/overlap-native-before.2tty9I` (expected boundary failures) and
`.tools/overlap-native-after.F6r2Y3` (all ten fixtures pass). They use non-root,
read-only ARM64 CPU1/512MiB/swap0,128MiB tmpfs and Go soft limit96MiB. Whole-run
cgroup peaks139,137,024/138,940,416B include different completed work and are not
memory-saving evidence; all OOM counters are zero. The common native library
SHA is84ad753acc1390e13ce56e728d75d379bebeedec7f3df58ce071c1b373e4c79f;
test binaries are746eb77f504c8eb5c3b77f7c52589ca27f8d69e65e5d3f8f20b608d719b4facd
andd49d77d564a75338ae25e1376b904a5085255e34ea5db5c325af6bd08d1caea8.

The matched20s/300s/90s one-worker service pair did **not** improve:
`.tools/comparison-report-1.ZdK5Gl/report.json` versus
`.tools/comparison-report-1.PLuASA/report.json`. Both use CPU1/512MiB/swap0 per
role with isolated fresh PG/MinIO and no overlapping builds/tests.

| Measurement | Retained disjoint policy | Rejected overlap candidate |
|---|---:|---:|
| Rows / histogram p95 |293 /585ms|302 /625ms|
| Rows / histogram job p95 |141 /411ms|153 /469ms|
| Rows / histogram pre/post overhead p95 |173 /175ms|183 /190ms|
| Maximum queried files |589|619|
| ACK / visibility p95 |372 /731ms|371 /772ms|
| Final backlog / drain |0 /1,001ms|0 /1,006ms|
| Backlog slope per minute |−0.358|−0.438|
| Whole-installation sampled peak |809,972,529B|828,561,685B|
| Worker observed cgroup peak |66,916,352B|68,497,408B|
| S3 PUT requests / bytes |5,960 /34,401,018|5,984 /35,123,287|
| S3 HEAD requests |50,637|51,051|
| S3 full GET requests / bytes |14,525 /83,478,355|14,593 /85,153,139|
| S3 Range requests / bytes |2,007 /18,859,206|2,012 /19,222,994|
| PG WAL |75,003,600B|74,971,752B|
| Cold / warm all-run regex |864 /609ms|846 /619ms|
| Cold / warm Range requests |555 /3|569 /3|
| Cold / warm Range bytes |6,558,590 /24,664|6,568,838 /25,426|

Both verify33,600 public records,60 completed queries, zero conflicts/OOM,
restart/cache consistency and10s idle with zero query work. All-attempt main
maintenance/idle is15,611/77,600ms versus14,888/74,900ms; post-load1,751/8,200ms
versus1,690/8,200ms, with zero budget overruns. Both commands exit1 because
histogram p95 exceeds500ms. Fresh timing/IDs/retries produce different actual
input hashes:10,788 envelopes/42,207,389B,
`3b4a1c4cd4ef5c567450b149f723ada53f36e382fc5a9b416d0ffb91b9425c48`, versus
10,795/43,472,462B,`ba1c79f05eca01dfdd12eb5618635ad765f31e1bfa53f12320c93be3417cdb27`.
One pair does not isolate the cause of every difference, but supports no service
improvement or R3 completion. Candidate build/runtime IDs are
152ae1fa355ea67a962bdc605ba2ca9daed73e6fd7991f3274f96ac5758dc7a0 /
c916c9555835560f721518d830ce9a1245c2fa35600189d63e22fc2fe12d2f35,
product SHA303cf25f38208ce2f73707420ff61a84fce8fd126ff03aecaf83739b4e1e9595.
The baseline retains product517c3d57cbd6c8af52529bd37e54fcd8df8ee5d57c335df83b793f6bdb4843e0.

Before rejection, Go1.27.1 ARM64 unit/layout/architecture/vet and codegen passed.
Control/maintenance race checks pass1.304/1.744s; real PG/S3 integration passes
33.353s and the selected pinned-native query/authorization/snapshot/maintenance
suite72.959s. Actual child contracts pass0.860s with the binary path set; the
earlier invocation without it skipped tests and is not evidence. Fresh final-
image Chromium passes3.0s. Logs are `.tools/overlap-{unit,codegen,race,integration,
contracts-required,browser}.log`. The product selector and architecture/operations
contracts were restored to0740ba8; the exact alternative is retained locally in
`.tools/overlap-rejected.diff`. Final tests characterize the retained disjoint
boundary (no work, no object I/O, all original records still readable), not the
rejected candidate's progress. Its native fixture and prefix-bound tests remain
reusable evidence; no production source or API/schema changes. R3's latest
official404/874ms result, pending2/4 profiles and R4 remain unchanged. Physical
GC still requires fresh real backup evidence.

After restoring the selector, unit/layout/architecture/vet/codegen pass again.
Final real-PG prefix/boundary tests pass in
`.tools/compaction-tiers-before.7ckBmN` (34,988,032B cgroup peak, OOM0).
The final actual-native fixture passes both five-sample cases in
`.tools/overlap-native-before.RMjsNU`: quiet-large rewrite7.47s and retained
quiet-boundary read oracle6.17s,136,503,296B whole-run cgroup peak and OOM0.
These final checks verify the retained product and tests, not the rejected
candidate. No measured service speedup or new completed release gate is claimed.

### Rejected 512-file analytics scan-bound trial

A `20s/300s/90s` quick one-worker ARM64 comparison tested raising the analytics
scan cap from256 to512 with the same CPU1/512MiB role limits, pinned DuckDB
2.0, and isolated fresh PostgreSQL/MinIO. The retained 256 baseline is
`.tools/comparison-report-1.ZdK5Gl/report.json`; the 512 candidate is
`.tools/comparison-report-1.4Ti890/report.json`. Both accepted33,600 records,
drained to backlog0, had no conflicts or cgroup OOM, and passed backlog,
visibility and resource checks, but both runs used fresh, non-identical input
envelopes (10,788/42,207,389B versus10,784/42,882,500B). Do not attribute the
whole difference to the scan bound. These first reports are exploratory, not a
valid paired conclusion: `search` and later full-run bounds used the test host's
wall clock although `time_basis=received` is stored on PostgreSQL in Colima.
That can select different catalog populations when the clocks are offset.

| Measurement | 256 baseline | 512 candidate |
|---|---:|---:|
| Rows / histogram end-to-end p95 |293/585ms|9,739/348ms|
| Rows / histogram job p95 |141/411ms|151/265ms|
| Rows / histogram HTTP overhead p95 |173/175ms|9,668/108ms|
| Successful query jobs / query failures |60/0|33/8 (503 dependency unavailable)|
| ACK / visibility p95 |372/731ms|375/823ms|
| Final backlog / drain |0/1,001ms|0/1,003ms|
| Whole-installation sampled peak |809,972,529B|761,319,651B|
| Worker cgroup peak / OOM kills |66,916,352B /0|63,713,280B /0|
| S3 PUT requests / bytes |5,960/34,401,018B|5,983/36,892,721B|
| S3 HEAD requests |50,637|16,922|
| S3 full GET requests / bytes |14,525/83,478,355B|15,474/93,700,476B|
| S3 Range requests / bytes |2,007/18,859,206B|1,367/12,989,175B|
| PG WAL |75,003,600B|73,720,824B|

Two pre-correction repeats show why the first pair cannot decide the cap:
`.tools/comparison-report-1.p65N6B/report.json` (256) measured rows/histogram
p95 of628/1,454ms,59 successful queries,0 failures and a maximum1,597 selected
files; `.tools/comparison-report-1.E3UsQv/report.json` (512) measured494/1,053ms,
59 successful queries,0 failures and a maximum1,185 selected files. The first
512 report selected at most272 files and missed query/post-load checks, while
the second selected1,185 and passed all but the histogram p95 target. Fresh
input hashes and compaction trajectories differ across all runs; these p95 and
S3 deltas do not prove either a speedup or a regression. Lower sampled memory
and request counts are not a service improvement claim.

The first trial also failed before report generation because interval checking
compared host HTTP timestamps with PostgreSQL timestamps as if they shared a
clock; a contemporaneous read showed Colima about114ms ahead. The test helper
now samples PostgreSQL time per query, derives `received` query windows and
full-run/cold-history bounds from that clock, and aligns HTTP interval checks
to it. Boundary coverage includes the114ms skew. These changes correct test
instrumentation only.

With those corrections, a fresh one-worker pair compared the retained256 bound
(`.tools/comparison-report-1.5iL543/report.json`) with512
(`.tools/comparison-report-1.CCgbLZ/report.json`):

| Measurement | 256 | 512 |
|---|---:|---:|
| Rows / histogram end-to-end p95 |350/780ms|374/784ms|
| Rows / histogram job p95 |158/540ms|164/519ms|
| Rows / histogram HTTP overhead p95 |244/240ms|267/267ms|
| Successful query samples / failures |59/0|59/0|
| Maximum selected files |849|885|
| ACK / visibility p95 |374/851ms|374/863ms|
| Final backlog / post-load cold query |0/1,044ms|0/1,121ms|
| Whole-installation sampled peak |815,754,377B|829,242,209B|
| Worker cgroup peak / OOM kills |69,742,592B /0|74,743,808B /0|
| S3 PUT requests / bytes |5,945/32,977,615B|5,895/32,753,293B|
| S3 HEAD requests |68,912|68,542|
| S3 full GET requests / bytes |14,093/78,385,657B|13,877/77,125,351B|
| S3 Range requests / bytes |2,059/18,685,119B|1,982/18,280,025B|
| PG WAL |76,342,160B|75,876,992B|
| Projected monthly cost / S3 request share |$650.37/$516.38|$645.90/$512.21|

Both accept33,600 records, pass every target except histogram p95<=500ms, and
complete cold/warm/idle verification. Actual submitted inputs remain fresh and
different:11,288/39,410,743B SHA
`ac8e4dc7b31517c8b004194bf7cb4b89634769badccbe4d519fd1714e976a7d0` versus
11,321/36,681,314B SHA
`633fca2fd203738fe14436f33f81add1bd3fae2ad0c0f379fb2f88b78bdc17cf`. The
histogram end-to-end p95 is effectively unchanged, so this pair provides no
basis to adopt512 or claim a cost/S3 improvement. R3 and the256-file product
bound remain unchanged. At that checkpoint the5min/30min/10min1/2/4-worker gate
was still required; its later corrected results follow.

### Corrected official 1/2/4-worker profiles

On2026-09-23 the corrected ARM64 official profile completed at1,2,and4 workers
with5min warmup,30min offered load,10min drain, worker restart/cold replacement,
post-load verification, and the full10k/100k/1m/10m native oracle. Each accepted
220,500 records with35 duplicate retry envelopes, zero conflicts/query failures,
zero final backlog, and no cgroup OOM. The fixed500ms query targets still fail
at1/2 workers; the4-worker run passes all recorded targets.

| Workers | Rows p95 | Histogram p95 | ACK p95 | Visibility p95 | Load backlog slope/min | Target result |
|---:|---:|---:|---:|---:|---:|---|
|1|565ms|1,353ms|372ms|982ms|−0.0318|rows and histogram fail|
|2|336ms|612ms|380ms|740ms|+0.00377|histogram fails|
|4|244ms|436ms|373ms|531ms|−0.00331|all targets pass|

The joined query diagnostics show a correlation, not an isolated causal result:
histogram `JobMS` p95/object-count p95/scan-task-count p95 were965ms/1,553/7
at1 worker,434ms/311/2 at2, and321ms/312/2 at4. Scanned-byte p95 was19.0MB,
11.8MB, and11.7MB. Earlier1-worker compaction completed420 tasks/11,698 inputs
and left15 tasks/429 inputs queued, versus1,650/13,688 at2 workers and
1,667/13,703 at4. Fresh inputs and different maintenance trajectories mean
these comparisons do not establish worker count as the sole cause. No resource,
query, or500ms threshold was relaxed. The4-worker native10m dataset profile took
1,007.98s; analytics output was528,495,771B, journal output682,231,716B,
cgroup peak536,879,104B, and OOM0.

The archived run cost totals are preliminary: the report collector had no S3
LIST-page counter. The runtime now counts every ListObjectsV2 paginator page;
the comparison collector, per-phase deltas, and whole-installation cost model
include LIST at the PUT/COPY/POST/LIST request tier. The fixture uses
USD0.005/1,000 LIST requests, matching S3 Standard PUT-tier pricing on the
[AWS S3 pricing page](https://aws.amazon.com/s3/pricing/). Unit tests verify
multi-page counting and exact PUT+LIST+GET/HEAD request-cost composition; the
real MinIO integration and complete10,000-day publication boundary passed on
the same source tree. Corrected official cost totals still require rerunning
the official profiles. R3 remains incomplete because1/2-worker query SLOs and
corrected official cost evidence are open.

### Current-source fixed-work capacity matrix

`./scripts/check capacity` completed on ARM64 source revision
`1cf35380c31c3d0f8d3ed78eed9b2f157061e542` using nine fresh isolated
installations: three each at1/2/4 workers. Each run durably preloaded and
published the same13,440 records in128 cycles, verified exact public counts,
and measured drain including sequential Docker worker resume. Preload and
post-drain query verification are excluded from throughput. This is a fixed-work
publication-capacity test, not steady-state maximum capacity or the mixed-load
R3 SLO.

| Workers | Median records/s (range) | Speedup / efficiency | Median sampled installation peak | Median PG WAL | Median S3 requests PUT/HEAD/LIST/GET/Range |
|---:|---:|---:|---:|---:|---:|
|1|124.9 (118.9–126.8)|1.000 /1.000|569,859,111B|22,906,136B|2,309/3,855/0/4,619/768|
|2|230.5 (226.6–246.1)|1.846 /0.923|616,965,338B|23,272,296B|2,309/3,858/0/4,620/768|
|4|401.2 (384.0–407.2)|3.212 /0.803|696,541,770B|23,273,912B|2,309/3,864/0/4,622/768|

Median S3 PUT/GET/Range transfer was12,029,570/24,059,307/6,784,771B,
12,029,684/24,059,702/6,784,319B, and12,029,861/24,061,714/6,784,102B
for1/2/4 workers respectively. All nine capacity reports passed durable
preload, complete publication, public-count oracle, resource completeness and
Go-unit512MiB checks; cgroup OOM events/kills were0. The source image used the
pinned DuckDB2.0 recipe and the fixed Go1.27.1 ARM64 build. Retained per-run
reports and the nine-sample matrix are under `.tools/capacity-report-*` and
`.tools/capacity-matrix.6hCBeY`. These measurements do not change the failing
1/2-worker query SLOs or close R3.

### Rejected 64MiB compaction target

A fresh ARM64 one-worker `20s/900s/90s` Colima profile tested the DESIGN.md
32–64MiB compaction range by changing only `TargetCompactionBytes` from32 to
64MiB. The 32MiB comparison was
`.tools/comparison-report-1.wxEjUA/report.json`; the 64MiB candidate was
`.tools/comparison-report-1.jKBYtI/report.json`. Both used Go1.27.1, the pinned
DuckDB2.0 ARM64 image, CPU1/512MiB Go-unit limits, disposable PostgreSQL/MinIO,
and accepted96,600 records with15 duplicate retries, zero conflicts, zero final
backlog and zero cgroup OOM kills. Fresh actual submitted inputs differ:
122,006,241B / SHA
`e9effc6556b3d2f610e8c87769b8c5ec1554543ca9b1183dccc17eb060d7ddc8` versus
124,093,974B / SHA
`0d6c27dff5afcc0d137156119700eee37b9ac6c958859d71061de8b5b101d705`.

| Measurement | 32MiB | 64MiB |
|---|---:|---:|
| Rows / histogram p95 |337 /558ms|345 /594ms|
| Histogram server / HTTP overhead p95 |373 /204ms|404 /209ms|
| Visibility p95 / backlog slope per minute |784ms /−0.0591|795ms /−0.0699|
| Maximum query files / active catalog bundles |729 /674|782 /767|
| Completed compaction tasks / input bundles |375 /5,333|352 /5,248|
| Whole-installation sampled peak |1,280,437,123B|1,465,947,039B|
| Worker cgroup peak / OOM kills |138,403,840B /0|134,602,752B /0|
| Cold / warm post-load query |979 /658ms|1,022 /707ms|

Both miss the500ms histogram target; cold/warm/idle, visibility, backlog,
resource and durability checks pass. The larger target did not improve the
measured query/object counts, so it is not adopted. Because the submitted input
hashes and compaction trajectories differ, these results do not establish that
the target alone caused the deltas. The projected local monthly totals
($668.20/$680.58) are run-specific estimates, not comparable cloud bills or a
cost improvement claim. R3 remains open.

### Historical official R3 profiles excluded after integrity regression

The profiles below are retained for traceability but do not establish current-
source latency, resource, S3 or cost behavior. They were run from dirty source
that skipped payload HEAD verification for analytics operations. A later
ARM64 `TestPublicQueryEndToEndFirstPagePruning` failed its full paired-catalog
HEAD assertion for both sort orders. The candidate optimization was removed;
`LoadVerifiedCatalog` again checks every analytics/payload pair before planning.
The restored behavior has focused unit coverage and the pruning integration
test passes. The later current-source official-duration rerun is recorded
below; reports in this historical section remain excluded from G07.

Historical ARM64 `5m/30m/10m` reports are
`.tools/comparison-report-1.mLGrRk/report.json`,
`.tools/comparison-report-2.pMhYAN/report.json`, and
`.tools/comparison-report-4.kFHQi8/report.json`. The1-worker report is based on
`36d4342` with a dirty worktree; the2/4-worker reports are based on `e9472a8`
with dirty worktrees. All use the same frozen10m fixture-definition checksum
`9326f0f7…`, but actual submitted envelope hashes differ
(`b8efcd62…`, `fa8a9ebb…`, `2aa6c71c…`). These are official workload profiles,
not byte-identical before/after samples; differences across reports are not
causal performance claims.

| Workers | Accepted | Rows / histogram p95 | Histogram server / HTTP overhead p95 | ACK / visibility p95 | Load backlog slope/min; max; final | Whole-installation peak / OOM kills | Estimated monthly cost |
|---:|---:|---:|---:|---:|---:|---:|---:|
|1|220,500|2,862 /6,876ms|5,722 /1,264ms|375 /813,164ms|+393.05;11,194;10,910|1,793,640,625B /0|$772.59|
|2|220,500|435 /754ms|556 /239ms|382 /846ms|+0.0312;43;0|2,033,146,919B /0|$541.92|
|4|220,500|250 /521ms|395 /147ms|380 /642ms|−0.00127;7;0|2,195,005,109B /0|$596.79|

Histogram p95 misses the500ms target at every worker count (by6,376/254/21ms).
The1-worker profile also misses rows, visibility, backlog-drain and post-load
completion; its idle phase retains10,860 backlog jobs. The2/4-worker profiles
drain and pass all targets except histogram p95. All profiles have zero query
failures, conflicts and cgroup OOM kills. Costs are us-east-1 monthly projections
from the report's 2026-09-21 pricing inputs, not bills; their Fargate, PostgreSQL,
backup, network, logging, tax and support assumptions/exclusions stay attached
to each full report.

| Workers | S3 HEAD / GET / Range / PUT requests | Range / GET / PUT bytes | PG end / WAL bytes |
|---:|---:|---:|---:|
|1|863,910 /53,457 /11,896 /28,353|94,603,751 /261,619,940 /124,677,616|303,363,763 /633,784,336|
|2|143,386 /105,450 /15,821 /40,813|194,936,942 /702,496,809 /282,918,622|212,162,227 /659,831,328|
|4|137,385 /105,291 /18,819 /40,757|270,986,544 /698,687,954 /281,462,324|211,605,171 /691,936,592|

S3 LIST requests were0 in these profiles. Post-load cold/warm checks measured
1-worker16,476/13,138ms and Range4,847→22 (45,609,707→182,352B), but idle
backlog remained10,860. At2 workers they measured456/314ms and Range290→2
(14,511,128→16,505B), with idle backlog0. At4 workers they measured680/633ms
and Range305→305 (14,685,055→14,685,055B), also with idle backlog0; that sample
shows no warm Range-cache reduction. Do not generalize these cache observations
to other cohorts.

The historical candidate retained the8-way catalog verifier but used HEAD for
selected analytics objects and only HEADs payload objects for detail
operations; that behavior violated the paired-catalog integrity contract and
was removed. A four-worker16-way
candidate report, `.tools/comparison-report-4.u10Qzx/report.json`, recorded
histogram p95=791ms, server551ms, visibility709ms and a whole-installation peak
of2,265,008,045B (versus2,195,005,109B in the separate8-way report); it was
reverted to8. The submitted-envelope hashes differ, so this rejects16-way as an
unsupported candidate, not as an isolated causal comparison. A local synthetic
1ms-HEAD benchmark is not provider or end-to-end evidence.

A separate aggregate scan-size128 experiment ran only the quick `20s/60s/30s`
diagnostic, accepted8,400 per profile and collected5–6 histogram samples:

| Workers | Rows / histogram / overall query p95 | ACK p95 | Load backlog slope/min | Post-load Range GET cold→warm |
|---:|---:|---:|---:|---:|
|1|391 /809 /809ms|376ms|+13.38|471→2|
|2|256 /486 /486ms|379ms|+3.16|176→0|
|4|394 /599 /1,167ms|557ms|+20.90|144→144|

The2-worker quick profile passes both query p95 checks but fails backlog slope;
the1-worker profile misses histogram/backlog/maintenance, and4 workers miss
histogram/ACK/backlog. Post-load cold/warm Range bytes were4,183,259→16,484B,
1,995,766→0B, and7,198,415→7,198,415B respectively. All reported OOM kills
were0, but the short profiles are under-sampled, use different submitted
envelopes and are not comparable to the official gate. This candidate is not
adopted; the product retains the256-file scan bound. The fixed-dataset native
oracle passed; the overall quick comparison exited1 on its failed profile
targets. R3 remains incomplete.

### Current-source official R3 rerun with full catalog verification

The ARM64 `5m/30m/10m` profiles completed on2026-09-24 against revision
`6c96edf3cb25626a1d4030ff0a5d72bfd9f669ad-dirty`, after restoring HEAD
verification for every analytics/payload pair. Reports are
`.tools/comparison-report-1.A6Y6Ee/report.json`,
`.tools/comparison-report-2.X9qQdr/report.json`, and
`.tools/comparison-report-4.3JEPGa/report.json`. Each accepted220,500 records,
had zero query failures/conflicts and passed the fixed-dataset native oracle.
The actual submitted envelope hashes differ across fresh installations, so
worker-count deltas are not isolated causal comparisons.

| Workers | Rows / histogram p95 | Histogram server / overhead p95 | ACK / visibility p95 | Backlog slope/min; max; final | Failed workload gates | Whole-installation peak / OOM kills | Projected monthly cost |
|---:|---:|---:|---:|---:|---|---:|---:|
|1|1,351 /4,458ms|3,543 /905ms|371 /137,262ms|+35.566;2,488;914|rows, histogram, visibility, growing backlog, drain/publication/post-load|2,039,792,793B /0|$1,142.59|
|2|528 /1,070ms|732 /337ms|379 /1,021ms|+0.01762;22;0|rows, histogram|2,050,364,536B /0|$589.90|
|4|423 /615ms|440 /249ms|376 /872ms|−0.00199;6;0|histogram|2,201,537,738B /0|$644.28|

All profiles kept Go units within512MiB and recorded zero cgroup OOM events or
kills. The1-worker public oracle returned202,100 logs and10,105 errors rather
than the full210,000/10,500; the2/4-worker public counts were exact. The4-worker
histogram p95 sample selected231 objects, scanned8,956,716B in one scan task,
and measured440ms durable-job time. These are measured phases, not exclusive
CPU timings. The4-worker query target is615ms end-to-end against500ms; the
server-only p95 of440ms does not pass that end-to-end gate.

S3 request/transfer counts and PG WAL are included to retain cost context, not
to claim the runs are directly comparable:

| Workers | PUT / HEAD / LIST / GET / Range requests | PUT / GET / Range bytes | PG WAL bytes |
|---:|---:|---:|---:|
|1|38,539 /1,437,132 /0 /88,950 /14,034|213,374,484 /502,541,704 /125,040,054|794,155,472|
|2|40,795 /241,759 /0 /105,349 /15,159|282,500,123 /700,988,468 /189,940,604|664,904,440|
|4|40,856 /231,770 /0 /105,555 /19,329|282,761,357 /701,706,017 /277,079,032|689,365,328|

The report's date-stamped us-east-1 model projects `$1,142.59/$589.90/$644.28`
per month for1/2/4 workers. These are run-specific projections, not bills or
cross-profile cost claims. The1-worker post-load phase retained571 jobs and
did not satisfy drain/post-load completion. The2-worker cold/warm regex was
504/421ms with Range GET309→2 and14,685,567→16,326B; the4-worker result was
551/504ms with Range GET294→294 and14,569,618B unchanged. Those post-load
observations do not alter the failed histogram SLO. R3 remains incomplete; a
fresh measurement alone did not fix the service gate.

### Full-window R3 cache diagnostic (not an official profile)

On2026-09-25, a separate4-worker ARM64 diagnostic used15minutes of the same
105-records/s warmup to fill the real15-minute received-time query window, then
measured5minutes of load and90seconds of drain. It accepted126,000 records and
collected30 rows plus30 histogram samples. Rows/histogram p95 were251/490ms;
histogram durable-job and HTTP-overhead p95 were371/123ms. The30-sample
histogram p95 is below the official180-sample run and is not an official
5m/30m/10m gate result. The report labels its reused application images
`unverified-reused-image-6c96edf3-dirty`, so this diagnostic is not source
provenance or a release claim.

The added per-query `cache_bytes` evidence shows29/30 histogram samples served
some verified bytes from the Range block cache; cache-byte p95 was7,541,834B
against scanned-byte p95 of8,575,777B. Thus a general absence of cache hits is
not supported as the cause of the earlier615ms official p95. The sample also
passed local ACK, visibility, backlog/drain, public-count, and cold/warm checks:
ACK/visibility p95 were370/533ms, backlog slope was−0.1802/min, final backlog0,
and drain1,003ms. Go-unit memory limits and cgroup OOM checks passed. This does
not supersede the current-source official failures at1/2/4 workers, the separate
fixed-work capacity matrix, or release evidence.

After those runs, review of the comparison gate found that both its load
monitor and drain count omitted active `maintenance_tasks` and their reserved
`maintenance_inputs`. The historic backlog slopes, peaks, and final counts
above are therefore foreground-only undercounts; the old2/4-worker results do
not prove the combined durable-work backlog gate. The harness now includes
queued/running/prepared maintenance tasks and inputs in the same query, with a
regression check for every queue. A fresh official1/2/4-worker profile is
required before assessing that gate.

### Corrected current-source R3 one-worker official rerun

On2026-09-24 UTC, `./scripts/check-comparison` rebuilt both ARM64 images from
the current dirty tree, reused the pinned local DuckDB2.0 native dependency,
passed the10,000/100,000/1,000,000/10,000,000-row independent native oracle,
and ran the official5m/30m/10m profile at one worker. The10,000,000-row native
oracle reached a recorded cgroup peak of536,875,008B against a536,870,912B
limit, with OOM/OOM-kill counters0; do not describe that sample as below the
limit. The sustained report is
`.tools/comparison-report-1.M4vwC2/report.json`; source revision is
`6c96edf3cb25626a1d4030ff0a5d72bfd9f669ad-dirty`.

The profile accepted220,500 records (210,000 logs+10,500 errors), had35
duplicates, zero conflicts/query failures, exact public counts, ACK p95=370ms,
and visibility p95=1,111ms. Rows/histogram end-to-end p95 were689/1,638ms
(server179/1,127ms; HTTP/other overhead530/556ms), and the overall query p95
was1,571ms. Its corrected combined durable-work backlog slope was+16.652/min
(360 measured samples), load peak852 items, then a drain peak843 and final0 in
55,449ms. The load-backlog gate and both query p95 gates failed; all other
profile targets, including publication, maintenance accounting, post-load,
and zero OOM, passed.

The179 histogram samples all used some verified Range-cache bytes; p95 selected
1,750 objects across7 scan tasks and scanned20,920,745B, of which20,450,478B
came from the cache. That makes cold Range misses an unlikely explanation for
the query-job latency. At load end, compaction had444 completed tasks over
10,830 inputs, plus15 queued tasks reserving760 inputs and one running task
reserving53. This is direct evidence of file fan-out and maintenance backlog,
not proof that either alone causes the SLO misses.

The run recorded PUT/HEAD/LIST/GET/Range requests of39,234/903,012/0/102,344/
13,388; PUT/GET/Range bytes were242,768,948/626,165,654/132,045,347, and PG
WAL was708,535,048B. Whole-installation sampled peak was1,961,665,493B;
worker peak was164,499,456B, with zero cgroup OOM events/kills. Its dated
us-east-1 fixture model projects `$883.34/month` (not a bill). A fresh worker
with empty Eventglass block cache passed the post-load same-snapshot oracle:
cold/warm363/223ms, Range requests167→0 and bytes13,803,454→0; the60s idle
window produced no query work. This does not close R3.

The run recorded3,866 S3 Range requests/76,586,637B,56,331 HEADs,23,089 PUTs,
59,811 full GETs, and334,811,960B PostgreSQL WAL. Its report projects
$566.92/month from the dated fixture pricing inputs, not a bill or a directly
comparable cost result. Whole-installation sampled peak was1,555,405,207B;
individual Go containers remained within their512MiB cgroup limits with zero
OOM kills. Report: `.tools/comparison-report-4.2jfeTq/report.json`.

A separate1-worker20s/60s/30s quick run then exercised the corrected combined
backlog SQL against real disposable PostgreSQL/MinIO. It reported a
`+77.34/min` load slope, max79 outstanding items and final0 after an8,025ms
drain; its query and load-backlog targets failed. At load end it had queued or
prepared compaction work across four lanes. This short run reuses images marked
`unverified-reused-image-6c96edf3-dirty`; it proves the query executes and
detects active maintenance work, not source provenance or an official R3 pass.
Report: `.tools/comparison-report-1.Mzwbit/report.json`.

### Rejected official one-worker 1,024-file scan cap

On2026-09-25, an ARM64 official5m/30m/10m one-worker profile tested an
experimental shared native/planner cap of1,024 analytics inputs per scan
against the corrected256-file baseline above. The run rebuilt the source at
`6c96edf3cb25626a1d4030ff0a5d72bfd9f669ad-dirty`, used the pinned DuckDB2.0
image, accepted220,500 records, and passed the independent10k/100k/1M/10M fixed
dataset oracle. Report: `.tools/comparison-report-1.NoACJb/report.json`;
failed-run copy: `.tools/comparison-workers-1-failed.json`.

The candidate is rejected. Compared with the corrected256-file one-worker
profile (rows/histogram p95=689/1,638ms), end-to-end p95 regressed to6,332/
6,614ms; visibility p95 reached841,445ms. Combined backlog grew+623.420/min,
peaked at18,854 and remained18,638 after the full600,285ms drain. Only50,200
logs and2,513 errors were visible to the publication-count oracle. Histogram
p95 scan tasks fell7→5, but selected objects grew1,753→2,826; per-query p95
timings also worsened (rows pre/job479/179→1,530/6,295ms; histogram
531/1,140→1,408/6,265ms). The run had no query errors or conflicts, ACK
p95=386ms, whole-installation sampled peak1,523,231,751B, worker cgroup
peak104,779,776B, and zero OOM events/kills. These are independent fresh
installations, so do not assert the file cap alone caused the backlog
trajectory; the observed end-to-end regression is sufficient to reject this
candidate. The production cap remains256 and R3 remains open.

Post-load same-snapshot rows matched, and the60s idle window submitted no new
query work, but backlog remained18,602 and maintenance-time accounting failed.
Cold/warm latency was6,587/4,943ms; Range GETs fell3,453→7 and Range bytes
29,894,457→57,027. S3 recorded19,817 PUT,877,944 HEAD,29,975 GET and6,921
Range requests (zero LIST), with69,031,383 PUT,122,224,439 GET and56,728,845
Range bytes; PG WAL was397,615,312B. The dated pricing model projects
$694.07/month, not a bill. All resource and post-load results remain
run-specific.

### Rejected official catalog metadata-fanout16 experiment

On2026-09-25, `EVENTGLASS_COMPARISON_WORKERS='1 2 4' ./scripts/check-comparison`
tested a bounded increase of catalog metadata verification concurrency from8
to16. It used Go1.27.1 ARM64, the pinned DuckDB2.0 image and isolated local
PostgreSQL/MinIO. The independent10k/100k/1M/10M native oracle passed; the10M
sample processed528,495,771B analytics and682,231,716B journal data in
1,153,174ms, with no cgroup OOM. Its sampled peak536,875,008B was4,096B above
the536,870,912B container limit, so it is not evidence of staying under that
limit. The product fanout was restored to8 after the failed experiment.

The completed one-worker report is `.tools/comparison-report-1.w538gI/report.json`.
It accepted220,500 records, with overall/rows/histogram query p95
7,904/6,594/8,694ms, visibility p95 849,387ms, and three client/decode query
failures. Combined backlog grew637.215/min, peaked at18,836 and remained18,770
after600,123ms drain. ACK p95 was409ms. Same-snapshot post-load rows matched,
but idle backlog remained18,734; cold/warm latency was11,345/9,659ms. The
whole-installation sampled peak was1,429,590,768B; worker cgroup peak was
83,038,208B, with zero cgroup OOM events/kills. The run measured728,187 HEAD,
28,114 GET,5,779 Range and19,168 PUT requests; PG WAL was
368,960,032B. Its dated fixture cost projection was$612.45/month, not a bill.
The independent installation's maintenance trajectory differs from earlier
runs, so this sample rejects the candidate but does not prove fanout alone
caused the regression.

The two-worker profile did not produce a complete report: the30-minute load
schedule elapsed30m8.462s and hit its two-second lateness guard before report
serialization. The four-worker profile also stopped before report
serialization, when input sequence425 received HTTP429 `admission_limited`.
The independent fixed-dataset oracle then passed. These are failures and
incomplete samples, not a passing1/2/4 matrix; R3 remains open. The code retains
the original bounded eight-reader fanout.

### Rejected R3 catalog page-size 1,024 quick diagnostic

On2026-09-25, a one-worker `20s/300s/90s` quick comparison tested catalog pages
of1,024 against the production256-file page bound. Both fresh isolated
PostgreSQL/MinIO ARM64 runs used Go1.27.1 and the pinned DuckDB2.0 build. The
256-file baseline report is `.tools/comparison-report-1.MY834M/report.json`
(`4c564ff`); the candidate report is
`.tools/comparison-report-1.CE5Bu6/report.json` (`4c564ff-dirty`). Both accepted
33,600 records (112/s over the300s load) with zero query failures; rows and
histogram percentiles had30 and29 samples respectively.

The256-file baseline rows/histogram p95 was538/1,420ms (server192/904ms,
other overhead435/367ms); the1,024 candidate was1,811/3,043ms
(server524/1,945ms, other overhead1,612/1,098ms). Visibility p95 changed from
10,061ms to98,243ms. Combined load-backlog slope/max was+84.425/min and433
versus+266.422/min and1,359; after90.346/90.138s drain,90/469 items remained.
Both quick profiles miss R3 targets, and the candidate additionally missed
publication, maintenance-progress, no-growth and post-load drain/accounting
checks. The short diagnostic is not an official G07 profile.

S3 HEAD counts were68,624/100,991. Full GETs were15,323/10,610 requests and
89,963,238/55,096,906B; Range GETs were2,027/2,032 and19,096,061/17,240,963B;
PUTs were5,963/5,440 and35,247,077/27,759,181B. PostgreSQL WAL was80,720,896/
77,034,320B. Whole-installation sampled peaks were887,703,467/835,944,707B;
worker cgroup peaks were77,737,984/62,664,704B; both runs recorded zero cgroup
OOM events/kills. The model projected$654.36/$722.42 per month from its dated
inputs; these independent-run estimates are not a comparative cost claim.
Different fresh-installation object/maintenance trajectories prevent attributing
the regressions solely to the page size. The candidate was not adopted and the
production limit remains256.

The added pagination test proves cursor progression across three bounded pages
and HEAD-verifies every analytics/payload pair. Real PostgreSQL integration
checks accept the256 boundary and reject257 for both snapshot and durable query
catalog routes. `./scripts/check integration` passed on Go1.27.1 ARM64: host
integration completed in128.839s, real-MinIO missing/corruption/retry checks in
1.203s, and the pinned-native query/maintenance/tenant-scheduling selection in
67.682s. The final10,000-event-day conversion test passed in927.21s with exact
per-day identities, one outstanding pair and zero residual disk reservation;
its ARM64 cgroup peak was155,635,712B against536,870,912B, swap0, and OOM/OOM-kill
counters0. These checks validate the retained bound and integration contracts;
they do not close the sustained R3 SLO.

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

### R4 local self-host recovery rehearsal

On2026-09-25, a fresh `./scripts/check recovery` run used isolated Colima
volumes, PostgreSQL17.11 and pinned pgBackRest2.59.1. It created and restored a
real base backup plus WAL to LSN`0/502CF28`; the two restore clusters completed
in10s after a5s backup. The restored generation verified one referenced18B S3
object, passed `TestActivatedRestoreReadsVerifiedOldGenerationWithoutAdoptingNewerObject`,
and did not adopt a newer unreferenced object. Deleting the referenced S3 object
made verification fail with `NoSuchKey`; the installation remained
`verification_required:true`. The script's random local rehearsal key/report
and all PG/S3 volumes were confined to that disposable run and removed during
cleanup. This passes the local self-host restore portion only; it is not AWS
identity/restore evidence, a production attestation, authorization for physical
GC, or a release. G08 remains incomplete.

On2026-09-26, `scripts/check-recovery` gained an opt-in AWS provider path using
a pre-created scratch bucket, expected account/owner checks, required
short-lived session credentials, a random run prefix shared by live objects and
pgBackRest, and cleanup scoped to that exact prefix. AWS endpoint overrides,
profiles and non-session credentials fail before any S3 write. A fresh local
ARM64 rerun passed with the AWS-only Compose override removed from MinIO mode:
pgBackRest backup1s, both restores2s, WAL target`0/502CF28`, new-object
non-adoption, and missing-reference fail-closed. The first attempt exposed an
empty-token variable reaching local pgBackRest; that was fixed by isolating the
token in an AWS-only Compose override. The AWS branch itself is not verified:
this host has neither the AWS CLI nor an AWS identity. This code path does not
close G08 or authorize physical GC.

The same source also passed `./scripts/check-browser`: a fresh Linux ARM64 final
image completed the Playwright Chromium SDK-to-UI scenario (1/1,3.1s) against
disposable PostgreSQL/MinIO. The local BuildKit output identified image/index
`sha256:55dadd53af9bb2ca20230622161df5f7f693ac88f54329451e2f3a96ef19c8ef`
as `linux/arm64` and emitted attestation manifest
`sha256:50c62b03bebe387d0b37117f322ce05f46283725c89c9275b238e30156bad47b`;
this local build metadata is not signed or independently trusted release
provenance. `./scripts/check sdk` passed fixture verification and the live
localhost Go/API/Go-ingest/SDK suites (0.119/1.129/11.778/10.785s). Its first
attempt failed only because the sandbox denied local TCP bind; the rerun with
local networking permission passed and contacted no external service.

### R4 local ARM64 release and supply-chain checks

On2026-09-26, the pinned Go dependencies `pgx` v5.9.0 and `x/crypto` v0.55.0
replaced scanner-flagged releases. The runtime base moved from the stale pinned
Bookworm image to official `debian:trixie-slim` index
`sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a`,
with matching `libcurl4t64`/`libssl3t64`. Go unit/vet/layout checks passed,
`./scripts/check release` passed its ARM64 non-root/read-only version smoke and
`./scripts/check-browser` passed1/1 on this runtime with disposable
PostgreSQL/MinIO, and the clean pushed`d4c0392` release image was rebuilt and
re-scanned locally. Its63MiB archive checksum is
`c3ec428cb5134c40522346bc9a213edc59c22d0c86f6a7f35ce6474712419711`; image ID
is`sha256:0bef92ebb9159b594a8012181128f4e99c97463feb5e6b7a163f7bfc914dd7a5`.
It was not pushed to a registry or deployed. The exact DuckDB2.0 native
contract build was
started, but intentionally interrupted while the engine package produced no
output for several minutes; it is not a passing result or a substitute for the
browser E2E.

On2026-09-26, current `68c1a24` source was rebuilt as
`eventglass-go:native-contract-68c1a24` for Linux ARM64 with Go1.27.1 and the
recipe-verified pinned DuckDB2.0 cache. A focused native contract selection
passed in0.549s: `TestReadParquetThroughCapabilityGatewayUsesRanges`,
`TestConvertBulkAppendPairedSchemaIdentityAndBounds`,
`TestConvertCancellationRemovesPartialOutputsAndSpill`,
`TestHistogramUsesOneNativeInputScan`,
`TestAttributeDoubleGroupsAndHistogramProjection`, and
`TestCompactionPreservesExactPairedIdentityAndBounds`. This confirms those six
contracts on the current source; the full `./scripts/check contracts` gate
remains incomplete and is not inferred from this selection.

The workflow-pinned Syft v1.42.3 ARM64 binary produced SPDX JSON containing147
packages from the63MiB app image archive. Trivy v0.74.0 scanned that same
archive with `os,library`, `HIGH,CRITICAL`, and unfixed findings included. The
scan failed its threshold with0 critical and47 high Debian13 package findings;
Trivy reported no fixed version for those remaining high findings. This is an
explicit release blocker, not a clean scan. The prior pinned Bookworm candidate
had6 critical and71 high findings in the same local procedure; the dependency
and base updates removed all critical and the Go-module findings without
weakening the threshold. Reports remain in the ignored local
`.tools/r4-release-scan/` directory.

The workflow-pinned go-licenses v2.0.1 successfully reported and saved49
third-party license entries for `./cmd/eventglass-go`, excluding only this
repository's own module. Using `./...` instead fails because the tool treats
Go1.27 standard-library packages as license-less and includes Eventglass's
own package with no root LICENSE; the workflow now scans the shipped binary's
dependency graph. This does not choose a license for Eventglass itself.

The main-only GitHub workflow itself has not run. This host still has no AWS CLI
or AWS identity, so actual AWS restore remains unverified. Signed provenance/
SBOM attestations and a real prior-release rollback rehearsal also remain open.
The changed-file `detect-secrets` hook passed with no new findings. The
documented SDK support matrix above is the scope verified by the passing live
gate; no additional SDK or version compatibility is claimed. No production
deploy or external alert was sent.

On2026-09-26, the Go build and final runtime APT indexes were pinned to the
official Debian snapshot date`20260926` in `deploy/versions.lock`. The dated
snapshot was fetched for Bookworm and Trixie on ARM64 and selected the same
package versions as the prior rolling-mirror build. `Check-Valid-Until=false`
is used only for the historical snapshot freshness field; APT signature
verification remains enabled. The DuckDB2.0 static bundle SHA-256
`84ad753acc1390e13ce56e728d75d379bebeedec7f3df58ce071c1b373e4c79f` is now
locked and checked before linking. The native build recipe was unchanged, so
the recipe-verified local bundle cache was reused.

A clean Colima build from source revision`9c509068a200fad8883508eaa0ca6aba44d475ca`
passed `./scripts/check release` on2026-09-26. Its image is
`sha256:cdabe801130038828e3b4031a4f53b151a68b2582ba03aa8af23d09dff3f5213`,
`linux/arm64`, user`65532:65532`, with `source_dirty=false`; the saved image
archive SHA-256 is
`8c3d028507fc8f2430b3efd10f0dbc7c6160a139da2b564e3fb864190b6f328d`. The
same commit passed `./scripts/check-browser` (Playwright1/1,5.9s) against
disposable PostgreSQL/MinIO. Syft1.42.3 generated an SPDX SBOM with147 packages
from that exact archive. Trivy0.74.0 scanned the same archive with vulnerability
scanners `os,library`, severities `HIGH,CRITICAL`, and unfixed findings enabled;
it exited1 with0 critical and47 high Debian13 package findings. This improves
input reproducibility but does not clear the security gate. The archive was not
pushed or deployed; AWS restore, signed attestations and rollback remain open.

### R4 Garage S3 and coordinated recovery contract

On2026-09-26, `./scripts/check garage` exercised pinned ARM64 Garage2.4.1 with
the real application S3 Range/list/multipart/abort/delete/auth tests and durable
ingest publication. It also ran the Garage mode of `scripts/check-recovery`:
pgBackRest2.59.1 created a PostgreSQL17.11 base backup and WAL, restored two
isolated PGDATA volumes through an internal TLS proxy, and reached LSN`0/502CF28`
(backup1s, both restores2s in this small fixture).
Recovery activation preserved the prior referenced
18B S3 object, did not adopt the newer unreferenced object, and passed
`TestActivatedRestoreReadsVerifiedOldGenerationWithoutAdoptingNewerObject`.
Deleting the referenced object returned `NoSuchKey` and kept the installation
`verification_required:true`. Both Compose projects used unique disposable
buckets/volumes and were removed on exit. This proves the tested single-node
S3/recovery path only; it is not Garage multi-node durability, production HA or
security evidence, AWS evidence, or release approval. AWS OIDC/restore,
rollback rehearsal and the workflow's scans/attestations remain open.

The same clean `f03b800` source passed `./scripts/check release` on Colima
ARM64. The local image is `linux/arm64`, runs as`65532:65532`, uses the expected
entrypoint, and passed the read-only version smoke (`0.0.0-g06`). The60MiB
saved image archive checksum verified as
`63bd07a2c4163e831c2da3c0ff275baeead67b783cf20711a535e16ae82d6987`;
image ID is`sha256:b640dffb380f86b497983dcada0f849cf69a991320c291857f86bf7c1202c8cc`.
The image was not pushed or deployed; the GitHub workflow, scans, attestations,
AWS restore, and rollback evidence remain unrun.

### Latest corrected current-source R3 official 1/2/4-worker matrix

On2026-09-25, `EVENTGLASS_COMPARISON_WORKERS='1 2 4' ./scripts/check-comparison`
rebuilt the ARM64 application from the dirty `a3a0bca` worktree, reused the
recipe-verified local DuckDB2.0 dependency, and ran each5m-warmup/30m-load/
10m-drain profile in a fresh isolated PostgreSQL/MinIO installation. All three
passed the independent10k/100k/1M/10M native oracle, accepted220,500 records,
and had zero query failures or conflicts. The submitted-envelope SHA differs
for each fresh profile, so worker-count comparisons are not causal scaling
measurements. Reports:
`.tools/comparison-report-1.EyBU9L/report.json`,
`.tools/comparison-report-2.URYCwK/report.json`, and
`.tools/comparison-report-4.Rxqn3p/report.json`.

The profiled dirty source includes the page-first catalog CTE in
`internal/control/catalog.go`: all tenant/snapshot/generation/cut/kind/time/
retention/cursor/project filters and file ordering/limit run before analytics
and payload block-manifest aggregation, which is scoped to the at-most256-row
page. The existing paired metadata checks and downstream full object HEAD
verification remain. Focused control tests and the PostgreSQL/MinIO integration
gate passed for this path. These official profiles are not a matched A/B against
the clean source, so no latency gain is attributed to the CTE.

| Workers | Rows / histogram p95 | Histogram server / overhead p95 | ACK / visibility p95 | Combined load-backlog slope/min; max; final; drain | Failed gates | Post-load idle backlog | Whole-installation peak / max Go-unit peak / OOM kills | Projected monthly cost |
|---:|---:|---:|---:|---:|---|---:|---:|---:|
|1|603 /1,361ms|928 /455ms|370 /1,111ms|+8.186;693;0;49.524s|rows, histogram, backlog, post-load|393|1,908,156,659B /161,734,656B /0|$857.11|
|2|294 /513ms|352 /183ms|382 /704ms|−0.003704;8;0;1.004s|histogram|0|1,988,540,495B /189,644,800B /0|$588.41|
|4|289 /569ms|393 /198ms|379 /685ms|+0.009581;14;0;1.009s|histogram|0|2,208,772,911B /170,156,032B /0|$644.55|

The1-worker profile missed the<500ms rows/histogram p95 targets and retained393
items during the60s idle/post-load check despite reaching final backlog0 after
the documented drain. The2/4-worker runs met the other workload gates but
missed histogram p95 by13/69ms. All Go units stayed within512MiB and all three
profiles recorded zero cgroup OOM events/kills. The10M native oracle's cgroup
peak was536,875,008B against536,870,912B,4,096B over the limit; it had zero OOM
events/kills and passed the exact output oracle, but is not evidence of staying
under the limit.

| Workers | S3 PUT / HEAD / GET / Range requests | S3 PUT / GET / Range bytes | PG WAL bytes |
|---:|---:|---:|---:|
|1|39,104 /855,153 /100,417 /13,465|235,352,559 /599,958,808 /131,003,713|699,134,056|
|2|40,814 /239,008 /105,386 /14,902|279,684,468 /696,164,464 /186,246,721|659,329,928|
|4|40,756 /233,448 /105,291 /19,683|281,611,106 /699,121,333 /280,384,550|693,465,888|

The dated us-east-1 fixture cost projections are run-specific estimates, not
bills or comparative claims. The1-worker post-load check measured cold/warm
1,279/923ms and failed its idle-backlog criterion;2/4-worker cold/warm timings
were489/474ms and713/752ms. No profile passes G07, so R3 remains incomplete.

#### Rejected histogram empty-bucket join candidate

A same-fixture pinned ARM64/DuckDB2.0 CPU1/512MiB benchmark tested replacing the
bounded `NOT EXISTS` empty-bucket fill with a `LEFT JOIN`. At256 input files,
five3-iteration samples measured67.77–72.13ms and1,284,701B/1,582 allocations
per operation, versus the preceding same-fixture5-sample baseline of61.7–65.4ms
and approximately1.284MB/1,582 allocations. The32-file case also regressed from
26.3–28.9ms to29.54–31.93ms. Correctness tests passed, but the measured SQL
shape was slower, so the candidate was reverted. This isolated native benchmark
is not a full-service SLO or causal R3 result.

#### R3 input provenance separates planned work from retries

The comparison runner now sets one UTC input epoch per worker matrix (or accepts
`EVENTGLASS_COMPARISON_INPUT_EPOCH` for separate paired invocations). Reports
retain the sorted hash/count/bytes of actual HTTP attempts, including 429
retries, and separately hash/count/measure the planned envelopes once, including
intentional duplicate requests but excluding transport retries. The planned hash
normalizes only the installation-generated DSN key. Its fixed-memory limit
covers both digest sets. Go1.27.1 ARM64 comparison and report contract tests pass;
the workspace first denied httptest loopback binds, and the same tests passed
with local-only networking permission. No service profile was run for this
harness correction. Existing official reports have not been rewritten and R3
remains incomplete.
