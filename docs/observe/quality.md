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

## Release and workflow

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
