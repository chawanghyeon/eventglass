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
Catalog verification uses at most four readers per page, preserves catalog order,
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
not a Vite development server. Release still requires M4/R1-R4 evidence.

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
