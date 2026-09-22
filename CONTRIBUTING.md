# Contributing to Eventglass

The active product is the root Go module. Use Go 1.27.1 exactly and run commands from the repository root unless noted otherwise.

```sh
./scripts/bootstrap
./scripts/check unit
./scripts/check codegen
./scripts/check architecture
./scripts/check focused ingest
./scripts/check journal-bench
./scripts/check perf
./scripts/check contracts
./scripts/check integration
./scripts/check recovery
./scripts/check resource
./scripts/check scale
./scripts/check comparison
./scripts/check capacity
./scripts/check native-oracle
./scripts/check sdk
./scripts/check crash
./scripts/check web
./scripts/check-browser
```

Commands for later implementation gates intentionally fail until their gate is implemented. `./scripts/check codegen` regenerates the Go and TypeScript wire contracts in a temporary tree and requires byte-identical output; install its exact tool lock with `npm ci --prefix tools/codegen --ignore-scripts` when changing `api/openapi.yaml`, then run `./scripts/generate-api`. `./scripts/check sdk` replays committed captures and runs the pinned SDK applications against a localhost Go handler; run `tools/sdk-fixtures/bootstrap.sh` once to install its locked tools. Docker is required for the PostgreSQL/S3 integration environment and Linux ARM64 image checks. Tests must use temporary databases, buckets, prefixes, directories, and localhost receivers. Linux AMD64 is not a currently verified or supported release target.

Image checks use `scripts/build-image` to build the current root Dockerfile,
Go sources and UI. If BuildKit prunes an expensive native intermediate stage,
preserve its library from a **trusted, already verified local ARM64 build image**:

```sh
./scripts/cache-native eventglass-go:comparison-build
```

Import checks the exact native Dockerfile section, architecture and real engine
probe against the version lock. It records the source image ID and library SHA,
then stores only that dependency in a recipe-addressed local image. Subsequent
checks reuse it through a named BuildKit context; changed native recipes select
a different cache identity. No old application binary or UI is reused. The
local cache is not a signed release-provenance claim and must not be imported
from an untrusted image. With no matching local cache, builds use the pinned
source stage normally. The comparison quick-mode application-image reuse remains
diagnostic-only; it is separate from this unchanged-dependency cache.
The static Go build retains all module checksums but does not download the
adapter's unused platform-bundled DuckDB engines into its dependency layer.

The operator UI uses the exact Node/npm versions in `web/package.json`. Install
its locked dependencies with `npm ci --prefix web --ignore-scripts`; then
`./scripts/check web` runs Vitest, strict TypeScript, and the Vite production
build. Generated API types remain owned by `./scripts/generate-api`.
`./scripts/check-browser` builds the final ARM64 release image, including the
production UI, and runs API/worker/scheduler non-root with a read-only root and
bounded scratch against disposable PostgreSQL and MinIO. Playwright exercises
SDK-to-UI on the same origin without Vite. It never sends an external alert and requires local Docker plus the locked
Playwright Chromium installation.

`./scripts/check recovery` builds the pinned pgBackRest source, performs an
actual PostgreSQL base+WAL restore into isolated volumes, verifies restored S3
references, imports a signed rehearsal report, and exercises the missing-object
fail-closed path. It uses only disposable Colima/Docker resources.

`./scripts/check integration` also runs the pinned-native maintenance path:
real durable ingest, paired S3 publication, compaction, mixed-retention rewrite
at the exact received-time boundary, fully expired retirement, pinned old
snapshot reads, authority revocation and retry/cancellation checks. It does not
enable physical GC without signed backup evidence. Metadata-only maintenance
fixtures are not a substitute for this end-to-end path.
The same gate includes a real approximately72MiB-per-file paired bundle,
operation-specific verified-download limits (journal24MiB, result64MiB,
bundle128MiB), and actual S3 cancellation/retry. Passing a128MiB raw-object
download is not evidence that a maximum-size Parquet rewrite fits the worker
memory/time budget; retain those resource tests as separate requirements.
The conversion leg compiles its test binary outside the resource container,
then checks8-day errors and a legal10,000-day log container through real durable
ACK, the pinned child, S3 upload and fenced publication under CPU1/512MiB/swap0,
non-root, a read-only root and384MiB scratch. It checks exact per-day record
identity, one outstanding pair, complete catalog coverage and released disk
reservations. The runner prints cgroup peak/limits/events and fails on OOM;
it waits for PostgreSQL readiness and heartbeats the normal60-second job lease
while large conversion/publication work runs. This slow boundary check does
not substitute for sustained R3 SLOs.

`./scripts/check resource` runs the native ARM64 binary under CPU1/512MiB/swap0
for two minutes by default. `EVENTGLASS_RESOURCE_DURATION=30s` is available only
for development feedback; completion evidence uses the default. The gate checks
the target logical log/error mix, cgroup peak/OOM counters, native OOM and cancel
cleanup, permit ownership and scratch reclamation. It does not replace R3's
30-minute end-to-end PostgreSQL/S3 load.

`./scripts/check scale` verifies autoscale decisions, the PG64 replica budget,
tenant round-robin dispatch, identical 1/2/4 worker logical results and the
Kubernetes/KEDA bounds on Linux ARM64. It is not a throughput benchmark.

`./scripts/check comparison` runs the R3 end-to-end comparison against fresh
PostgreSQL and MinIO installations with 1, 2, and 4 ARM64 workers. The official
profile uses a five-minute warmup, 30-minute load, and ten-minute drain for each
worker count, exercises a real worker restart, and writes its measured latency,
backlog, whole-installation RSS, PG/WAL, S3 request/transfer, and dated cost
report under `.tools/`. Each completed measurement retains its report, cgroup
samples and runtime logs in a unique `.tools/comparison-report-N.*` directory;
the convenience `comparison-workers-N*.json` files represent only the latest
run. Reports also accumulate private role-operation counters across the worker
restart to distinguish empty claims from work time.
ACK latency samples exclude the five-minute warmup; the gate requires exactly
six measured original requests per load second. Warmup and duplicate/retry
traffic still contribute to actual input provenance and storage costs. Legacy
reports with12,600 ACK samples for a30-minute load included warmup and are not
load-only ACK SLO evidence. Maintenance evidence requires measured spare time,
all retention/compaction/GC attempt time (not only successful work), actual
compaction progress and zero cancellation-budget overruns. The aggregate check
supplements the rolling admission/cancellation tests; it does not assert a hard
CPU ratio for every retrospective sliding window.
Reports distinguish the fixed fixture-definition checksum from a streaming
checksum of actual envelope bodies submitted to HTTP (including retries and
duplicates, length-framed in submission order, not TCP arrival/ACK order).
After drain, the public aggregate API must return the complete generated log
and error counts across the whole run; receipts alone cannot satisfy this target.
The post-drain last15min regex field is explicitly not cold-cache evidence.
The runner then replaces every disposable worker and its tmpfs, verifies empty
block caches and new container IDs, and runs an all-run received-time regex
twice with the same read token. It records cold/warm latency, Range/full GET/
HEAD/PUT requests and bytes, exact returned-row hash, and actual warm misses
(work can move between workers). Only Eventglass's block caches are cold, not
the provider or host OS cache. A60-second idle phase checks no new query work;
quick diagnostics use10 seconds. These phases still run when load SLOs miss.
Retained evidence includes pre-cold worker logs, replacement IDs, sampled
whole-installation usage, and cgroup memory.peak/OOM/limit observations for
every container incarnation before replacement and at the end. Missing samples,
backends, previous workers or OOM observations fail resource verification;
Go cgroups must actually show512MiB memory and zero swap. The observation command
itself is included in the cgroup; its peak is observed before stop/kill, not a
promise of a final kernel value after the container has ceased to exist.
The official comparison command also requires `./scripts/check native-oracle`.
This Mode A check parses real SDK envelope bytes, normalizes them, writes and
verifies journals, then uses the pinned native conversion and existing query
scan/reduce planner for10k/100k/1m/10m records. Independent arithmetic and identity
oracles check typed filters, null/missing/dotted keys, half-open time bounds,
every partition's exact record set and equal-time keyset pages. Retry checks
distinguish occurrence IDs from source-event dedupe keys; they are not PG Accept
or durable ACK checks. The native-fixture-v1 byte hash is separate from the older
selector-definition checksum, whose numeric selector did not represent a typed
query result. All S3/network counts are zero in this network-denied local check;
Mode B measures actual provider requests, durability and authorization.
Each size runs in a fresh CPU1/512MiB/no-swap cgroup with96MiB Go soft limit,
256MiB conversion/192MiB query native limits and256MiB spill. Isolated disk
volumes, not memory-backed tmpfs, hold at most2GiB retained analytics plus
bounded stage/journal/output/spill; volumes are removed after child termination.
Logs, source/image identity, actual input hashes, timings, supervisor allocations
and cgroup peak/OOM evidence remain in `.tools/native-oracle.*`. Allocation
counts exclude unmanaged native memory; cgroup peaks include filesystem cache.
For matched diagnostics the native test binary accepts
`EVENTGLASS_NATIVE_ORACLE_QUERY_MEMORY_MIB=192` or`256`, selecting only existing
native profiles. The official runner does not forward this override and always
uses192MiB; neither product limits nor a failed gate may be bypassed with it.
Capacity/scaling measurements and all phases under the official-duration
profile remain required before R3 closure.
`./scripts/check capacity` adds a separate fixed-work publication-capacity
matrix: three fresh installations per1/2/4-worker profile,128 cycles each
(12,800 logs+640 errors, exactly768 durable jobs across four projects). It
preloads actual HTTP ACKs while the disposable workers are paused, awaits each
ACK to prevent random lane coalescing, then starts observation before resuming
them. Throughput includes resume overhead and ends at complete publication;
preload and the per-project public-query count oracle are recorded separately.
This is independent queued-work processing, not maximum live-ingestion capacity
or a replacement for the5min/30min/10min mixed-load SLO. It preserves every
role's CPU1/512MiB/no-swap profile, samples PG/S3 as shared costs, and records
actual input hashes, image/test identities, progress, S3 operations/bytes, WAL,
cgroup peaks/OOM and median speedup/efficiency without assuming linear scaling.
Fixed-work drain reports do not extrapolate a steady-state monthly bill.
`EVENTGLASS_COMPARISON_QUICK=1 ./scripts/check capacity` uses32 cycles and one
sample per worker count for harness diagnosis only. Dirty/reused sources,
shortened workloads or fewer samples remain diagnostic. Evidence is retained
under `.tools/capacity-report-N.*` with a `.tools/capacity-matrix.*` summary;
do not run heavy work alongside either capacity or sustained measurements.
`EVENTGLASS_COMPARISON_QUICK=1` permits shorter local
diagnostics but cannot complete R3. A generated report whose target map contains
`false` remains a failed gate; do not relabel the measurement as a pass.
For diagnostic iteration only, combine `EVENTGLASS_COMPARISON_QUICK=1` with
`EVENTGLASS_COMPARISON_REUSE_IMAGES=1` to reuse pre-existing ARM64 build/runtime
images. Optional `EVENTGLASS_COMPARISON_BUILD_IMAGE` and
`EVENTGLASS_COMPARISON_RUNTIME_IMAGE` select named local candidates; set
`EVENTGLASS_COMPARISON_REUSE_REVISION` to their actual source revision or the
report says `unverified-reused-image`. The runner mounts current comparison
test sources but does not rebuild the reused product binary. Never use this mode
to claim official completion; the full command always builds fresh images.

Complete and verify one gate at a time, then commit directly to `main` and run `git push origin main`. Use commit subjects such as `feat: 한국어 변경 요약`, selecting `fix`, `perf`, `test`, `docs`, or `chore` as appropriate. Do not bypass hooks or rewrite published history merely to normalize messages.

The former Rust implementation is recoverable from the `rust-version` tag and is not an active build, test, or deployment target.

Read DESIGN.md for behavior and ARCHITECTURE.md for package/state ownership.
Before continuing implementation, read docs/implementation/README.md and the
selected packet in docs/implementation/work-plan.md. Its linked contracts fix
schema/API/transaction decisions; do not infer missing behavior from old Rust docs.
Use focused pure-Go checks during development; run unit and relevant SDK/PG/S3
checks before shipping. Native changes additionally require contracts. The native
Docker stage is cached separately from Go sources; use ARM64 only. The journal
benchmark reports host allocations/time, not Linux RSS or production throughput.

Current pre-push runs unit/layout/architecture checks. Production deployment and
historical Rust artifact staging are not implemented by this hook. Gate completion
requires its executable evidence, not just the hook passing.

Unit tests discover all internal/cmd packages except native `internal/engine`,
which executes only under the pinned ARM64 `contracts` gate; vet covers all.
The ARM64 CI workflow runs unit, codegen, web and controlled benchmark samples.
CI configuration is not a claim that the remote run or branch protection passed.
`perf` prints revision/dirty state/toolchain/architecture and repeated journal
allocation and synthetic metadata-latency samples. It does not close R1-R3.
Accept/alert integration fixtures own isolated schemas, including installation
state; use their pool's DSN when constructing a runtime in those tests.

`EVENTGLASS_WEB_DIR` optionally enables same-origin production UI serving. The
final image sets it to `/usr/share/eventglass/web`; local API-only runs may omit
it. Missing assets fail startup. Unknown API/asset paths never return SPA HTML.
`EVENTGLASS_METRICS_ADDR` optionally opens a separate private listener exposing
only `/metrics`; the Kubernetes baseline binds it to port 9090 and permits only
the monitoring namespace. Do not route this listener through the public Service.

The `run` command starts the API and/or publication worker roles against the
exactly migrated PostgreSQL schema and matching S3 identity. Configure
`EVENTGLASS_DATABASE_URL`, `EVENTGLASS_PUBLIC_URL`, `EVENTGLASS_ROLES`,
`EVENTGLASS_SCRATCH_DIR`, `EVENTGLASS_S3_REGION`, `EVENTGLASS_S3_BUCKET`, and
optional endpoint/prefix/path-style settings. The API role also requires
`EVENTGLASS_AUTH_HASH_KEY_FILE`, whose file contains one random 32-byte value as
64 lowercase hexadecimal characters, and `EVENTGLASS_TOKEN_KEY_FILE` with an
independent value in the same format for signed read/cursor tokens. API and
worker roles additionally require `EVENTGLASS_ALERT_ENCRYPTION_KEY_FILE` with a
third independent value in the same format; PostgreSQL stores only its key ID
and AES-256-GCM ciphertext for destination secrets. A fresh installation additionally uses
`EVENTGLASS_BOOTSTRAP_TOKEN_FILE` in the same format; startup stores only its
hash, exposes the recoverable setup surface, and keeps readiness and ingestion
closed until setup commits. Production management cookies require HTTPS.
`EVENTGLASS_INSECURE_COOKIE=true` is accepted only with an explicit loopback HTTP
public URL for local development. AWS credentials use the default SDK chain.
The scheduler role runs retention and alert evaluation. The worker role runs
publication, queries, and fenced at-least-once webhook delivery. Production
deployment remains a later gate.

`./scripts/check crash` cross-compiles the crash and ingress-resource tests for
Linux ARM64, then runs them with CPU1/512MiB/no-swap limits against disposable
PostgreSQL and MinIO. It exercises test-only inherited IPC barriers and SIGKILL;
no failpoint is exposed by the product HTTP server or runtime environment.
