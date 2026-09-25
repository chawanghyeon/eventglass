# Eventglass

Eventglass is being rebuilt as a Go product using PostgreSQL, S3-compatible object storage, and an isolated DuckDB analytics child. The Go module, implementation, tests, and pinned build environment live at the repository root.

The former Rust product is no longer present on `main`. Its final Rust-only state is preserved by the repository tag `rust-version` (`ceb2ed7`) and can be checked out independently if needed.

Current status: U4, M1–M4, R1 and R2 are implemented with scoped tests. M3 connects the bounded Range/block cache to real query reads; M4 verifies coordinated PostgreSQL base/WAL plus S3 recovery. Fresh local self-host base/WAL+S3 restore, missing-object fail-closed, ARM64 browser E2E and pinned live SDK tests pass; physical GC still requires a fresh authorized backup verification. R3 is the first incomplete packet. The latest current-source ARM64 official 5m/30m/10m profiles completed at1/2/4 workers on `a3a0bca-dirty`, with full analytics/payload HEAD verification and the corrected combined-work backlog gate. Each accepted220,500 records with zero query failures/conflicts; rows/histogram p95 were603/1,361ms,294/513ms and289/569ms. The1-worker run also failed combined-backlog and post-load idle-backlog checks;2/4 workers failed only the histogram p95 target. All passed ACK, visibility, Go-unit limits and OOM checks; the independent native oracle passed, though its10M cgroup peak was4KiB above the configured limit. These runs do not pass G07; different submitted-envelope hashes prevent treating worker-count deltas as causal scaling comparisons. Reports, S3/WAL traffic, measured resources and run-specific cost projections are in [quality evidence](docs/observe/quality.md). R4 remains incomplete: the scoped AWS scratch-prefix restore path is implemented but unverified without AWS CLI/identity; signed ARM64 release provenance, image/dependency security and license scans, and rollback evidence are also open. No production release is claimed. [Capability coverage](api/capabilities.json) distinguishes registered APIs from UI and pending features; [work plan](docs/implementation/work-plan.md) owns packet status. See [DESIGN.md](DESIGN.md), [SDK-SUPPORT.md](SDK-SUPPORT.md), and [CONTRIBUTING.md](CONTRIBUTING.md) for behavior, verified SDK scope and checks.

The comparison backlog gate was corrected to include queued/running/prepared maintenance tasks and reserved inputs. Older historical1/2/4-worker figures under-counted total durable work; the latest official profiles above use the corrected gate.

An earlier corrected current-source one-worker official rerun (5m/30m/10m) accepted220,500 records but failed rows/histogram p95 (689/1,638ms) and combined backlog slope (+16.652/min). It drained to zero and passed ACK, visibility, resource/OOM, exact-count and post-load checks. It is superseded for current gate status by the later1/2/4 matrix above; its S3/WAL/cost/resource and compaction-queue evidence remains in [quality evidence](docs/observe/quality.md).

A subsequent official one-worker1024-file-cap experiment regressed rows/histogram p95 to6,332/6,614ms, visibility p95 to841,445ms and combined backlog slope to+623.420/min;18,638 items remained after the10-minute drain. ACK, resource/OOM and accepted-count checks passed, but publication, maintenance progress and post-load gates failed. The experimental cap is rejected; the production limit remains256. R3 is still incomplete. Full evidence and cost/resource measurements are in [quality evidence](docs/observe/quality.md).

A separate official catalog metadata-fanout16 experiment is also rejected; production fanout remains8. Its one-worker report accepted220,500 records but measured query p95 7,904ms, visibility p95 849,387ms, combined backlog growth+637.215/min, and18,770 outstanding items after600,123ms drain; three queries failed. It had no cgroup OOM. The two-worker run stopped before report serialization because its load schedule fell8.46s behind, and the four-worker run stopped before report serialization on HTTP429 `admission_limited` at input sequence425. These failed/incomplete runs do not establish causality; R3 remains open. See the detailed evidence and limits in [quality evidence](docs/observe/quality.md).

A later one-worker `20s/300s/90s` catalog-page diagnostic compared the retained256-file bound with a1,024-file candidate. Both accepted33,600 records, but rows/histogram p95 changed538/1,420ms→1,811/3,043ms and combined backlog slope+84.425→+266.422/min. The candidate was rejected; production remains256 and R3 remains incomplete. This short comparison is not an official SLO pass; see [quality evidence](docs/observe/quality.md).

The attached source brief at [`docs/observe/source-design.md`](docs/observe/source-design.md) remains byte-identical. Corrections and the Go architecture are documented separately.

Read [`ARCHITECTURE.md`](ARCHITECTURE.md) for package ownership, request/batch/file
boundaries, resource lifetimes, fencing and upgrade rules. Architecture checks,
multi-request streaming journals, shared byte permits and the ingestion schema
receipt/index policy, the I2 Accept transaction, the I3 verified upload and
rebatching path, and the I4 durable API runtime/lease primitives are implemented
foundations. I5 proves process-crash ACK recovery with empty local scratch; P2
adds verified staging and the isolated DuckDB 2.0 paired-Parquet child. P3 adds
durable output intents, paged Prepare metadata, ordered fenced Publish, Issue
application, and API/worker runtime execution. P4 proves Resolve-cut lifecycle,
multi-lane regression, SIGKILL recovery, scratch-independent publication, and
the initial ACK-to-visible/file-size distribution. Q1 adds hash-only
bootstrap/session authority, stable CSRF, current-grant reload, management key
lifecycle, and deterministic OpenAPI generation. Q2 adds bounded typed filter
IR, CEL and exact JSON adapters, canonical dataset hashing, mandatory snapshot
scope composition, and pinned DuckDB 2.0 semantic contracts. Q3 adds atomic
snapshot admission, revision-bound catalog manifests, monotonic retention floors,
and purpose-separated signed read/cursor tokens. Q4 adds durable fenced query
tasks, deterministic fan-in reduction, isolated native execution, bounded global
Top-K, exact integer aggregation, and histogram completion. Q5 exposes strict
rows, aggregate, detail, snapshot and async-job routes; sync and background work
share durable claims/fences and one native-child gate, while result decoding is
bounded and token-authorized. U1 adds the generated-contract React operator
shell, login/setup, logs/detail and Explore rows+histogram, plus isolated
project/Issue component fixtures,
with URL/server/form state separated and exact Int64 display. U2 adds received-time
Live polling over fresh snapshots, signed per-lane resume checkpoints, bounded
SSE admission, and a reconnecting deduplicating operator view. A1 adds
revisioned Issue/threshold rules, complete-window rule-principal queries,
cooldowns, encrypted destination secrets, and durable delivery outbox creation.
A2 adds generation- and lease-fenced delivery claims, exact-body HMAC signing,
bounded retries, DNS/IP revalidation with pinned connections, and authorized
delivery inspection/manual retry. U3 connects project/key and Issue/occurrence
state, adds SDK outcome and alert/system inspection, and proves an SDK envelope
through durable ACK, publication, safe detail rendering and diagnostics in
Chromium. M1 adds pressure-aware bounded compaction reservations, paired-file
identity verification in the pinned DuckDB 2.0 child, fenced output intents,
and an atomic generation swap that preserves old readers without blocking
unrelated publication. M2 adds received-time retention rewrites and full expiry,
snapshot- and backup-interlocked mark/delete/confirm GC, journal retirement summaries,
an eight-day recovery grace, and late-PUT resweeps. Physical deletion requires an unexpired verified backup horizon; it does not
interpret an empty backup inventory as safety. A real durable-ingest/native-retention
regression also verifies the one-bundle rewrite boundary, complete expiry and
old paired-file snapshot reads. M3's block-cache gate and M4's coordinated
PostgreSQL/WAL/S3 recovery gate pass on Linux ARM64; sustained
resource and provider release evidence remain open.
Pure-Go checks do not rebuild DuckDB.

Scoped R3 optimizations prune proven first-page row scans after full catalog
verification and eliminate duplicate histogram scans when filling empty buckets.
Bounded compaction-prefix ranking avoids lexical error/log priority. The latest
matched short diagnostic measures rows/histogram p95=341/949→315/714ms and fewer
S3 HEADs, but histogram still misses500ms; RSS/full-GET bytes and post-load
cold/warm latency increase. A later joined-cleanup/accounting correction measures
416/949ms; it adds full replacement-worker maintenance verification and removes
compilation from startup/measurement. Histogram still fails, so this is not a
performance improvement. A subsequent bounded warm-input transport reduces a
matched256-file native scan111.6→62.3ms and short-service rows/histogram416/949
→295/566ms. Histogram still misses500ms; RSS and some S3 transfer costs increase.
At that earlier checkpoint, the clean02c98f2 official one-worker run failed both query targets:
rows/histogram p95=591/1,737ms, despite exact220,500 public records, final backlog0,
passing maintenance accounting and OOM0. The runner exited1 before2/4 workers;
those profiles were unexecuted then. See quality.md for retained native-oracle
and service evidence. Later corrected full1/2/4 profiles are summarized at the
top of this README; R3 remains incomplete.
Two separate maximum-pair real-dispatcher runs pass with zero OOM or
budget overrun. These scoped results do not close G07 or establish a release.
The next bounded maintenance-download change reduces actual same-input S3
download time but increases benchmark RSS. Its matched short service profile
measures303/548ms versus295/566ms; histogram still fails500ms. No official SLO
pass or broad cost/memory improvement is inferred from that small difference.
Size-cohort compaction avoids repeatedly rewriting a quiet large pair with tiny
arrivals. Actual mixed-size workflow median149.0→132.0ms and GET2.48MB→92KB
are scoped savings; the short service306/558ms still fails histogram500ms and
does not improve on303/548ms. Quiet cohorts may retain more files. R3 stays open.
An app-owned conversion cursor fixes repeated older-tenant selection after work
finishes. Real single-worker PG/S3/native tests preserve all IDs and rotate ready
tenants. A separate query cursor and capacity filter also pass actual A/B/A
search execution, exact results, cross-tenant denial and concurrent task caps.
Actual1/2/4-process queued-work tests now pass three repetitions, preserving
all IDs and checking each worker's participation and tenant claim-count skew.
This is not elapsed-time fairness or per-replica throughput evidence. The earlier
clean04ec2c2 official profile measured404/874ms versus02c98f2's591/1,737ms.
The latest full-verification1/2/4 profiles are summarized at the top of this
README and in quality.md: the1-worker run also misses visibility/backlog/drain,
the2-worker run misses both query targets, and4 workers miss histogram p95.
R3 remains open; R4 also remains open pending external restore and release
evidence.

Implementation handoff: [`docs/implementation/README.md`](docs/implementation/README.md)
contains the complete v1 design reading map; the
[`work plan`](docs/implementation/work-plan.md) breaks remaining work into ordered
packets with files, prerequisites, negative tests and completion gates. These are
design contracts, not evidence that R1–R4 have been implemented or verified.

The handoff also includes [cross-boundary correctness contracts](docs/implementation/correctness.md)
and [fixed machine-readable examples](docs/implementation/contract-cases.json)
for restart recovery, exact arithmetic, sessions, retention and bounded queries.
