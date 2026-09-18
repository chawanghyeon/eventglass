# Eventglass Go: Sentry-compatible, S3-centered error and log analytics

Date: 2026-09-18. Status: **implementation specification**, not an implemented or performance-verified product.

This is the normative handoff for a new agent, independent of conversation history or implicit Rust behavior. MUST/prohibitions are correctness contracts; initial values are tunable policies. Close unverified library assumptions through section22 gates, not by silently weakening contracts. Keep Go documentation in English.

## 0. Decisions and scope

### 0.1 Final decisions

- Implement the product and coordination layer ourselves in Go; do not place Quickwit/OpenObserve/ClickHouse behind it.
- Use embedded DuckDB execution, immutable Parquet/Zstd, S3-compatible object storage, and PostgreSQL.
- DuckDB is an analytics executor, not transactional metadata storage, a distributed scheduler, or a shared `.duckdb` file.
- Required search is structured filtering, literal substring/regex, and exact aggregation. Inverted full-text indexing, BM25, fuzzy matching, stemming, and language analyzers are not required in v1.
- Reuse a CEL parser for the restricted filter language. SDK ingestion compatibility does not imply Sentry UI or Tantivy query-syntax compatibility.
- S3 is authoritative for event bytes; PostgreSQL for accepted receipts, published file sets, permissions, and mutable Issue state. Recovery needs both.
- Small/distributed installations share data contracts. Worker scaling must not require shard ownership transfer, data movement, or reindexing.
- One vCPU/512MiB is an API or worker execution-unit target, not the whole installation. Count PG, object storage, and host overhead in total cost.

Stack: standard Go net/http and slog, pgx/v5, AWS SDK for Go v2 (S3), official duckdb-go/v2, cel-go, Prometheus client. UI: React/TypeScript/Vite, TanStack Query, generated OpenAPI DTOs, Vitest/Playwright. Version SQL migrations and API schemas; reuse drivers/codecs/cryptography. G00 establishes tested patches and image digests.

Known tradeoffs: broad substring/regex scans can read more S3 bytes than inverted indexes. PG+S3 add minimum cost/dependencies versus a single server. Pruning, batching, caches, and compaction reduce cost; superiority remains unproven until section21. Never promise instant arbitrary all-history queries on one512MiB worker.

### 0.2 Paths and preserved baselines

New root: `go/eventglass/`; planned module `eventglass/go/eventglass`, binary `eventglass-go`. Preserve `rust/` and `go/benchmark/`. The design-time behavioral baseline was commit `9b8c7c0`, before the layout migration. Record actual revisions and dirty state for comparisons.

Original attachment `docs/observe/source-design.md` MUST retain SHA-256:

```text
4cccddc98f76d5c38099958587386c55756a08cf366c26ed9e65eeedec0526e5
```

SQLite/sequential Indexer/Tantivy remain Rust choices; this document replaces them only for Go. Search semantics and S3 ACK cost differ, so these are not language-only benchmarks.

### 0.3 Required v1 scope

Include Error/default Event, Structured Log, client-report diagnostics, basic transaction preservation/related logs, SDK frame display, Issue grouping/resolve/ignore/regression, project/key/user authorization, search/detail/histogram/group-by, Live, alerts, retention/automatic compaction/recovery/autoscaling metrics, UI, and real SDK verification.

A transaction becomes one `kind=transaction` row with nested spans retained in detail, not separate log/Issue rows. Trace correlation is supported, not complete APM waterfalls/sampling/performance-product compatibility.

Explicitly unsupported: Replay, profiles, metrics, sessions/release health, check-ins/cron, feedback, native minidumps, arbitrary attachments, artifact/source-map uploads, symbolication. Mixed envelopes ingest valid supported items and diagnose unsupported type/count/bytes. Do not secretly retain unsupported bytes or convert them into errors/logs. Go v1 does not inherit Rust's Replay/Feedback UI.

Compatibility means passing fixtures for **declared versions/items/behavior**, not every Sentry feature or future SDK. Include enable/flush/transport settings in support documentation.

## 1. Components and ownership

```text
SDK -> API(normalize/scrub) -> S3 journal -> PG Accept -> HTTP ACK
                                         | jobs
                                  converter supervisor
                                         | DuckDB child
                                 S3 Parquet bundle
                                         | PG Publish
UI -> API(scope/plan) -> PG snapshot -> workers(DuckDB) -> reducer -> response
                                         |
                                compact / retain / repair
```

| Component | Owned state | Durable? |
|---|---|---|
| API | Bounded ingress buffers, auth, query plans | No |
| Worker supervisor | Lease heartbeat, downloads, outputs | No |
| DuckDB child | One task's DB/cache/spill/execution | No |
| Scheduler | Periodic inspection/scheduling | Replaceable through leases |
| PostgreSQL | Catalog, receipts, jobs, Issues, auth, outbox | Authoritative |
| S3 | Sanitized journals, queryable bundles, recovery artifacts | Authoritative |

Small installations use `run --roles=api,worker,scheduler`; distributed deployments separate roles. Native execution belongs only in the binary's `engine-child` subcommand. Measure supervisor+child in one cgroup. Never share one DuckDB file across workers.

Dependencies: HTTP -> operations -> domain/storage/query. Concrete operations own transactions outside handlers. Interfaces belong at real boundaries (Clock/ObjectStore/EngineProcess), not speculative generic Repository/Service layers.

```text
cmd/eventglass-go/       CLI and child entry point
internal/app/           Role assembly and shutdown
internal/ingest/        Framing, normalization, scrub, accept
internal/sdk/           Wire DTOs and version adapters
internal/control/       pgx SQL, transactions, jobs, catalog
internal/model/         Canonical records, IDs, typed attributes
internal/issues/        Pure grouping/lifecycle calculations
internal/query/         CEL allowlist, plans, SQL, scope, snapshots, merge
internal/engine/        Child, conversion, projection, execution
internal/storage/       S3, cache, inventory, verified downloads
internal/maintenance/   Compaction, retention, GC, backups
internal/alerts/        Evaluation, outbox, delivery
internal/api/           OpenAPI DTOs, auth, HTTP, SSE
migrations/             PostgreSQL SQL migrations
web/                    Independent UI and generated DTOs
tests/                  Contracts, SDK, integration, crash, resources, comparison
deploy/                 Local/cluster configs and version/digest locks
scripts/                Development/verification entry points
```

## 2. SDK findings and policy

Inspected: Python2.69.0, Node/Browser JavaScript10.73.0, Go v0.49.0, matching existing fixture locks. [SDK-SOURCES.md](SDK-SOURCES.md) lists pinned commits and evidence. No new Go product SDK tests were executed for this design.

### 2.1 Semantic distinctions

| Input | Canonical kind | Issue? | Meaning |
|---|---|---|---|
| event with/without exception | error | Yes | Includes captureMessage |
| log.items[] | log | No | Even error/fatal remains a log |
| Event breadcrumbs | Parent detail | No | Never duplicate into rows |
| transaction | transaction | No | Basic trace-related storage, not APM |
| client_report | SDK outcome | No | Best-effort pre-transmission loss |
| Unsupported item | Unsupported outcome | No | Validate framing; discard bytes |

One Python logging call can produce breadcrumb+log+event; do not merge them by message/time hash. Server code cannot recover SDK hook/sampling/buffer losses.

### 2.2 Wire differences

| SDK | Container | Severity/trace/template |
|---|---|---|
| Python2.69.0 | log, version2, items, item_count | Typed sentry.severity_number/text; epoch seconds; optional trace_id/span_id |
| JavaScript10.73.0 | version2; optional browser ingest_settings | Top-level severity_number/trace_id; sentry.trace.parent_span_id; sentry.message.parameter.N |
| Go0.49.0 | Versionless items exists | RFC3339 time.Time; top-level severity_number/span_id; plural sentry.message.parameters.N exists |

Use distinct absent-version, version1, version2 adapters. Verify version1 against historical fixtures in G01 rather than guessing. Unknown numeric versions return400. Accept observed Go wrapper metadata; do not invent items-less logs wrappers or single-log formats.

Severity precedence: valid top-level number -> typed sentry.severity_number -> representative level: trace1/debug5/info9/warning13/error17/fatal21. Valid range1..24; preserve invalid originals with warnings and fall back. Preserve original level separately. Normalize warn->warning and critical->fatal; unknown values remain unknown. Missing event level defaults error; missing log level is unknown. Retain display level on conflict and diagnose it.

Logs use top-level trace_id/span_id, with sentry.trace.parent_span_id only a span fallback. Events/transactions use contexts.trace. Never copy envelope trace to all logs. Normalize valid32/16-hex IDs to lowercase; zero IDs mean unset. Retain invalid IDs in raw with warnings/null projections without rejecting valid records.

## 3. HTTP/envelope contract

### 3.1 Endpoints and auth

- POST `/api/{project_id}/envelope/`; legacy event POST `/api/{project_id}/store/`.
- Require X-Sentry-Auth, query sentry_key/sentry_version, or envelope dsn. Multiple sources must agree on public key/project; DSN secrets grant no extra authority.
- DSN is identity only: never fetch/proxy its host. Path project must match authenticated DB project.
- Accept revalidates project/key/scrub revision. Later revocation does not undo ACKed data.
- CORS: project origin allowlist, Vary: Origin, necessary headers only, no ingestion cookies. Expose Retry-After, X-Sentry-Rate-Limits, X-Eventglass-Receipt.
- Public DSN grants ingestion only, not search/management/artifact upload.

### 3.2 Framing and limits

Parse UTF-8 JSON headers with LF boundaries. Item length is bytes: consume exactly length then LF/EOF; without length, consume until LF/EOF. Skip unsupported binary items by length. Final LF is optional; validate unsupported framing too.

Duplicate keys in supported JSON/headers return400. Invalid UTF-8 returns400; escaped lone surrogates normalize to U+FFFD with warnings. Event and transaction combined are limited to one per envelope; duplicates/both return400. Multiple logs plus one event are valid. Header-only is valid empty input; trailing bytes without item headers are invalid.

| Product hard limit | Initial value |
|---|---:|
| Wire / decompressed body | 20MiB each |
| Total canonical request | 20MiB |
| Canonical event/log | 1MiB |
| Envelope/item header | 16KiB each |
| Items / canonical records | 1,000 / 10,000 |
| JSON depth / nodes per record | 64 / 20,000 |
| Typed attrs per log | 1,000 |
| Concurrent decoders/API | 2 plus byte admission |

These are product limits, not claims about Sentry SaaS. Never load an unbounded request DOM. Only scrubbed parsed records may enter bounded disk spools; pre-scrub bytes must never reach disk/log/S3. A last-item validation failure rejects the entire supported request; staged objects remain unaccepted orphans.

Target identity/gzip/deflate(zlib)/br/zstd, each bounded and disabled until G01 real transport fixtures pass. Unknown Content-Encoding415; expansion limit413. Legacy store base64+zlib is a separate tested adapter; do not sniff/guess compression.

### 3.3 Handling and responses

Invalid supported items reject the whole request with400/413 and zero records. Unknown items are framing-validated, skipped, and diagnosed by type/count/bytes: this is not a forwarding Relay.

After commit, envelope returns200 `{}` with receipt; store returns200 `{"id":"..."}`. Empty/unsupported-only input returns200 plus X-Eventglass-Unsupported-Items and UI diagnostics, not a claim of feature support. Persist client-report outcomes before200.

Errors:400 malformed,401 key,403 disabled project,413 limit,415 encoding,429 quota/admission,503 dependency unavailable. Sanitize reason codes. Include Retry-After and X-Sentry-Rate-Limits on429; whole-request rejection uses empty categories/all. Distinguish log type from log_item/log_byte categories. SDK429 typically drops subsequent data, not durably retries it; neither429 nor503 guarantees retransmission. Maintain warm ingress/burst budgets.

## 4. Normalization, scrubbing, identity

### 4.1 Time and numbers

Use json.Number, never float64 for integer decoding. Accept RFC3339 or decimal epoch seconds. Integer arithmetic yields UTC microseconds plus0..999ns remainder; negative timestamps use floor division. Keep original representation and sub-microsecond precision in raw.

Missing timestamp uses arrival_time_us with timestamp_source=arrival. Invalid supplied time rejects the record. Preserve sent_at separately without clock correction. Diagnose skew, do not silently discard old/future events.

Under lane lock, Accept assigns the batch received_time_us=max(DB clock,last_received_time_us), updating the lane. Journal stores arrival; converter overlays committed received time/seq from receipts. Retention uses received time. Clamp backward DB-clock movement per lane and diagnose it. Section15's barrier prevents late acceptance into closed alert windows.

Seq/count use signed64. Typed integers remain exact through DECIMAL(38,0); larger integers become big_integer with original bytes and explicit exclusion from numeric aggregates. Reject non-JSON NaN/Infinity. Preserve declared double even when integral. UI64-bit IDs/counts/decimal integers are strings.

### 4.2 Attributes and promotion

Unwrap `{type,value,unit?}` preserving type. Support integer/string/boolean/double/array; distinguish null/missing/empty array/string "null". Preserve element types including heterogeneous arrays. Retain mismatches as invalid with diagnostics, not a valid typed projection.

Namespaces: attributes/tags/extra/contexts/user/request/sdk. Paths use RFC6901: `/a.b` differs from `/a/b`, array index `/a/0`; object order is irrelevant. Do not create unlimited dynamic Parquet columns.

| Column | Log | Event/transaction |
|---|---|---|
| message | body | logentry.formatted -> message string/interface formatted -> exception type/value -> logentry.message -> empty |
| message_template | sentry.message.template | Unformatted logentry.message |
| release/environment | sentry.* -> corresponding attribute | Top-level |
| service | service.name -> project configured service | tags service.name -> contexts service.name -> project service |
| logger | logger.name -> logger | logger |
| sdk_name/version | sentry.sdk.* -> envelope sdk | event sdk -> envelope sdk |
| trace/span | Section2.2 | contexts.trace |
| server_name | server.address -> sentry.server.address -> server.name | server_name |

Fallback only on missing, not empty. Preserve unknown properties in scrubbed raw. Retain parameter.* and parameters.* keys; normalize only the analytical view. Never reinterpret SDK-stringified objects as JSON objects.

### 4.3 Scrubbing

Apply project/global rules in one traversal: authorization/cookie/password/token/secret keys, header pairs, URL queries, nested attrs, request body, stack vars, breadcrumbs, user data, typed values, and derived message/template. Bounded RE2 handles body rules; do not promise discovery of every prose secret.

Create projections, Issue title/fingerprint, and checksums after scrub. Ignore IP/agent inference requests in v1 with inference_disabled diagnostics. Never log DSN/header/raw request values.

### 4.4 Identity/dedupe

- acceptance_id is UUIDv4 per validated HTTP request, stable for internal retries, new for another HTTP request.
- Envelope event_id overrides payload event_id; diagnose conflict, normalize32hex/hyphenated UUID to lowercase32hex.
- Missing source ID derives from acceptance/item ordinal without masquerading as an SDK ID.
- record_id is domain-separated, length-prefixed SHA-256 over `(project,kind,event_id)` or `(project,acceptance_id,item_ordinal,record_ordinal)`.
- Logs have no assumed standard event_id; never dedupe by envelope event_id, JS sequence, trace/span, or message hash.
- Accept claims source IDs by project+kind; first committed payload wins. Conflicting content never overwrites. Duplicates ACK with zero new rows/Issue count.
- Retain dedupe for event retention+7 days; later retransmission may be new. No exactly-once claim for ID-less logs across HTTP requests.

## 5. Durable records and Parquet schema

Separate wire/canonical/analytics DTOs. Journal canonical JSON is versioned internal format, not raw envelopes. Persist normalizer/grouping/scrub/schema versions; retries do not recalculate them differently.

### 5.1 Analytics rows

| Columns | DuckDB type/meaning |
|---|---|
| tenant_id, project_id | BIGINT mandatory scope |
| record_id, acceptance_id, batch_id | VARCHAR hex/UUID |
| lane_id, batch_seq, record_ordinal | INTEGER, BIGINT, INTEGER |
| kind | VARCHAR error/log/transaction |
| event_time_us, arrival_time_us, received_time_us | BIGINT UTC microseconds |
| event_time_ns_remainder | USMALLINT0..999 |
| source_event_id, trace_id, span_id | Nullable validated VARCHAR |
| level, original_level, severity_number | VARCHAR, VARCHAR, nullable SMALLINT |
| message, message_template | Nullable scrubbed VARCHAR |
| service, environment, release, logger | Nullable VARCHAR |
| sdk_name, sdk_version, platform, server_name | Nullable VARCHAR |
| issue_id, exception_type, exception_value, handled | Nullable projections |
| attrs | STRUCT[] below |
| search_values | VARCHAR[] scrubbed searchable scalars |
| schema_version, normalizer_version, grouping_version | INTEGER |
| warnings | Bounded VARCHAR[] codes |

attrs element: namespace VARCHAR, path VARCHAR, value_type VARCHAR, string_value VARCHAR, integer_value DECIMAL(38,0), double_value DOUBLE, boolean_value BOOLEAN, json_value VARCHAR, unit VARCHAR. Set only the appropriate scalar slot; object/array/null/invalid/big_integer use type+json_value. namespace+path is unique per row. Preserve containers and scalar children without multiplying counts during array membership/grouping.

search_values includes message, exception, breadcrumb messages, and queryable namespace scalar values, not key names/secrets/JSON escaping. Match within one scalar, never across concatenation boundaries. No silent64KiB truncation; fail413/explicitly if full projection cannot satisfy limits.

### 5.2 Payload/bundles

payload.parquet contains record_id/raw_json/envelope_sdk_json/normalization_warnings_json. Raw is scrubbed original shape, not the sole canonical reconstruction source. Publish analytics+payload atomically. Lists/counts never read payload; detail locates record_id in catalog-selected bundles.

Initial Zstd3, suitable dictionaries, row-group target16,384 and8MiB canonical processing flush. Verify actual writer behavior in G00. Compressed file target32–64MiB, hard target128MiB; low-volume time flush must not wait indefinitely. Partition `(tenant,lane,event UTC day,kind)`, sort `(project_id,service,event_time_us,record_id)`. Wide-time input spills/repartitions; no unbounded per-project writers. Late events use their real day. Catalog both event/receipt time bounds.

## 6. Journal and batching

Microbatch first of4MiB canonical,1,000 records,100ms oldest wait. Never split requests; larger legal requests get a dedicated batch up to20MiB/10,000 records. Enforce64MiB ingress admission and spool quota. Journal is Zstd JSONL with format/normalizer/schema/scrub header, request headers, canonical records, ordinal ranges/counts/checksums. Validate replay; do not renormalize SDK envelopes.

Create16 virtual lanes per tenant; request lane=hash(acceptance_id) modulo lane_count. Batch tenant+lane, allocate buffers on demand. Lane count is versioned topology, not worker count; v1 does not auto-change it. Any worker processes any lane.

Allocate seq via transactional lane counter, not nextval/time/UUID. Rollbacks leave no committed holes. No total order across lanes. Keys: `v1/{installation}/journals/{tenant}/{lane}/{batch_uuid}.jsonl.zst`; server-generated only, no credentials/user paths.

## 7. PostgreSQL and transactions

Initial support PostgreSQL17; G00 pins actual patch/digest. synchronous_commit=on. HA needs synchronous replication for ACK-safe failover; asynchronous replicas are not RPO0.

### 7.1 Logical tables

| Table | Required identity/state |
|---|---|
| installations | Singleton ID, storage_generation, schema/topology versions |
| tenants/projects/project_keys | State/auth_revision/scrub_revision/key hash/config |
| users/memberships/sessions | Scoped auth, hash-only tokens |
| lanes | PK(tenant,lane), accepted_seq/published_seq/catalog_generation/last_received_time_us |
| object_intents | Object PK, unique server key, kind/state/owner/fence/expiry/bytes/checksum |
| ingest_batches | PK(tenant,lane,seq), unique UUID, journal FK, count/selection/state |
| receipts | Unique acceptance_id, batch FK, ordinal range, accepted/duplicate/unsupported counts |
| event_dedupe | UNIQUE(project,kind,source_event_id), record_id/receipt/hash/expiry |
| jobs | PK, UNIQUE(kind,input_identity), state/attempt/fence/owner/lease/retry/error |
| job_outputs | Job+fence, objects/counts/checksums/prepared manifests |
| bundles/files | Scope, generation interval, schema/checksums/counts/time bounds/object refs |
| query_snapshots | Principal/scope hash, lane cut+generation vectors, TTL/heartbeat |
| issues | UNIQUE(project,grouping_version,fingerprint_hash), status/revision/count/first/last/resolved_cut |
| issue_occurrences | Unique record_id, Issue FK, lane/seq/ordinal/times/release/payload locator |
| alerts/evaluations/deliveries | Revision/cut/window, unique evaluation/delivery keys, state/retry |
| sdk_outcomes | Unique receipt/item ordinal, category/reason/count/approximate flag |
| schema_migrations | Unique version/checksum |

Scope FKs or operation checks must prevent cross-tenant references with negative tests. No raw records in giant receipt JSON; no PG row per ordinary log. Source-ID dedupe and Issue occurrences incur per-event cost and must be counted.

### 7.2 Object intents

Before upload, commit pending intent/key/expiry/fence. Verify uploaded size/checksum. Accept/Publish locks and validates the intent before referencing it. GC locks expired intents and sets deleting before S3 DELETE; deleting objects cannot publish. Retain tombstones/resweep because stale workers may PUT after deletion. Never adopt late uploads or delete live files solely from LIST. Quarantine unknown ownership.

### 7.3 Accept

After journal upload, one transaction:

1. Return matching existing receipt on internal retry, validating batch/content.
2. Lock project/key rows in stable order; recheck auth/scrub revision. Changed scrub means renormalization or internal409/503 retry, never stale ACK.
3. Lock lane; claim source IDs in stable order; persist winner/duplicate/conflict ordinal intervals/bitmap/checksum.
4. Increment accepted_seq, assign received time, insert batch/receipts/convert job/outcomes.
5. Reference intent, COMMIT, then durable ACK.

Conversion uses accepted selection, not every journal candidate. Lost replies do not undo commits. Batch only fully validated requests; accept atomically. Bounded DB retries preserve IDs. Assigned empty/duplicate-only sequences complete empty publication without Parquet; otherwise they would stall the lane. Empty requests without seq may commit diagnostics only.

### 7.4 Claim/prepare/publish

Short FOR UPDATE SKIP LOCKED claim increments fence and sets owner/60s DB-clock lease; heartbeat15s. No transaction throughout native execution. Disconnected/expired workers still need valid fencing.

Parallel conversion may prepare N+1 before N; publish only contiguous seq. Poison ACKed input visibly stalls its lane for repair, never silently skips; other lanes proceed.

Publish transaction validates receipt scope, locks lane/job/fence/intents/Issues in stable order, requires seq=published_seq+1, inserts unique occurrences and updates current Issue state, publishes all analytics/payload at one generation, completes input/job, inserts outbox, advances watermark, and commits. Identical completed output is no-op; conflicting output is error. Later key revoke/project disable cannot prevent already ACKed publication.

Measure10,000-row transaction cost without breaking atomicity. Common lock order: project/key -> lane -> event_dedupe -> job/intents -> Issues -> child rows; stable within each kind. FOR SHARE status checks permit concurrent Accept; config changes conflict exclusively. Claim/heartbeat/GC never acquire earlier locks afterward. Retry DB conflicts, not external transmissions inside transactions.

## 8. Snapshots, cursors, authorization

Use vectors Cut={(tenant,lane):published_seq} and Generations={(tenant,lane):catalog_generation}, not a scalar watermark. Accepted cut uses accepted_seq. Compare componentwise; do not invent global ordering for correctness.

Snapshot creation:

1. Verify current principal/project permissions. In PG REPEATABLE READ, acquire shared lane locks in stable order.
2. Read cut/generation vectors and register snapshot plus lane refs atomically against retention/GC's exclusive locks.
3. Select catalog files with valid_from_generation<=G<valid_to_generation, null end=infinity; prune by selected time basis/projects.
4. Supply identical scope/cut/manifests to workers/reducer and enforce tenant/project/cut at row level too.
5. Recheck auth revision before responding; revoke aborts responses. Streams recheck each batch/heartbeat.

TTL15min, heartbeat30s, maximum active extension1h. Reuse only existing live snapshot rows; tokens cannot resurrect retired generations. Rows/histogram/detail share a snapshot. Sort `(event_time_us DESC,event_time_ns_remainder DESC,record_id DESC)` with stable tuple cursor, never OFFSET. Received-time sorting is a separate enum.

HMAC token: version, installation generation, snapshot_id, principal/scope hash, normalized query hash, sort, cursor tuple, expiry. Protect secret; never expose it in URLs. Tamper/mismatch400, forbidden403, expired410, restore generation mismatch409. Detail validates snapshot and permissions.

Any file selection/download/SQL/merge failure fails the whole result. Progressive UI output must say complete=false and cannot serve as exact count/alert success.

## 9. Search expressions and semantics

### 9.1 Public input

Accept exactly one of typed JSON filter AST or expression. Reuse github.com/google/cel-go parser/type checker; do not use CEL evaluator for data execution. Lower only approved nodes into DuckDB. This is a filter adapter, not a general SQL compiler.

```text
level == "error" && contains(message, "connection refused")
(service == "api" || service == "worker") && severity_number >= 17
exists("attributes", "/http.response.status_code")
iattr("attributes", "/http.response.status_code") >= 500
sattr("tags", "/region") == "ap-northeast-2"
matches(message, "timeout|timed out")
text("database") && !text("healthcheck")
array_contains("attributes", "/features", "checkout")
```

Fixed fields: kind/level/severity_number/message/message_template/service/environment/release/logger/sdk_name/sdk_version/platform/server_name/trace_id/span_id/source_event_id/issue_id. Time/project scope is separately required.

- contains/starts_with/ends_with are literal, not LIKE wildcards; `%` and `_` remain literal.
- matches uses DuckDB RE2 search semantics; pattern<=1KiB, max4 regex/query, unsupported syntax400.
- text matches any one search_values scalar, not serialized raw JSON.
- sattr/iattr/dattr/battr(namespace,pointer) return only the requested type; missing/mismatch is absent. No implicit integer->double.
- exists includes explicit null; is_null matches only explicit null.
- array_contains uses typed literal equality and returns one Boolean per record, never multiplies rows.
- Allow fixed-field ==/!=/</<=/>/>=, string/number/bool literals, &&/||/!, parentheses, bounded literal `in [...]`.

Every ordinary comparison on missing/type-mismatch is false, including !=. Negation can therefore make missing true. Lower each comparison leaf through COALESCE(...,FALSE) before combining Boolean nodes; freeze with SQL golden tests.

Equality/substring are case-sensitive without Unicode normalization. Explicit icontains uses the pinned DuckDB lower semantics; golden-test Unicode. Regex (?i) follows RE2. No stemming/token-phrase/fuzzy/BM25. Spaces in quoted substrings are literal, not Tantivy phrase semantics.

Limit8KiB expression,128 AST nodes, depth16, literal lists100. Reject CEL macros/comprehensions/all/map/filter, arbitrary member/index access, user functions, duration math, dynamic paths, eval, SQL. JSON AST uses the same validator. Beyond-signed64 integers use tagged decimal-string DTO literals for iattr/DECIMAL bindings.

### 9.2 Shared SQL scope

Field/operator/function names come from fixed maps; bind values and validated namespace/pointers. Never interpolate user URLs/table functions/identifiers. Mandatory predicates enforce tenant/project, `[start,end)` time, and lane cutoff; OR true cannot bypass them. API/rows/aggregate/related/Live/alerts share one typed QueryPlan builder. No SQL endpoint. ATTACH/COPY/INSTALL/LOAD/external HTTP are reachable only by internal operations, never user AST.

## 10. Distributed query correctness

### 10.1 Tasks

Plan contains snapshot_id/scope/filter hash/schema/projection/sort/aggregate/explicit file manifests/estimated bytes/deadline. Workers cannot rediscover a different file set. Initial task target64MiB compressed or8files, max4 parallel tasks/snapshot; small queries stay single-worker. Unless verified native row-group selection exists, assign each file to exactly one partition. G00 must prove finer splitting before use.

query_tasks has UNIQUE(query_id,stage,partition_id), attempt/fence/output checksum. Reducer consumes one winning attempt per partition, never sums retries twice or accepts late canceled outputs. Large partials use internal S3 temporary objects under query TTL/GC.

### 10.2 Rows

Each task selects limit+1 using identical scope/filter/cursor/sort; reducer performs bounded stable k-way merge. Default limit100, maximum1,000. Local Top-K is safe for row ordering, not group rankings. Read list projections only; fetch raw payload for final detail.

### 10.3 Aggregates

Required count/sum/min/max/avg/histogram, max2 group dimensions and8 metrics. DISTINCT/percentile/join/arbitrary UDF return400 unsupported_operation. Future approximations need explicit names/errors/versions.

- Count: checked BIGINT, empty0. Integer sum: DECIMAL(38,0), overflow422. Double sum/avg must be finite, merge in stable order, oracle tolerance max(1e-9,abs(expected)*1e-9).
- Merge avg from sum+valid count, never average partial averages. All-null/missing numeric input yields null sum/min/max/avg. Expose valid numeric and excluded-type counts.
- Histogram uses UTC microseconds, `[start,end)`, floor(time/interval)*interval, tested negative epochs/boundaries. Declare empty-bucket policy in DTO.
- Group key includes namespace/path/type/value. String"1", integer1, double1.0, true, null, missing remain distinct.
- No array grouping in v1; array_contains is filtering only, no hidden Cartesian products.
- Emit all local groups, merge through DuckDB GROUP BY, then apply final Top-K. Never truncate local groups first.
- Initial maximum20,000 final groups and64MiB intermediate bytes/query. A task with20,001 distinct groups can fail immediately; shared groups across tasks count only after merging. Spill within budget; fail explicitly, never return partial exact counts.

## 11. S3 reads, cache, native isolation

Initial candidates: DuckDB1.5.5, official github.com/duckdb/duckdb-go/v2 v2.10505.0 from the official mapping. This is not a tested lock. G00 validates releases/checksums/Linux amd64+arm64/CGO/extension ABI. Do not use2.0 alpha. Start from the existing benchmark's Go1.26.5 and lock a verified patch.

Child defaults: threads=1, memory_limit=256MiB, max_temp_directory_size=2GiB, one query. Supervisor+child cgroup512MiB, CPU1, swap0. GOMEMLIMIT alone does not bound native memory. Treat OOM/segfault as child failure with bounded split/retry, then explicit error; no endless retries.

Bundle pinned json/parquet/httpfs and required extensions in the image; offline LOAD only, no runtime INSTALL/automatic downloads. Do not pass cloud credentials to the child.

Use supervisor localhost range gateway plus DuckDB httpfs. Opaque capability URLs expose only task-allowed objects; this is not a general proxy. Gateway uses manifest key/size/checksum for S3 ranges. G00 tests HEAD/GET/Range/Content-Range/206/416 and parameterized read_parquet file lists.

Manifests contain full SHA-256 and1MiB fixed-block hashes. Expand requested ranges to block boundaries, verify, then return requested bytes; check last-block length, ETag changes, short reads. ETag is not a content checksum. Count read amplification.

Disk LRU cache key=(installation,object_id,content_hash,block_index), single-flight misses, active pins, no user paths. Full-download fallback is an explicit experiment; accidental full GET in the default path fails G00. Combined cache/staging/spill/output disk budget4GiB, reserve512MiB. Evict unpinned data; stop new work if insufficient space. Cache affinity is a performance hint, never correctness ownership.

## 12. Conversion and compaction

Stream journal decode, apply accepted selection, bulk append through verified DuckDB API. Per-row INSERT/CGO is baseline only; initial append batch2,048 rows, reduced for large records by bytes. G00 verifies nested attrs/decimals.

Preserve record_id/lane/seq. Do not duplicate mutable Issue status/count in Parquet. Derive manifest statistics from actual output, never estimates used for pruning.

Compact only same tenant/lane/schema/event-day/kind. Reserve bounded input files/generation; verify analytics/payload identity sets via counts/hashes. CAS that reserved inputs are still current; atomically close their valid_to_generation and open replacements at valid_from_generation. Never retire unreserved late files. Compaction does not advance published_seq. Failed outputs are orphans while originals remain readable. Schedule by size/read demand/backlog/expected GET reduction, within maintenance budget; pause under ingest/query pressure, not indiscriminate periodic rewrites.

## 13. Detail and UI

Error detail displays exception chain/mechanism/handled, SDK frame order, in_app/context line/threads/breadcrumbs/release/environment/request/user/context/warnings. CaptureMessage without frames is valid. Distinguish event/received time. Transactions expose nested spans in detail/basic list, not log counts/span analytics. Logs expose typed attrs/unit/template/parameters/original severity/trace/span/SDK; do not reformat body from template.

Related logs use trace within authorized scope, otherwise explicit project+service+narrow-time fallback. Trace IDs grant no cross-project access. Preserve debug_meta and SDK frames without v1 symbolication.

Required screens: setup/login, projects/DSN/revoke, Issues/detail/status, Logs/Live, Explore/histogram, alert rules/deliveries, system ingestion/publication/SDK losses/unsupported/resources/storage/backups. SDK examples match pinned fixtures. Do not show unsupported features as working.

TanStack Query owns server state, URL owns search state, local state owns forms/views. Generate DTOs from OpenAPI; i64 strings, text-render raw/message (no HTML). Rows/histogram share read_token and absolute time range.

## 14. Issue grouping and lifecycle

Label grouping eventglass-grouping-v1, not bit-for-bit Sentry grouping. Representative exception is last exception.values entry; keep chain type order. From that exception, prefer last8 in_app=true frames, else last8 all frames, retaining SDK order. Default fingerprint uses chain types and `(module,function,filename)`, excluding line/column/addresses. Without frames use exception type+value; without exception use message_template then message. Normalize slashes only, no basename truncation/number stripping.

Explicit string[] fingerprint is preserved; expand {{ default }} / {{default}} with default components, empty array means default. Canonical array-based JSON has fixed field order/UTF-8/escaping golden bytes. Hash version+project+canonical group using SHA-256; lowercase full hash is issue_id, computable before Parquet. Publish upserts that ID, not a late auto-increment ID. Algorithm changes require a new grouping version, not silent reclassification.

Count unique error occurrences only. First/last use `(event_time_us,ns_remainder,record_id)` min/max and that record's release including null. Activity uses received time.

Resolve locks and captures all project lane accepted_seq in resolved_cut in one transaction. Accept/resolve lane locks define order. Publish reads current Issue state; only a unique occurrence beyond its lane's resolved_cut regresses. Previously ACKed backlog never reopens it. Ignored stays ignored. Issue row locks serialize multi-lane updates; regression delivery key uses transition revision. Accept dedupe and occurrence uniqueness separately guard retries.

## 15. Live, alerts, SDK loss

Live follows received-time plus lane-position vector, not event-time. Notifications are hints; polling must catch up without loss. Query through each new public cut and checkpoint scanned position per lane, even when match count is0. No global cross-lane order guarantee; distinguish UI sorting from resume completeness.

Initial SSE limits256KiB/connection,32/API, heartbeat15s; catch-up15min, max10,000 rows/10s then resync_required. Slow clients cannot hold leases indefinitely.

Alerts are new-Issue/regression or received-time threshold. Threshold window `[E-window,E)` with60s-aligned E. Reservation locks target lanes in order, verifies E<=DB now, captures accepted cut, and raises each last_received_time_us to at least E. Pending Accept holding a lock commits before the cut is captured; later Accept gets received>=E. Wait for every published lane to reach cut before querying. Do not interpret backlog as zero or use arrival-time to claim completeness. Process delayed windows through bounded ordered catch-up.

UNIQUE(alert_id,revision,E) evaluations. After complete query, transactionally recheck revision/cooldown and commit outbox/last_completed_E. Cooldown uses E. Failure/partial results do not advance it. Webhooks are at-least-once with stable delivery ID, timeout10s, response64KiB, no redirects, SSRF/DNS pinning, only configured destinations. Test localhost only. Retry network/408/429/5xx, max12 attempts, jitter5s..1h; no exactly-once external claim.

Client reports are approximate SDK losses. Dedupe within receipt/item, not unknowable cross-request retransmission. Report server rejects, SDK reports, accepted, and published separately, not an exact generated total.

## 16. Retention, GC, backup, disaster recovery

Default received-time retention30days; receipts/dedupe retention+7days. Old event-time alone cannot immediately delete fresh ACKed records. Rewrite mixed-retention files into new generations. Pin retention cutoff per snapshot so rows do not disappear midway.

Delete objects only if unreferenced by current catalog, active query/detail leases, recoverable PG backup/PITR window, intents/jobs, and past safety grace. Initial7-day PITR requires at least8-day retirement grace, **including journals**. Longer backups extend object protection and cost. Never apply independent S3 lifecycle deletion to live/recovery objects.

Use verified PG backup tooling (e.g. pgBackRest) for base backups+WAL in separate S3 prefix. Separate backup and live-delete permissions. Public SDK keys cannot read backups. Daily isolated restore rehearsal; expose backup success/WAL lag/restore age.

Worker loss retries expired leases; compute loss starts with empty cache. PG loss restores base/WAL to a cut, validates referenced journals/catalog, retries pending jobs. Never automatically adopt newer LIST-discovered objects. ACKs after the last recoverable PG point cannot be claimed fully restored just because S3 bytes exist. Distinguish backup RPO from HA replication RPO.

Restore increments storage_generation/fences, invalidates sessions/cursors, and pauses outgoing alerts until verified. Restore roles/scrub/outbox/dedupe too. Missing/corrupt objects cause unhealthy/repair, never empty success. Physical privacy purge overriding backups is out of v1; hiding UI rows is not permanent erasure.

## 17. Automatic operation and budgets

Required operator inputs: DB DSN, S3 endpoint/bucket/prefix, public URL, admin bootstrap, retention, resource maxima. Normal operation must not require manual shard/file/cache tuning; advanced overrides are diagnostic.

| Resource | Initial policy |
|---|---|
| API/worker | CPU1/512MiB, swap0 |
| Ingress | 64MiB,2 decoders/API |
| Native concurrency | 1 child task/worker |
| PG connections | Worker2, API8, scheduler2 |
| Global PG pool budget | 64; autoscaler cannot exceed |
| Lease/heartbeat | 60s/15s |
| Native deadline | Interactive30s, async5min |
| Snapshot | TTL15min, active max1h |
| Shutdown grace | 30s; distinguish ACK from task completion |

Scale from estimated work bytes/CPU, oldest age, service-rate EWMA, and query queue delay, not raw job count. Use initial service-rate priors to avoid zero-worker deadlock. Default warm ingress and interactive pool minimum1; maintenance may reach0.

Initial desired=ceil(queued_work/(target_drain_seconds*work_per_worker_second)), clamped by pool min/max. Oldest/SLO breaches independently signal scale-out. Initial drain targets ingest5s/query0.5s/maintenance300s; sample10s, scale out after2 samples, scale-in stability300s, reduce<=25% per step. Measure/tune but test oscillation and connection surges.

Slow PG/S3 or429/5xx triggers dependency circuit/admission, not runaway replicas. Fair queues budget tenant concurrency/bytes; maintenance uses at most20% spare capacity initially. Separate pools/priorities so expensive tenants cannot starve ingest. Scale-in drains claims, finishes or safely abandons tasks, then exits; SIGKILL lease recovery must work. No data transfer/reindexing for worker scaling. Account for cold-cache cost.

## 18. Deployment, versions, security

AWS: S3, managed PostgreSQL HA/PITR, EC2 containers. V1 deployment automation is **local Compose and Kubernetes/KEDA only**, with HPA/KEDA plus node provisioning as appropriate. ECS desired-capacity adapter is future scope, not a parallel v1 implementation. Self-hosted uses the same images/SQL with PG and S3-compatible storage. Compose scales processes but does not provide host HA or physical capacity provisioning; clusters autoscale within available/provisioned capacity. Count storage replication/disk/operations.

S3 contract: SigV4, configurable path/virtual-host style, GET/HEAD/Range/PUT, multipart complete/abort, paginated LIST/delete, TLS/CA, read-after-write. Handle provider ETag differences; correctness uses own SHA-256. Do not use S3 conditional writes as distributed leases.

Preserve existing pinned MinIO Rust fixtures and support that test path. Do not recommend archived community distributions as a new production default. Garage is the initial self-host candidate, gated on S3 contracts/release provenance/restore and exact digest. If it fails, do not silently call an alternative production-ready. Release requires actual AWS plus at least one self-host backend.

Linux amd64/arm64 first; pin CGO/native/extension ABI in images with SBOM/notices/checksums. No runtime package downloads. Non-root, readonly root filesystem, scoped scratch, localhost child gateway, private PG, narrow S3 prefix rights.

Use verified Argon2id with memory admission, random hash-only sessions, Secure/HttpOnly/SameSite cookies, CSRF, rate-limited login, one-time hash-only setup tokens. Workers are not public; only authenticated internal operations create jobs. Check scope in SQL and gateway allowlists.

Roles: tenant admin changes users/memberships/projects/keys/retention/scrub; project operator reads and changes Issue state/alerts in assigned projects; viewer reads only. Any unauthorized requested project returns403, not silent intersection. Global system diagnostics and alert destination configuration are admin-only. Mutations enforce CSRF and revision; never mix DSN keys with management sessions.

Environment: EVENTGLASS_DATABASE_URL, EVENTGLASS_S3_ENDPOINT(optional AWS), EVENTGLASS_S3_REGION, EVENTGLASS_S3_BUCKET, EVENTGLASS_S3_PREFIX, EVENTGLASS_PUBLIC_URL, EVENTGLASS_ROLES, EVENTGLASS_SCRATCH_DIR. Prefer AWS default credential chain/workload identity; self-host secrets via environment/mount. Never put real secrets in CLI/examples. DB owns installation/topology/schema; mismatched storage identity fails startup.

## 19. Minimum API

Use `/v1/`; Sentry paths remain section3. Generate/version OpenAPI at implementation start.

| Endpoint | Contract |
|---|---|
| GET /livez, /readyz | Liveness versus core dependency readiness; backlog is system status |
| POST /v1/setup, /v1/sessions | Bootstrap/login |
| /v1/projects, /v1/projects/{id}/keys | List/create/revoke; admin mutations |
| POST /v1/search | Scope, filter/expression, projection, limit, cursor/read_token |
| POST /v1/aggregate | Same scope/token plus metrics/group/histogram |
| GET /v1/records/{id} | Authorized snapshot or current-catalog detail |
| GET /v1/live | Scoped SSE and vector resume token |
| /v1/issues, /v1/issues/{id} | Query/detail/status; revision conflicts409 |
| /v1/alerts, /v1/deliveries | Rules/results/retries |
| GET /v1/query-jobs/{id} | Authorized owner/scope async status/result |
| DELETE /v1/query-jobs/{id} | Cancel/cleanup |
| GET /v1/system | Cuts/backlog/SDK losses/limits/storage/backups |

SearchRequest: project_ids[], absolute start/end, time_basis(event/received), kind[], expression xor filter, optional read_token/cursor, limit, mode(auto/sync/async). UI computes absolute default time. Auto may return202/query_id above interactive cost budget; sync fails explicitly on429/503/timeout, never partial200.

SearchResponse: rows/read_token/next_cursor/complete/stats(scanned_bytes,objects,cache_bytes,elapsed_ms,cut,visibility_lag)/warnings. Error: code/message/retryable/request_id, excluding SQL/keys/raw. All i64/count/decimal integers are strings; distinguish limits/unsupported from empty.

## 20. Observability and unproven targets

Metrics: supported/unsupported, SDK drops/server rejects, unique/duplicate acceptance, ACK latency, publication lag/oldest lane, converted/compacted bytes, S3 operations/bytes/retries, cache hits/scanned rows/bytes/result bytes, spill/RSS/OOM, PG transaction/deadlock/pool/WAL, attempts/fencing/incomplete queries, backup lag, GC. No high-cardinality project/record/query Prometheus labels; use bounded cost rollups. Never log query bodies/messages.

Initial validation targets, not achieved claims:

- One CPU1/512MiB worker sustains100 logs/s+5 errors/s with mixed queries for30min without growing backlog.
- S3 durable ACK p95<=500ms including network/batching; not equivalent to Rust local SQLite50ms cost/durability.
- ACK-to-visible p95<=5s; measure SDK batching separately.
- Warm project-scoped last15min rows/histogram p95<=500ms; report cold/all-history regex separately.
- Independent throughput grows at1/2/4 workers before shared saturation; report efficiency, not guaranteed linearity.
- OOM/ACK loss/silent omission/auth leaks fail. Correct resource rejection does not excuse missed throughput targets.

## 21. Rust comparison contract

Mode A compares normalization/query oracles; mode B compares actual end-to-end durability. Do not alter Rust to fit Go. Separate shared and exclusive features.

Use identical10k/100k/1m/10m seeds/SDK fixtures, IDs/times/sizes/cardinality/error ratio. Do not call Tantivy token/phrase and Go substring equivalent; separately report semantic differences. Compare exact shared filters/time/count against an independent oracle. Include retries/duplicates/ID-less logs/equal timestamps/late events/null/missing/dotted paths/mixed numeric types.

Build release first, then measure same Linux architecture/cgroups/CPU/RAM/swap/disk/network/cache/concurrency/duration. Report per-Go-unit512MiB and whole installation including PG/S3. Do not exclude new shared costs. Run5min warmup,30min ingest+mixed queries,10min drain;1/2/4 workers, SIGKILL/restart, idle, expensive regex, cold cache.

Cost includes compute seconds, storage/journal/backup/index GB-month, S3 PUT/GET/LIST, transfer, PG/PITR, self-host resources; record region/date/pricing parameters, no invented monthly price. Report revisions/dirty state/toolchains/locks/engine/exact semantics/dataset checksum/cgroup peak/OOM/latency percentiles/scans/network/PG bytes/completeness. Existing go/benchmark is SQLite FTS5, not DuckDB evidence. Compare record/result identities, not Rust scalar seq to Go vectors.

## 22. Implementation gates

Preserve earlier contracts at each gate. Record actual checks in commit bodies, not per-stage diaries. Do not implement successful stubs for missing checks.

### G00 — Isolation and engine/S3 contracts

Deliver independent go.mod/go.sum, pinned toolchain/native/extensions/images, CLI skeleton, scripts/check, Compose PG+S3, fixture manifest, baseline hashes. Verify driver Linux amd64/arm64, nested attrs/DECIMAL roundtrip, memory/spill/cancel/kill, bound read_parquet lists, real gateway Range/block hashes, Zstd/row-group metadata, remote permissions, offline startup, migrations/rollback/checksums. Separate localhost from actual AWS validation. Failure requires library/API investigation, not hand-written codecs/parsers or invented performance.

### G01 — SDK fixtures and normalization

Deliver adapters/canonical DTO/scrub/IDs/goldens/support report. Capture actual Python2.69.0, Node+Browser10.73.0, Go0.49.0 HTTP against localhost recorder and Go API. Existing Go fixture lacks structured logs: add real logger fixtures. Include compression/versionless/mixed event+logs/unknown binary/header precedence/CORS/flush/rate limits/client reports/fork/trace scope/large integers/arrays/unit/Unicode. Source inspection is not a passing test. Other languages stay unverified until pinned source+wire fixtures; close these four runtimes first without asking the user to prioritize again.

### G02 — Durable ACK and leases

Deliver intent/receipt/dedupe/lane/job SQL, Accept, byte admission, drain. Inject failures around upload/Accept commit/reply loss, concurrent source IDs, identical ID-less requests, revoke/scrub races, PG/S3 outage, GC/late PUT. Parent process records successful ACK oracle and compares restored data.

### G03 — Publication and Issues

Deliver converter/child/bundles/fenced Publish/grouping/resolve/outbox. Test out-of-order preparation, contiguous public cuts, poison batches, duplicate publication, payload mismatch, resolve/backlog/new error races, multi-lane Issues, received/event time, deletion of all local cache followed by restart.

### G04 — Search, snapshots, aggregation

Deliver CEL/JSON validator, SQL lowering, scope/snapshot/cursors, rows/detail/histogram/groups, tasks/reducer. Test independent expectations, types/missing/null/dotted/nested/Unicode/literal wildcard/regex errors, leaf NULL semantics, OR auth bypass/revoke, globally winning group outside local Top-K, weighted avg,20,000/20,001 groups, empty metrics, timeout/partial/retry duplication, pagination during compaction.

### G05 — UI, Live, alerts, diagnostics

Deliver independent OpenAPI/UI/screens/Live vectors/alert cuts/local webhook harness. Test browser ingestion-to-UI, no Issue from error-level logs, no breadcrumb rows, zero-match Live progress, slow clients/revoke, no false alerts from pending ACKs, HTML injection/CSRF.

### G06 — Compaction, retention, recovery

Deliver compaction/retention/GC/block cache/PG backup-PITR runbook and automatic checks/restore. Test snapshot registration versus GC, crashes around replacement, mixed retention rewrite, last-block checksum/late upload, full PG+WAL+S3 restore, no accidental newer-object adoption, protected journals, missing files fail-closed.

### G07 — Resources, scaling, comparison

Deliver Linux harness/reproducible report/Kubernetes-KEDA manifests/bounded autoscale metrics/warm pool. Test CPU1/512MiB,1/2/4 workers, cancel/child cleanup, scale-in/SIGKILL/pool budgets, dependency slowdown without replica storms, fairness/cache warming, explicit fixed-budget rejection. Report missed targets honestly.

### G08 — Release handoff

Require actual AWS and S3-compatible smoke/restore, explicit SDK support matrix, reproducible amd64/arm64 images/SBOM, secret scans, migration/backward-schema reads/rollback compatibility, admin guide/sample DSN/retention/resources/license notices. Do not replace production Rust without a separate request.

Planned commands, not claims that they already exist; run from this directory:

```text
./scripts/check unit
./scripts/check contracts
./scripts/check sdk
./scripts/check integration
./scripts/check crash
./scripts/check web
./scripts/check resource
./scripts/check scale
./scripts/check comparison
./scripts/check release
```

Use isolated temporary directories/DBs/bucket prefixes; clean only verified owned targets. Only explicit AWS gates use external test infrastructure; never production buckets/alerts.

## 23. Required failure injection outcomes

| Failure | Required result |
|---|---|
| Journal uploaded only | No ACK/receipt reference; reclaimable orphan |
| Reply lost after Accept commit | Same internal receipt; disclose possible SDK ID-less retransmission duplicates |
| Late expired worker | Publish fenced out |
| N+1 ready, N failed | N+1 prepared, lane cut stalled, other lanes proceed |
| Crash after Publish commit | Files/Issue/outbox/watermark applied once |
| Snapshot/compaction/GC race | Stable results or explicit expiry, never silent loss |
| Duplicate reducer result | Count once |
| Missing/forbidden/corrupt object | No complete=true success |
| Child OOM/crash/timeout | API survives; bounded retry/failure, permit reclaimed |
| PG/bucket auth failure | Never mistaken for a new empty installation |
| SDK429/network loss | Measure actual SDK retry/drop separately from server loss |
| PG PITR restore | Explicit cut/RPO, generation/token changes, matching refs |

## 24. Tunable policies versus invariants

Tunable: batch size/wait, file/row-group size, compression, cache/spill budgets, workers, hysteresis, measured SLA policies. Invariants: authority committed before ACK, authorization, typed SDK values, error/log distinction, exact aggregates, snapshot completeness, fences/idempotent publication, recoverable object protection, honest unsupported scope.

Before adding automatic schema promotion/FTS/replacing engines/external brokers, document measured bottleneck and cost. Never change result semantics merely because execution is slow. Report the concrete failed gate, not sweeping claims that DuckDB can do everything or cannot search.

## 25. Sources and verification level

Pinned SDK evidence: [SDK-SOURCES.md](SDK-SOURCES.md). Official library documentation inspected for this design is not equivalent to project execution verification:

- [Official Go driver and engine mapping](https://github.com/duckdb/duckdb-go)
- [Parquet projection/filter pushdown](https://duckdb.org/docs/stable/data/parquet/overview)
- [Parquet row groups](https://duckdb.org/docs/current/data/parquet/tips)
- [RE2/regex semantics](https://duckdb.org/docs/current/sql/functions/regular_expressions)
- [Memory limits and OOM](https://duckdb.org/docs/current/guides/performance/oom)
- [FTS update limitations](https://duckdb.org/docs/lts/core_extensions/full_text_search)
- [PostgreSQL locking](https://www.postgresql.org/docs/17/explicit-locking.html)
- [S3 consistency](https://aws.amazon.com/s3/consistency/)
- [KEDA PostgreSQL scaler](https://keda.sh/docs/2.20/scalers/postgresql/)
- [Fargate CPU/RAM combinations](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-tasks-services.html)

Declare completion only with actual G00–G08 evidence; this design cannot replace that evidence.
