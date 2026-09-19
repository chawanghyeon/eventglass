# Control-plane schema and transaction contract

Read with [ingest-publication](ingest-publication.md), [query](query.md) and
[operations](operations.md). All tables below are planned additions unless
explicitly called existing. Do not edit committed migrations 0001/0002.

## Database conventions

PostgreSQL 17, pgx/v5; all authoritative writes use synchronous_commit=on.
UUIDs are generated cryptographically by the Go coordinator. IDs exposed as
BIGINT are positive, serialized as decimal strings; add SQL identity sequences
for tenant/project/user creation starting above existing IDs. Sequence gaps
are allowed for these IDs, never for lane batch_seq. Lane IDs are 0..15.
SHA values are full lowercase hex with length/pattern checks; secrets are
random bytes and persisted only as SHA-256 digests, except encrypted destination
secrets described in the API contract. Revision/fence counters never wrap.

Use NOT NULL unless a column is explicitly optional below. Common mutable rows
have created_at/updated_at timestamptz with DB-clock values. All durations are
validated positive with configured maxima. Use checked addition for counters.
Persist versioned bounded JSON only for small vectors/config/manifest metadata,
not raw events or unbounded lists. All foreign references include tenant_id
and project_id where relevant. Add UNIQUE composite identities where required
for FKs even when a globally unique primary key exists. Use RESTRICT deletion
except explicitly owned child rows; lifecycle code decides deletion order.

[Correctness C04](correctness.md#c04--preserve-legal-strings-across-postgresql-boundaries)
defines the exception to jsonb/TEXT for arbitrary input strings: preserve query
and rule JSON as BYTEA; encode event-derived scalar strings as JSON-string TEXT.
This applies to every new table below, not just user-facing fields.

Indexes below are minimum access paths. Query plans must be verified against
10k+ rows, not only empty fixtures. Do not add an index for every field.

## Existing schema and planned migration order

0001 owns installation identity. 0002 owns tenants/projects/project_keys,
lanes, object_intents, ingest_batches, receipts, event_dedupe, jobs and
sdk_outcomes. Their source is authoritative for current column names.

| Migration | Gate | Additions; never rewrite previous migrations |
|---|---|---|
| 0003_ingest_policy | G02 | Tenant auth_revision; project name/origins/scrub policy; retention policy; receipt unsupported metadata; intent retirement/protection fields; jobs prepared support |
| 0004_publication | G03 | job_outputs, bundles, files, file_blocks, bundle_projects, Issues, occurrences, issue_transitions |
| 0005_auth_queries | G04 | users/memberships/grants/sessions/login limits/audit, snapshots/query tasks, retention/recovery/bootstrap policy fields |
| 0006_query_snapshots | G04 | immutable post-Q1 snapshot authority revisions and installation retention policy |
| 0007_query_execution | G04 | immutable query task shape and temporary-output producer authority |
| 0008_query_results | G04 | sealed input-byte accounting used by public query routing and result statistics |
| 0009_alerts | G05 | destinations, alerts, evaluations, deliveries, audit action additions |
| 0010_maintenance | G06 | maintenance_tasks, maintenance_inputs, backup_sets |

The sequence is a starting manifest for this tree. If a packet requires an
additional migration, append the next free version and update this table in
the same commit; numbers are not permission to rewrite an applied file.
Migration runner must also verify the *binary manifest itself* is contiguous
from 1, not merely that the database is a prefix. Additive upgrade tests include
existing receipts, prepared jobs, snapshots and Issue states, not empty DB only.

## G02 additions

- tenants: auth_revision bigint default 1; name text (1..128 chars). Tenant
  disable is an auth revision change and blocks new Accept/read operations.
- installations: global_scrub_policy_sha and global_scrub_policy_revision,
  checked at API startup against mounted global rules; mismatched policy refuses
  ingress. Existing empty-rule installations receive a deterministic default.
- projects: name text, allowed_origins jsonb array (max 100 exact origins),
  scrub_rules jsonb (versioned; max 64 KiB), config_revision bigint default 1.
  Existing auth_revision changes on state/key/origin changes; scrub_revision
  changes on scrub rules. Service changes bump config_revision and the auth
  snapshot checked by Accept. Never confuse policy revision with algorithm v1.
- project_keys: key_id UUID unique, label text max 128, key_prefix text containing
  only first 8 hex chars for display. Never store complete public-key bytes.
- receipts: policy_revision integer, normalizer_version integer,
  grouping_version integer, dedupe_hash_version integer, all positive;
  these freeze replay. selection_json uses the exact representation below.
- receipt_unsupported: PK(acceptance_id,item_ordinal), receipt FK CASCADE,
  item_type text max 128, byte_count bigint >=0. No unsupported body storage.
- object_intents: retired_at nullable timestamptz, protect_until nullable
  timestamptz. referenced objects are not collected by expires_at alone.
  Add conversion_job_id nullable scoped FK, producer_generation/fence nullable
  pair. Later migrations add query task tuple and maintenance task FK; at most
  one family. Live producer or prepared reference prevents GC (C01).
- jobs: expand state enum/check to queued/running/prepared/completed/failed;
  keep owner/lease nonnull **iff running**. Add prepared_output_id nullable UUID
  when job_outputs is introduced in 0004. `prepared` is durable but unleased.
  Add partial index (tenant_id,lane_id,batch_seq) WHERE state='prepared'.

Selection v1 is JSON object `{version:1,accepted:[[a,b]],duplicate:[[a,b]],
conflict:[[a,b]]}`; ranges are inclusive *request-local record positions*.
Empty lists represent zero positions. Each class is sorted, nonoverlapping and
maximally coalesced. Classes partition exactly 0..record_count-1. No holes,
negative indices or overlapping classes. SHA hashes a Go struct encoded with
field order version,accepted,duplicate,conflict, compact JSON, nonnil arrays,
no trailing LF. Use the same encoder in writer and verifier, never JSONB's
rendering. Convert to journal positions using receipt.ordinal_first.

## G03 catalog and Issues

| Table | Columns/keys/constraints |
|---|---|
| job_outputs | output_id UUID PK; job_id FK; prepare_fence bigint; manifest_version int; header_json <=64 KiB; manifest_sha; state prepared/published/discarded; UNIQUE(job_id,prepare_fence); prepared_at; output manifest immutable |
| job_output_parts | PK(output_id,part_index); output FK CASCADE; metadata_json <=64 KiB; metadata_sha; ordered bundle/file/block metadata only, <=32 MiB total; no raw records |
| job_output_occurrences | PK(output_id,record_id); selected error summaries, scoped receipt/project FKs; exact fields and root manifest digest in correctness C01; no raw/log rows |
| bundles | bundle_id UUID PK; tenant/lane FK; schema/grouping versions; event_day date; kind; input_seq_min/max; row_count bigint; identity_sha; valid_from_generation bigint; valid_to_generation nullable >from; retired_at nullable; UNIQUE(tenant,bundle_id) |
| files | file_id UUID PK; tenant/bundle FK; intent_id unique scoped FK; role analytics/payload; bytes >0; full_sha; row_count; min/max event_us, received_us, batch_seq; nullable bounds only for no rows (do not create empty files); UNIQUE(bundle_id,role) |
| file_blocks | PK(file_id,block_index); file FK CASCADE; sha; exactly ceil(bytes/1MiB) sequential blocks; final length derived from bytes |
| bundle_projects | PK(tenant_id,bundle_id,project_id); scoped bundle and project FKs; exact member set, not approximate bloom filter |
| issues | PK(tenant_id,project_id,issue_id); UNIQUE(project_id,grouping_version,fingerprint_sha); status unresolved/resolved/ignored; revision bigint; occurrence_count bigint; first/last event tuple and nullable release_json; last_received_us; title_json (decoded <=512 code points); resolved_cut nullable vector16; grouping_version/fingerprint_sha |
| issue_occurrences | record_id char(64) PK; tenant/project/issue FK; receipt FK; lane/seq scoped batch FK; ordinal; event_us/ns; received_us; nullable release_json; no permanent physical file FK |
| issue_transitions | transition_id UUID PK; tenant/project/issue FK; issue_revision; type created/regressed/resolved/ignored/reopened; received_us; nullable record_id and actor_user_id; UNIQUE(issue_id,issue_revision) |

Bundles contain exactly one analytics and one payload file with identical
record ID sets, possibly several bundles per input batch. Compute identity_sha
by streaming sorted unique lowercase record IDs, each followed by LF; reject
duplicate IDs. The set digest is not XOR and is independent of row order.
Keep full file SHA and block hashes separately. Catalog stats come from the
finished files. Occurrence detail resolves ID using its lane/seq and snapshot
catalog, so compaction never requires occurrence-file pointer rewrites.

Catalog index (tenant_id,lane_id,valid_from_generation,valid_to_generation),
files time bounds via bundle join, bundle_projects(project_id,bundle_id).
Issues index (tenant_id,project_id,status,last_received_us DESC,issue_id DESC);
occurrences(issue_id,event_us DESC,event_ns DESC,record_id DESC) and
(tenant_id,received_us,record_id). Logs have no occurrence table rows.
Issue counts are lifetime accepted/published unique occurrences. Retention
removes occurrence detail but does not decrement lifetime count or erase the
Issue's first/last summary. Label this explicitly in the API/UI.

Prepared manifest root SHA covers version/header plus ordered part index/hash
pairs with fixed-field JSON encoding; parts are streamed and verified before
Publish. This accommodates up to10,000 small day/kind partitions in one legal
request without retaining all metadata in memory or inventing an after-ACK
partition-count rejection. Each nonempty bundle contains at least one record,
so conversion bundle count cannot exceed selected record count. Split metadata
across parts at field boundaries, never split an encoded string. Insert parts
in Prepare's atomic transaction; native metadata is a private streamed spool
until then. Benchmark the worst-case metadata transaction in G03.

## G04 identity and query state

- users(user_id PK, email_normalized UNIQUE, password_phc, state active/disabled,
  auth_revision, credential_revision, is_installation_admin Boolean default false, created_at,
  updated_at). Email normalization is trim + lowercase;
  no provider-specific dot/plus rewriting. Passwords never normalize.
- memberships PK(tenant_id,user_id), role admin/member, revision. Project grants
  PK(tenant_id,project_id,user_id), role operator/viewer, scoped FKs. A member has
  no project access without a grant; admin covers all tenant projects. Changes
  lock and increment user.auth_revision, including removal of memberships.
- sessions(token_hash bytea32 PK, user_id FK, csrf_hash bytea32, credential_revision,
  storage_generation, created_at, last_seen_at, expires_at, revoked_at nullable).
  Expiry index; revoke on password/state change. Do not store browser tokens.
- login_limits(bucket_hash bytea32, window_start timestamptz, count integer,
  expires_at; composite PK(bucket_hash,window_start)); atomic increments and
  expiry cleanup; bounded1h retention. Setup state/token_hash and completed_at
  nullable live on installation; install lock serializes consumption. Last-admin
  safety checks lock the tenant row before user rows.
- query_snapshots(snapshot_id UUID PK, tenant_id, user_id, principal_kind
  user/alert, principal_ref, auth_revision, storage_generation, dataset_hash,
  dataset_bytes BYTEA <=32 KiB, retention_floor_us, tenant_auth_revision,
  created_at, expires_at, max_until, state active/released). For alerts user_id
  is nullable and principal_ref is an alert UUID; user snapshots have user_id
  nonnull. Unique scoped identity.
- snapshot_projects PK(snapshot_id,project_id), tenant/project FK and captured
  project_auth_revision.
  snapshot_lanes PK(snapshot_id,tenant_id,lane_id), lane FK, cut_seq and
  catalog_generation bigint; indexed (tenant_id,lane_id,catalog_generation).
- query_jobs(query_id UUID PK, tenant_id, user_id nullable, principal_ref,
  snapshot_id FK, operation_hash, operation_bytes BYTEA <=64 KiB, state
  planning/queued/running/succeeded/failed/canceled, deadline, coordinator_owner nullable,
  coordinator_fence bigint, lease_until nullable, result_intent_id nullable,
  result_sha nullable, result_bytes nullable, error_code nullable, expires_at,
  sealed_plan_sha nullable, plan_file_count, plan_scan_count, plan_bytes).
- query_tasks PK(query_id,stage,level,partition_id), query FK CASCADE, state
  queued/running/succeeded/failed/canceled, fence, attempt, owner/lease iff
  running, manifest_json <=1 MiB, result_intent_id nullable, result_sha nullable,
  result_rows/result_bytes nullable, retry_at, error_code. stage scan/reduce;
  unique manifest partitions fixed before execution. Claims indexed by
  (state,retry_at,query_id,stage,level,partition_id). Jobs/results expire with snapshot.
- query_task_inputs: consumer task tuple + ordinal PK, producer task tuple FK;
  same query, lower level, no duplicate producer per consumer. Immutable after
  plan seal; task claim requires all producer outputs succeeded. Per-level byte
  budget rows (query_id,level PK,reserved_bytes,committed_bytes) enforce C06 limits.

Tasks do not require a FK to conversion jobs or ingest batches. Inline small
results are held only during execution; even synchronous requests use the same
task scheduler/authority. Store result artifacts for resumable async retrieval
as temporary S3 intents, not arbitrary large PG JSON blobs. Scope columns plus
FKs prevent a task from referencing another tenant's snapshot/result intent.

## G05/G06 additional state

| Table | Required columns beyond scoped ID/timestamps |
|---|---|
| alert_destinations | tenant_id,destination_id UUID; name; https URL; encrypted secret + encryption key ID; revision; enabled; admin only |
| alerts | tenant/project/alert UUID; name; kind issue/threshold; revision; enabled; validated rule_bytes BYTEA <=32 KiB + hash; destination FK; cooldown_seconds; enabled_from_public_cut vector16; enabled_at; last_completed_end_us nullable; last_fired_end_us nullable |
| alert_evaluations | evaluation UUID; alert/revision; window_end_us; window_start_us; cut vector16; state waiting/queued/running/succeeded/failed/canceled; lease/fence; snapshot_id nullable; complete result summary; UNIQUE(alert_id,revision,window_end_us) |
| issue_alert_evaluations | PK(alert_id,alert_revision,transition_id); scoped alert/transition FKs; decision sent/cooldown/disabled; nullable delivery_id; records suppressed transitions too |
| deliveries | delivery UUID; tenant/alert/revision; own revision bigint; destination ID/revision and encrypted destination snapshot; dedupe_key unique per tenant; immutable body_bytes BYTEA <=64 KiB + hash; state queued/running/succeeded/failed/canceled; fence/lease/attempt/retry_at; last_status nullable; error_code nullable |
| audit_events | tenant/id UUID; actor ID nullable; action enum; target type/ID; revision; request_id; occurred_at; nullable operation_id UUID; UNIQUE(tenant,actor,operation_id) when nonnull; sanitized operation hash/result revision; no secret/request body; admin read index (tenant,occurred_at,id) |
| maintenance_tasks | task UUID; tenant/lane nullable only for installation-wide GC; kind compact/retain/gc/backup_verify; unique input_identity; state queued/running/prepared/completed/failed; generation/fence/owner/lease/retry/attempt; bounded input/output manifests |
| maintenance_inputs | PK(task_id,bundle_id); scoped FKs; unique active reservation enforced by reserved_by nullable task FK on bundles; expected valid_from_generation |
| backup_sets | backup UUID; external_tool_id UNIQUE; base_start/end; earliest/latest recoverable_time; state pending/verified/expired/failed; protected_until; verified_at nullable; object inventory manifest URI/hash; installation/generation |

Alerts are single-project in v1. This avoids multi-project permission ambiguity
and keeps their 16-lane cut complete; search can still span authorized projects.
Separate scheduler singleton leases use an explicit scheduler_leases table
(name PK, owner, fence, generation, lease_until); no process-local leader flag.
Installation also holds recovery_state ready/restoring/verification_required,
retention_days (1..3650), retention_revision bigint, retention_floor_us monotonic,
retention_tick_at timestamptz, encryption key ID,
and alerts_paused Boolean. Encryption key material is a mounted secret, not PG.
Project retention days/revision are added by0003 because dedupe needs them;
snapshot floor, recovery state and bootstrap state are added by0005;0006 adds
installation retention policy and immutable snapshot authority revisions;0010
adds backup/task state.
G04 management mutations require
audit_events: create that table in0005 and extend its action enum in0009.
G03 Issue mutation operation IDs live in issue_transitions.operation_id with a
scoped unique index; add the nullable actor FK in0005. Preserve these IDs when
adding general management auditing; don't erase retry history on upgrade.

0003 encodes existing SDK outcome/category/type text columns once into `_json`
columns per C04; preserve exact decoded values.
Replace sdk_outcomes string-key PK with category/reason digests per C04 while
retaining full encoded strings; never index unbounded SDK values directly.
0005 adds setup attempt/
state/marker/fingerprint/lease fields per C08.0010 adds batch recovery_state
live/retired, journal_retired_at, nullable journal FK with retired/published CHECK,
and compact retirement summaries per C07. No current applied migration changes.

## Transaction and lock rules

All control methods own Begin/Commit/Rollback. No caller receives a pgx.Tx.
Default READ COMMITTED; snapshot registration REPEATABLE READ. Retry 40001 and
40P01 at most 3 times after the initial attempt, with 10/30/90 ms randomized
delay bounded by deadline.
User snapshot admission first takes a tenant-scoped PostgreSQL session advisory
lock, then begins the single REPEATABLE READ registration transaction. This
prevents a retry storm from exposing serialization failures when several API
instances submit against the same cap. Always unlock before returning the
connection to the pool; close the session if unlock cannot be confirmed.
Network work, password hashing, native execution and webhook sends occur outside
transactions. statement_timeout 5s and lock_timeout 2s initially, overridable
per maintenance operation (30s maximum); never disable timeouts globally.

Total lock order, adding omitted authority rows to ARCHITECTURE's order:
installation shared -> tenant -> user/membership -> projects/keys sorted ->
lanes sorted -> source dedupe sorted -> jobs/evaluations/maintenance rows ->
intents sorted -> Issues sorted -> catalog/snapshot/outbox child rows.
Updates to installation generation are offline restore operations only.
Normal code takes an installation shared lock to exclude configuration/restore
transitions; do not serialize all writes with an installation exclusive lock.
Claim/heartbeat operations touching only late rows must never acquire earlier
locks afterward in the same transaction. Resolve first locks all 16 lanes,
then Issue. Accept locks one lane; Publish locks its lane then sorted Issues.
Retention/GC lock the relevant lanes before observing pins or reserving deletes.

Tenant/project/key validation uses FOR SHARE, not FOR KEY SHARE: ordinary
non-key revision updates must conflict. Ingestion takes tenant and project
locks even when authorizing from cache. Membership mutations serialize through
user row; read operations obtain current scope and revision through that row.
The final response authorization check is a defined linearization point:
revocation committed before that check prevents output; already transmitted
bytes cannot be recalled. SSE repeats the check before each bounded batch.

Commit I/O error is **unknown outcome**, not rollback proof. Reconnect and read
the idempotency key using fresh READ COMMITTED transaction. If DB is unreachable,
return retryable dependency/commit_unknown, never an ACK or a fabricated result.
Every operation documents its durable idempotency key in its subsystem spec.

PostgreSQL [row-lock semantics](https://www.postgresql.org/docs/17/explicit-locking.html)
and [repeatable-read conflict behavior](https://www.postgresql.org/docs/17/transaction-iso.html)
justify the lock modes and whole-transaction retry above; concurrency tests,
not this documentation, establish their correct use in this application.
