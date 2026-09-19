# Cross-boundary correctness contracts

Read the relevant section with the owning subsystem document. These rules close
failure cases that cannot be specified by a package diagram alone. They are
planned implementation requirements, not claims of executed integration tests.
Stable C identifiers are acceptance-test references in work-plan.md.

## C01 — A prepared job is independently publishable

After Prepare commits, killing the converter and deleting its entire scratch
must not lose anything the publisher needs. A manifest containing only file IDs
and counts is insufficient: Publish also needs the selected errors' Issue data.

Add job_output_occurrences keyed (output_id,record_id), with scoped project,
receipt acceptance_id, lane/seq/global ordinal, event_us/ns, received_us, nullable
release, issue_id, grouping_version, fingerprint_sha, safe Issue title. No raw
event/body/frames. Supervisor derives these from verified canonical input using
pure issues.Group, sorts by record_id and hashes the exact versioned encoding.
Root output manifest binds that hash and selected error count in addition to
bundle/file metadata. Prepare inserts all summaries and manifest parts atomically.
Publish reads only committed summaries/catalog metadata inside its transaction;
there is no hidden S3 read or dependence on the previous converter's memory.
Issue summary record IDs must equal selected error IDs and the errors represented
in analytics, with no duplicates. Error summaries are per-error, never per-log.

Conversion outputs may take longer than the upload intent's10min expiry to
finish. Add explicit producer links from intents to conversion job, query task,
or maintenance task, with producer_generation and producer_fence; at most one
producer family and real scoped FKs (added with each family's migration).
GC checks live producer authority as well as intent expiry. Every producer
upload registration checks its lease/fence in SQL. A still-running matching
producer protects earlier uploaded parts even before Prepare. After Prepare,
durable manifest references protect them without a running lease. When a newer
fence takes over, unprepared old-attempt parts are collectible after expiry;
verified prepared parts are explicitly adopted, not mixed with new partials.
Job heartbeat does not update20,000 individual intents every15s.

Object storage_generation is immutable creation provenance. Active job authority
uses the installation's current generation. Restore records verification of old
objects and rebinds producer authority; it does not rewrite creation generation
or reject every historical file. Completed idempotent results require matching
immutable content; an expired lease cannot perform a new mutation.

## C02 — Upload verification establishes bytes, not user metadata

Head metadata `eventglass-sha256` is an uploader assertion, not a provider's
calculation of the stored bytes. Local spool SHA + matching Head metadata alone
does not justify marking an intent verified.

Provider capability is pinned/tested by backend profile:

- Prefer an explicitly supplied full-object SHA-256 checksum with provider
  validation and a verified checksum response. Test that incorrect checksum is
  rejected. Match size, own metadata and returned calculated checksum.
- If that capability is absent or multipart returns only composite checksums,
  stream a full GET through SHA-256 after upload, before uploaded/Prepare/Accept.
  Use bounded buffers and exact length; include this GET in ACK/cost measurements.
- Do not silently switch mode on permissions, timeout or checksum mismatch.
  Those fail the upload. Backend capability changes require contract tests.

Initial journals/bundles are<=128MiB and use single PUT; multipart remains a
tested storage/backup capability, not necessary for ordinary v1 publication.
Unknown-reply retry never creates a different payload under the same key. All
stale attempts, even retrying identical bytes, must recheck authority before
starting a new request. Provider faults after verification are detected on read
and fail closed; S3 redundancy is still an external durability dependency.
[S3 PutObject checksum fields](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
are capability evidence, not proof for another S3-compatible provider.

## C03 — Exact integer aggregation survives arbitrary partitions

Do not SUM DECIMAL(38,0) directly for intermediate state: values may cancel
globally after a local partial overflows. Do not use decimal `/` for exact avg;
the result type/precision must not be assumed from its operands. DuckDB's
[numeric types documentation](https://duckdb.org/docs/current/sql/data_types/numeric)
explains fixed-width bounds and decimal division, but pinned2.0 execution tests
remain mandatory.

Use fixed5 signed base-10^9 limbs for integer sum/avg. For each valid integer x,
decompose abs(x)'s decimal digits into5 low-to-high chunks, multiply each by
sign(x). Each limb is within +/-999,999,999. Native SQL groups/sums these columns
as DECIMAL(38,0); partial reducers sum corresponding limbs and valid counts.
With total valid_count<=2^63-1, any limb sum's absolute bound is below10^28, so
no intermediate can overflow38 digits. Native extraction must use exact integer
or decimal-string operations, never floating division/pow. Existing Go math/big
may finalize the5 reduced limbs for at most20,000 groups; it is not a new query
engine and never iterates every raw row outside native execution.

Reconstruct S=sum(limb[j]*10^(9*j)) in big.Int (at most57 decimal digits under
the count bound). For sum, return422 only if **final** |S|>10^38-1. For avg, divide
S by valid_count with scale9, half-even rounding using integer quotient/remainder;
it can succeed even if S would overflow a requested sum result. avg alone must
not fail merely because its unexposed numerator exceeds38 digits. If any requested
metric fails, whole aggregate fails. min/max use original exact scalar values.
Preserve limbs through every reduction level; never narrow before finalization.
Count overflow is checked before reconstruction. All-null retains count0 and
null numeric result. Intermediate-byte accounting includes all limb columns.

Final Top-K ordered by an integer sum/avg cannot cast that metric to DOUBLE.
Stream root groups through a bounded Go heap (max requested top<=1,000), compare
big.Int sums or exact avg cross-products S1*n2 versus S2*n1, null-last, then
canonical key bytes. Apply display rounding only after selection. Native still
does all scans/grouping; this bounded finalization only compares reduced states.
Charge heap/key bytes to the coordinator budget and8MiB public result cap; do
not materialize20,000 full group strings in Go. Count/min/max/double comparisons
use their documented types. Exact average rank can distinguish values displayed
the same at9 decimal places; document this in API sort help.

Double metrics use native compensated summation and fixed partition/reduction
order, not an exact-real arithmetic claim. Apply the existing finite-result and
oracle-tolerance tests, including cancellation/mixed magnitudes. A known finite
accuracy failure blocks the gate rather than being waved through as "floats".

## C04 — Preserve legal strings across PostgreSQL boundaries

PostgreSQL text/jsonb cannot represent U+0000; jsonb also transforms JSON
representation. Do not put every accepted/query string into jsonb merely because
it is valid JSON. See [JSON constraints](https://www.postgresql.org/docs/17/datatype-json.html)
and [character constraints](https://www.postgresql.org/docs/17/datatype-character.html).

| Data | Required durable representation |
|---|---|
| Canonical records / raw | Existing sanitized S3 format, exact values retained |
| Dataset, query operation, alert rule | Canonical UTF-8 JSON bytes in BYTEA + version/hash; Go validates structure before storage and after read |
| Delivery signed body | Immutable UTF-8 JSON BYTEA, hash exact bytes; never reserialize jsonb for a retry |
| Issue title, release summary, SDK outcome/category/type display | JSON string encoding stored in PG TEXT (quotes included), decoded when returning DTO; JSON escaping makes U+0000 SQL-safe |
| Controlled enums, IDs, SHA, server paths, selection/range vectors | Typed columns or bounded jsonb; no arbitrary record string is allowed here |
| Admin names/email/URLs/origins/config labels | Reject U+0000 with400 before mutation; do not silently replace identity strings |

Encoded display columns use explicit `_json` suffix, e.g. title_json/release_json,
reason_json/category_json/item_type_json. SDK outcomes use fixed32-byte category/
reason SHA columns in the receipt/item composite primary key, not arbitrarily
long strings in a B-tree index. On matching hashes also compare full strings;
disagreement is a collision error, never silent merging. A null release is SQL NULL, distinct
from JSON string "null". Their byte caps account for JSON escapes (up to6x UTF-8
input length plus quotes). Update sdk_outcomes PK by additive migration with
old strings encoded/hashed once and exact decoded values preserved. Diagnostic
truncation is presentation only; it must not merge distinct durable outcome keys.
Long unsupported type displays retain their hash suffix; original type can remain
in the sanitized journal. Query literal NUL round-trips and binds to DuckDB as NUL,
not the six-character text `\u0000`. Never hex-escape it twice to appease jsonb.

## C05 — Session reload and permission changes are defined

CSRF token is base64url(HMAC-SHA256(session_secret_bytes,
UTF8("eventglass-csrf-v1"))). Cookie secret remains HttpOnly. Persist only the
session-secret hash and CSRF-token hash. Login and GET /v1/session can derive
the same CSRF token from the presented valid cookie; a GET does not rotate state
or invalidate another tab. Never try to reverse a stored CSRF hash. Tokens for
different sessions differ; changing password/revoking session invalidates both.

Separate users.auth_revision (permissions, state, membership) from
users.credential_revision (password/session-wide invalidation). Sessions capture
credential_revision; queries/snapshots capture auth_revision. Grant removal
invalidates in-flight/old-snapshot scope, but a still-valid session can reload its
new permissions without an unexplained forced login. Disabled user, password
change, explicit logout or restore invalidates session itself. Project auth
revisions are also rechecked. GET /v1/session returns current grants; it never
returns the grant snapshot copied at login. All checks use DB authority, not
positive auth cache TTL. Expired request credentials fail before expensive work.
Password/state changes bump both revisions. Login/password-change hashes outside
SQL then locks user and rechecks the observed credential revision before creating
a session/changing credentials, so a concurrent reset cannot validate a stale
password and commit a fresh session afterward.

Browser errors after SSE headers cannot become HTTP403. Before headers, send403;
afterwards send a bounded `error` event `{code:"forbidden",retryable:false}`
without data, then close. Client clears Live state and does not auto-retry403.
An SSE id is a resumable server checkpoint, **not** proof the UI rendered data.
Browser EventSource or fetch-SSE client must apply each delivered row batch before
persisting its checkpoint in memory; reconnect may replay, so dedupe record IDs.

## C06 — Query planning and merging are bounded, including metadata

Snapshot cap4 live/user,32 live interactive snapshots/tenant, plus8 alert
snapshots/tenant. Under tenant then user locks, count/allocate slots atomically;
expiry frees slots; no unbounded snapshots from repeated idle browser requests.
Queries keep the existing2 active/user,8 active/tenant and32 queued/tenant caps.
Enforce shared counts in PG, not per API replica. Workers revalidate authority
at result commit; a gateway's local cached permission is never sufficient.

Create query_jobs in planning state, with coordinator lease/fence. Stream catalog
metadata in file_id-keyset pages<=256 at pinned generation; write partition rows
in fenced short transactions. No task may claim work while parent is planning.
Maximum32,768 selected files,4,096 scan partitions,16MiB serialized scan manifests
per query. Exceeding any limit is explicit422 query_limit_exceeded; never keep
only the first page. On completed planning, commit plan hash/counts and state
queued atomically. A takeover of planning deletes only its unsealed child tasks
and restarts against the **same snapshot**, or fails deadline; a sealed plan is
immutable. Stats include planning time. These caps are resource limits, not a
silent time-range restriction. All new API limits appear in /v1/system.

Native reduction has fan-in<=8, not one open stream for every scan partition.
Build a deterministic tree over ascending partition_id: level1 groups consecutive
scan partitions by8; repeat until one root. Extend task identity to
(query_id,stage,level,partition_id), stage scan(level0)/reduce(level>=1). Dependencies
are explicit query_task_inputs rows pointing to unique successful winning task
outputs. Each row-merge node retains limit+1 under the original cursor/order;
aggregate nodes retain **all** groups/limbs, never Top-K until root. Count cap
and distinct-group cap apply at each level (a subtree cannot have more distinct
keys than the final union). Query max4 concurrent tasks includes reducers.

Apply64MiB byte admission to total scan partials and a separate64MiB ceiling per
reduction level; account bytes in PG with winning task commit, not per-worker
guesses. Retry of same winning result adds zero. Each task caps its output64MiB
and must stop before writing over its reserved allowance. Combined disk/memory
caps still apply; reading8 huge inputs is streaming, not8 whole allocations.
Intermediate objects expire with query; bytes rewritten at each level are
included in actual I/O cost even though the semantic ceiling is per level.
At4,096 scan partitions there are585 reducers; enforce this metadata cap.

## C07 — Retention cannot revive previously hidden rows

New snapshots use **persisted** installation.retention_floor_us, not a larger
locally computed now-days that is forgotten. Scheduler advances it every60s in
a short installation-exclusive transaction to max(previous,DBnow-days). If tick
age>120s, new snapshots fail503 retention_clock_stale; existing snapshots retain
their floor until expiry. Deletion uses the persisted floor only. Expose tick
age; logical expiry has up to60s normal scheduling granularity, not instant
per-request wall-clock precision.

Changing retention locks installation exclusively and advances floor to
max(old_floor,DBnow-old_days,DBnow-new_days) before storing new days/revision in
the same transaction. Initialization records floor and tick time. DB-clock
regression never lowers floor. Widening retention only affects future expiry;
it cannot expose rows hidden by an earlier snapshot. Hold global policy locks
only for these brief operations, not while rewriting files or calculating hashes.

Reference release is distinct from physical byte deletion. Purge a published
batch's receipts/diagnostics only after minimum dedupe protection, no remaining
occurrences/dedupe claims, no required replay/repair/backup holds. Mark
ingest_batches recovery_state retired (separate from publication state); clear
journal_intent_id and set journal_retired_at in the same SQL transaction after
recording a compact immutable receipt/count/hash summary. Pending/failed batches
cannot retire. Completed job outputs/parts/occurrence summaries are then removable;
catalog still independently protects published files. Old PG backup retains its
own old FK and therefore extends object protection through backup interlock.
Make journal FK nullable only for retired published batches, with SQL CHECK.
Retain small lane-seq ledger rows for cut/history checks; never reset accepted_seq.
This breaks the accidental permanent FK-to-journal pin without erasing ACKed
pending data. Ordinary retention is not cryptographic privacy erasure.

## C08 — Initialization is a recoverable cross-store operation

Avoid startup waiting for setup while setup waits for a ready public server.
Add installation setup_state uninitialized/provisioning/ready, setup_attempt UUID,
marker key/SHA, bootstrap hash and lease/fence in the auth migration. Expose only
setup/login-status/livez while provisioning; readyz503 and ingress/query disabled.
Compute password hash outside SQL. Setup transaction reserves attempt and stable
installation ID (or resumes same attempt for valid bootstrap token), commits,
then uploads/verifies a deterministic marker using storage verification C02.
Final transaction rechecks attempt/fence, creates first admin/tenant/16 lanes and
session, marks ready and consumes bootstrap hash atomically. No S3 call under lock.
Marker contains identity/schema format only, no password/token/tenant PII.
The setup_attempt row is the marker's durable intent (a tenant does not exist
yet). Use server key `v1/{installation}/installation.json`; marker bytes are
fixed-field JSON {format_version:1,installation_id,storage_identity_sha}. Owner/
fence lease60s, heartbeat15s. Same fingerprint during live attempt returns409
setup_in_progress with Retry-After; expired attempt may be reclaimed with a new
fence. A different fingerprint409 even after lease expiry; explicit offline
admin reset is required, never silent replacement of chosen credentials.

Crash before marker -> retry same attempt. Crash after marker -> verify same
marker then finish; conflicting marker fails closed. Crash after final commit ->
setup409, user logs in with chosen password; no duplicate user/tenant or second
bootstrap. Two concurrent setup callers cannot overwrite one another's marker
or choose different admin credentials under one attempt: persist a keyed request
fingerprint HMAC-SHA256(bootstrap_token,canonical setup request), not plaintext
password or an unkeyed password hash, and reject changed request409. Runtime does
not accept an installation marker as proof of completed PG setup. Restore starts
with existing identity, never re-enters uninitialized merely because S3 is down.

## C09 — Cost and remaining experiments are explicit

16 random lanes with a100ms first-arrival flush are not guaranteed to produce
large journals. Under a Poisson single-record100 requests/s, one API and uniform
lanes, expected batches/s is approximately100/(1+100*0.1/16)=61.54. With2 APIs
splitting arrivals evenly it is76.19 total, before retries. One conversion pair
per nonempty batch adds2 bundle PUTs, so this workload can approach184.62 PUTs/s
before compaction on one API. This is a batching model, not measured throughput.

I5/P4 must measure one-record and actual SDK-batched workloads,1/2 APIs, request/
journal/bundle ratios, checksum readback GETs and resulting PG/S3 costs. Root
latency targets alone cannot justify the architecture's cost. Do not secretly
change lane hash/topology or withhold publication to force large files. If the
cost comparison fails, keep release blocked and propose a measured design change.
The existing query/storage engines remain; no custom index or broker is justified
by this arithmetic alone.
