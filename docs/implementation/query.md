# Shared query planning, snapshots and exact execution

All readers, including related records, Live and threshold alerts, use these
operations. There is no separate ad-hoc SQL path for dashboards or alerts.
Types shared with engine/control live in model without drivers. CEL parsing
and SQL lowering belong to query; native engine accepts internal plans only.

## Dataset versus operation

DatasetSpec contains tenant, sorted unique project IDs (1..100), sorted unique
kinds (error/log/transaction), time_basis event/received, start_us inclusive,
end_us exclusive, and normalized typed predicate. start<end is required;
unbounded time is not accepted, but no arbitrary maximum span is imposed.
Empty expression is true; omitted kinds means all3. Principal and scope are
supplied by authentication, never trusted from client-supplied tenant IDs alone.

Dataset hash is SHA-256 over a versioned fixed-field canonical encoding of that
spec. Preserve expression node order; equivalent expressions need not share a
hash. Same public expression/AST round-trip must yield identical bytes. Token
reuse requires identical dataset hash. A snapshot also binds principal/auth
revision, generation and retention floor. OperationSpec adds rows projection,
sort/limit/cursor or aggregate metrics/group/histogram; operation_hash covers
those fields except cursor (cursor carries its own previous last tuple).
Rows/histogram share dataset snapshot, not necessarily execution tasks.

## Filter IR and public JSON AST

Each JSON node is a tagged object with exactly the documented fields; reject
unknown properties. Limits apply equally after CEL lowering and JSON parsing.
Only expressions <=8KiB, total AST<=128 nodes, depth<=16, list<=100, regex<=4
and1KiB each. Include literals and all arguments in the node count.

| Node | Shape |
|---|---|
| Field | `{op:"field",name:"service"}` from DESIGN fixed field allowlist |
| Attribute | `{op:"attr",type:"string\|integer\|double\|boolean",namespace:"attributes",path:"/x"}` |
| Literal | `{op:"literal",type:"string\|integer\|double\|boolean",value:...}`; integer value decimal string, double finite JSON number |
| Comparison | `{op:"eq\|ne\|lt\|le\|gt\|ge",left:<value>,right:<literal>}` |
| Boolean | `{op:"and\|or",args:[<predicate>,...]}` (2..16); `{op:"not",arg:<predicate>}` |
| Set | `{op:"in",value:<value>,items:[<literal>,...]}` (1..100, one type) |
| String | `{op:"contains\|starts_with\|ends_with\|icontains\|matches",value:<string-value>,pattern:"..."}` |
| Full projection | `{op:"text",value:"literal substring"}` |
| Presence | `{op:"exists\|is_null",namespace:"attributes",path:"/x"}` |
| Array | `{op:"array_contains",namespace:"attributes",path:"/x",value:<scalar-literal>}` |
| Constant | `{op:"constant",value:true}` or false |

`<value>` is Field or Attribute, not arbitrary arithmetic or another predicate.
eq/ne allow same scalar types, ordering only string/integer/double; booleans
eq/ne/in only. Null is not an ordinary literal: use is_null. Namespace exactly
attributes/tags/extra/contexts/user/request/sdk, pointer valid RFC6901 and<=1KiB.
No dynamic pointers, attr-to-attr comparisons or arrays as scalar literals.
Fixed severity_number is integer; all other fixed fields are strings. Integer
literals support signed38 decimal digits (after sign/leading-zero normalization);
outside that range422 numeric_overflow. CEL signed64 limits remain; use JSON AST
for wider integers. CEL identifiers/functions map one-to-one to these nodes;
reject CEL macros and unapproved AST forms even if its type checker accepts them.
Filter syntax/type errors400 with safe field path/offset, never SQL.

Every comparison leaf compiles to COALESCE(predicate,FALSE). IS NULL on a SQL
value is not is_null(attribute): the latter requires existence+value_type=null.
Attribute lookup is a scalar subquery/unnest filtered by namespace/path and type;
duplicate attribute paths are a stored-format error, not arbitrarily first().
For array_contains use typed scalar children at direct array indices, not every
descendant; one Boolean per record. All data values become bound parameters.
contains uses literal substring function, never unescaped LIKE. matches uses
RE2 search semantics. String grouping/sorting uses binary UTF-8 ordering; no
locale collation. Lower Unicode icontains behavior is tested on pinned2.0.

Compiler shape:

```text
SELECT <fixed projection> FROM read_parquet(<internal bound file list>) r
WHERE tenant_id = $tenant AND project_id IN <bound project list>
  AND kind IN <bound kinds>
  AND <selected time column> >= $start AND <selected time column> < $end
  AND received_time_us >= $snapshot_retention_floor
  AND <per-lane batch_seq <= snapshot cut>
  AND (<validated predicate>) AND (<keyset predicate if present>)
ORDER BY <fixed enum ordering> LIMIT $limit_plus_one
```

No user identifiers, URLs, paths, SQL fragments, functions or sort text enter
generated SQL. Fixed internal COPY is separate from query lowering. Detail and
related reads still enforce tenant/projects/cut/retention even when ID is known.

## Snapshot operation

In one REPEATABLE READ transaction: shared-lock installation, tenant, user,
project authorization rows and all16 tenant lanes in stable order; validate
full requested scope; read public cut/generation and current retention floor;
insert active snapshot/projects/lane refs and commit. Retry serialization
failure as whole transaction. Catalog selection may stream after commit against
the captured generation because those generation intervals are pinned. Do not
hold SQL transactions while waiting for S3/native work. A consistent generation
is not a PostgreSQL transaction held open for the browser's whole session.

Scope revision is user's auth_revision plus tenant/project auth revisions in
dataset authorization state. Disabling any requested project aborts read even
if files remain. Reuse revalidates all scope, snapshot active/TTL/generation,
query hash and principal. New snapshot stores absolute retention floor=max of
installation monotonic floor and DBnow-retention; existing snapshot retains
its own floor. Extension30s heartbeat up to created+1h, TTL15min. Releasing or
expiring snapshot invalidates associated tasks and capabilities. A worker must
finish or be canceled before supervisor releases task pins; no reading after
successful release. Physical GC handles expired readers by fencing gateway I/O.

Catalog: valid_from<=captured_G and (valid_to is null or captured_G<valid_to),
exact bundle project intersection, event/received range bounds. Never use a
statistic that can prune a matching record falsely. Empty catalog is a valid
empty result only after authorization/snapshot succeeds. Missing manifest,
file or a failed download is not an empty catalog.

## Tokens and paging

Tokens are base64url(payload JSON) + '.' + base64url(HMAC-SHA256(key,payload)).
Require key ID/version, constant-time MAC comparison, max8KiB decoded payload,
canonical encoding, bounded expiry and exact field set. HMAC key is a mounted
random32-byte secret. Tokens contain no S3 paths or key material. Rotate with
key IDs and at most one previous signing key until its maximum1h TTL expires.
Restore generation invalidates both keys' old tokens.

- read_token: version/key_id/generation/snapshot_id/principal_hash/dataset_hash/
  expires_at. Scoped to the same authenticated session user, not a bearer grant.
- cursor: same identity plus operation_hash/sort/last tuple/expiry. Limit is
  bound; changing it restarts paging. Responses return a fresh read_token if
  snapshot TTL has been renewed; old tokens don't outlive their own expiry.
- live token: separate purpose tag, scope/filter hash and16 lane positions;
  never accepted as read_token. See below.

event_desc tuple=(event_us,ns,record_id), all DESC. received_desc tuple=
(received_us,lane_id,batch_seq,record_ordinal,record_id), all DESC. A global
record position within the journal is stored in analytics.record_ordinal;
original item/record ordinals remain canonical. Lexicographic '< last' in all
workers, no OFFSET. Fetch limit+1 locally and globally; next_cursor derives from
last delivered row only if an extra global row exists. Equal-time records on
different files/lanes appear once in stable order. Token error precedence:
malformed/MAC400, valid token wrong generation409, current auth403, expired410,
dataset/operation mismatch400. Unknown opaque record/job IDs return404 after
scope checks; never reveal another tenant's existence.

## Task partitioning and execution

Coordinator writes immutable partitions before dispatch: sorted file_id lists,
up to8 files or target64MiB compressed; a larger file is its own task. Each
analytics file belongs to exactly one scan partition. Payload files are excluded
except detail. Row-group splitting remains disabled unless independently proven.
Plan envelope includes protocol version, query/snapshot/task/fence/generation,
deadline, operation IR, cut/scope, complete manifests and supervisor capability
handles. No user S3 credential or URL can create a capability.

Per query max4 running scan tasks; per worker1 native child. Admission limit
2 active queries/user, 8/tenant initially; queued max32/tenant, deadline starts
at submission. Auto uses sync if planned analytics bytes<=64MiB and<=8 files
and a slot is immediately available, otherwise202. Sync deadline30s, async5min,
both at most snapshot max lifetime. Plan byte threshold is routing, not a promise
about scan latency. Empty input executes reducer's empty result without a child.

Each task has60s lease/15s heartbeat/fence and an output object named by task
attempt. Complete atomically records the single winning output if query active,
deadline valid and live authority matches. Expired/canceled outputs are orphans.
Retry transient native crash at most2 retries (3 total), with unchanged partition
identity. Deterministic resource-limit failure is terminal. Adaptive splitting
is outside v1; explicit failure is safer than double-counting an old partition
and its replacements. Do not add superseded states or partial success to v1.

Coordinator takeover increments its fence, reads the existing manifest and
winning task refs, resumes merge, never replans a different snapshot. Cancellation
sets query terminal canceled and fences late task/result writes in SQL; sends
child cancel, kills after2s, revokes gateway capabilities, waits for process exit,
then frees pins/permits. HTTP disconnect cancels sync query; async is unaffected.
Partial results are not public success. TTL reclamation includes query temporary
outputs and native spill; result artifact missing means query_failed, not rerun
against a new snapshot.

## Exact merge contracts

Rows: bounded k-way merge of sorted local limit+1 results, identical tuple
comparison. Detail: exactly0/1 canonical ID; >1 is data-corruption error. Return
one record's scrubbed raw and projections, not full payload files to browser.

Aggregates: at most2 dimensions,8 metrics. Each metric names a fixed numeric
field (severity_number) or typed numeric attr. count counts rows; sum/min/max/avg
count only matching numeric type. Missing/invalid/other types increment excluded
count. Each task emits all groups (not local Top-K), and per metric count/sum/
min/max sufficient state. avg merges sum+valid_count. Integer sum is exact
DECIMAL(38,0); integer avg result is decimal string with scale9 and half-even
rounding **after global sum/count**. Double metrics are finite JSON numbers;
reduce partitions in ascending partition_id and test DESIGN tolerance. Integer
overflow or nonfinite output422, never coerced string Infinity or float fallback.

Group keys encode type tags and canonical values. Missing and explicit null are
separate tags; integer1/double1.0/string"1" remain separate. Normalize -0 double
to0 for grouping; no NaN. Non-scalar group values422 unsupported_group_type.
Group-only `{op:"group_attr",namespace,path}` reads any stored scalar type;
Field or typed Attribute grouping is also allowed, with type mismatch mapped to
missing. group_attr is not allowed in filter/metric AST. See the independently
calculated [examples](examples.md) for expected typed groups and global ranks.
Default rank count DESC then canonical group-key bytes ASC, optional sort by one
requested metric ASC/DESC with null last and same tie break. top<=1,000 applies
only after global merge. If union exceeds20,000 groups or total intermediate
uncompressed bytes exceeds64MiB, fail422 query_limit_exceeded. Track total bytes
across all tasks, not just each task's cap. Reducer uses isolated native process
under same worker budget/spill cap; streaming partials never all load into Go.

Histogram chooses interval from 1s,10s,1m,5m,1h,1d; <=2,000 buckets. Start bucket
floor(start/interval)*interval, include only buckets intersecting [start,end).
empty_buckets true emits zero count/null numeric aggregates for empty buckets;
false omits them. Grouped histogram's bucket+dimension combinations count toward
20,000 limit. Epoch arithmetic uses checked integers and floor for negative times.

## Related and Live

Related endpoint derives trace from an authorized visible detail record; caller
specifies authorized project scope and time bounds. If no trace, require explicit
fallback confirmation; use same project + exact service including null, default
event_time +/-5min. No implicit cross-project service scan. Parent ID/correlation
does not grant access. Result labels correlation mode trace/time_service.

Live starts with current published cut unless catchup_start_us is provided
(<=15min ago). Token positions per lane=(batch_seq,ordinal), initialized to end
of the chosen starting cut. Poll captures a new snapshot, queries each lane's
positions greater than last checkpoint up to captured cut under received time
and filter, delivers bounded batches. Within a lane ascending(seq,ordinal), UI
may visually sort across lanes. After *all* results in the scanned interval are
sent, emit checkpoint to cut including zero-match intervals. If more matches
than one batch, checkpoint only fully emitted prefix; don't skip unsent rows.
State advanced only by client-confirmed Last-Event-ID on reconnect. Replays can
repeat rows; frontend dedupes by record_id in its bounded list.

SSE events: rows, checkpoint, heartbeat, resync_required, error. id is live token
on rows/checkpoint; max256KiB pending data/connection, max32/API, heartbeat15s.
Query pages cap100 rows and output bytes; oversized single detail is never sent
on Live (list projection only). Resync if catchup>15min, >10,000 rows/10s, expired
generation or slow client buffer limit. Abort with403 on auth recheck before
next batch. Release each polling snapshot after transmission; never keep an
unbounded persistent snapshot just because SSE stays open.
