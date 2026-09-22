# Runtime, maintenance, recovery and release operations

This specifies planned commands/configuration; current executable availability
is listed in [CONTRIBUTING.md](../../CONTRIBUTING.md) and the work plan. Design
text is not a passing operational gate. Use ARM64 only and pinned DuckDB2.0.

## App assembly and configuration

`app.Run` constructs validated Config, shared byte/disk/task budgets, pgx pools,
S3 client, control operations, engine supervisor, concrete role loops, then HTTP.
Constructors receive dependencies; no globals, default-client mutation or init
goroutines. A root errgroup/context owns every loop and child. Fatal invariant
failure cancels the role; dependency outage changes readiness/circuit state,
not uncontrolled process churn. Single combined process and separate roles call
the same constructors. No in-memory queue may replace durable PG work.

Required environment variables from DESIGN keep their names. Planned additional
settings: EVENTGLASS_HTTP_ADDR (default :8080), EVENTGLASS_INTERNAL_ADDR (loopback
by default), EVENTGLASS_TOKEN_KEY_FILE, EVENTGLASS_ENCRYPTION_KEY_FILE,
EVENTGLASS_BOOTSTRAP_TOKEN_FILE (setup only), EVENTGLASS_RETENTION_DAYS (initial
setup only,30 default), EVENTGLASS_MAX_WORKERS (default4),
EVENTGLASS_TRUSTED_PROXIES (empty default). Resource overrides are a validated
config file with documented units and generated effective-config output that
redacts credentials. Unknown role/setting, unsupported version, nonpositive
budget, missing keys or storage identity mismatch fails startup.

Post-setup retention is PG-authoritative: differing environment values are an
error, not a silent policy change. Storage identity includes normalized endpoint
or AWS region, bucket,prefix; `installations.storage_identity` must match. S3
prefix is server configured, not request-supplied. Startup does read-only HEAD/
scoped capability checks; missing installation marker on an already initialized
PG instance fails, never auto-creates a new empty installation. Initial setup
reserves identity, creates/verifies a scoped marker, then finalizes PG authority
per [correctness C08](correctness.md#c08--initialization-is-a-recoverable-cross-store-operation).
Setup/status routes stay available while readyz is503; readiness cannot be a
prerequisite to reach setup. No S3 I/O while holding the installation SQL lock.
Credentials/default chain run only in supervisor. TLS verification mandatory;
custom CA allowed, skip-verify not a production option.

Explicit startup order: config -> schema/installation compatibility -> storage
identity -> budgets/pools -> role recovery scan -> workers/schedulers -> ready.
Separate `migrate` command performs migrations once under existing advisory lock;
`run` refuses out-of-range schema and does not race DDL on every pod startup.
Release manifest advertises min/max reader schema and format versions, not just
latest migration number. Current binary only supports exact schema until a
rolling-compatibility test explicitly broadens the range.

## Budgets and bounded overload

| Profile | Initial reservations/caps within CPU1/512MiB cgroup |
|---|---|
| API only | wire/decompressed64MiB, parser/projection working256MiB, 2 decoder slots, password hash64MiB shared working reservation, Go GOMEMLIMIT352MiB |
| Worker only | native256MiB memory_limit, supervisor working64MiB, gateway8MiB, Go GOMEMLIMIT96MiB, one child |
| Combined | working pool192MiB shared by parser and native-task admission, wire32MiB, supervisor/gateway32MiB, Go GOMEMLIMIT224MiB; serialize decoder and native child when their reservations conflict |
| Scheduler only | working64MiB, Go GOMEMLIMIT128MiB; no native child by default |

Combined native task reserves the whole192MiB working pool and sets native
memory_limit192MiB; it does not also receive the worker256MiB limit. Parser
reservation formula from ARCHITECTURE still applies, so a legal large request
can429 under this small profile. Login shares the same byte pool; don't promise
all maxima simultaneously. Count actual Go graphs, SDK buffers, DuckDB unmanaged
allocations and OS overhead. These are admission policies, not RSS proofs;
G07 must demonstrate target workload and OOM containment or adjust within512MiB.

Shared disk budget4GiB with512MiB unused safety reserve. Spill cap2GiB, cache cap
1GiB soft, remaining active spools/outputs via one reservation manager; these
are not independent quotas summing beyond available disk. Before native work,
reserve estimated input/output+spill allowance; evict unpinned cache first, fail
resource_exhausted if insufficient. Track actual growth and kill task on excess.
Aggregate query tasks also reserve8MiB for optional warm small-input staging;
only already cached, verified files<=64KiB qualify, with8MiB total. Cold/large
inputs stay on the Range gateway. The reservation is part of the same shared
disk pool, and remains through native join and private-file cleanup.
No accidental use of system /tmp outside app's owned directory. Startup reaps
only task directories bearing a validated ownership marker; never broad rm/globs.

Role pool caps API8,worker2,scheduler2. Combined uses one pool cap12, not multiple
hidden copies. Global PG64 budget: reserve8 for migrations/admin/backup, admit
role replicas only if sum(max pools)<=56. Deployment/scaler computes bounds from
all role pools, including warm replicas; connection errors trigger admission
rather than exceeding budget. Transaction retry never holds a second connection
while waiting for its first pool slot.

## Child IPC and file boundaries

Extend existing probe-only engine protocol to version1 operations probe/convert/
query/reduce/compact. Parent writes a length-prefixed JSON control message
(4-byte big-endian length, max1MiB). Child emits bounded frames (same framing):
file_pair_ready (conversion/compaction only), then exactly one completed/error
terminal response. On file_pair_ready parent verifies and uploads that pair,
persists local manifest parts, removes only those owned output files, then sends
continue or abort on control input. At most one unacknowledged pair; no native
child uploads itself. A parent/pipe failure aborts the child. The terminal frame
plus successful process exit is required before Prepare; uploaded intermediate
pairs from an incomplete task remain unreferenced intents. Query/reduce emit
only terminal response. Large canonical input, manifests and
results are files referenced through supervisor-created handles/relative names
in the private task directory, never giant stdin/stdout JSON. Query result is
streaming JSONL with schema header, typed rows and checksum footer; aggregate
partials use typed Parquet to feed the reducer. Control response lists result
relative paths, byte counts, hashes, row counts and engine version; streaming
pair frames keep output disk bounded even for10,000 day partitions. Parent checks
exit0 plus terminal response/complete manifest; either alone is insufficient. Reject
symlinks/path traversal/unknown fields/operations. stdout is protocol only;
stderr bounded64KiB and sanitized. Child cannot select arbitrary filesystem roots.

Current conversion wire names are `bundle`, `summary`, and a versioned/indexed
`continue` acknowledgment, each with the4-byte/1MiB framing above. A missing,
truncated, repeated or count-inconsistent terminal frame fails the task even
when the process exits successfully. The supervisor verifies regular-file type,
declared size, fullSHA and blockSHA before invoking the consumer. It reaps on
consumer/protocol failure and does not acknowledge a failed upload. Other
single-response operations retain their existing strict JSON protocol; this
conversion implementation is not evidence that every operation has migrated to
the framed format.

Conversion reserves2457MiB from the shared disk budget before creating its task
directory: native spill2GiB, one output pair256MiB, journal24MiB, selected
stage64MiB, occurrence/manifest allowances64MiB and protocol overhead1MiB.
Staging rejects overflow before writing excess bytes. Successful pair consumers
may retain files only by moving them to their own accounted storage before
returning. Reservations are released only after joined work and successful
task-directory cleanup; this conservative admission calculation is not an
observed filesystem or RSS measurement.

Parent constructs capability allowlist from exact catalog manifests. Gateway
binds loopback with per-task256-bit random handles, validates method/path/range/
task deadline, and logs no handle. Revoke on cancellation/expiry; every block
request checks task still live. Cap8 active range requests/child and1MiB block
buffers within shared8MiB. HEAD contains exact length, range responses206 with
verified blocks, invalid/outside ranges416. FullSHA is verified on full spool;
partial reads verify blockSHA, exact last-block size and pinned object identity.
No child AWS environment, no instance metadata route, no unapproved extension
autoload/network install. Use deployment network policy plus restricted child
environment; loopback gateway alone is not a general filesystem/network sandbox.

One child owns one task; context cancellation interrupts native call, SIGTERM,
then SIGKILL after100ms and wait/reap before reclaiming permits. Native children
produce only private scratch, not durable publication; this bounded cooperative
exit avoids reserving unusable multi-second credits in a busy worker. Process-group
termination kills descendants. cgroup OOM tests must verify the supervisor/API
survive; use separately bounded child cgroup/container if needed in deployment.
If combined-role512MiB cannot isolate survival, fail G07 combined profile instead
of claiming separation guarantees it. Never leave orphan child work on scale-in.

## Compaction and retention transactions

Compaction candidate: same tenant/lane/schema/grouping version/event_day/kind,
current generation and no active reservation. Prefer >=8 files smaller than8MiB
or predicted at least50% GET reduction, output target32–64MiB, max input256MiB
compressed/128 bundles/task and estimated scratch allowance. Maximum one active
maintenance task/lane. Small quiet datasets need not be rewritten forever.
Candidate ranking compares the actually selectable prefix of each partition:
oldest generation/ID order, at least8 inputs, stop after first reaching32MiB,
never above128 inputs/256MiB. Prefer the largest prefix count (expected paired
file/GET reduction), then the fewest rewrite bytes; use tenant/lane/partition
order only for ties. Ranking an entire unbounded partition would overestimate
the benefit of a prefix cut off by the size target. PostgreSQL returns only the
winning prefix, at most128 rows. Reservation/fence checks remain authoritative;
selection does not promise fairness or alter the measured spare-time budget.
Scheduler pauses maintenance if ingest oldest>5s or query queue>0.5s; at most20%
of measured spare worker time per rolling60s, no stealing reserved ingest slots.
Both compaction and retention selectors exclude lanes already holding a queued,
running or prepared maintenance task. The reservation transaction remains the
authority if another scheduler races the candidate read; it never steals work.

App owns this dispatch budget, not a new maintenance service. A complete empty
foreground sweep earns credit only for the subsequent measured idle wait. A
delayed timer earns at most its requested100ms, not unobserved descheduled time. A
shared native helper starting during that wait invalidates the sample. Retention,
compaction and GC share61 fixed one-second buckets; partial oldest idle buckets
are discarded and partial oldest work buckets are fully charged. Work allowance
is I/4-M (20% of I+M), forecasting idle-credit expiry through the end of the
grant. There is no initial free burst. Foreground claims always run first.
The task deadline leaves200ms of the grant for cancellation/join:100ms native
termination grace plus100ms joined cleanup/scheduling margin. This is not a
cleanup timeout. Actual time until return, including no-work/failed claims and
cleanup, is charged. Aging credit or
a slow join can create debt: it denies further admission, never releases a live
permit or retrospectively erases work. A join overrun is observable and fails
comparison verification. This wall-time admission policy is not a hard CPU-time
ratio for every retrospective sample. Budget cancellation leaves the durable
lease to expire/recover with its existing fence rules; it cannot abandon live
native work or adopt an unfinished result. A budget-deadline failure delays that
fixed operation slot for one60s accounting window, allowing fresh measured idle
credit to accumulate instead of repeatedly canceling the same partial rewrite.
Ordinary failures retain250ms retry delay; foreground and other maintenance
slots remain eligible. Workers recheck the pressure predicate
in the transaction claiming queued/prepared/expired compaction and retention
work, since pressure can start after reservation. Publication/delivery keep
their separate joined loops; GC's fresh-backup interlock remains mandatory.

Reserve transaction locks lane then task and inputs: record selected bundle IDs,
their exact valid_from and input identity hashes, set reserved_by; do not close
catalog intervals yet. Read inputs with a maintenance pin and verify pairs.
Input metadata/file sizes are loaded with one bounded, caller-ordered locking
query; reservation updates and input insertion each use one set operation whose
affected count must equal the full request. Missing, foreign, closed or already
reserved inputs roll back the whole transaction. The existing sorted JSON identity
hash is byte-identical; neither batching nor a partial success may change it.
The fenced control loader permits exactly one retention input and two to128
compaction inputs. It reads ordered project associations with the reserved
metadata and all referenced analytics/payload pairs in one bounded file query;
input count must not introduce a database round trip per bundle. A missing,
duplicate or unknown file role fails closed. This is a control transaction,
not a new workflow/repository layer; native pair verification remains required.
Native merge preserves canonical values, IDs/seq/received time and grouping
version, changes physical layout only. Upload outputs with fenced intents.
Native input verification scans analytics and payload roles in two ordered
queries, validating each input's exact identity/count separately (not only the
union) and every analytics row's scope. Go retains at most128 counters and one
streaming hash; native sorting uses the existing memory/spill limits. Explicit
scan provenance must not be replaceable by a Parquet filename column. Input
full-SHA verification remains at the download boundary; output SHA/block and
pair evidence remains mandatory before upload/Prepare. Do not recalculate input
full hashes merely to discard them. File128MiB and total256MiB bounds still apply.
Wide native rewrites must not keep a full sorted value vector beside the Parquet
writer. Small inputs retain the direct COPY path, selected by actual Parquet
uncompressed metadata, not compressed file length. Larger inputs materialize
only scalar sort keys and byte sizes, partition into4MiB key ranges with a4KiB
minimum row charge, and write at most eight ranges per native invocation,
further bounded to one range per24MiB of managed native memory (four at96MiB). At
most1,024 intermediate partitions are admitted. Their conservative byte
reservation is subtracted from the existing native spill allowance; insufficient
budget fails before writing partitions. Actual partition counts/bytes and paths
are checked before numeric-order concatenation into one paired replacement.
Intermediate canonical columns never round-trip through JSON. Analytics sizing
counts UTF-8 bytes of every typed variable column and nested string, with4KiB
per-row,256B per-attribute and32B per-list-element overhead; payload uses its
existing string byte lengths. It does not allocate complete-row JSON solely to
measure size. The2x intermediate allowance and actual manifest checks remain
mandatory. App caps compaction/retention managed native memory at96MiB (or the
smaller role/request limit), independently of conversion/query. This reserves
space for native allocations outside that managed limit and does not promise
headroom in the512MiB process cgroup. Final native scans still prove exact identity, statistics and
SHA/block evidence. The repeated scans are sequential reads of already verified
local inputs, not additional S3 downloads; this trades local I/O/latency for
bounded native buffers and requires matched measurements, not a speedup claim.
The engine owns these private files and removes them only after the native call
has joined; app continues to own child lifetime and maintenance admission.
Conversion and maintenance share physical order `(project_id, service NULLS
FIRST, event_time_us, record_id)` for both files. Receipt/lane order remains
authoritative for publication and retention boundaries, not physical layout.
Payload does not persist these extra layout columns: the engine materializes
only retained analytics IDs/project/service/event-time keys once, reuses that
bounded native table during payload output, and projects the original five
payload columns at final COPY. Never reconstruct sort keys from raw JSON or
today's normalizer. Exact per-input and final pair verification still applies.
Swap transaction locks lane/task/intents/bundles, rechecks live tuple and **each
reserved input still current**; unrelated newer publications may exist. Increment
current catalog_generation, close only reserved inputs at G, insert replacements
at G, release reservations, complete task. Do not require the whole lane's
generation stayed unchanged (that would starve compaction under ingest). Do not
advance published_seq. Failure before swap leaves originals current; retries
never retire unrelated files.

Retention advances persisted monotonic installation floor every60s; policy changes
preserve old-policy expiry before installing a new policy (correctness C07).
This is logical visibility for **new snapshots**; widening retention does not
resurrect data. Scheduler scans current bundles by received bounds. Fully older
bundles close their interval without replacement; mixed bundles use same reserve/
rewrite/swap protocol keeping received>=cutoff. Existing snapshots keep their
old floor/generation and pin old files until expiry. Snapshots created during
rewrite use either complete old or complete new catalog under lane locks.
Occurrence detail rows delete only after no snapshot can need them; lifetime
Issue summary remains. Dedupe/receipts retain at least retention+7days and until
no pending job/outcome/occurrence FK needs them; expiry is minimum, not automatic
cascade deletion. Remove referenced child/parent rows explicitly in bounded
transactions (<=1,000 records) while preserving batch recovery metadata. Follow
C07's retirement transaction to release the journal FK only for a fully retired
published batch; a permanent FK/reference cannot coexist with claiming its
journal is eventually collectible. Pending/failed batches never release it.
On retention increase, extend existing dedupe/receipt protection to at least
received_time+new_retention+7days. Accept's expired-dedupe check uses the maximum
of stored expires_at and that current-policy bound, so it remains safe while
background extension is incomplete. Reducing retention never shortens already
promised dedupe protection. Snapshot/backup protection can extend it further.
Completed job/output rows are historical metadata, not perpetual object pins:
only unfinished/prepared jobs protect temporary output refs; published catalog
and backup references independently protect their live objects.

## Object lifecycle and backup interlock

GC is mark -> delete -> confirm, never LIST -> blindly delete. Eligible means:
not current catalog, no live snapshot generation referencing it, no job/prepared
output/task pin, no retained receipt/journal replay need, no protected backup
window, and past conservative retirement grace. GC locks installation shared,
lane(s), intent; rechecks authoritative refs, marks deleting with fence and
commits. Only then issue S3 DELETE. Readers never acquire a new pin after mark.
For retired published objects require retired_at+max(8days,configured backup
protection) and dynamic backup interlock. Intent expires_at is only for never
referenced uploads. Journals remain protected until batch publication **and**
all above recovery conditions; they are not disposable at ACK or conversion.

Backup interlock: freeze physical deletion when backup health/oldest recoverable
point is unknown. Record verified backup sets/PITR horizon and conservative
object protection time. Any retained manual/base backup extends horizon until
explicitly expired; never claim8days protects an indefinitely retained backup.

The implemented gate is installation `gc_safe_before` plus `gc_verified_until`:
both default NULL. A signed M4 isolated-restore report establishes the horizon
for at most 24 hours. There is deliberately no CLI/env switch to bypass it.
GC chooses bounded eligible candidates before lane locking, rechecks eligibility
after locks, and confirms the exact `gc_attempt`; it never updates the entire
retired-intent table ahead of lane locks. Tombstone resweeps remain fail-closed
when verification expires. Completed conversion producer tuples are cleared
atomically before deleting completed job metadata; catalog refs remain intact.
A backup begins under installation coordination before export and registers
protection before a GC pass can mark needed objects. Snapshot its reference
inventory, including pending journals/prepared outputs, not only current files.
For WAL recovery between inventories, preserve every object referenced at any
point in the retained horizon using retirement-time protection. Delete backup
set only after its replacement and WAL continuity are verified.

Unknown LIST objects are quarantined for operator inspection, not auto-adopted.
Tombstones retain installation/key/generation/expectedSHA. Periodic paginated
inventory resweeps deleting/deleted keys to remove late stale PUTs. Keep
tombstones while the generation's writer credentials could still complete a
write; default never prune until generation retired and credentials revoked.
Abort owned multipart uploads after24h only when no live intent/worker owns
them; backup prefix/foreign prefixes excluded. Provider retries bounded, missing
object on DELETE counts success, forbidden does not. Metrics distinguish live,
retired protected, reclaimable and orphan bytes.

## Backup and restore runbook specification

Use pgBackRest with pinned image/tool version and tested PG17 support, daily
base backup plus continuous WAL; live-object and backup-delete roles separated.
S3 versioning alone is not coordinated PG recovery. No independent lifecycle
rule deletes journals/bundles. WAL age>60s warning, >5min readiness for backup
status degraded (ingestion can remain ready with explicit RPO warning); oldest
restorable age and last isolated rehearsal displayed. Every24h perform isolated
restore rehearsal with outgoing network denied and alerts paused.

Implemented recovery CLI (see the [operator runbook](../operations/recovery.md)):

```text
eventglass-go doctor --read-only
eventglass-go repair inspect --tenant <id> --lane <0..15>
eventglass-go repair retry --job <uuid> --expected-fence <n>
eventglass-go backup register --backup-id <uuid> ...
eventglass-go restore verify --backup-id <uuid> --expected-generation <n> --verification-id <uuid> --recovery-lsn <lsn> --report <new-private-path> --attestation-key-file <private-key>
eventglass-go backup verify --expected-generation <n> --report <private-path> --attestation-key-file <private-key>
eventglass-go restore activate --expected-generation <n> --report <private-path> --attestation-key-file <private-key>
```

Inspect is default; retry never skips a seq/changes selected records. No force
publish, reconstruct-from-LIST or drop-lane command. Mutating CLI requires admin
DB role, explicit scoped arguments and records audit event. Verification report
stores checksum/installation/PG recovery point/object inventory/generation/test
outcomes, no credentials. Reports are evidence artifacts, not stage diaries.

Disaster procedure:

1. Stop API/workers/schedulers; isolate old DB endpoints and revoke old writers'
   S3 credentials/network access. Confirm no old process can reach restored DB.
2. Restore PG base+WAL in isolated environment to explicit time/LSN, preserving
   original installation_id and storage identity. Record achievable RPO; lost
   commits after that point are not recovered merely by finding newer S3 bytes.
3. Keep recovery_state=restoring, alerts_paused=true, external delivery denied.
   Verify every restored referenced journal/file/prepared output: size/SHA,
   scope, schemas, selection, file-pair identity, counter/job consistency. Query
   temporary results may be invalidated; durable accepted data may not be lost.
4. Increment storage_generation; revoke all sessions/tokens/leases, fence jobs
   and tasks. Requeue unfinished conversion/publication from original receipts
   and valid prepared artifacts. Cancel transient user queries/snapshots. Record
   old object's creation generation separately: new readers may read verified
   old-generation objects; new writes/authority use new generation. Do not reject
   all historical files just because their intent generation is older.
5. Replay pending journals without renormalizing, verify contiguous cuts and
   known ACK oracle up to recovery point. Missing/corrupt durable refs keep
   unhealthy; provide exact object/job failure, never empty successful query.
6. Activate only matching verification report/current generation with fresh
   credentials. Outgoing alerts stay paused until admin explicitly resumes;
   pending deliveries may repeat after disaster, preserve stable delivery IDs.
7. Rehearse SDK ingest/read/detail/Issue mutation/retention/snapshot/restart;
   remove only isolated test resources with ownership markers. Production
   activation is never implied by push or a successful local test.

## Scaling and release evidence

Separate ingress, ingest-worker, query-worker, maintenance pools use same image,
explicit role/pool labels (worker capability config ingest/query/maintenance).
Warm ingress and query min1; ingest min1 when pending work, maintenance min0.
EWMA throughput alpha0.2 every10s with conservative prior1MiB/s/worker, convert
queued bytes and oldest age into DESIGN drain targets. Out2 samples, in300s,
max25% reduction; clamp PG budget and configured worker max. If dependency error
ratio>20% over30s or pool wait p95>1s, freeze scale-out, circuit-break new work
with jittered probes every5s; don't hide stalled age. Tenant round-robin claims
max1 child/tenant/worker and max4 global query tasks/query; document that global
fairness is approximate until G07 measures skew across replicas.

The implemented `deploy/kubernetes/base.yaml` baseline selects Linux ARM64 at
release scheduling time and fixes resource requests/limits, startup/
readiness/liveness, grace30s, private PG/S3/gateway network, non-root read-only
root, bounded scratch, secrets mounts. Query scaler metric comes from queued
estimated bytes/service-rate target, not merely jobs count. Min/max and pool
connection equations must agree across rendered manifests. Compose is local
demonstration, not host-HA/autoscaling guarantee. Provisioning cloud resources
requires separate user authorization; commit manifests only.

Release matrix requires real AWS S3 and one proven self-host backend (Garage
candidate, MinIO local fixture is not production endorsement), cold/warm cache,
1/2/4 workers and whole-installation costs. Exact dataset/engine/images/host/
cgroup/network/settings/seeds/SHA recorded by harness. Numeric targets in DESIGN
must pass or release stays blocked. Schema rollback requires demonstrated reader
compatibility or coordinated restore; never run Down blindly on live data.

Open assumptions are **experiments**, not implementation choices: native peak
RSS/128MiB output sizing, cgroup OOM survival, throughput/cost, provider multipart/
Range semantics, PITR object protection and rolling-version reader compatibility.
Their owner, test and failure behavior are explicit in the work plan. No stable
DuckDB2.0 release claim: the currently approved official prerelease stays pinned.
