# Durable ingestion, publication and Issues

Inputs are the existing sdk/model/ingest types. Extend their boundaries without
rewriting successful normalization fixtures. The SQL contract is in
[control-plane](control-plane.md). Tests and implementation order are in
[work-plan](work-plan.md).

## Request lifetime and batching

1. API validates transport/auth, reserves shared ingress/working bytes, parses
   and scrubs using a versioned immutable project policy snapshot.
2. Generate acceptance UUID once; normalize into one immutable Command. The
   authority snapshot includes tenant/project/key/config/scrub revisions. Do not
   put key hashes or policy authorization into canonical records/S3.
3. `ingest.Accept(ctx,command)` synchronously registers with a bounded batcher.
   The call does not return while the batcher retains its Command. The handler
   continues to own/release the parsed-data permit after Accept returns. Waiting
   callers canceled after batch sealing wait for ownership to end; cancellation
   can suppress HTTP output but cannot free a live upload's memory.
4. Queues are keyed by tenant/lane, allocated on demand, capped by shared bytes
   and 1,024 waiting requests/API (whichever limit first). Fair round-robin across
   active tenants; a full queue returns429 before transfer. No unbounded channel.
   On first arrival arm a 100ms timer. Seal before adding a request that would
   exceed 4MiB canonical/1,000 records/1,000 requests; enqueue that request next.
   A single larger legal request seals a dedicated batch. A batch never splits
   a request or mixes tenants/lanes. Zero-record requests count toward request
   cap and timer and use the same durable path.
5. Write sanitized Zstd journal into a 0600 file in an app-owned 0700 temporary
   directory. Reserve up to the codec's 24MiB framing bound before writing;
   wire/canonical20MiB limits still apply separately. Close+rewind immutable file,
   compute compressed size/SHA; no fsync is needed for ACK because local disk is
   not the durable authority. Failed spool is never uploaded or accepted.
6. Register intent, upload, verify, Accept transaction, deliver receipt results,
   then release request/spool ownership. At most2 simultaneous upload workflows
   per API initially, with SDK automatic retries capped to3 attempts and total
   request deadline30s. Each actual application upload attempt uses a fresh
   batch UUID/key/intent; SDK retry of one identical PUT is the same attempt.
7. On process drain close admission, seal queued batches, wait within30s, then
   cancel workflows. No ACK after an unconfirmed commit. Server read-header
   timeout5s, body/read timeout30s, max headers32KiB, write timeout35s for SDK
   routes; SSE has a separate response policy.

Cancellation before sealing removes the waiting request without affecting other
callers. After sealing, workflow context belongs to app, bounded by the earliest
30s workflow deadline rather than one caller disconnect; all waiters learn their
own result. Ensure drain can cancel it. Do not spawn detached goroutines from
handlers. Admission reservations include queued graphs and pending uploads.
The receipt response value is acceptance_id, not batch_id. Returning nil from
the old test Acceptor is not proof of durability; production adapter must return
a ReceiptResult with counts/received_time/commit confirmation.

## Two different content hashes

Receipt content_sha is the existing journal request checksum. It includes
canonical occurrence identity and diagnostics, but normalizes physical ordinal
ranges for rebatching. Do not replace it with a hash of the batch or JSONB.
Extend WriteJournal's result with per-request immutable index metadata (scope,
range, request checksum, counts). Extend ReplayJournal to return the same
validated index after EOF. Metadata is not trusted if replay returns an error.
Preserve existing v1 encoded bytes; this is a Go return-type extension, not a
new wire format. Compare replayed index with the committed receipts.

Source-event equivalence hash v1 is SHA-256 of a fixed-order JSON struct:
`{version:1,kind,source_event_id,raw}`. raw is the scrubbed original event with
top-level event_id removed (effective ID is already explicit); recursively sort
object keys, preserve array order, preserve json.Number lexical form, disable
HTML escaping, omit terminal LF. Do not include acceptance/record ID, arrival,
received time, envelope sent_at, warnings, policy revisions or batch metadata.
This is conservative structural identity: 1 versus 1.0 and different scrubbed
payloads are conflicts, not overwrites. Same sanitized payload on a later HTTP
retry is duplicate even if its timestamp was missing. Schema versions are
recorded separately and old receipts are never rehashed with a new algorithm.
Only error/transaction with valid SourceEventID uses this hash. Logs and missing
source IDs always select a new candidate. No message/trace dedupe.

## Intent workflow

Intent registration is a short committed transaction with installation/generation,
tenant, immutable object key, expected size/SHA, owner random process UUID,
fence1, state pending, expires_at DBnow+10min. Before upload check authority is
still live. After PUT, verify Head size+own SHA metadata; local hashing and
the provider transport establish uploaded bytes, while replay/block verification
detects later corruption. ETag alone never establishes SHA. Mark uploaded only
with matching tuple and pending state and live expiry; failed conditional update
means orphan, not permission to call Accept.

Never reuse keys after a different attempted payload. Persisting uploaded state
is not ACK. GC can transition expired pending/uploaded to deleting; Accept rejects
deleting/deleted/expired/stale-generation intents. Failed/uncertain PUTs leave
inspectable intent records. See operations for late PUT tombstones.

## Accept transaction algorithm

`VerifiedBatch` contains journal index, expected object metadata, Command auth
snapshots, canonical candidate identities/dedupe hashes and diagnostics; no SQL
or provider request objects. The operation validates every count before Begin.

1. Lock installation/tenant shared; validate generation and active tenant.
   Look up all acceptance IDs. If all exist with matching tenant/project/content
   hashes, return original receipts without assigning another seq. Their stored
   batch identity may differ after rebatching; content identity is authoritative.
   If any content differs, return idempotency_conflict. If only a subset exists,
   abort with internal already_accepted subset; coordinator removes those IDs,
   rebuilds/uploads the rest and returns original results for the subset. Never
   insert the existing IDs into another batch or force mixed replay into SQL.
2. Lock all project/key rows in sorted project/key order FOR SHARE. Revalidate
   enabled state and exact revisions, including current scrub/config policy.
   Lookup/retry of already committed receipts does not re-authorize old data,
   but HTTP requests always pass ingress auth before this internal retry path.
3. Lock target lane FOR UPDATE. Recheck acceptance IDs after waiting to handle
   concurrent same-ID retries. Lock/claim source keys ordered by project,kind,ID.
   Use INSERT ON CONFLICT DO NOTHING followed by SELECT FOR UPDATE in READ
   COMMITTED. Do not assume a row returned by ON CONFLICT is this transaction's
   winner. A missing concurrent row requires bounded transaction retry.
4. Expired dedupe row may be replaced under its row lock. For repeated source
   IDs within this batch, first journal position wins; compare following hashes
   against it. Existing same hash -> duplicate; different hash -> conflict.
   Both duplicate/conflict are ACKed, zero new rows, visible counters; neither
   overwrites. Accepted selection covers all other candidate positions.
5. seq=accepted_seq+1; received_us=max(floor(DB clock epoch*1e6),last_received_us).
   Check overflow. Insert batch, one receipt per request, outcomes/unsupported
   metadata and one queued convert job even if accepted_count=0. Unsupported type
   diagnostic strings longer than128 UTF-8 bytes become a rune-safe first100-byte
   prefix plus `#` plus first16 hex chars of SHA256(original type); normalize this
   before journal encoding so SQL and receipt hashes match without rejecting
   correctly framed unknown items. Repeated SDK
   outcome category/reason for one item is summed with checked integer addition.
   Insert dedupe claims referencing these receipts (deferrable FK allows order).
6. Lock intent; verify expected hash/size, state uploaded, tenant, installation,
   generation, owner/fence and live expiry. Reference it, update lane counters,
   COMMIT. SQL errors roll back selection/counter/claims together. Only confirmed
   commit or confirmed receipt lookup permits HTTP200/receipt header.

Bounded SQL retries preserve command IDs, journal and hashes. If auth becomes
stale, rollback **everything**. Return rejected request IDs to coordinator;
rebuild still-valid requests retaining acceptance IDs. Policy changes use the
same rule; v1 chooses **reject changed-policy requests with503**, without keeping
raw input for renormalization. Their next HTTP attempt receives current rules.
This closes the optional renormalization path in DESIGN without retaining secrets.
Project/key disabled/revoked map to403/401. Unknown commit uses fresh receipt
lookup; unreachable PG returns503. An SDK may retry ID-less records and duplicate
them after an unknown reply; do not advertise cross-HTTP exactly once.

## Conversion and durable preparation

Claims use SKIP LOCKED queued jobs due now, or running jobs with expired lease.
Prioritize each lane's next unpublished seq, then following seq, round-robin
tenants. Atomically increment fence and attempt, set owner/generation and60s
lease. Heartbeat15s conditioned on live tuple; clock is PostgreSQL. Child work
stops on failed heartbeat. Initial maximum8 automatic attempts with jitter
1s..60s; dependency outage opens a circuit rather than burning all attempts.
Corrupt journal, unsupported version or impossible selection fails immediately
with a durable code and blocks the lane. Explicit repair requeues same job with
next fence; never generates new identities or skips ACKed input.

For one claimed batch:

1. Download exact committed journal into a quota-owned spool; size/SHA verify.
2. Replay and compare complete request index/selection hashes/counts against PG.
   Feed selected records only into a staged child conversion stream. Since a
   bad final line can invalidate earlier records, staging cannot be prepared
   until ReplayJournal completes successfully. Overlay PG seq/received time;
   retain arrival/event time and versions. Compute pure Issue grouping only for
   errors, using receipt's grouping version. Never consult today's scrub config.
3. Native task appends bounded chunks (2,048 rows or4MiB, whichever first), using
   spillable tables then deterministic partition/sort/COPY per partition. Do not
   keep an open writer per project/day. Iterate distinct partitions using a
   bounded cursor and finish/upload one pair before opening the next. Up to
   selected_record_count bundles can exist for extreme day/kind cardinality;
   metadata uses paged job_output_parts, not an unbounded Go slice. G03 must
   publish a legal10,000-day input (possibly slowly), not poison an ACKed lane
   with a newly invented partition-count cap. No minimum file size blocks it.
4. Create paired analytics/payload, verify set identity/counts/schema, compute
   bounds/fullSHA/block hashes from finished files. Zero accepted rows yields
   an empty manifest and no child/COPY/Parquet.
5. Upload each output using unique intents and keys
   `v1/{installation}/bundles/{tenant}/{lane}/{job_uuid}/{fence}/{bundle_uuid}/{role}.parquet`.
   Manifest records exact input batch/hash/selection, all output refs and stats,
   grouping version and aggregate accepted counts. Total selected record set
   must match outputs; duplicate ID or missing output is fatal.
6. `control.Prepare` checks job live authority and all verified intents, inserts
   immutable job_outputs, references outputs as prepared, sets job prepared and
   clears owner/lease. No catalog visibility, Issue changes or cut movement yet.

One batch per conversion is a deliberate v1 simplification. Small outputs are
time-flush files, not a failure of the steady-state32–64MiB compaction target.
Large partitions split before128MiB. Never wait to ACK/publish for target size.
No cross-batch merge in converter; later maintenance merges contiguous or
noncontiguous current bundles without changing logical seqs.

## Publication claim and transaction

ClaimPublication takes lane lock first, selects prepared job at published_seq+1,
locks job then intents, increments job fence, sets running owner/lease. It binds
the existing immutable prepared_output_id to this new authority and rebinds
referenced intents' owner/fence; it does **not** authorize rewriting object bytes.
Prepared outputs remain GC-protected through the job reference. Expired running
jobs with prepared_output_id are publication retries, not conversion reruns.
Recovery may re-HEAD/verify outputs before publishing; corruption fails closed.

Publish transaction (no external I/O):

1. Lock installation/tenant (tenant need not be active), lane, job/intents in
   order. Match current generation/live lease/fence/owner and manifest SHA.
   Completed job with identical output is no-op; conflicting output is an error.
2. Require batch_seq=published_seq+1 and accepted state. Require output identity
   evidence/counts and all referenced inputs still agree with receipts. No new
   project/key checks; ACK survives revocation/disable.
3. Lock/create Issues ordered by issue_id. Insert unique error occurrences;
   lifecycle deltas count inserted rows only. Apply the transitions below.
4. At G=catalog_generation+1 insert bundles/files/block hashes/project members.
   Set output published, batch published, job completed+clear lease. Insert
   unique issue_transitions/outbox source events in same tx. Set published_seq
   and generation G. Empty publication still increments both counters, creates
   no catalog file/Issue/outbox. Commit all or none.

If N+1 is prepared first, leave it prepared with no active child/lease. Other
lanes continue. Unknown commit rechecks job output identity before retrying.
Exactly-once catalog/Issue application uses durable unique keys, not notifications.

## Grouping and Issue transitions

Group bytes v1: fixed JSON array `["eventglass-grouping-v1",project_id_string,
components]`, UTF-8, JSON escaping with HTML escaping disabled, no LF.
Default components are `["stack",chain_types,frames]`, frames each
`[module_or_empty,function_or_empty,slash_normalized_filename_or_empty]`;
without frames `["exception",type_or_empty,value_or_empty]`; without exception
`["message",template_if_present_else_message]`. Apply representative/last8/in_app
rules from DESIGN. Explicit fingerprint becomes `["custom",expanded_items]`;
each literal is `["literal",value]`, each exact default marker expands to
`["default",default_components]`; empty array uses default directly. Nested
arrays prevent delimiter collisions. Freeze exact bytes with Unicode/escaping
goldens. SHA is issue_id, includes full project and version. Title is message,
else representative type/value, truncated to512 Unicode code points after scrub.

| Current state | Action | Next state/effects |
|---|---|---|
| Absent | First unique published error | unresolved, revision1, created transition |
| unresolved | Unique occurrence | Count/first/last/activity update; no transition |
| resolved | Occurrence seq <= resolved_cut[lane] | Remain resolved; count updates |
| resolved | Occurrence seq > resolved_cut[lane] | unresolved; revision+1; one regressed transition |
| ignored | Any occurrence | ignored; count updates; no regression |
| Any | Authorized resolve at expected revision | Capture accepted cut under all16 lane locks, resolved, revision+1 |
| Any | Authorized ignore/reopen at expected revision | ignored/unresolved, revision+1, clear cut on reopen |

Revision is lifecycle revision, not every count update; use it for status CAS
and unique transition delivery. Same-state commands with matching revision are
no-op; resolve-while-already-resolved does not move the original cut. Status
mutation retries use expected revision; if prior commit succeeded, return409
with current revision unless supplied operation id matches recorded audit event.
Occurrence updates compute min/max event tuple and associated release together,
not independent min(time)/max(release). Retained lifetime summaries survive
physical retention; detail can explicitly be expired.
