# Management API, UI and alerts contract

This is the source contract for the **planned** `api/openapi.yaml` (OpenAPI3.1).
Generate Go wire DTOs and TypeScript client/DTOs from that file in packet Q1;
do not generate domain/storage models or hand-maintain duplicate frontend types.
Select maintained generators at implementation time, pin exact versions in
tool locks, and prove deterministic regeneration produces no diff. Route/field
changes require updating this contract, schema, generated code and contract tests
in one commit. Existing Sentry ingestion routes remain compatible.

## Common wire rules

HTTPS public origin; JSON UTF-8, management request body<=64KiB (setup/login8KiB),
unknown JSON fields rejected, duplicate keys rejected. Lists have default100,
max1,000 unless noted; opaque cursor rather than offset. A missing optional
field is distinct from explicit null. Patch APIs explicitly list clearable
nullable fields; other null values400. UUID/hex IDs are lowercase strings.
All signed64 counts/IDs/time microseconds, exact integers/decimals are decimal
strings. limit/ordinal/lane/ns_remainder are bounded JSON integers. Double values
are finite JSON numbers. Timestamps are never JavaScript numeric microseconds.

Error response `{code,message,retryable,request_id,details?}`. details only safe
field paths/offsets, current revision, limit name, or retry_after_seconds. No SQL,
raw request, stack, DSN or S3 key. Codes/status:

| HTTP | Code/use |
|---|---|
| 400 | invalid_input, invalid_filter, invalid_token, unsupported_operation |
| 401 | unauthenticated (management), unauthorized key (SDK) |
| 403 | forbidden, csrf_failed, disabled scope |
| 404 | not_found or inaccessible opaque resource |
| 409 | revision_conflict, generation_changed, setup_complete |
| 410 | snapshot_expired, record_expired |
| 413 / 415 | payload_too_large / unsupported_encoding |
| 422 | numeric_overflow, query_limit_exceeded, unsupported_group_type |
| 429 | admission_limited; Retry-After initially1s |
| 503 / 504 | dependency_unavailable, query_failed / query_timeout |

All management success/error responses set Cache-Control:no-store. Requests
carry/generated safe X-Request-ID (UUID); reject arbitrary reflected strings.
Mutation bodies carry revision decimal string except creation/login/setup.
Return new revision. Standard lists `{items,next_cursor:null|string}`.
DELETE successful/no-op204; no response body. No public SQL, arbitrary upload,
remote URL fetch, backup download or repair mutation endpoint.

## Sessions, setup and permission matrix

Setup bootstrap token32 random bytes, mounted file, compare stored SHA-256;
not in logs/CLI args. `POST /v1/setup` follows the recoverable reserve-marker-
finalize protocol in [correctness C08](correctness.md#c08--initialization-is-a-recoverable-cross-store-operation).
The final SQL transaction creates first tenant/admin/16 lanes and consumes
token. Conflicting concurrent setup409. It never treats PG/S3 auth errors as empty
installation. Request `{bootstrap_token,email,password,tenant_name}`; response201
Session. Setup API disabled after success, even after process restart. New tenant
creation beyond first tenant is an offline admin CLI operation in v1, not public
self-registration. No email/password-recovery service or SSO promised.

Passwords: x/crypto/argon2.IDKey, random16-byte salt, initial64MiB/time3/threads2,
32-byte output, PHC encoded with version/parameters. Permit one hash/API with
64MiB reservation shared with ingress; login queue<=8,429 on pressure. Length
12..1,024 UTF-8 bytes, no silent truncation/normalization. Fixed dummy hash path
for unknown user; generic401. Initial login limit5/min per IP+normalized account,
20/min per IP, DB-backed fixed windows with bounded TTL to work across replicas;
record only hashed bucket keys, no raw credentials. Proxy IP accepted only from
explicit trusted proxies. Parameter values are initial project policy, not a
claim that library docs benchmark this workload. Use library constant-time
comparison and parameter upper bounds when loading PHC hashes. See official
[Argon2 API](https://pkg.go.dev/golang.org/x/crypto/argon2) for primitive usage.

Session secret32 bytes, SHA only in PG; cookie `__Host-eventglass_session`,
Secure/HttpOnly/SameSite=Lax/Path=/, no Domain, expires24h with no sliding extension.
Derive stable CSRF token from that session's cookie secret using C05, persist only
its hash, and return it in authenticated session DTO; client memory only. Session
reload does not rotate CSRF or invalidate another tab. Mutations require exact configured Origin and
X-CSRF-Token (including logout); setup/login instead require Origin and JSON
content type. CLI login may omit Origin only with explicit same-origin API mode
disabled by default; don't silently bypass CSRF for missing Origin. Local HTTP
development uses an explicit loopback-only insecure-cookie mode and different
cookie name, never automatically for arbitrary HTTP public URLs.

| Ability | Viewer | Project operator | Tenant admin |
|---|---|---|---|
| Read assigned project, Issues, records, Live, alert rule/status | Yes | Yes | All tenant projects |
| Resolve/ignore/reopen Issue, edit own project's alerts | No | Yes | Yes |
| Retry failed delivery in project | No | Yes | Yes |
| Create/disable project; create/revoke key; scrub/origin/service policy | No | No | Yes |
| Users/memberships/grants, destinations, tenant system/audit | No | No | Yes |
| Installation retention policy | No | No | Installation admin only |

Each request chooses one tenant membership; no multi-tenant search. Every
requested project must be allowed;403 instead of silently narrowing. Admin cannot
remove/disable the last active tenant admin. Password reset by tenant admin is
allowed only for users whose memberships are solely that tenant; cross-tenant
users require offline installation-admin reset. A tenant admin also cannot
globally disable a user belonging to another tenant; it can remove only that
user's local membership/grants (role=null in the membership patch). State/password
changes across memberships require installation admin through offline CLI.
No password appears in responses.
Membership/grant updates immediately bump user's auth revision and invalidate
old scope state, not the session credential itself. Session credential_revision
separately controls password/logout invalidation (C05). Users may change own password with current password; revoke all
sessions and require login. Destination secrets and DSN key creation return
secret once; lists never reveal full values. Public SDK keys authorize ingestion
only. Default project CORS denies browser origins until configured (server SDKs
without Origin still work); exact scheme/host/port allowlist, no wildcard default.

Scrub policy DTO v1: `{version:1,redact_keys:[],redact_paths:[],body_patterns:[]}`.
Each list max32, key<=128 UTF-8 bytes, path<=1KiB RFC6901 rooted at each supported
raw record, pattern<=1KiB valid Go RE2. No replacement templates/backreferences,
custom code or "disable default rules" toggle. Key match is case-insensitive
exact after trim; default suffix/header/URL protections in existing Scrub remain
mandatory. Paths select whole subtrees, replaced with `[Filtered]`; missing path
no-op. Body patterns replace matches in every string leaf with `[Filtered]` in
configured order; never recursively rescrub replacements. Process built-in,
installation, then project rules. Installation rules are mounted config and
fingerprinted in installation policy; change requires coordinated revision bump
for all projects before ingress resumes. No replica may use different rules
with the same revision. URL userinfo is always stripped and query values
redacted; malformed request URL values are wholly redacted, not retained on parse
failure. Compile regex at config mutation, max32KiB total rule bytes. Sentinel
tests cover header pairs/typed attrs/body/URL userinfo and all derived outputs.

## Route inventory and exact payloads

`{id}` below is a validated resource ID, not an arbitrary name. All authenticated
routes carry tenant_id in query for GET/DELETE or body for POST/PATCH except
session endpoints. Authorization derives permitted tenant from session, not
that field. Creation returns201 unless noted. Mutation response is full resource.

| Method/path | Request | Response / authorization |
|---|---|---|
| GET /livez | none | 200 `{status:"alive"}` even on dependency failure |
| GET /readyz | none | 200 `{status:"ready"}` or503 sanitized code; no secrets |
| GET /v1/setup | none | 200 `{state:"required"\|"in_progress"\|"complete"}`; no tenant/user details |
| POST /v1/setup | setup fields above | Session; bootstrap only |
| POST /v1/sessions | email,password | 200 Session+cookie |
| GET /v1/session | none | Session+stable CSRF/current grants;401 absent/expired |
| DELETE /v1/session | CSRF header | 204 revoke current session |
| POST /v1/session/password | current_password,new_password | 204 revoke all sessions |
| GET /v1/projects | tenant_id,limit,cursor | Project list, only authorized projects |
| POST /v1/projects | tenant_id,name,default_service,allowed_origins | Project; admin; creates project policy rev1 |
| PATCH /v1/projects/{id} | tenant_id,revision,name?,state?,default_service?,allowed_origins?,scrub_rules? | Project; admin; no physical delete |
| GET /v1/projects/{id}/keys | tenant_id | Key metadata list; admin |
| POST /v1/projects/{id}/keys | tenant_id,label | Key+public_key+dsn once; admin |
| DELETE /v1/projects/{id}/keys/{key_id} | tenant_id,revision | 204 revoked permanently; admin |
| GET /v1/users | tenant_id,limit,cursor | User membership/grants list; admin |
| POST /v1/users | tenant_id,email,initial_password,role,project_grants | User; admin; existing cross-tenant email409, no account linking |
| PATCH /v1/users/{id} | tenant_id,revision,state?,role?,project_grants?,new_password? | User; admin; applies complete grants replacement atomically |
| POST /v1/search | SearchRequest below | 200 SearchResult or202 QueryJob |
| POST /v1/aggregate | Dataset + AggregateSpec,read_token?,mode? | 200 AggregateResult or202 QueryJob |
| GET /v1/records/{id} | tenant_id,project_id,read_token? | RecordDetail; viewer; no token creates authorized current snapshot |
| POST /v1/records/{id}/related | Dataset,read_token?,limit?,allow_time_service_fallback? | SearchResult+correlation; viewer |
| GET /v1/live | tenant_id,project_ids,kinds,expression?,catchup_start_us?,resume_token? | SSE; viewer; no JSON filter in query URL |
| GET /v1/issues | tenant_id,project_ids,status?,limit?,cursor? | Issue list; viewer; current PG state |
| GET /v1/issues/{id} | tenant_id,project_id | Issue+current occurrence page; viewer |
| GET /v1/issues/{id}/occurrences | tenant_id,project_id,limit?,cursor? | Occurrence list; viewer; current PG state |
| PATCH /v1/issues/{id} | tenant_id,project_id,revision,status,operation_id? | Issue; operator; status unresolved/resolved/ignored |
| GET /v1/destinations | tenant_id | Admin: configured metadata; operator: enabled IDs/names only |
| POST /v1/destinations | tenant_id,name,url,secret? | Destination; admin |
| PATCH /v1/destinations/{id} | tenant_id,revision,name?,url?,secret?,enabled? | Destination; admin; secret null clears |
| GET /v1/alerts | tenant_id,project_id,limit?,cursor? | Rule list; viewer |
| POST /v1/alerts | tenant_id,project_id,name,kind,destination_id,cooldown_seconds,rule | Rule; operator |
| PATCH /v1/alerts/{id} | tenant_id,project_id,revision,name?,enabled?,destination_id?,cooldown_seconds?,rule? | Rule; operator |
| GET /v1/deliveries | tenant_id,project_id,alert_id?,state?,limit?,cursor? | Delivery metadata list; viewer; no signing secret/response body |
| POST /v1/deliveries/{id}/retry | tenant_id,project_id,revision | Delivery; operator; failed only, same delivery ID |
| GET /v1/query-jobs/{id} | tenant_id | QueryJob or final result; same owner/current scope |
| DELETE /v1/query-jobs/{id} | tenant_id | 204 cancel idempotently; same owner/current scope |
| POST /v1/snapshots/{id}/heartbeat | tenant_id,read_token | renewed read_token; same owner |
| DELETE /v1/snapshots/{id} | tenant_id | 204 release; same owner |
| GET /v1/system | tenant_id | System DTO below; admin |
| PATCH /v1/system/retention | tenant_id,revision,retention_days | Policy; installation admin only |
| GET /v1/audit | tenant_id,limit?,cursor? | AuditEvent list; admin |

First bootstrap user is the installation admin, recorded separately from tenant
role on users.is_installation_admin; only offline CLI can transfer that role.
Tenant admins do not gain another tenant's management or retention authority.
Installation retention applies to all tenants in v1; UI labels that distinction.
No tenant-hard-delete/privacy-purge API. Disable is reversible access policy.

Resource DTOs (all fields required unless `?`; secret fields only at creation):

- Session: user_id,email,tenants[{tenant_id,name,role,project_grants}],expires_at
  (RFC3339),csrf_token,is_installation_admin. No session token in JSON.
- Project: tenant_id,project_id,name,state,default_service,allowed_origins,
  revision,auth_revision,scrub_revision. scrub_rules admin-only; viewers receive
  neither rules nor key data. revision maps config_revision, every mutation bumps
  it plus appropriate auth/scrub revisions.
- Key: key_id,label,key_prefix,state,revision,created_at; public_key/dsn once.
  Random32-byte hex key, hash SHA256 of exact normalized key; DSN generated using
  configured public URL and project_id, never a user-provided destination.
- User: user_id,email,state,revision,role,project_grants[{project_id,role}].
- Issue: issue_id,project_id,status,revision,title,grouping_version,
  lifetime_occurrence_count,first{event_us,ns,record_id,release},last{same},
  last_received_us,detail_retention_floor_us. No mutable status in Parquet.
- Occurrence: record_id,project_id,event_us,ns,received_us,release,detail_available.
- Destination: destination_id,name,revision,enabled,url(admin only),has_secret.
- Rule: alert_id,project_id,name,revision,enabled,kind,rule,destination_id,
  cooldown_seconds,last_completed_end_us?,last_fired_end_us?.
- Delivery: delivery_id,alert_id,state,revision,attempt,next_retry_at?,
  last_http_status?,error_code?,created_at. Revision is CAS counter on delivery.
- System: generation,recovery_state,lanes[{lane_id,accepted_seq,published_seq,
  oldest_pending_received_us?,error_code?}],ingest/SDK counters, resource used/max,
  dependencies[{name,status}],backup{state,last_success_at?,wal_age_seconds?,
  last_restore_at?},retention{days,revision,floor_us},alerts_paused. Missing metric
  is null/unavailable, not invented zero or healthy.

System counter objects are exact: ingest `{accepted_requests,accepted_records,
duplicate_records,conflict_records,published_records,rejected_requests}`; sdk
`{reported_drops,reported_drops_approximate:true,unsupported_items,by_reason:[
{sdk_name,category,reason,count,approximate}]}`. Counters expose their `since`
RFC3339 and `scope` tenant/process so callers cannot mistake a process reset for
lifetime totals. by_reason is paged separately via GET /v1/system/sdk-outcomes
with tenant_id,start_us,end_us,limit,cursor (admin only); system response gives
latest100 and next_cursor. Never infer generated total by adding these counters.
Resources are arrays `{name,unit,used,max}`, unit bytes/count, values decimal
strings; names ingress,working,spool,cache,spill,pg_connections,native_tasks.
v1 reads accepted/published/SDK counters from indexed durable rows within the
selected time interval; rejected_requests is a process counter because rejected
requests have no durable receipt. Mark scopes explicitly. A future diagnostic
rollup needs benchmark evidence and an atomic watermark; it is not part of v1.

List cursor shapes are signed purpose-specific last tuples: projects/users by
numeric ID ascending; Issues by last_received_us DESC,issue_id DESC; occurrences
by event tuple DESC; rules by alert_id ASC; deliveries/audit by created_at/id DESC.
Bound filter/scope and principal. These are current-state PG lists, **not** query
snapshots: Issue counts/status can change during paging. UI labels live state;
do not assert frozen Issue history. key/destination lists cap1,000/tenant, return
explicit limit error for creation beyond policy rather than silently truncate.

## Search/aggregate wire values

Dataset: tenant_id,project_ids[],start_us,end_us,time_basis,kinds[],expression?
xor filter? (neither means true). Filter schema is [query.md](query.md). A request
with both fields400 even if one empty. No relative "now" goes to backend.
SearchRequest adds read_token?,cursor?,limit(default100),sort(event_desc default
or received_desc),mode(auto default/sync/async),projection(list only in v1).

ListRow: record_id,project_id,kind,event_time_us,event_time_ns_remainder,
received_time_us,level,severity_number(nullable),message,service(nullable),
environment(nullable),release(nullable),trace_id(nullable),issue_id(nullable),
message_truncated. List message max2,048 UTF-8 bytes cut on rune boundary;
this **display** truncation never changes stored searchable values. Detail keeps
full allowed canonical content. No raw/attrs in list rows; limits100/1,000 remain
bounded by8MiB serialized result cap. Exceed result cap422, no silently short page.
SearchResult: rows[],read_token,next_cursor(nullable),complete(true),stats,warnings[].
Stats: scanned_bytes,objects,cache_bytes,elapsed_ms,cut[{lane_id,seq}],
visibility_lag_ms nullable. All counter/time quantities here decimal strings.

AggregateSpec: metrics[{name,op,field?}] with op count/sum/min/max/avg, group_by[] of
Field/Attribute/group_attr nodes, histogram?{interval,empty_buckets},top(default100),
order?{metric,direction}. count forbids field; other ops require numeric Field/Attribute.
Histogram time uses dataset.time_basis. metric name regex `[a-z][a-z0-9_]{0,31}`,
unique and not reserved. Group type/null/missing tags are explicit.
AggregateResult: groups[{keys:[{type,value?}],bucket_start_us?,metrics:{name:
{type,value,valid_count,excluded_count}}}],read_token,complete,stats,warnings.
Ungrouped empty input has one metrics group; grouped empty has none except
requested empty histogram buckets. value null for no valid numeric values.

RecordDetail: record(ListRow plus canonical typed attrs/full message/template/
SDK/time originals/versions/warnings),raw(scrubbed JSON),envelope_sdk(nullable),
read_token. Missing within current retention404; known Issue occurrence expired410.
With token, detail honors dataset filter/time as well as scope/cut. Current
detail without token builds an ID-targeted scope with retained received-time
range, not an arbitrary all-history scan. Record ID is not authorization.

QueryJob: query_id,state,expires_at,poll_after_ms(default500),result? (SearchResult
or AggregateResult),error? (Error).202 includes Location. GET returns200 status
while pending and200 wrapper with result when succeeded; failed wrapper carries
error, not successful empty result. Unknown/other-owner404, expired410. Persist
operation kind so client discriminates exact result type. Cancellation never
sets complete=true.

## Alert evaluation and delivery

Issue rule `{events:["created","regressed"]}` with nonempty subset. It consumes
issue_transitions committed by Publish, no retrospective sends at rule creation.
On create/enable/revision change capture published cut under all16 lane locks as
enabled_from_public_cut; only a source occurrence beyond that cut can trigger.
Use unique delivery key `issue:<alert>:<revision>:<issue>:<transition_revision>`.
User resolve/ignore transitions are not notification triggers. For each unseen
eligible transition, transactionally insert issue_alert_evaluations even when
suppressed by cooldown; this avoids committed-order gaps from an auto-increment
cursor or wall-clock ordering. Scan transitions by lane/seq with that ledger;
received_time is for cooldown, not for deciding whether a transition existed
at rule creation. Concurrent publication/rule edit serializes through lane locks.
Scheduler enqueues; outgoing worker sends. Disable/config change cancels
unstarted work, resets revision window progress and captures a new cut on enable;
already transmitted bytes cannot be recalled. Issue cooldown uses max(previous
evaluation time,DBnow), distinct from threshold cooldown's window E.

Threshold rule `{expression|filter,kinds,window_seconds,metric,operator,threshold}`.
window_seconds one of60,300,900,3600; metric=count only in v1; operator gt/ge/lt/le;
threshold nonnegative signed64 decimal string; no group dimensions. This explicit
subset suffices for threshold alerts without promising arbitrary rule languages.
Every60s reserve next E in order, at most10 pending windows/rule. On creation
first E is next aligned minute, require full lookback >=created time before first
evaluation; never retroactively alert on historical data. Lock project+all lanes,
capture accepted cut and raise last_received_us>=E as DESIGN. Later wait for
published>=cut. Query received [E-window,E), *exact reserved cut*, permissions and
complete result. Snapshot must select current generation including that cut;
if compaction changed generation, row-level cut still preserves exact semantics.
If retention already erased part of window, mark evaluation failed/expired and
surface gap; don't count missing history as zero. Daily backlog policy never
advances last_completed_end_us for failed/partial windows. Admin may disable and
recreate a rule to intentionally restart its window history.

After query, lock alert/evaluation; revision/enable recheck, compare count;
cooldown(E-last_fired_E>=cooldown_seconds) where cooldown0..86400. Atomically mark
succeeded, move last_completed_E and insert unique delivery
`threshold:<alert>:<revision>:<E>` if true+cooldown. First fired uses no previous
cooldown. Do not let out-of-order evaluation complete across a missing E.

Webhook body version1: delivery_id,alert_id,alert_revision,project_id,type,
occurred_at,issue?{issue_id,transition,revision,title},threshold?{window_start_us,
window_end_us,observed,operator,threshold},link(public UI URL only). Exclude raw
records and user-supplied links. Header X-Eventglass-Delivery; optional signature
`v1=<hex HMAC-SHA256(secret,timestamp+"."+exact body)>`, timestamp header Unix
seconds. Retries keep delivery ID/body, refresh timestamp/signature. Encrypt
secret with AES-256-GCM using random nonce+mounted key ID; decryption unavailable
fails delivery readiness, never sends unsigned as fallback.

Only admin-configured HTTPS destinations, max URL2KiB, no credentials/fragment,
port443 by default; deny loopback/private/link-local/multicast/reserved IPv4/IPv6
and metadata endpoints after DNS resolution. Validate every retry; pin resolved
IP for dialing while retaining TLS hostname/SNI, use no environment proxy or
redirects. Local tests use an explicit test-only loopback transport unavailable
in production config. If DNS answers mix allowed/denied, reject all.2xx success;
408/429/5xx/network retry, other4xx permanent; capped Retry-After<=1h; max12 attempts
full jitter base5s exponential max1h. Timeout10s incl DNS/TLS/read, read<=64KiB
then close; never store/log response body. Lease60s heartbeat15s; crash after
successful remote receive but before SQL commit may send twice (at-least-once).
Manual retry resets bounded attempt counter and keeps same delivery ID. Disabling
destination stops queued retries. Query/alerts auth revalidation shares planner;
alerts principal is server rule scoped solely to its enabled project.

## Frontend structure and behavior

Planned layout:

```text
web/src/app/                 router, providers, auth bootstrap, error boundaries
web/src/api/generated/       generated DTOs/client; never hand edit
web/src/features/{auth,projects,issues,logs,explore,alerts,system}/
                            routes, queries, components, local schemas/tests
web/src/shared/ui/           accessible presentation components
web/src/shared/search/       URL codec, absolute time conversion, dataset builder
web/src/shared/format/       dates, BigInt/decimal display, severity badges
```

TanStack Query keys include user,tenant,sorted projects,dataset hash,read_token
and operation. Never mirror server records into a global mutable store. Logout
or tenant switch cancels requests/Live, clears cache and all tokens. URL owns
project/kind/filter/time-basis/absolute range/sort; transient cursor/token and
CSRF remain memory, not browser URL/localStorage. Filter draft is component state
until submit. Changing dataset clears cursor/token and launches new snapshot.
First rows request acquires token; histogram uses it; subsequent panels use same
token.410 prompts refresh of both, not only one stale panel. Auto202 polls500ms
then backoff max2s; navigation cancels owned pending query jobs best-effort.

Required route flows:

- `/setup`, `/login`: generic errors, password no logging, expired setup handled.
- `/projects`: scoped list; admin editor; show DSN once, copy warning, revoke
  confirmation; exact fixture-tested SDK integration snippets.
- `/issues`, `/issues/:id`: lifetime count/status, ordered exceptions/SDK frames,
  breadcrumbs as parent detail only, revision conflict refresh (no blind retry),
  retained occurrences with expired detail states.
- `/logs`: default kind log (including error-level logs), typed detail, related
  trace action, Live with bounded latest1,000 IDs, paused/resync indicator.
- `/explore`: chosen kinds, expression input, rows+histogram, groups/counts and
  visible complete/failed state; transactions keep spans in detail only.
- `/alerts`: rules and delivery attempts; destination secrets admin-only, no
  "test external webhook" action. Disabled/lagging/failed-window shown distinctly.
- `/system`: accepted versus published versus approximate SDK drops, blocked
  lanes, resource admission, backup age/recovery health. No fabricated totals.

Render messages/raw/frames/URLs as text, not HTML. Do not fetch request URLs,
debug_meta URLs or source paths. External links only to configured app/docs
origins and use noopener. Provide labels/keyboard focus, live announcements only
for state changes, reduce-motion preference, loading/empty/forbidden/retry states
for each view. UI hiding is not authorization; API tests prove all roles. Vitest
tests URL/cache/BigInt/null logic; Playwright uses real localhost backend, PG/S3,
SDK fixture ingestion, visible publication, forbidden scope and HTML injection.
