# Worked contract examples and independent test vectors

These are normative expectations, not executed test results. Turn them into
table-driven tests before implementing the corresponding operation. SQL/native
results must agree with hand-calculated values, not an oracle calling the same
production compiler/reducer/grouping implementation.

## E1: acceptance selection and rebatching

One tenant/lane journal has requests A with4 records, B with0, C with1. Global
ranges: A=[0,3], B=[4,3], C=[4,4]. A positions0 and3 are accepted; position1 is an
identical existing source event; position2 conflicts with an existing source.
Its selection bytes are exactly (no final LF):

```json
{"version":1,"accepted":[[0,0],[3,3]],"duplicate":[[1,1]],"conflict":[[2,2]]}
```

B has all3 arrays empty and an empty publication contribution. C's request-local
accepted range is[[0,0]], not[[4,4]]. Batch counts total5, accepted3, duplicate1,
conflict1. Empty B still gets a committed receipt and diagnostics.

If C becomes unauthorized after upload, the whole attempted Accept rolls back:
no A/B receipts, no seq advance, no source claims. Rebuild A+B under a new batch
UUID/intent retaining acceptance IDs and request hashes. C gets auth failure.
If A already committed in an earlier unknown-reply attempt, return its original
receipt and build a fresh journal for B only. Never charge A twice or claim a
new batch's physical range is its original committed range.

## E2: missing/null/typed filters

Five rows with attributes /v:

| Row | Value |
|---|---|
| a | missing |
| b | explicit null |
| c | integer1 |
| d | string "1" |
| e | integer2 |

- exists(attributes,/v) -> b,c,d,e.
- is_null(attributes,/v) -> b only.
- iattr(attributes,/v)==1 -> c only.
- iattr(attributes,/v)!=1 -> e only (not a,b,d).
- !(iattr(attributes,/v)==1) -> a,b,d,e.
- sattr(attributes,/v)=="1" -> d only.
- Group-by /v emits5 type-tagged groups; type-preserving generic grouping reads
  the attribute's stored type, while numeric/string filters use a typed accessor.

For grouping DTO use `{op:"group_attr",namespace:"attributes",path:"/v"}`
(separate from predicate attr nodes) to preserve all scalar types. A typed attr
group is also allowed and treats mismatched types as missing, but it must not be
silently substituted for the generic group. Array/object ->422 as documented.
Message `a_%b` contains literal `_%` and does not contain literal `aXXb`.
Paths `/a.b` and `/a/b` never match the same key unless both explicitly exist.

## E3: global aggregation and paging

Partition1 groups A=11,X=10; partition2 groups B=11,X=10. Each localTop1 omits X,
yet globalTop1 is X=20, not A/B=11. Therefore emit all local groups. Average
partition1 values[2,4], partition2[100] is106/3=`35.333333333`, not average(3,100).
Integer avg uses global half-even scale9; sum106 and valid_count3 are exact.
Two20,000-group partitions containing identical keys have20,000 final groups
and can pass cardinality cap; disjoint sets totaling20,001 must422. Both still
obey shared64MiB intermediate cap.

Event sort tuples (time,ns,id): (100,1,c),(100,1,b),(100,0,z),(99,999,z).
With limit2 page1 contains first2; cursor=(100,1,b); page2 contains remaining2.
Compaction between requests does not alter snapshot generation/cut or order.
Histogram interval1,000,000us and window[-1,1,000,001) has intersecting bucket
starts[-1,000,000,0,1,000,000]; event exactly end is excluded. A received-time
retention floor can hide a record regardless of its future event timestamp.

## E4: Issue group bytes and lifecycle

Project42, no exception, message_template="failed {id}", message="failed 19",
no explicit fingerprint has group bytes exactly (no final LF):

```json
["eventglass-grouping-v1","42",["message","failed {id}"]]
```

Explicit fingerprint["tenant-rule","{{default}}"] has bytes:

```json
["eventglass-grouping-v1","42",["custom",[["literal","tenant-rule"],["default",["message","failed {id}"]]]]]
```

SHA-256 of exact bytes becomes issue_id. Changing only stack line number does
not change default group; changing preserved module/function/file does. Custom
literal marker with extra spaces other than the two exact accepted marker
spellings remains literal. Error-level log never calls grouping.

Resolve captures lane0=10,lane1=4 (other lanes omitted here only for brevity).
Publish lane0seq9 after resolve: remains resolved. Publish lane1seq5: regresses,
revision increments once. Concurrent lane0seq11 increases count but doesn't
produce a second regression once unresolved. A retry publishing seq5 adds no
occurrence/count/transition. Ignored never regresses. Event time older than first
still updates first-event tuple/release but is new received activity.

## E5: threshold fence and recovery

At E=120,000,000us accepted seq7, published seq6. Reserve window
[60,000,000,120,000,000), capture cut7, raise last_received_us>=E. New Accept
seq8 receives time>=E even if SDK event time is10,000,000. Evaluation waits for
publication7; query cut7 excludes seq8. Missing predecessor is not a zero result.
An alert rule created at published cut6 can receive a subsequent created-Issue
transition from seq7 even if that batch was ACKed before rule creation.

PITR restores PG at seq7 while S3 has journals for seq8/9. Read/replay only PG
authorized refs; do not adopt8/9. Increment storage generation but allow verified
old-generation files for seq<=7. Old worker can't publish using its old generation
even if its fence happens to exceed the restored fence. Isolate old writer
credentials before activation, since SQL fencing alone cannot prevent late PUT.

## E6: operation idempotency keys

| Operation | Retry key / matching condition |
|---|---|
| Accept | acceptance_id + tenant/project + request content SHA |
| Prepare | job_id + prepare_fence + manifest root SHA |
| Publish | job_id + prepared_output_id + manifest root SHA, current claimed authority |
| Query task complete | query_id/stage/partition + live fence + result SHA |
| Issue mutation | optional scoped operation_id + actor + sanitized action hash; otherwise revision CAS |
| Threshold evaluation | alert_id/revision/window_end_us |
| Issue alert decision | alert_id/revision/transition_id |
| Delivery | stable delivery_id and exact body; remote exactly-once is not guaranteed |
| Compaction/retention swap | maintenance task + input IDs/versions + output SHA + live fence |
| GC delete | intent_id/key + deleting state/fence; repeat DELETE is safe |

Same key/different immutable content is an error, not a successful retry. A
fence-only check without generation, owner, state and live lease is insufficient.
