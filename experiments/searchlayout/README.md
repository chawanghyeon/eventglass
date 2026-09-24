# S3 search layout experiment

This isolated Go program compares physical formats for **BM25 top K plus exact `COUNT`/`SUM` by group over every matching document**. It does not change Eventglass's production DuckDB/Parquet path or its PostgreSQL/S3 authority rules. All results are checked against an independent compressed-JSONL full scan, including group counts and sums, hit IDs, scores, and source text.

Every indexed format has immutable S3-range-addressable segments, a compact vocabulary/catalog, and a shared local document ID. Five complete persisted formats were built and reloaded:

| Format | Per-term postings | Per-document aggregate/scoring fields | Source |
| --- | --- | --- | --- |
| `shared` | Delta doc ID + term frequency | Zlib-compressed six-byte rows | Compressed pages |
| `packed_shared` | Same | Per-page minimum and bit-packed deltas | Same pages |
| `hybrid` | Smaller zlib-compressed choice of delta postings or dense bitmap plus TF | Same as `packed_shared` | Same pages |
| `covering` | Doc ID, frequency, and **repeated** fields | Inside every term posting | Same pages |
| `row_only` | None | Same columns as `shared` | Same pages; scans all rows |

The indexed query walks all matching IDs once, updates exact aggregates and a BM25 top-K heap, then fetches source pages for winners. It never aggregates only the top K. `shared` and `packed_shared` use the same **fixed** read policy: one column span per matching segment. The earlier sparse/coalesced controls remain in the code; they do not select a production query strategy. This separation of postings, fast fields, and result source is also a [documented search-engine architecture](https://github.com/quickwit-oss/tantivy/blob/main/ARCHITECTURE.md); this experiment tests its S3-oriented byte layout and exact aggregation tradeoff.

Run on the root Go 1.27.1 ARM64 module:

```sh
go test ./experiments/searchlayout -count=1
go vet ./experiments/searchlayout
go run ./experiments/searchlayout -matrix -n 1000000 -vocabulary 4093 -segment 100000 -page 256 -reps 3 -profile baseline > /tmp/searchlayout.json
```

`-profile` also accepts `noisy` (two unique tokens per row), `clustered` (term bursts), and `wide_fields` (full-width tenant/group/duration values). Each format is built in a temporary directory, loaded from its manifest, and compared to the same oracle. `-minio-endpoint` plus temporary credentials uploads all four formats and checks signed S3 Range GETs. Query medians below use warm local files on `darwin/arm64`; they are **not** DuckDB, Lucene, AWS S3, or end-to-end service comparisons.

Additional correctness checks use 20 fixed random seeds: 19 queries per seed against all five reloaded layouts, or **1,900 layout/query comparisons** with exact full-scan counts, sums, groups, ranked IDs, scores, and source text. They vary segment/page boundaries, full-width group and duration fields, Korean and accented text, AND/OR, tenant filters, missing terms, top-K including zero, and case/duplicate query terms. The randomized check exposed inconsistent query-term normalization in the full-scan control; all query paths now share one normalization function. Eight concurrent packed queries also passed Go's race detector.

A separate Linux ARM64 run built one million documents and checked a broad packed query plus four concurrent identical queries against the full scan under **1 CPU, 512 MiB memory, no swap**, a read-only non-root container, and 128 MiB temporary storage. It passed with 247,500 matching documents, 512 groups, 10,202,556 indexed bytes, and cgroup peak memory of **331,436,032 bytes (316.1 MiB)**; cgroup `max`, `oom`, and `oom_kill` events were all zero. This peak covers fixture creation and verification in that container. It is one synthetic run, not an S3/PG service memory or throughput guarantee. Reproduce with `go test -c` for `linux/arm64`, then run `TestPackedResource` with `EVENTGLASS_LAYOUT_RESOURCE=1` inside the bounded container; the ordinary test suite skips this large case.

The [one-million-document result](matrix-arm64-million.json) used 10 segments of 100,000 documents and 256-document pages. It had 4,100 indexed terms. Storage includes every segment and its catalog; the control compressed JSONL was 10.83 MB.

| Format | Stored | `fatal`, 100 matches | `timeout`, 9,900 matches | `request`, 990,000 matches |
| --- | ---: | ---: | ---: | ---: |
| `shared` | 13.39 MB | 1.80 ms; 30 GET / 6.03 MB | 13.45 ms; 30 GET / 6.03 MB | 55.70 ms; 21 GET / 6.03 MB |
| `packed_shared` | **10.20 MB** | 1.51 ms; 30 GET / 2.84 MB | 23.78 ms; 30 GET / 2.84 MB | 64.08 ms; 21 GET / 2.85 MB |
| `covering` | 30.38 MB | 0.51 ms; 20 GET / 9.8 KB | 1.57 ms; 20 GET / 53 KB | 76.51 ms; 11 GET / 4.02 MB |
| `row_only` | 9.47 MB | 297.79 ms; 10 GET / 9.39 MB | 303.46 ms; 10 GET / 9.39 MB | 350.90 ms; 10 GET / 9.39 MB |

The [noisy result](matrix-arm64-noisy.json) used 50,000 rows and 104,100 actual terms. Compressed JSONL was 1.64 MB; `packed_shared` was 4.12 MB, with **2.19 MB in its catalog alone**. [Wide-field data](matrix-arm64-wide-fields.json) reduced packed-versus-plain storage by only 4.8%, while `timeout` took 4.37 ms packed versus 1.84 ms plain locally. The [clustered result](matrix-arm64-clustered.json) reduced `timeout` to one matching segment: packed read 3 ranges / 31 KB instead of 30 ranges / 293 KB on the same-size scattered baseline. These fixtures deliberately expose limits, not a representative production mix.

With one 100,000-document segment and 1,024-document pages, the [local MinIO result](matrix-arm64-minio.json) persisted `packed_shared` at 1.00 MB, `shared` at 1.12 MB, `covering` at 3.00 MB, and `row_only` at 0.72 MB; compressed JSONL was 1.08 MB. The signed S3 Range `timeout` query returned the exact oracle result at 5 GET / 310 KB packed, 5 GET / 427 KB plain, 4 GET / 14 KB covering, and 1 GET / 713 KB row-only. MinIO localhost timings varied across runs and cannot predict AWS latency. The original [read-policy result](results-arm64-1m.json) and [long-tail vocabulary result](results-arm64-longtail.json) are retained as controls.

**Decision from these tests:** `packed_shared` is the best storage/I/O compromise for the narrow-field fixture and gives exact search plus aggregation without duplicating values in every term. It is a candidate, not a universal winner: covering postings buy much faster sparse queries with roughly 3× storage here; full-width fields make bit packing less useful; unique tokens make catalog cost dominant. For a single-copy column layout, scattered matches require either many small Range GETs or a larger contiguous column read. Avoiding both requires replicated covering data or a warm cache. This is a physical tradeoff, not a missing query optimizer.

The [follow-up design and evidence](../../docs/observe/search-engine-design.md) rejects `hybrid` as a default: it saved only 0.12–1.85% across four profiles without a reliable read/latency benefit. Word-at-a-time packed-column decoding keeps the same stored bytes and measured 2.69×/5.74× faster page decode on bounded Linux ARM64 narrow/wide pages; full-query improvement on the 100,000-document fixture was 1.65× for `timeout` and 1.26× for `request`. These are codec/fixture results, not service p99 claims. The new format also passed signed local MinIO Range queries against the oracle.

Same-region AWS S3 transfer to an AWS service is generally uncharged, but [GET requests are billed](https://docs.aws.amazon.com/AmazonS3/latest/userguide/download-objects.html). No AWS price or latency was inferred from MinIO. Production acceptance still requires real sanitized workload distributions, query mix and concurrency, CPU/RSS limits, Korean analyzers and phrase/position search, updates/deletes/compaction, actual AWS measurements, and integration with Eventglass's durable receipts, authorization, fenced publication, and PG/S3 recovery.
