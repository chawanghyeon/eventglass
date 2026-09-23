# S3 search layout experiment

This is an isolated Go experiment, not a production Eventglass query path. It tests one physical idea: each immutable object holds sorted term postings, compressed typed column pages, and compressed source pages. All three use the same local document ID. A small binary catalog stores the vocabulary once, document frequencies, and byte ranges; it is written after the objects. The query walks matching IDs once to update exact `COUNT`/`SUM` by group and BM25 top 10, then reads source pages only for the winning hits. It never treats the top 10 as the aggregation input.

The recommended **fixed read policy** for this experiment is one column span per matching segment, plus postings and winning source pages. This uses no per-query optimizer. Sparse pages, a column-and-source span, and two coalescing budgets remain here only as controls. A production implementation would need its own measured segment size and page size; the values below are experiment settings, not universal constants.

Run on the repository's Go 1.27.1 ARM64 host:

```sh
go test ./experiments/searchlayout -count=1
go vet ./experiments/searchlayout
go run ./experiments/searchlayout -n 1000000 -vocabulary 4093 -segment 100000 -page 256 -reps 5 > /tmp/searchlayout.json
```

The checked-in [one-million-document result](results-arm64-1m.json) was generated on `darwin/arm64` with warm local files. Every indexed result, including all groups, sums, hit IDs, scores, and source text, was compared with an independent zlib JSONL full scan. The 10 segments plus catalog occupied **13.39 MB**, versus **10.83 MB** for the compressed JSONL without an index (1.24×). The indexed size comprised 3.56 MB postings, 6.02 MB columns, 3.37 MB source, and 0.44 MB catalog. These numbers reflect a narrow, repetitive synthetic corpus, not general log compression.

| Query | Matching documents | Exact groups | Full scan median | Fixed column-span median | Range GETs / bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fatal` | 100 | 100 | 857 ms | 1.46 ms | 30 / 6.03 MB |
| `timeout` | 9,900 | 2,048 | 861 ms | 14.33 ms | 30 / 6.03 MB |
| `request` | 990,000 | 2,048 | 899 ms | 56.52 ms | 21 / 6.03 MB |
| `fatal OR timeout` | 10,000 | 2,048 | 857 ms | 14.18 ms | 40 / 6.03 MB |
| `service AND request`, tenant 3 | 247,500 | 512 | 717 ms | 42.83 ms | 31 / 6.03 MB |

The sparse policy reduced `fatal` to 0.16 MB but needed 120 GETs; for `timeout`, it needed 3,930 GETs and still read 6.03 MB. The column-and-source span used 20–30 GETs but about 9.4 MB. A query whose matches scatter across every page cannot retain both sparse transfer and low request count with this one-copy column layout. The [long-tail vocabulary result](results-arm64-longtail.json), with 100,003 distinct terms over 100,000 documents, occupied 2.44 MB versus 1.09 MB compressed JSONL (2.25×); the catalog alone was 1.16 MB. The storage advantage is therefore workload dependent.

The optional `-minio-endpoint` path uploaded the segment objects to disposable local MinIO and used AWS SDK signed S3 Range GET. In the [MinIO result](results-arm64-minio.json), the 100,000-document `timeout` query matched the local answer; three runs had a 23.28 ms median, 30 GETs, and 612,154 transferred bytes. MinIO checks the wire path, **not** AWS latency or price. Local filesystem medians are also not a Lucene or DuckDB comparison. The full scan is a simple Go control, not DuckDB.

**Conclusion:** this layout demonstrates exact aggregation and ranked search over a shared, single-copy source/column/posting segment, with moderate storage overhead on the main fixture and bounded Range GETs under the fixed policy. It does **not** prove superiority or a universally optimal layout. Production acceptance still requires representative real logs, Korean tokenization and phrase/position search, updates/deletes/compaction, concurrent S3 measurements, and integration with Eventglass's PostgreSQL authorization, durable receipts, fenced publication, and PG/S3 recovery. No production data format or transaction semantics were changed here.
