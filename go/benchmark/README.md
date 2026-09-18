# Go core-path benchmark

This directory is an experimental comparison target for Eventglass. It mirrors the Rust capacity benchmark's generated records and durable boundaries:

`acceptance → SQLite WAL Inbox → sequential indexer → Issue occurrence update → structured search/text search/histogram`.

The Go target uses the same SQLite engine through `github.com/mattn/go-sqlite3` and SQLite FTS5 for text search. Rust uses Tantivy 0.26.1 for its native index, so the result is a practical backend comparison, not a language-only comparison. The report records this limitation.

Run the local smoke profile:

```sh
./scripts/check-benchmark-go smoke
./scripts/compare-benchmark /path/to/rust-100k.json .tools/benchmark/go-100k.json
```

The supported profiles are `smoke`, `100k`, `1m`, and `10m`. The script uses the same generated workload and release-oriented Go build flags, but it does not create Linux cgroup limits. For an apples-to-apples resource result, run both binaries in the same pinned Linux image with the same CPU, memory, swap, filesystem, and page-cache conditions.

The Go benchmark intentionally does not claim coverage for HTTP wire ACK, Sentry normalization, S3 checkpoint/cold restore, replay, alerts, or the web UI. Those are part of the Rust product and remain outside this first comparison target.

The JSON report's `total_elapsed_ms` includes database setup, seed, visibility drain, and final query measurements. It excludes the Go compiler invocation performed by the helper script, matching the Rust comparison where the benchmark test executable is built before the measured run.
