# Eventglass Go implementation entry point

This is the independent **Go + DuckDB + PostgreSQL + S3-compatible storage** product requested on 2026-09-18. Currently it contains an implementation handoff, not an executable Go product.

Read [AGENTS.md](AGENTS.md), then all of [DESIGN.md](DESIGN.md), and implement section22 starting at G00. DESIGN.md is normative and self-contained; [SDK-SOURCES.md](SDK-SOURCES.md) records inspected sources and evidence levels. Keep Go documentation in English.

- Goal: low total operating cost, Sentry SDK errors and structured logs, flexible filtering/analytics, S3-centered durability, and autoscaling replaceable workers.
- Preserve the product in `../../rust/` and Go/SQLite FTS5 experiment in `../benchmark/` as comparison baselines.
- Planned module: `eventglass/go/eventglass`; binary: `eventglass-go`. Add dependencies and code with G00 verification.
- Numbers are initial policies or validation targets, not demonstrated compatibility, 512MiB operation, throughput, or cost advantages.
- Existing main deployment remains Rust-only; do not automatically replace production Rust.

## New-session implementation prompt

> Read all of `go/eventglass/AGENTS.md` and `go/eventglass/DESIGN.md`, then implement G00 onward in order. Preserve `rust/` and the existing benchmark as comparison baselines. Mark only the scope verified by real SDK wire fixtures, independent correctness oracles, failure injection, and Linux resource limits as complete. Do not weaken required search/durability semantics for performance; report failed assumptions with measurements. Use temporary databases, buckets, and localhost receivers. Do not confuse the design handoff with an implemented product. Keep documentation in English.
