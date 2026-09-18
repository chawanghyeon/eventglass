# Eventglass Go implementation instructions

This is an explicitly authorized independent Go product. The Rust choices of one SQLite database, one sequential Indexer, and Tantivy QueryParser do not apply here. Follow [DESIGN.md](DESIGN.md)'s PostgreSQL/S3/DuckDB contracts while preserving durable ACK, authorization, correctness, recovery, and scrubbing.

## Scope

- Read the entire DESIGN.md before implementation; consult SDK-SOURCES.md for evidence.
- Keep new code, migrations, UI, tests, deployment, and dependencies here. Treat `../../rust/`, `../benchmark/`, and existing shared SDK fixtures as read-only baselines.
- Do not edit `../../docs/observe/source-design.md` or commit unrelated user changes to root AGENTS.md.
- Record departures from the original architecture in DESIGN.md, not by changing Rust.
- Start with G00; do not advertise features before their gates pass.
- The primary agent works directly; do not use subagents.
- Keep all Go design, handoff, and implementation documentation in English.

## Implementation and verification

- Reuse SQL/Parquet/regex/aggregation engines. Do not implement a SQL parser, Parquet codec, or general distributed SQL executor.
- Parse the restricted CEL language with an existing parser, lowering only allowlisted AST nodes. Never expose arbitrary SQL.
- Execute native DuckDB in an isolated child. A shared DuckDB file is not the operational DB or durable source of truth.
- Tests use temporary PostgreSQL databases, owned S3 buckets/prefixes, directories, and localhost targets. Never use production DSNs/buckets or external alert receivers.
- Section22 commands are planned until implemented. Missing required checks must fail rather than silently skip/succeed.
- Distinguish source inspection, executed fixtures, resource measurements, and actual AWS checks. Do not claim universal SDK compatibility.
- Compare equivalent requests/data/results and CPU/RAM/disk/network budgets. Include shared database costs and honest Rust conditions.

## Commits and deployment

Follow root CONTRIBUTING.md without bypassing hooks. Pre-push stages Rust artifacts using `rust/Dockerfile` and the repository-root context. Keep Go deployment definitions here; production replacement/data migration requires a separate user request. Report committed work and environmental push failures accurately.
